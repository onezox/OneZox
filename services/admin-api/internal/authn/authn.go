// Package authn is AUTHENTICATION only — Step F of the Phase-05 plan.
// Its entire job ends at "this request carries a verified admin_user
// identity, with this role, attached to the context." It never decides
// what that role may DO — that is authz's job (Step G), a separate
// package this one has no dependency on and no knowledge of. Keeping the
// layers apart means authz's RBAC table can be tested against a fake
// Identity with no real credential/DB involved at all, and it means
// audit_log always has a reliable authenticated actor to attribute a
// request to regardless of whether it's later authorized or denied.
//
// DISJOINT from tenant api_keys is a security boundary, not a naming
// convenience: this package's Store never queries, joins, or falls back
// to api_keys under any circumstance — Authenticate below has no
// parameter, branch, or code path that could reach that table. A tenant's
// raw API key, hashed the exact same SHA-256 way admin tokens are, simply
// has no row to match here; the two credential spaces occupy the same
// hash algorithm but structurally disjoint data (mirrors the Phase-05
// migration 0015 comment on admin_user.credential_hash's own UNIQUE
// constraint — disjoint by construction, not by a check that could drift).
// TestApiKeyHashNeverAuthenticatesAsAdmin below proves this directly, not
// just by inspection. The live version of this proof (a REAL tenant key
// tried against the real deployed admin-api) is Steps G/J, once authz
// exists to be the second half of that adversarial test.
package authn

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

// AuthError mirrors edge-gateway's own auth::AuthError shape (Phase-01
// Step C) deliberately: a flat variant set with no "which check failed"
// detail exposed past this package — every caller (grpc_interceptor.go,
// http_middleware.go) maps every variant to the same generic
// Unauthenticated response, so a caller probing with different bad tokens
// can't distinguish "unknown token" from "revoked token" (no oracle for
// credential enumeration, same posture Phase-01.txt's own security
// section already established for tenant auth).
var (
	ErrMissingCredential = errors.New("missing admin credential")
	ErrInvalidCredential = errors.New("invalid admin credential")
	ErrRevoked           = errors.New("admin credential revoked")
)

// Identity is deliberately the ONLY thing Authenticate produces — no role-
// permission decision baked in anywhere near it. Role is carried as plain
// data for authz (Step G) to read, not interpreted here.
type Identity struct {
	UserID string
	OrgID  string
	Role   string
}

// Row is what the store's lookup needs to return — no credential_hash
// field, the same reasoning edge-gateway's own ApiKeyRow doc comment
// gives: the store looks up BY hash, it doesn't need to hand it back.
type Row struct {
	UserID    string
	OrgID     string
	Role      string
	RevokedAt *time.Time
}

// Store is implemented by a real CockroachDB-backed lookup
// (cockroach_store.go, SELECT-only against admin_user per migration
// 0018's own grant) in production, and by an in-memory fake in tests.
// Its SQL is hardcoded to admin_user and nothing else — there is no
// generic "credential store" abstraction shared with any tenant-facing
// code, which is what makes the disjointness structural rather than a
// runtime check that could be bypassed or drift.
type Store interface {
	LookupByHash(ctx context.Context, hash string) (*Row, error)
}

// HashToken — SHA-256 hex digest, the exact same convention
// data/seed/seed-test-tenant.sh and edge-gateway's own hash_api_key use
// for api_keys.hash. Using the identical algorithm is deliberate: it is
// what makes TestApiKeyHashNeverAuthenticatesAsAdmin a meaningful proof
// rather than a trivially-true one — the two spaces are disjoint DESPITE
// sharing a hash function, not because a different algorithm happens to
// keep them apart.
func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Authenticate is pure authentication logic: hash the raw token, look it
// up in Store (admin_user, never api_keys), reject if unknown or revoked,
// return an Identity. No authorization decision of any kind — a viewer
// and an admin authenticate identically here; Step G is what tells them
// apart.
func Authenticate(ctx context.Context, store Store, rawToken string) (*Identity, error) {
	if rawToken == "" {
		return nil, ErrMissingCredential
	}

	hash := HashToken(rawToken)
	row, err := store.LookupByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, ErrInvalidCredential
	}
	if row.RevokedAt != nil {
		return nil, ErrRevoked
	}

	return &Identity{UserID: row.UserID, OrgID: row.OrgID, Role: row.Role}, nil
}
