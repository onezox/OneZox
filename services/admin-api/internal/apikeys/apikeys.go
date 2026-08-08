// Package apikeys is admin-api's own boundary onto api_keys (P01's table,
// migration 0004, reused here — not rebuilt). Local to admin_api's own
// grant (migration 0018: SELECT+INSERT+UPDATE, no DELETE) — this package
// never reaches control-plane at all (admin.proto's own header comment),
// the same "narrow interface per external dependency" shape audit.Writer
// and rollout.Store already established.
//
// Revoke is UPDATE, not DELETE: revoked_at is a soft-delete timestamp
// (api_keys.revoked_at, nullable) — matching the Phase-05 plan's own
// blast-radius table ("revokeApiKey: fails safe, an outage not a leak").
// A revoked row stays queryable (listApiKeys can still show it, dimmed),
// which a hard DELETE would prevent, and admin_api has no DELETE grant on
// this table regardless.
package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateRawKey mints a new raw API key — 32 random bytes, hex-encoded,
// under the "oz_" prefix this project's other minted-credential scripts
// already use (data/seed/seed-test-tenant.sh's own "oz_test_" and
// scripts/seed-admin-user.sh's own "oz_admin_<role>_", same convention,
// no "test"/"admin" segment here since this is a real tenant credential).
// Printed to the caller exactly once by CreateApiKeyResponse — never
// persisted; only HashRawKey's own output is stored (CLAUDE.md: store
// API-key HASHES, never raw keys).
func GenerateRawKey() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating random key material: %w", err)
	}
	return "oz_" + hex.EncodeToString(buf), nil
}

// HashRawKey — the SAME SHA-256 hex convention authn.HashToken and
// edge-gateway's own hash_api_key use, so a key minted here authenticates
// at the edge exactly like one seeded by seed-test-tenant.sh.
func HashRawKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Summary is the ONLY shape List returns — no Hash field exists on this
// struct at all (structurally, not by convention: there is nothing to
// forget to omit). Mirrors admin.graphql's own ApiKeySummary type field
// for field ("Never exposes hash or raw key — raw_key is returned
// exactly once, by the createApiKey gRPC command's own response, never
// by a query").
type Summary struct {
	KeyID     string
	OrgID     string
	Scopes    []string
	CreatedAt string
	RevokedAt *string
}

// Store is implemented by CockroachStore in production and FakeStore in
// tests.
type Store interface {
	// Create inserts a new row and returns its key_id. orgID must
	// reference an existing tenants row (api_keys.org_id's own FK,
	// migration 0004) — an unknown org_id fails here, not silently.
	Create(ctx context.Context, orgID, hash string, scopes []string) (keyID string, err error)

	// Revoke sets revoked_at = now() for an active (not already revoked)
	// key. found=false covers BOTH "no such key_id" and "already revoked"
	// uniformly — a caller doesn't need to distinguish which, since
	// either way there is no active key left to revoke a second time.
	Revoke(ctx context.Context, keyID string) (found bool, err error)

	// List — Step S, the GraphQL apiKeys query's own backing read. Most
	// recent first, every key regardless of revoked status (the panel
	// dims revoked ones, per admin.graphql's own listApiKeys comment
	// history; nothing here filters them out).
	List(ctx context.Context) ([]Summary, error)
}

type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) Create(ctx context.Context, orgID, hash string, scopes []string) (string, error) {
	// scopes passed as a plain []string — pgx/v5's stdlib driver encodes a
	// Go string slice directly as a Postgres/CockroachDB STRING[] literal,
	// no pq.Array-style wrapper needed (that's a lib/pq-specific
	// requirement; this project's driver is pgx, migration 0004's own
	// scopes column).
	if scopes == nil {
		scopes = []string{}
	}
	var keyID string
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO api_keys (org_id, hash, scopes) VALUES ($1, $2, $3)
		RETURNING key_id
	`, orgID, hash, scopes).Scan(&keyID)
	return keyID, err
}

func (c *CockroachStore) Revoke(ctx context.Context, keyID string) (bool, error) {
	res, err := c.db.ExecContext(ctx, `
		UPDATE api_keys SET revoked_at = now() WHERE key_id = $1 AND revoked_at IS NULL
	`, keyID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// List's SELECT deliberately never names the hash column — not "select
// it and drop it before returning," there is no code path here that
// ever reads api_keys.hash out of the database in the first place. A
// future bug in this function's own body cannot leak a hash it never
// fetched.
func (c *CockroachStore) List(ctx context.Context) ([]Summary, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT key_id, org_id, scopes, created_at, revoked_at
		FROM api_keys
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Summary
	for rows.Next() {
		var s Summary
		var createdAt time.Time
		var revokedAt sql.NullTime
		if err := rows.Scan(&s.KeyID, &s.OrgID, &s.Scopes, &createdAt, &revokedAt); err != nil {
			return nil, err
		}
		s.CreatedAt = createdAt.Format(time.RFC3339)
		if revokedAt.Valid {
			v := revokedAt.Time.Format(time.RFC3339)
			s.RevokedAt = &v
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
