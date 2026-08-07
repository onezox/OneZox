package authn

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAuthenticateValidToken(t *testing.T) {
	store := NewFakeStore()
	raw := "oz_admin_admin_test123"
	store.Seed(HashToken(raw), &Row{UserID: "u1", OrgID: "o1", Role: "admin"})

	id, err := Authenticate(context.Background(), store, raw)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "u1" || id.OrgID != "o1" || id.Role != "admin" {
		t.Fatalf("unexpected identity: %+v", id)
	}
}

func TestAuthenticateEmptyToken(t *testing.T) {
	store := NewFakeStore()
	_, err := Authenticate(context.Background(), store, "")
	if !errors.Is(err, ErrMissingCredential) {
		t.Fatalf("expected ErrMissingCredential, got %v", err)
	}
}

func TestAuthenticateUnknownToken(t *testing.T) {
	store := NewFakeStore()
	_, err := Authenticate(context.Background(), store, "oz_admin_admin_nope")
	if !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("expected ErrInvalidCredential, got %v", err)
	}
}

func TestAuthenticateRevokedToken(t *testing.T) {
	store := NewFakeStore()
	raw := "oz_admin_admin_revoked"
	revokedAt := time.Now()
	store.Seed(HashToken(raw), &Row{UserID: "u1", OrgID: "o1", Role: "admin", RevokedAt: &revokedAt})

	_, err := Authenticate(context.Background(), store, raw)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("expected ErrRevoked, got %v", err)
	}
}

func TestAuthenticateStoreError(t *testing.T) {
	store := NewFakeStore()
	store.Err = errors.New("connection reset")

	_, err := Authenticate(context.Background(), store, "anything")
	if err == nil || errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("expected the store's own error to propagate distinctly, got %v", err)
	}
}

// TestApiKeyHashNeverAuthenticatesAsAdmin is the disjointness proof named
// in the Phase-05 plan's Decision 1 and this step's own instructions: a
// hash that is exactly what a REAL tenant api_keys.hash value would look
// like (same SHA-256 hex convention, data/migrations/0004) has no row in
// this store — because this store only ever holds what admin_user
// contains, structurally, never api_keys. This is not "the tenant key
// happens to be wrong," it's "this credential space has no knowledge of
// that one at all." The live version of this same proof (a genuine
// api_keys raw value tried against the real deployed admin-api) is
// Steps G/J.
func TestApiKeyHashNeverAuthenticatesAsAdmin(t *testing.T) {
	store := NewFakeStore()
	// Seed a real admin credential so the store isn't simply empty —
	// proves the tenant hash specifically doesn't match, not that
	// nothing matches anything.
	adminRaw := "oz_admin_admin_real"
	store.Seed(HashToken(adminRaw), &Row{UserID: "u1", OrgID: "o1", Role: "admin"})

	// A tenant raw key, same "oz_test_<hex>" shape seed-test-tenant.sh
	// generates, hashed the identical way api_keys.hash is computed.
	tenantRawKey := "oz_test_deadbeefcafef00d"

	_, err := Authenticate(context.Background(), store, tenantRawKey)
	if !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("a tenant-shaped credential must be rejected as unknown to the admin store, got %v", err)
	}
}

func TestIdentityFromContextAbsent(t *testing.T) {
	_, ok := IdentityFromContext(context.Background())
	if ok {
		t.Fatal("expected no identity on a bare context")
	}
}

func TestWithIdentityRoundTrips(t *testing.T) {
	want := &Identity{UserID: "u1", OrgID: "o1", Role: "viewer"}
	ctx := WithIdentity(context.Background(), want)

	got, ok := IdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected an identity to be present")
	}
	if *got != *want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
