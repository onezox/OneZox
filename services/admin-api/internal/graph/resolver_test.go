package graph

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
)

func TestAPIKeysReturnsOnlySafeMetadata(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	if _, err := keys.Create(context.Background(), "org-1", "some-hash-value", []string{"chat.completions"}); err != nil {
		t.Fatalf("setup Create: %v", err)
	}

	r := &Resolver{Keys: keys}
	got, err := r.Query().APIKeys(context.Background())
	if err != nil {
		t.Fatalf("APIKeys: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}

	entry := got[0]
	if entry.KeyID == "" || entry.OrgID != "org-1" || entry.CreatedAt == "" {
		t.Errorf("unexpected entry: %+v", entry)
	}
	if !reflect.DeepEqual(entry.Scopes, []string{"chat.completions"}) {
		t.Errorf("scopes = %v, want [chat.completions]", entry.Scopes)
	}
	if entry.RevokedAt != nil {
		t.Errorf("revokedAt = %v, want nil for a never-revoked key", entry.RevokedAt)
	}

	// The structural proof, not just "this test didn't print a hash":
	// APIKeySummary (models_gen.go, generated straight from
	// admin.graphql) has no field that COULD hold a hash or raw key —
	// reflect over the struct's own fields and confirm neither name
	// appears, so this test breaks loudly if a future schema change
	// ever adds one back.
	typ := reflect.TypeOf(*entry)
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if name == "Hash" || name == "RawKey" {
			t.Fatalf("APIKeySummary has a %s field — a hash or raw key could leak through listApiKeys", name)
		}
	}
}

func TestAPIKeysShowsRevokedKeysWithTheirStatus(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	keyID, err := keys.Create(context.Background(), "org-1", "some-hash-value", nil)
	if err != nil {
		t.Fatalf("setup Create: %v", err)
	}
	if _, err := keys.Revoke(context.Background(), keyID); err != nil {
		t.Fatalf("setup Revoke: %v", err)
	}

	r := &Resolver{Keys: keys}
	got, err := r.Query().APIKeys(context.Background())
	if err != nil {
		t.Fatalf("APIKeys: %v", err)
	}
	if len(got) != 1 || got[0].RevokedAt == nil {
		t.Fatalf("got %+v, want exactly one entry with a non-nil revokedAt", got)
	}
}

func TestAPIKeysPropagatesStoreError(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ListErr = errors.New("connection refused")

	r := &Resolver{Keys: keys}
	if _, err := r.Query().APIKeys(context.Background()); err == nil {
		t.Fatal("APIKeys: want an error when the store fails, got nil")
	}
}
