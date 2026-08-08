package apikeys

import (
	"context"
	"fmt"
	"sync"
)

// FakeStore is an in-memory Store for unit tests — no CockroachDB needed,
// same discipline as audit.FakeWriter and rollout.FakeStore.
type FakeStore struct {
	mu   sync.Mutex
	keys map[string]bool // key_id -> active (true) / revoked (false)
	next int

	CreateErr error
	RevokeErr error

	// ValidOrgIDs, when non-nil, makes Create fail for any org_id not
	// listed — standing in for api_keys.org_id's own FK constraint
	// against tenants (migration 0004) without a real CockroachDB.
	ValidOrgIDs map[string]bool

	CreateCalls []struct {
		OrgID, Hash string
		Scopes      []string
	}
	RevokeCalls []string
}

func NewFakeStore() *FakeStore {
	return &FakeStore{keys: make(map[string]bool)}
}

func (f *FakeStore) Create(ctx context.Context, orgID, hash string, scopes []string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CreateCalls = append(f.CreateCalls, struct {
		OrgID, Hash string
		Scopes      []string
	}{orgID, hash, scopes})
	if f.CreateErr != nil {
		return "", f.CreateErr
	}
	if f.ValidOrgIDs != nil && !f.ValidOrgIDs[orgID] {
		return "", fmt.Errorf("insert on table api_keys violates foreign key constraint: org_id %q not found", orgID)
	}
	f.next++
	keyID := fmt.Sprintf("key-%d", f.next)
	f.keys[keyID] = true
	return keyID, nil
}

func (f *FakeStore) Revoke(ctx context.Context, keyID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.RevokeCalls = append(f.RevokeCalls, keyID)
	if f.RevokeErr != nil {
		return false, f.RevokeErr
	}
	active, exists := f.keys[keyID]
	if !exists || !active {
		return false, nil
	}
	f.keys[keyID] = false
	return true, nil
}
