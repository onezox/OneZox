package apikeys

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FakeStore is an in-memory Store for unit tests — no CockroachDB needed,
// same discipline as audit.FakeWriter and rollout.FakeStore.
type FakeStore struct {
	mu      sync.Mutex
	records map[string]*Summary // key_id -> its own Summary; RevokedAt nil means active
	next    int

	CreateErr error
	RevokeErr error
	ListErr   error

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
	return &FakeStore{records: make(map[string]*Summary)}
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
	f.records[keyID] = &Summary{
		KeyID:     keyID,
		OrgID:     orgID,
		Scopes:    scopes,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	return keyID, nil
}

func (f *FakeStore) Revoke(ctx context.Context, keyID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.RevokeCalls = append(f.RevokeCalls, keyID)
	if f.RevokeErr != nil {
		return false, f.RevokeErr
	}
	rec, exists := f.records[keyID]
	if !exists || rec.RevokedAt != nil {
		return false, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	rec.RevokedAt = &now
	return true, nil
}

func (f *FakeStore) List(ctx context.Context) ([]Summary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	out := make([]Summary, 0, len(f.records))
	for _, rec := range f.records {
		cp := *rec
		out = append(out, cp)
	}
	return out, nil
}
