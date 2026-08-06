package policy

import (
	"context"
	"sync"
	"time"
)

// FakeStore is an in-memory Store for unit tests — no CockroachDB needed.
type FakeStore struct {
	mu      sync.Mutex
	entries []Entry
}

func NewFakeStore() *FakeStore {
	return &FakeStore{}
}

func (f *FakeStore) Insert(ctx context.Context, orgID, rulesJSON string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, Entry{
		PolicyID:  "fake-policy-id",
		OrgID:     orgID,
		RulesJSON: rulesJSON,
		UpdatedAt: time.Now(),
	})
	return nil
}

func (f *FakeStore) GetCurrent(ctx context.Context, orgID string) (*Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var latest *Entry
	for i := range f.entries {
		e := f.entries[i]
		if e.OrgID != orgID {
			continue
		}
		if latest == nil || e.UpdatedAt.After(latest.UpdatedAt) {
			cp := e
			latest = &cp
		}
	}
	if latest == nil {
		return nil, ErrNotFound
	}
	return latest, nil
}
