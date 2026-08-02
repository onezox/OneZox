package breaker

import (
	"context"
	"errors"
	"sync"
)

// FakeStore is an in-memory Store for hermetic tests — no live Redis
// needed, same rationale as quota.FakeCounter.
type FakeStore struct {
	mu      sync.Mutex
	records map[string]*record
}

func NewFakeStore() *FakeStore {
	return &FakeStore{records: make(map[string]*record)}
}

func (f *FakeStore) Get(ctx context.Context, key string) (*record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[key], nil
}

func (f *FakeStore) Set(ctx context.Context, key string, r *record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[key] = r
	return nil
}

// FailingStore always errors, for exercising Check/ReportResult's
// fail-open path.
type FailingStore struct{}

func (FailingStore) Get(ctx context.Context, key string) (*record, error) {
	return nil, errors.New("simulated outage")
}

func (FailingStore) Set(ctx context.Context, key string, r *record) error {
	return errors.New("simulated outage")
}
