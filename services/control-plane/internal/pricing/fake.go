package pricing

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

func (f *FakeStore) Insert(ctx context.Context, modelRef string, unitCosts UnitCosts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, Entry{
		PricingID:   "fake-pricing-id",
		ModelRef:    modelRef,
		UnitCosts:   unitCosts,
		EffectiveAt: time.Now(),
	})
	return nil
}

func (f *FakeStore) GetCurrent(ctx context.Context, modelRef string) (*Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var latest *Entry
	for i := range f.entries {
		e := f.entries[i]
		if e.ModelRef != modelRef {
			continue
		}
		if latest == nil || e.EffectiveAt.After(latest.EffectiveAt) {
			cp := e
			latest = &cp
		}
	}
	if latest == nil {
		return nil, ErrNotFound
	}
	return latest, nil
}

func (f *FakeStore) ListCurrent(ctx context.Context) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	latestByModel := map[string]Entry{}
	for _, e := range f.entries {
		cur, ok := latestByModel[e.ModelRef]
		if !ok || e.EffectiveAt.After(cur.EffectiveAt) {
			latestByModel[e.ModelRef] = e
		}
	}
	out := make([]Entry, 0, len(latestByModel))
	for _, e := range latestByModel {
		out = append(out, e)
	}
	return out, nil
}
