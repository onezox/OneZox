package promclient

import (
	"context"
	"sync"
)

// FakeQuerier returns a canned value per query string — lets a resolver
// test assert the EXACT PromQL each dashboard number is built from,
// which a fake returning one value for everything could not.
type FakeQuerier struct {
	mu       sync.Mutex
	Results  map[string]float64
	Err      error
	Queries  []string
	Fallback float64
}

func NewFakeQuerier() *FakeQuerier {
	return &FakeQuerier{Results: make(map[string]float64)}
}

func (f *FakeQuerier) QueryScalar(ctx context.Context, promQL string) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Queries = append(f.Queries, promQL)
	if f.Err != nil {
		return 0, f.Err
	}
	if v, ok := f.Results[promQL]; ok {
		return v, nil
	}
	return f.Fallback, nil
}
