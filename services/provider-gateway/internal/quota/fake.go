package quota

import (
	"context"
	"errors"
	"sync"
	"time"
)

// FakeCounter is an in-memory Counter for hermetic tests — no live Redis
// needed, same rationale as edge-gateway's FakeRateLimitCounter in Rust.
type FakeCounter struct {
	mu     sync.Mutex
	counts map[string]int64
}

func NewFakeCounter() *FakeCounter {
	return &FakeCounter{counts: make(map[string]int64)}
}

func (f *FakeCounter) Increment(ctx context.Context, key string, window time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[key]++
	return f.counts[key], nil
}

// FailingCounter always errors, for exercising Enforce's fail-open path.
type FailingCounter struct{}

func (FailingCounter) Increment(ctx context.Context, key string, window time.Duration) (int64, error) {
	return 0, errors.New("simulated outage")
}
