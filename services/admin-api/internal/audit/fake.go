package audit

import (
	"context"
	"sync"
)

// FakeWriter records every Write call in order — used by server.go's own
// unit tests (Step H onward) and authz's denial-audit tests to assert
// exactly which entries were written, without a real CockroachDB.
type FakeWriter struct {
	mu      sync.Mutex
	Entries []Entry
	Err     error
}

func NewFakeWriter() *FakeWriter {
	return &FakeWriter{}
}

func (f *FakeWriter) Write(ctx context.Context, e Entry) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Entries = append(f.Entries, e)
	return nil
}
