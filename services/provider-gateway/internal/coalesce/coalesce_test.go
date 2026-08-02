package coalesce

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

// sliceStream is a minimal adapters.Stream backed by a fixed slice, for
// building test closures without needing a real adapter.
type sliceStream struct {
	deltas []adapters.Delta
	idx    int
	err    error // returned instead of io.EOF once deltas are exhausted, if set
}

func (s *sliceStream) Recv() (adapters.Delta, error) {
	if s.idx < len(s.deltas) {
		d := s.deltas[s.idx]
		s.idx++
		return d, nil
	}
	if s.err != nil {
		return adapters.Delta{}, s.err
	}
	return adapters.Delta{}, io.EOF
}

func strPtr(s string) *string { return &s }

func canned() []adapters.Delta {
	return []adapters.Delta{
		{Content: strPtr("hello ")},
		{Content: strPtr("world")},
		{FinishReason: strPtr("stop"), IsFinal: true},
	}
}

func drain(t *testing.T, s adapters.Stream) []adapters.Delta {
	t.Helper()
	var out []adapters.Delta
	for {
		d, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("unexpected error draining stream: %v", err)
		}
		out = append(out, d)
		if d.IsFinal {
			return out
		}
	}
}

func TestConcurrentIdenticalCallsInvokeTheClosureOnlyOnce(t *testing.T) {
	g := NewGroup()
	var callCount int64

	call := func() (adapters.Stream, error) {
		atomic.AddInt64(&callCount, 1)
		return &sliceStream{deltas: canned()}, nil
	}

	s1 := g.Invoke("same-key", call)
	s2 := g.Invoke("same-key", call)

	d1 := drain(t, s1)
	d2 := drain(t, s2)

	if got := atomic.LoadInt64(&callCount); got != 1 {
		t.Errorf("closure invoked %d times, want exactly 1", got)
	}
	if len(d1) != 3 || len(d2) != 3 {
		t.Fatalf("expected both subscribers to see all 3 deltas, got %d and %d", len(d1), len(d2))
	}
	if *d1[0].Content != *d2[0].Content {
		t.Errorf("subscribers saw different content: %q vs %q", *d1[0].Content, *d2[0].Content)
	}
}

func TestDifferentKeysEachInvokeTheirOwnClosure(t *testing.T) {
	g := NewGroup()
	var callCount int64
	call := func() (adapters.Stream, error) {
		atomic.AddInt64(&callCount, 1)
		return &sliceStream{deltas: canned()}, nil
	}

	drain(t, g.Invoke("key-a", call))
	drain(t, g.Invoke("key-b", call))

	if got := atomic.LoadInt64(&callCount); got != 2 {
		t.Errorf("closure invoked %d times for 2 distinct keys, want 2", got)
	}
}

func TestALateSubscriberStillGetsTheFullSequence(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})

	call := func() (adapters.Stream, error) {
		close(started)
		<-release // hold the leader open so a follower can join mid-flight
		return &sliceStream{deltas: canned()}, nil
	}

	s1 := g.Invoke("same-key", call)
	<-started

	s2 := g.Invoke("same-key", call) // joins while the leader is still blocked
	close(release)

	d1 := drain(t, s1)
	d2 := drain(t, s2)

	if len(d1) != 3 || len(d2) != 3 {
		t.Fatalf("expected both subscribers to see all 3 deltas via replay, got %d and %d", len(d1), len(d2))
	}
}

func TestAnErrorFromTheClosurePropagatesToAllSubscribers(t *testing.T) {
	g := NewGroup()
	wantErr := errors.New("adapter unavailable")
	call := func() (adapters.Stream, error) {
		return nil, wantErr
	}

	s1 := g.Invoke("same-key", call)
	s2 := g.Invoke("same-key", call)

	_, err1 := s1.Recv()
	_, err2 := s2.Recv()

	if !errors.Is(err1, wantErr) {
		t.Errorf("subscriber 1 error = %v, want %v", err1, wantErr)
	}
	if !errors.Is(err2, wantErr) {
		t.Errorf("subscriber 2 error = %v, want %v", err2, wantErr)
	}
}

func TestAMidStreamErrorPropagatesToAllSubscribers(t *testing.T) {
	g := NewGroup()
	wantErr := errors.New("upstream disconnected")
	call := func() (adapters.Stream, error) {
		return &sliceStream{
			deltas: []adapters.Delta{{Content: strPtr("partial")}},
			err:    wantErr,
		}, nil
	}

	s1 := g.Invoke("same-key", call)
	s2 := g.Invoke("same-key", call)

	// Both should see the one partial delta, then the error.
	d, err := s1.Recv()
	if err != nil || d.Content == nil || *d.Content != "partial" {
		t.Fatalf("subscriber 1's first Recv = (%v, %v), want the partial delta", d, err)
	}
	_, err = s1.Recv()
	if !errors.Is(err, wantErr) {
		t.Errorf("subscriber 1's second Recv error = %v, want %v", err, wantErr)
	}

	d, err = s2.Recv()
	if err != nil || d.Content == nil || *d.Content != "partial" {
		t.Fatalf("subscriber 2's first Recv = (%v, %v), want the partial delta", d, err)
	}
	_, err = s2.Recv()
	if !errors.Is(err, wantErr) {
		t.Errorf("subscriber 2's second Recv error = %v, want %v", err, wantErr)
	}
}

func TestAKeyIsReleasedAfterCompletionSoANewCallStartsFresh(t *testing.T) {
	g := NewGroup()
	var callCount int64
	call := func() (adapters.Stream, error) {
		atomic.AddInt64(&callCount, 1)
		return &sliceStream{deltas: canned()}, nil
	}

	drain(t, g.Invoke("same-key", call))

	// Give the leader goroutine a moment to release the key after
	// finishing (release happens right before b.finish, which is what
	// drain's last Recv already waited on — this is a defensive margin,
	// not a real race in the test itself).
	time.Sleep(10 * time.Millisecond)

	drain(t, g.Invoke("same-key", call))

	if got := atomic.LoadInt64(&callCount); got != 2 {
		t.Errorf("closure invoked %d times across two sequential (non-overlapping) calls, want 2", got)
	}
}

func TestKeyIsStableForIdenticalContentRegardlessOfRequestID(t *testing.T) {
	req1 := adapters.InvokeRequest{
		RequestID: "req-a",
		Model:     "gpt-4o-mini",
		Messages:  []adapters.Message{{Role: "user", Content: "hi"}},
	}
	req2 := adapters.InvokeRequest{
		RequestID: "req-b", // different ID, same everything else
		Model:     "gpt-4o-mini",
		Messages:  []adapters.Message{{Role: "user", Content: "hi"}},
	}
	if Key(req1) != Key(req2) {
		t.Errorf("Key() differed for identical content with different request IDs: %q vs %q", Key(req1), Key(req2))
	}
}

func TestKeyDiffersForDifferentContent(t *testing.T) {
	req1 := adapters.InvokeRequest{Model: "gpt-4o-mini", Messages: []adapters.Message{{Role: "user", Content: "hi"}}}
	req2 := adapters.InvokeRequest{Model: "gpt-4o-mini", Messages: []adapters.Message{{Role: "user", Content: "bye"}}}
	if Key(req1) == Key(req2) {
		t.Error("Key() was the same for genuinely different message content")
	}
}
