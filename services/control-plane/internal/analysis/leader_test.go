package analysis

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// memLock is a shared in-memory resourcelock.Interface — the same lock
// object handed to several candidates, standing in for one Lease object
// several pods contend for. Optimistic concurrency is modelled the way
// the real API server enforces it: a write only lands if the caller's
// view of the record is current, so a candidate that lost the race
// cannot overwrite the winner.
type memLock struct {
	mu       *sync.Mutex
	state    *lockState
	identity string
}

type lockState struct {
	record  *resourcelock.LeaderElectionRecord
	raw     []byte
	version int
}

func newMemLockSet(identities ...string) []*memLock {
	shared := &lockState{}
	mu := &sync.Mutex{}
	out := make([]*memLock, 0, len(identities))
	for _, id := range identities {
		out = append(out, &memLock{mu: mu, state: shared, identity: id})
	}
	return out
}

var groupResource = schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}

func (m *memLock) Get(_ context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.record == nil {
		return nil, nil, errors.NewNotFound(groupResource, leaseName)
	}
	cp := *m.state.record
	return &cp, m.state.raw, nil
}

func (m *memLock) Create(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.record != nil {
		return errors.NewAlreadyExists(groupResource, leaseName)
	}
	return m.store(ler)
}

func (m *memLock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.record == nil {
		return errors.NewNotFound(groupResource, leaseName)
	}
	// Only the current holder, or a candidate whose Get observed the
	// present record, may write — the API server's own conflict rule.
	if m.state.record.HolderIdentity != m.identity &&
		m.state.record.HolderIdentity != ler.HolderIdentity &&
		ler.HolderIdentity != m.identity {
		return errors.NewConflict(groupResource, leaseName, nil)
	}
	return m.store(ler)
}

func (m *memLock) store(ler resourcelock.LeaderElectionRecord) error {
	raw, err := json.Marshal(ler)
	if err != nil {
		return err
	}
	cp := ler
	m.state.record = &cp
	m.state.raw = raw
	m.state.version++
	return nil
}

func (m *memLock) RecordEvent(string)  {}
func (m *memLock) Identity() string    { return m.identity }
func (m *memLock) Describe() string    { return "memlock/" + leaseName }

// holder reports who currently owns the lock, for assertions.
func (m *memLock) holder() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.record == nil {
		return ""
	}
	return m.state.record.HolderIdentity
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// THE SINGLETON PROPERTY. Several candidates contend for one lock; at no
// instant may more than one be inside run(). This is the property the
// two-replica bug violated: both replicas ran the reconciler always.
func TestOnlyOneCandidateRunsAtATime(t *testing.T) {
	locks := newMemLockSet("pod-a", "pod-b", "pod-c")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var everRan atomic.Bool

	run := func(c context.Context) {
		everRan.Store(true)
		n := concurrent.Add(1)
		for {
			m := maxConcurrent.Load()
			if n <= m || maxConcurrent.CompareAndSwap(m, n) {
				break
			}
		}
		<-c.Done()
		concurrent.Add(-1)
	}

	var wg sync.WaitGroup
	for _, l := range locks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = runWithLock(ctx, l, discardLog(), run)
		}()
	}

	// Long enough for the lock to be acquired and for the two losers to
	// have retried several times (retryPeriod is 2s).
	time.Sleep(6 * time.Second)

	if !everRan.Load() {
		t.Fatal("no candidate ever acquired leadership — the mechanism is dead, not safe")
	}
	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("%d candidates ran concurrently, want exactly 1", got)
	}
	if h := locks[0].holder(); h == "" {
		t.Fatal("lock has no holder despite a candidate having run")
	}

	cancel()
	wg.Wait()
}

// A demoted leader must STOP. If losing the lease left the loop running,
// leader election would be decorative — the failure mode that looks safe
// while two reconcilers keep writing.
func TestLosingLeadershipCancelsTheRunContext(t *testing.T) {
	locks := newMemLockSet("pod-a")
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once

	run := func(c context.Context) {
		once.Do(func() { close(started) })
		<-c.Done() // the assertion: this returns when leadership ends
		close(stopped)
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = runWithLock(ctx, locks[0], discardLog(), run) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("candidate never started leading")
	}

	cancel() // simulates shutdown / lease loss

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("run() kept going after leadership ended — the loop was never cancelled")
	}
	<-done
}

// FAILOVER. When the holder goes away, another candidate must take over
// — a singleton that stops driving after failover looks safe while being
// broken, which is worse than the original bug.
func TestLeadershipPassesToAnotherCandidateAfterTheLeaderStops(t *testing.T) {
	locks := newMemLockSet("pod-a", "pod-b")

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	var ranA, ranB atomic.Bool
	runA := func(c context.Context) { ranA.Store(true); <-c.Done() }
	runB := func(c context.Context) { ranB.Store(true); <-c.Done() }

	doneA := make(chan struct{})
	go func() { defer close(doneA); _ = runWithLock(ctxA, locks[0], discardLog(), runA) }()

	// Let A win first, deterministically.
	deadline := time.Now().Add(10 * time.Second)
	for !ranA.Load() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !ranA.Load() {
		t.Fatal("pod-a never acquired leadership")
	}

	go func() { _ = runWithLock(ctxB, locks[1], discardLog(), runB) }()
	time.Sleep(2 * time.Second)
	if ranB.Load() {
		t.Fatal("pod-b started leading while pod-a still held the lease")
	}

	// A goes away. ReleaseOnCancel means it gives the lease up promptly.
	cancelA()
	<-doneA

	deadline = time.Now().Add(leaseDuration + 15*time.Second)
	for !ranB.Load() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !ranB.Load() {
		t.Fatal("pod-b never took over after pod-a stopped — failover is broken")
	}
}
