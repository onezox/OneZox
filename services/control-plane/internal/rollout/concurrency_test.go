package rollout

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Post-M2 CRITICAL fix — the concurrency tests for the compare-and-swap.
//
// The bug these exist for: control-plane runs 2 replicas, each running
// the canary reconciler in-process with no coordination. advanceStage
// re-read the rollout itself and advanced from whatever stage it found,
// with nothing tying that to the stage the caller had actually observed
// an AnalysisRun for. One Successful analysis at canary_10 could
// therefore walk a rollout 10 -> 50 -> 100, skipping the 50% stage's
// analysis entirely — the staged gate EC1/EC2 certify, bypassed.
//
// Every test here asserts the STAGE MOVED EXACTLY ONE STEP, not merely
// that no error came back. "No error" was already true of the broken
// version; the stage count is what actually distinguishes fixed from
// broken.

// startedRollout seeds a rollout and walks it to the requested stage,
// returning its id. Uses the human promote path, which is itself CAS'd.
func startedRollout(t *testing.T, svc *Service, upTo string) string {
	t.Helper()
	ctx := context.Background()
	id, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if err != nil {
		t.Fatalf("CreateRollout: %v", err)
	}
	for {
		r, err := svc.store.GetRollout(ctx, id)
		if err != nil {
			t.Fatalf("GetRollout: %v", err)
		}
		if r.Stage == upTo {
			return id
		}
		if _, err := svc.PromoteRollout(ctx, id); err != nil {
			t.Fatalf("PromoteRollout to reach %s: %v", upTo, err)
		}
	}
}

func seededService(t *testing.T) (*Service, *FakeStore) {
	t.Helper()
	svc, store, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")
	return svc, store
}

// THE HEADLINE TEST. Two reconcilers observe the SAME stage and both act
// on it — exactly the two-replica situation. Exactly one may win.
func TestTwoConcurrentAutoAdvancesFromTheSameStageAdvanceExactlyOneStep(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	// Both "reconcilers" decided from an AnalysisRun for canary_10.
	const observed = "canary_10"

	var wg sync.WaitGroup
	results := make([]error, 2)
	stages := make([]string, 2)
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // maximise the overlap
			stages[i], results[i] = svc.AutoAdvance(ctx, id, observed)
		}()
	}
	close(start)
	wg.Wait()

	wins, losses := 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			wins++
			if stages[i] != "canary_50" {
				t.Errorf("winner advanced to %q, want canary_50", stages[i])
			}
		case errors.Is(err, ErrConcurrentUpdate):
			losses++
		default:
			t.Errorf("unexpected error from concurrent advance: %v", err)
		}
	}
	if wins != 1 || losses != 1 {
		t.Fatalf("expected exactly 1 winner and 1 loser, got %d/%d", wins, losses)
	}

	// The property that actually matters: ONE step, not two.
	r, err := store.GetRollout(ctx, id)
	if err != nil {
		t.Fatalf("GetRollout: %v", err)
	}
	if r.Stage != "canary_50" {
		t.Fatalf("rollout is at %q; a double-advance would show canary_100 here", r.Stage)
	}
}

// The sequential form of the same race, and the clearest statement of
// the original bug: a decision made about canary_10 must not be applied
// to a rollout that has since moved to canary_50.
func TestStaleStageAdvanceIsRefused(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	// Replica A advances 10 -> 50.
	if _, err := svc.AutoAdvance(ctx, id, "canary_10"); err != nil {
		t.Fatalf("first advance: %v", err)
	}

	// Replica B, still holding its canary_10 observation, tries to act.
	_, err := svc.AutoAdvance(ctx, id, "canary_10")
	if !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("stale advance returned %v, want ErrConcurrentUpdate", err)
	}

	r, _ := store.GetRollout(ctx, id)
	if r.Stage != "canary_50" {
		t.Fatalf("rollout is at %q; the stale advance should have left it at canary_50", r.Stage)
	}
}

// POSITIVE CONTROL for every test above. If the CAS were inert, these
// tests would pass for the wrong reason — a matching precondition must
// still succeed, or "refused" proves nothing.
func TestAdvanceWithTheCorrectStageStillSucceeds(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	next, err := svc.AutoAdvance(ctx, id, "canary_10")
	if err != nil {
		t.Fatalf("matching-precondition advance was refused: %v", err)
	}
	if next != "canary_50" {
		t.Fatalf("advanced to %q, want canary_50", next)
	}
	r, _ := store.GetRollout(ctx, id)
	if r.Stage != "canary_50" {
		t.Fatalf("store shows %q, want canary_50", r.Stage)
	}
}

// THE HUMAN-VS-RECONCILER RACE, in its nastiest form: an operator aborts
// a canary while an analysis is resolving, and the reconciler's in-flight
// advance lands afterwards. The status='running' half of the CAS
// predicate is what stops an aborted rollout being resurrected.
func TestReconcilerCannotResurrectAnAbortedRollout(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	// Human aborts mid-flight.
	if err := svc.AbortRollout(ctx, id); err != nil {
		t.Fatalf("AbortRollout: %v", err)
	}
	before, _ := store.GetRollout(ctx, id)
	if before.Status != "aborted" {
		t.Fatalf("status is %q, want aborted", before.Status)
	}

	// The reconciler's advance, decided before the abort, now lands.
	_, err := svc.AutoAdvance(ctx, id, "canary_10")
	if err == nil {
		t.Fatal("advance succeeded against an ABORTED rollout — resurrection")
	}
	if !errors.Is(err, ErrNotRunning) && !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("advance returned %v, want ErrNotRunning or ErrConcurrentUpdate", err)
	}

	after, _ := store.GetRollout(ctx, id)
	if after.Status != "aborted" || after.Stage != before.Stage {
		t.Fatalf("aborted rollout was mutated: stage %q->%q status %q->%q",
			before.Stage, after.Stage, before.Status, after.Status)
	}
}

// The same guard for the automatic rollback path: a rollback decided
// from one stage must not fire against a rollout that has moved on.
func TestStaleRollbackIsRefused(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	if _, err := svc.AutoAdvance(ctx, id, "canary_10"); err != nil {
		t.Fatalf("advance: %v", err)
	}

	err := svc.AutoRollback(ctx, id, "canary_10")
	if !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("stale rollback returned %v, want ErrConcurrentUpdate", err)
	}
	r, _ := store.GetRollout(ctx, id)
	if r.Status != "running" {
		t.Fatalf("a stale rollback terminalized the rollout: status=%q", r.Status)
	}
}

// POSITIVE CONTROL for the rollback path.
func TestRollbackWithTheCorrectStageStillSucceeds(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	if err := svc.AutoRollback(ctx, id, "canary_10"); err != nil {
		t.Fatalf("matching-precondition rollback was refused: %v", err)
	}
	r, _ := store.GetRollout(ctx, id)
	if r.Status != "rolled_back" {
		t.Fatalf("status is %q, want rolled_back", r.Status)
	}
}

// Two concurrent rollbacks: exactly one records the outcome.
func TestTwoConcurrentRollbacksTerminalizeOnce(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	var wg sync.WaitGroup
	results := make([]error, 2)
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = svc.AutoRollback(ctx, id, "canary_10")
		}()
	}
	close(start)
	wg.Wait()

	wins := 0
	for _, err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConcurrentUpdate) && !errors.Is(err, ErrNotRunning) {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 rollback to apply, got %d", wins)
	}
	r, _ := store.GetRollout(ctx, id)
	if r.Status != "rolled_back" {
		t.Fatalf("status is %q, want rolled_back", r.Status)
	}
}

// The human promote path keeps NO client-controllable stage and is still
// race-safe, because it CASes on the stage the server itself read.
//
// The invariant here is deliberately "each successful call advances
// EXACTLY ONE step", not "only one call may succeed". Two operators both
// pressing Promote are making two separate, separately-audited requests
// to advance one step each; two steps is the correct outcome, and it is
// what the override exists for ("don't wait out the pause"). Collapsing
// them would require the caller to state which stage it expected, which
// is precisely the client-controllable stage parameter that must never
// exist — admin.proto's PromoteRolloutRequest has no stage field, and
// EC4's API-parameter proof rests on it staying that way.
//
// What must NEVER happen is one call moving more than one step. That is
// the property asserted below: k successes => exactly k steps.
//
// (This test originally asserted "exactly 1 winner" and failed. The two
// calls serialize — 10->50 then 50->stable — each advancing one step.
// The assertion was wrong, not the code; recorded here because the
// distinction between "the race is closed" and "concurrent requests are
// deduplicated" is easy to conflate, and only the former is the fix.)
func TestConcurrentHumanPromotesEachAdvanceExactlyOneStep(t *testing.T) {
	ctx := context.Background()
	svc, store := seededService(t)
	id := startedRollout(t, svc, "canary_10")

	// canary_10 -> canary_50 -> stable, so two successes lands on stable.
	stagesFromCanary10 := []string{"canary_10", "canary_50", "stable"}

	var wg sync.WaitGroup
	results := make([]error, 2)
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, results[i] = svc.PromoteRollout(ctx, id)
		}()
	}
	close(start)
	wg.Wait()

	wins := 0
	for _, err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrConcurrentUpdate), errors.Is(err, ErrNotRunning):
			// A legitimate loss; the winner already moved the rollout.
		default:
			t.Errorf("unexpected error from concurrent promote: %v", err)
		}
	}
	if wins == 0 {
		t.Fatal("no promote applied at all")
	}

	r, _ := store.GetRollout(ctx, id)
	want := stagesFromCanary10[wins]
	if r.Stage != want {
		t.Fatalf("%d successful promote(s) moved the rollout to %q, want %q — "+
			"a call advanced more than one step", wins, r.Stage, want)
	}
}
