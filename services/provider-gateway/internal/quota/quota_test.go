package quota

import (
	"context"
	"testing"
	"time"
)

func TestDecideAllowsAtAndBelowLimit(t *testing.T) {
	policy := Policy{Limit: 10, Window: time.Minute}
	if got := Decide(1, policy); got != Allow {
		t.Errorf("Decide(1, limit=10) = %v, want Allow", got)
	}
	if got := Decide(10, policy); got != Allow {
		t.Errorf("Decide(10, limit=10) = %v, want Allow", got)
	}
}

func TestDecideThrottlesAboveLimit(t *testing.T) {
	policy := Policy{Limit: 10, Window: time.Minute}
	if got := Decide(11, policy); got != Throttle {
		t.Errorf("Decide(11, limit=10) = %v, want Throttle", got)
	}
}

func TestEnforceAllowsWithinLimit(t *testing.T) {
	counter := NewFakeCounter()
	policy := Policy{Limit: 3, Window: time.Minute}
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		decision, err := Enforce(ctx, counter, "fake", policy)
		if err != nil {
			t.Fatalf("Enforce call %d: unexpected error: %v", i, err)
		}
		if decision != Allow {
			t.Errorf("Enforce call %d = %v, want Allow", i, decision)
		}
	}
}

func TestEnforceThrottlesOnceLimitExceeded(t *testing.T) {
	counter := NewFakeCounter()
	policy := Policy{Limit: 3, Window: time.Minute}
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if _, err := Enforce(ctx, counter, "fake", policy); err != nil {
			t.Fatalf("Enforce call %d: unexpected error: %v", i, err)
		}
	}

	decision, err := Enforce(ctx, counter, "fake", policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Throttle {
		t.Errorf("4th Enforce call (limit=3) = %v, want Throttle", decision)
	}
}

func TestEnforceFailsOpenOnStoreError(t *testing.T) {
	// soft limit=0 would throttle everything if the increment had
	// actually evaluated against a real count; failing open should skip
	// that entirely and allow, same reasoning as edge-gateway's
	// admission/ratelimit fail-open tests.
	policy := Policy{Limit: 0, Window: time.Minute}
	decision, err := Enforce(context.Background(), FailingCounter{}, "fake", policy)
	if err == nil {
		t.Fatal("expected an error from FailingCounter, got nil")
	}
	if decision != Allow {
		t.Errorf("Enforce with a failing store = %v, want Allow (fail open)", decision)
	}
}

func TestDifferentProvidersHaveIndependentWindows(t *testing.T) {
	counter := NewFakeCounter()
	policy := Policy{Limit: 1, Window: time.Minute}
	ctx := context.Background()

	// Exhaust "openai"'s quota.
	if _, err := Enforce(ctx, counter, "openai", policy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	decision, err := Enforce(ctx, counter, "openai", policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Throttle {
		t.Fatalf("openai's 2nd call = %v, want Throttle", decision)
	}

	// "anthropic" must be completely unaffected.
	decision, err = Enforce(ctx, counter, "anthropic", policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Allow {
		t.Errorf("anthropic's 1st call = %v, want Allow (independent of openai's exhausted window)", decision)
	}
}

func TestWindowKeyBucketsByFixedWindow(t *testing.T) {
	window := time.Minute
	t0 := time.Unix(1_700_000_000, 0) // an arbitrary fixed instant
	t1 := t0.Add(30 * time.Second)    // same minute-bucket
	t2 := t0.Add(90 * time.Second)    // next minute-bucket

	k0 := windowKey("fake", window, t0)
	k1 := windowKey("fake", window, t1)
	k2 := windowKey("fake", window, t2)

	if k0 != k1 {
		t.Errorf("same-window instants produced different keys: %q vs %q", k0, k1)
	}
	if k0 == k2 {
		t.Errorf("different-window instants produced the same key: %q", k0)
	}
}
