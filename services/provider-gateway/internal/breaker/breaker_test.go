package breaker

import (
	"context"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{FailureThreshold: 3, OpenDuration: 50 * time.Millisecond}
}

func TestCheckAllowsWhenClosed(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()
	decision, err := Check(ctx, store, "fake", testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Allow {
		t.Errorf("Check on a fresh breaker = %v, want Allow", decision)
	}
}

func TestRepeatedFailuresTripToOpen(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	for i := 1; i < cfg.FailureThreshold; i++ {
		if err := ReportResult(ctx, store, "fake", cfg, false); err != nil {
			t.Fatalf("ReportResult failure %d: unexpected error: %v", i, err)
		}
		decision, err := Check(ctx, store, "fake", cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision != Allow {
			t.Errorf("after %d/%d failures, Check = %v, want still Allow (below threshold)", i, cfg.FailureThreshold, decision)
		}
	}

	// The threshold-th failure trips it.
	if err := ReportResult(ctx, store, "fake", cfg, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	decision, err := Check(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Deny {
		t.Errorf("after %d consecutive failures, Check = %v, want Deny (Open)", cfg.FailureThreshold, decision)
	}
}

func TestASuccessResetsTheFailureCount(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	// One failure short of tripping.
	for i := 1; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
	}
	// A success resets it completely.
	if err := ReportResult(ctx, store, "fake", cfg, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Now it should take a FULL new threshold's worth of failures to trip,
	// not just one more.
	for i := 1; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
		decision, _ := Check(ctx, store, "fake", cfg)
		if decision != Allow {
			t.Fatalf("failure count did not reset: tripped after only %d failures post-reset", i)
		}
	}
}

func TestOpenTransitionsToHalfOpenTrialAfterOpenDuration(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
	}
	decision, _ := Check(ctx, store, "fake", cfg)
	if decision != Deny {
		t.Fatalf("breaker did not trip open as expected")
	}

	time.Sleep(cfg.OpenDuration + 20*time.Millisecond)

	decision, err := Check(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != AllowTrial {
		t.Errorf("Check after OpenDuration elapsed = %v, want AllowTrial (half-open)", decision)
	}

	// A second, concurrent-ish caller during the same half-open window
	// must NOT also get a trial — only one probe at a time.
	decision, err = Check(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Deny {
		t.Errorf("a second Check while a half-open trial is already in flight = %v, want Deny", decision)
	}
}

func TestSuccessfulTrialClosesTheBreaker(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
	}
	time.Sleep(cfg.OpenDuration + 20*time.Millisecond)

	decision, _ := Check(ctx, store, "fake", cfg)
	if decision != AllowTrial {
		t.Fatalf("expected AllowTrial, got %v", decision)
	}

	if err := ReportResult(ctx, store, "fake", cfg, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decision, err := Check(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Allow {
		t.Errorf("after a successful trial, Check = %v, want Allow (Closed)", decision)
	}
}

func TestFailedTrialReopensImmediately(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
	}
	time.Sleep(cfg.OpenDuration + 20*time.Millisecond)

	decision, _ := Check(ctx, store, "fake", cfg)
	if decision != AllowTrial {
		t.Fatalf("expected AllowTrial, got %v", decision)
	}

	if err := ReportResult(ctx, store, "fake", cfg, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Immediately re-open, not requiring another full threshold's worth
	// of failures.
	decision, err := Check(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Deny {
		t.Errorf("after a failed trial, Check = %v, want Deny (re-opened)", decision)
	}
}

func TestCheckFailsOpenOnStoreError(t *testing.T) {
	decision, err := Check(context.Background(), FailingStore{}, "fake", testConfig())
	if err == nil {
		t.Fatal("expected an error from FailingStore, got nil")
	}
	if decision != Allow {
		t.Errorf("Check with a failing store = %v, want Allow (fail open)", decision)
	}
}

func TestDifferentProvidersHaveIndependentBreakers(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "openai", cfg, false)
	}
	decision, _ := Check(ctx, store, "openai", cfg)
	if decision != Deny {
		t.Fatalf("openai's breaker did not trip as expected")
	}

	decision, err := Check(ctx, store, "anthropic", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision != Allow {
		t.Errorf("anthropic's breaker = %v, want Allow (independent of openai's trip)", decision)
	}
}

func TestCurrentStateReportsWithoutMutating(t *testing.T) {
	store := NewFakeStore()
	cfg := testConfig()
	ctx := context.Background()

	state, err := CurrentState(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != Closed {
		t.Errorf("fresh breaker CurrentState = %v, want Closed", state)
	}

	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = ReportResult(ctx, store, "fake", cfg, false)
	}
	state, err = CurrentState(ctx, store, "fake", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != Open {
		t.Errorf("after tripping, CurrentState = %v, want Open", state)
	}

	// Calling CurrentState repeatedly must not itself claim the half-open
	// trial the way Check does.
	time.Sleep(cfg.OpenDuration + 20*time.Millisecond)
	for i := 0; i < 3; i++ {
		state, err = CurrentState(ctx, store, "fake", cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if state != HalfOpen {
			t.Errorf("CurrentState call %d after OpenDuration elapsed = %v, want HalfOpen", i, state)
		}
	}
}
