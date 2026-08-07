package rollout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func testService() (*Service, *FakeStore, *FakePublisher, *FakeRegistry) {
	store := NewFakeStore()
	publisher := NewFakePublisher()
	reg := NewFakeRegistry()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(store, publisher, reg, log), store, publisher, reg
}

func TestCreateRolloutStartsAtCanary1(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if err != nil {
		t.Fatalf("CreateRollout: %v", err)
	}
	if rolloutID == "" {
		t.Fatal("expected a non-empty rollout_id")
	}

	// CreateRollout STARTS the canary in the same call — the first
	// publisher write must already be at 1%, not sitting at 0/pending.
	last := publisher.LastCall()
	if last.ModelRef != "openai" || last.StableVersionID != "stable-v1" ||
		last.CanaryVersionID != "canary-v2" || last.Percent != 1 {
		t.Fatalf("unexpected first canary write: %+v", last)
	}

	status, err := svc.GetRolloutStatus(ctx, rolloutID, "")
	if err != nil {
		t.Fatalf("GetRolloutStatus: %v", err)
	}
	if status.Stage != "canary_1" || status.Status != "running" {
		t.Fatalf("stage=%q status=%q, want canary_1/running", status.Stage, status.Status)
	}

	// stable must NOT have moved — CockroachDB's own model_active
	// equivalent (FakeRegistry.active) is untouched during an
	// in-progress canary.
	if reg.CurrentActive("openai") != "stable-v1" {
		t.Fatalf("active version changed to %q, want unchanged stable-v1", reg.CurrentActive("openai"))
	}
}

func TestCreateRolloutRejectsUnknownTargetVersion(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	// "canary-v2" never seeded — doesn't exist.

	if _, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`); err == nil {
		t.Fatal("expected an error for a target version that doesn't exist")
	}
}

func TestCreateRolloutRejectsModelRefWithNoActiveVersion(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	// No SeedActive call at all — "openai" has never been bootstrapped.
	reg.Seed("openai", "canary-v2")

	_, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("err = %v, want ErrNoActiveVersion", err)
	}
}

func TestCreateRolloutRejectsConcurrentRollout(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")
	reg.Seed("openai", "canary-v3")

	if _, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`); err != nil {
		t.Fatalf("first CreateRollout: %v", err)
	}

	_, err := svc.CreateRollout(ctx, "openai", "canary-v3", `{}`)
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("err = %v, want ErrAlreadyRunning", err)
	}
}

func TestCreateRolloutRejectsInvalidStrategyJSON(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	if _, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{not valid`); err == nil {
		t.Fatal("expected an error for invalid strategy_json")
	}
}

// TestPromoteRolloutAdvancesThroughEveryStage is the core state-machine
// proof: canary_1 -> canary_10 -> canary_50 -> stable, one PromoteRollout
// call per step, no skipping — PromoteRollout has no parameter that could
// request anything but "the next one."
func TestPromoteRolloutAdvancesThroughEveryStage(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, err := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if err != nil {
		t.Fatalf("CreateRollout: %v", err)
	}

	wantStages := []struct {
		stage   string
		percent int
	}{
		{"canary_10", 10},
		{"canary_50", 50},
	}
	for _, want := range wantStages {
		newStage, err := svc.PromoteRollout(ctx, rolloutID)
		if err != nil {
			t.Fatalf("PromoteRollout to %s: %v", want.stage, err)
		}
		if newStage != want.stage {
			t.Fatalf("newStage = %q, want %q", newStage, want.stage)
		}
		last := publisher.LastCall()
		if last.Percent != want.percent || last.CanaryVersionID != "canary-v2" || last.StableVersionID != "stable-v1" {
			t.Fatalf("publisher write at %s = %+v, want percent=%d canary=canary-v2 stable=stable-v1", want.stage, last, want.percent)
		}
		if reg.CurrentActive("openai") != "stable-v1" {
			t.Fatalf("active version changed mid-canary at stage %s: %q", want.stage, reg.CurrentActive("openai"))
		}
	}

	// Final promotion: canary_50 -> stable. This is the one transition
	// that activates the version for real (FakeRegistry.active), not
	// just another canary-percent write.
	finalStage, err := svc.PromoteRollout(ctx, rolloutID)
	if err != nil {
		t.Fatalf("final PromoteRollout: %v", err)
	}
	if finalStage != "stable" {
		t.Fatalf("finalStage = %q, want stable", finalStage)
	}
	if reg.CurrentActive("openai") != "canary-v2" {
		t.Fatalf("active version after promotion = %q, want canary-v2", reg.CurrentActive("openai"))
	}

	status, err := svc.GetRolloutStatus(ctx, rolloutID, "")
	if err != nil {
		t.Fatalf("GetRolloutStatus: %v", err)
	}
	if status.Status != "promoted" || status.EndedAt == nil {
		t.Fatalf("status=%q endedAt=%v, want promoted with a real end time", status.Status, status.EndedAt)
	}
}

// TestPromoteRolloutPastStableFails: once a rollout reaches "stable" its
// status flips to "promoted" in the same write — so the REAL error a
// caller sees for "already fully promoted" is ErrNotRunning (the status
// check runs before the stage check ever could observe stage="stable"
// with status still "running"). ErrAlreadyFullyPromoted exists as a
// defensive fallback for a data-integrity edge case that normal
// operation never produces — see advanceStage's own comment.
func TestPromoteRolloutPastStableFails(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, _ := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	for range 3 { // canary_1 -> canary_10 -> canary_50 -> stable
		if _, err := svc.PromoteRollout(ctx, rolloutID); err != nil {
			t.Fatalf("PromoteRollout: %v", err)
		}
	}

	_, err := svc.PromoteRollout(ctx, rolloutID)
	if !errors.Is(err, ErrNotRunning) {
		t.Fatalf("err = %v, want ErrNotRunning", err)
	}
}

func TestPromoteRolloutUnknownIDFails(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _ := testService()

	_, err := svc.PromoteRollout(ctx, "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestAbortRolloutRevertsToOriginalStable is EC2/EC4's own core
// mechanism proof at the unit level: aborting mid-canary must revert to
// EXACTLY what was stable before the rollout began, with canary cleared
// — never touching CockroachDB's own active pointer, since a canary in
// progress never did either.
func TestAbortRolloutRevertsToOriginalStable(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, _ := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if _, err := svc.PromoteRollout(ctx, rolloutID); err != nil { // -> canary_10
		t.Fatalf("PromoteRollout: %v", err)
	}

	if err := svc.AbortRollout(ctx, rolloutID); err != nil {
		t.Fatalf("AbortRollout: %v", err)
	}

	last := publisher.LastCall()
	if last.StableVersionID != "stable-v1" || last.CanaryVersionID != "" || last.Percent != 0 {
		t.Fatalf("revert write = %+v, want stable=stable-v1 canary=\"\" percent=0", last)
	}
	if reg.CurrentActive("openai") != "stable-v1" {
		t.Fatalf("active version = %q, want unchanged stable-v1 (abort never touches it)", reg.CurrentActive("openai"))
	}

	status, err := svc.GetRolloutStatus(ctx, rolloutID, "")
	if err != nil {
		t.Fatalf("GetRolloutStatus: %v", err)
	}
	if status.Status != "aborted" || status.EndedAt == nil {
		t.Fatalf("status=%q endedAt=%v, want aborted with a real end time", status.Status, status.EndedAt)
	}
	// Stage is left exactly where it was when aborted — an honest record
	// of "got this far before being cancelled," not reset to pending.
	if status.Stage != "canary_10" {
		t.Fatalf("stage=%q, want canary_10 (left as-is, not reset)", status.Stage)
	}
}

func TestAbortRolloutAlreadyTerminalFails(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, _ := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)
	if err := svc.AbortRollout(ctx, rolloutID); err != nil {
		t.Fatalf("first AbortRollout: %v", err)
	}

	err := svc.AbortRollout(ctx, rolloutID)
	if !errors.Is(err, ErrNotRunning) {
		t.Fatalf("err = %v, want ErrNotRunning", err)
	}
}

func TestGetRolloutStatusByModelRefResolvesMostRecent(t *testing.T) {
	ctx := context.Background()
	svc, _, _, reg := testService()
	reg.SeedActive("openai", "stable-v1")
	reg.Seed("openai", "canary-v2")

	rolloutID, _ := svc.CreateRollout(ctx, "openai", "canary-v2", `{}`)

	status, err := svc.GetRolloutStatus(ctx, "", "openai")
	if err != nil {
		t.Fatalf("GetRolloutStatus by model_ref: %v", err)
	}
	if status.RolloutID != rolloutID {
		t.Fatalf("rollout_id = %q, want %q", status.RolloutID, rolloutID)
	}
}

func TestGetRolloutStatusNotFound(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _ := testService()

	if _, err := svc.GetRolloutStatus(ctx, "does-not-exist", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if _, err := svc.GetRolloutStatus(ctx, "", "no-such-model"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestStagePercentMapping(t *testing.T) {
	cases := map[string]int32{
		"pending": 0, "canary_1": 1, "canary_10": 10, "canary_50": 50, "stable": 100,
	}
	for stage, want := range cases {
		if got := StagePercent(stage); got != want {
			t.Errorf("StagePercent(%q) = %d, want %d", stage, got, want)
		}
	}
}
