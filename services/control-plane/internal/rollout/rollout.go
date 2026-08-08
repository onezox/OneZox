// Package rollout implements the Phase-05 staged-canary state machine —
// real logic this time, replacing the Phase-04 doc.go-only scaffold
// (this file's own predecessor's comment: "canary controller and Argo
// Rollouts wiring are Phase-05 scope").
//
// This is the SANCTIONED, SOLE path that ever writes canary_percent (the
// etcd envelope Step K introduced) — every one of CreateRollout/
// PromoteRollout/AbortRollout is a real mutation with a real audit_log
// row at the RPC layer (server.go), and none of them pokes etcd directly
// from outside this package. GetRolloutStatus is the one read-only
// method (Step D's own commands-vs-queries split, extended here: within
// control-plane, the same split holds — this package writes through
// exactly one channel and reads through a separate one).
//
// ONE implementation of "advance a stage," TWO triggers (the Phase-05
// plan's own Decision 3 refinement, Step E): advanceStage is called by
// PromoteRollout (human, RBAC-guarded at admin-api, audited) and — once
// Step M exists — by an in-process reconciler reacting to Argo Rollouts'
// own CRD status, never reachable over the network at all. Same shape
// for reverting: revertCanary backs both AbortRollout (human) and a
// future automatic rollback (Step M/P, on a failed AnalysisRun),
// distinguished only by the status value recorded ("aborted" vs
// "rolled_back") — the underlying etcd write is identical either way.
//
// Promotion (a canary reaching its final stage) reuses registry.Service's
// own ActivateVersion — the EXACT bootstrap-activation path
// RegisterModelManifest already uses (Step H's own registry.go refactor)
// — rather than a second, parallel "make this version live" write path.
// There is exactly one way anything ever becomes the stable pointer in
// this codebase, regardless of which RPC triggered it.
package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/onezox/OneZox/services/control-plane/internal/registry"
)

var (
	ErrNotFound             = errors.New("rollout not found")
	ErrNotRunning           = errors.New("rollout is not running")
	ErrAlreadyRunning       = errors.New("model_ref already has a running rollout")
	ErrNoActiveVersion      = errors.New("model_ref has no active version to roll out against")
	ErrAlreadyFullyPromoted = errors.New("rollout is already fully promoted")
)

// stageOrder is the ENTIRE staged sequence, fixed — matching
// data/migrations/0016's own stage_is_known CHECK constraint exactly.
// Not configurable per-rollout: strategy_json (below) is stored for a
// future Argo Rollouts AnalysisTemplate to read (Step M), it does not
// alter this sequence. No stage-skip is possible by construction: the
// only way to reach a given stage is via nextStage from its immediate
// predecessor, which is also this package's own EC4 contribution — a
// crafted "jump to stable" input has nowhere to plug in, since nothing
// here accepts a target stage as a parameter at all.
var stageOrder = []string{"pending", "canary_1", "canary_10", "canary_50", "stable"}

var stagePercent = map[string]int{
	"pending": 0, "canary_1": 1, "canary_10": 10, "canary_50": 50, "stable": 100,
}

// StagePercent exposes the same fixed mapping for the RPC layer
// (server.go's GetRolloutStatus handler) to report canary_percent
// without duplicating this table.
func StagePercent(stage string) int32 {
	return int32(stagePercent[stage])
}

func nextStage(current string) (string, bool) {
	for i, s := range stageOrder {
		if s == current {
			if i+1 < len(stageOrder) {
				return stageOrder[i+1], true
			}
			return "", false
		}
	}
	return "", false
}

// Rollout mirrors the rollout table row (data/migrations/0016, 0019)
// field-for-field.
type Rollout struct {
	RolloutID       string
	ModelRef        string
	VersionID       string
	StrategyJSON    string
	Stage           string
	Status          string
	StableVersionID string
	StartedAt       time.Time
	EndedAt         *time.Time
	// StageEnteredAt is when the CURRENT stage began (distinct from
	// StartedAt, the whole rollout's start) — data/migrations/0021, Step
	// O fix. UpdateRollout refreshes it on every write; the reconciler
	// uses it to withhold a stage's AnalysisRun until real traffic has
	// had a chance to land, see analysis.stageGracePeriod.
	StageEnteredAt time.Time
}

// Store is rollout's own persistence boundary — CockroachStore
// (cockroach_store.go) in production, FakeStore (fake.go) in tests.
type Store interface {
	InsertRollout(ctx context.Context, r Rollout) error
	GetRollout(ctx context.Context, rolloutID string) (*Rollout, error)
	GetRunningRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error)
	GetMostRecentRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error)
	// ListRunningRollouts is Step M's own addition — the in-process
	// reconciler's restart re-attach depends on it: on boot (and on every
	// poll tick thereafter), it lists EVERY currently-running rollout
	// from the database itself, not from any in-memory list the process
	// might have lost across a restart. This is what makes re-attach a
	// property of the reconcile loop's own normal operation, not a
	// special-cased "recovery" code path run once at startup.
	ListRunningRollouts(ctx context.Context) ([]Rollout, error)
	UpdateRollout(ctx context.Context, rolloutID, stage, status string, endedAt *time.Time) error
}

// CanaryPublisher is the ONLY etcd write this package issues directly —
// intermediate canary stages (percent between 0 and 100 exclusive of the
// final promotion, which goes through registry.Service.ActivateVersion
// instead, see Registry below).
type CanaryPublisher interface {
	PublishCanaryState(ctx context.Context, modelRef, stableVersionID, canaryVersionID string, percent int) error
}

// Registry is the narrow slice of registry.Service this package depends
// on — GetModelManifest for validating a target version genuinely exists
// and independently re-verifies (defense in depth: never start a canary
// against a manifest whose signature control-plane's own read path would
// itself refuse to serve), ActivateVersion for the one, shared "make this
// version live" operation (see the package doc).
type Registry interface {
	GetModelManifest(ctx context.Context, modelRef, versionID string) (*registry.Manifest, error)
	ActivateVersion(ctx context.Context, modelRef, versionID string) error
}

type Service struct {
	store     Store
	publisher CanaryPublisher
	registry  Registry
	log       *slog.Logger
}

func NewService(store Store, publisher CanaryPublisher, reg Registry, log *slog.Logger) *Service {
	return &Service{store: store, publisher: publisher, registry: reg, log: log}
}

// CreateRollout validates the target version and starts the canary
// immediately (pending -> canary_1, in the same call) — "CreateRollout
// starts a canary," not "CreateRollout registers an intent that some
// other action later begins." No target-stage or percent parameter
// exists anywhere in this signature or admin.proto's own
// CreateRolloutRequest — a rollout can only ever begin at the first
// staged step.
func (s *Service) CreateRollout(ctx context.Context, modelRef, versionID, strategyJSON string) (string, error) {
	if !json.Valid([]byte(strategyJSON)) {
		return "", fmt.Errorf("strategy_json is not valid JSON")
	}

	// Target version must genuinely exist and independently re-verify —
	// never start a canary against an unverified or nonexistent manifest.
	if _, err := s.registry.GetModelManifest(ctx, modelRef, versionID); err != nil {
		return "", fmt.Errorf("target version invalid: %w", err)
	}

	// Current stable must exist (the bootstrap case, Step H) — a
	// model_ref with no live version yet has nothing to canary AGAINST.
	activeManifest, err := s.registry.GetModelManifest(ctx, modelRef, "")
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNoActiveVersion, err)
	}

	running, err := s.store.GetRunningRolloutByModelRef(ctx, modelRef)
	if err != nil {
		return "", fmt.Errorf("checking for an existing rollout: %w", err)
	}
	if running != nil {
		return "", ErrAlreadyRunning
	}

	rolloutID := uuid.NewString()
	r := Rollout{
		RolloutID:       rolloutID,
		ModelRef:        modelRef,
		VersionID:       versionID,
		StrategyJSON:    strategyJSON,
		Stage:           "pending",
		Status:          "running",
		StableVersionID: activeManifest.VersionID,
		StartedAt:       time.Now().UTC(),
	}
	if err := s.store.InsertRollout(ctx, r); err != nil {
		return "", fmt.Errorf("inserting rollout: %w", err)
	}

	if _, err := s.advanceStage(ctx, rolloutID); err != nil {
		return "", fmt.Errorf("starting canary: %w", err)
	}

	s.log.Info("rollout created", "rollout_id", rolloutID, "model_ref", modelRef,
		"version_id", versionID, "stable_version_id", activeManifest.VersionID)
	return rolloutID, nil
}

// PromoteRollout is the human "don't wait out the pause" override —
// admin-api's own RBAC-guarded RPC (Step L). Advances exactly one staged
// step via the SAME advanceStage the automatic reconciler will call
// (Step M) — there is no parameter here or anywhere upstream (admin.proto)
// that could request a different stage.
func (s *Service) PromoteRollout(ctx context.Context, rolloutID string) (string, error) {
	return s.advanceStage(ctx, rolloutID)
}

// AbortRollout is the human manual-rollback override. Reverts to
// whatever was stable before this rollout began — never re-derived,
// always r.StableVersionID (data/migrations/0019).
func (s *Service) AbortRollout(ctx context.Context, rolloutID string) error {
	return s.revertCanary(ctx, rolloutID, "aborted")
}

// AutoAdvance is the in-process reconciler's own trigger (Step M) —
// called when an AnalysisRun for the current stage reports Successful.
// Behaviorally IDENTICAL to PromoteRollout (both call the exact same
// advanceStage): this is a distinct exported name purely so the
// reconciler's own call site reads correctly ("the automatic path
// advanced this," not "a human promoted this"), never a second
// implementation. See the package doc's own "one implementation, two
// triggers" framing.
func (s *Service) AutoAdvance(ctx context.Context, rolloutID string) (string, error) {
	return s.advanceStage(ctx, rolloutID)
}

// AutoRollback is the in-process reconciler's own trigger — called when
// an AnalysisRun for the current stage reports Failed/Error. Same
// revertCanary AbortRollout uses, recorded with status="rolled_back"
// instead of "aborted" so an operator reading rollout.status (or
// audit_log — though this path itself is NEVER audited, see the
// reconciler's own doc comment) can tell "the system caught a regression
// and reverted" apart from "a human cancelled this."
func (s *Service) AutoRollback(ctx context.Context, rolloutID string) error {
	return s.revertCanary(ctx, rolloutID, "rolled_back")
}

// ListRunningRollouts backs the reconciler's own reconcile-everything
// pass (Step M) — every rollout currently in status="running", freshly
// read from the database on every call, never cached across calls or
// process restarts.
func (s *Service) ListRunningRollouts(ctx context.Context) ([]Rollout, error) {
	return s.store.ListRunningRollouts(ctx)
}

// GetRolloutStatus is read-only — the query-side counterpart within
// control-plane's own internal split, mirroring admin.graphql's
// commands-vs-queries design at Step D. rolloutID takes priority; an
// empty rolloutID resolves modelRef's own most recent rollout
// (regardless of status — a finished rollout's own final state is a
// legitimate answer to "what happened to modelRef's last rollout").
func (s *Service) GetRolloutStatus(ctx context.Context, rolloutID, modelRef string) (*Rollout, error) {
	if rolloutID != "" {
		r, err := s.store.GetRollout(ctx, rolloutID)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, ErrNotFound
		}
		return r, nil
	}
	if modelRef == "" {
		return nil, fmt.Errorf("either rollout_id or model_ref is required")
	}
	r, err := s.store.GetMostRecentRolloutByModelRef(ctx, modelRef)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ErrNotFound
	}
	return r, nil
}

// advanceStage is THE one implementation of "move this rollout to its
// next staged step" — see the package doc for why both the human
// (PromoteRollout) and automatic (Step M reconciler) triggers share it
// rather than each having their own.
func (s *Service) advanceStage(ctx context.Context, rolloutID string) (string, error) {
	r, err := s.store.GetRollout(ctx, rolloutID)
	if err != nil {
		return "", err
	}
	if r == nil {
		return "", ErrNotFound
	}
	if r.Status != "running" {
		// This is the error a caller actually observes for "already fully
		// promoted" in normal operation: reaching stage="stable" always
		// sets status="promoted" in the SAME UpdateRollout call (below),
		// so a real, successfully-persisted rollout never exists in the
		// state stage=stable/status=running for the nextStage check below
		// to ever catch. ErrNotRunning covers every terminal case
		// uniformly (promoted, aborted, rolled_back) — a caller doesn't
		// need three different errors to learn "this rollout is over."
		return "", ErrNotRunning
	}

	next, ok := nextStage(r.Stage)
	if !ok {
		// Defensive only, not reachable via any normal call path (see
		// above) — kept as a fail-closed guard against a data-integrity
		// edge case (e.g. a partial write that persisted stage="stable"
		// without also persisting status="promoted"), not as the signal
		// for the everyday "already promoted" case.
		return "", ErrAlreadyFullyPromoted
	}

	if next == "stable" {
		// Promotion: the canary version becomes the new stable. Reuses
		// registry.Service.ActivateVersion — see package doc: exactly one
		// "make this version live" path exists in this codebase.
		if err := s.registry.ActivateVersion(ctx, r.ModelRef, r.VersionID); err != nil {
			return "", fmt.Errorf("activating promoted version: %w", err)
		}
		endedAt := time.Now().UTC()
		if err := s.store.UpdateRollout(ctx, rolloutID, next, "promoted", &endedAt); err != nil {
			return "", fmt.Errorf("recording promotion: %w", err)
		}
		s.log.Info("rollout promoted to stable", "rollout_id", rolloutID,
			"model_ref", r.ModelRef, "version_id", r.VersionID)
		return next, nil
	}

	// Intermediate stage: canary still in progress, stable UNCHANGED —
	// r.StableVersionID, captured once at CreateRollout, never re-derived.
	if err := s.publisher.PublishCanaryState(ctx, r.ModelRef, r.StableVersionID, r.VersionID, stagePercent[next]); err != nil {
		return "", fmt.Errorf("publishing canary state: %w", err)
	}
	if err := s.store.UpdateRollout(ctx, rolloutID, next, "running", nil); err != nil {
		return "", fmt.Errorf("recording stage advance: %w", err)
	}
	s.log.Info("rollout advanced", "rollout_id", rolloutID, "model_ref", r.ModelRef,
		"stage", next, "canary_percent", stagePercent[next])
	return next, nil
}

// revertCanary is THE one implementation of "cancel this rollout's
// canary and go back to what was stable before it" — AbortRollout
// (human, resultStatus="aborted") and a future automatic rollback
// (Step M/P, on a failed AnalysisRun, resultStatus="rolled_back") both
// call this; only the recorded outcome differs, the etcd write is
// identical either way.
func (s *Service) revertCanary(ctx context.Context, rolloutID, resultStatus string) error {
	r, err := s.store.GetRollout(ctx, rolloutID)
	if err != nil {
		return err
	}
	if r == nil {
		return ErrNotFound
	}
	if r.Status != "running" {
		return ErrNotRunning
	}

	if err := s.publisher.PublishCanaryState(ctx, r.ModelRef, r.StableVersionID, "", 0); err != nil {
		return fmt.Errorf("reverting canary state: %w", err)
	}
	endedAt := time.Now().UTC()
	if err := s.store.UpdateRollout(ctx, rolloutID, r.Stage, resultStatus, &endedAt); err != nil {
		return fmt.Errorf("recording revert: %w", err)
	}
	s.log.Info("rollout reverted", "rollout_id", rolloutID, "model_ref", r.ModelRef, "result_status", resultStatus)
	return nil
}
