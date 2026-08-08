package analysis

import (
	"context"
	"log/slog"
	"time"

	"github.com/onezox/OneZox/services/control-plane/internal/rollout"
)

// RolloutDriver is the narrow slice of rollout.Service this package
// depends on. AutoAdvance/AutoRollback are the automatic-trigger names
// (rollout.go's own doc comment) — same underlying advanceStage/
// revertCanary functions PromoteRollout/AbortRollout already call, one
// implementation shared by both the human RPC path (Step L) and this
// automatic path, never a second one.
type RolloutDriver interface {
	ListRunningRollouts(ctx context.Context) ([]rollout.Rollout, error)
	AutoAdvance(ctx context.Context, rolloutID string) (string, error)
	AutoRollback(ctx context.Context, rolloutID string) error
}

// Reconciler is control-plane's in-process, network-UNREACHABLE
// automatic driver (the Phase-05 plan's own Decision 3 refinement,
// carried through Steps L/M): a plain background goroutine main.go
// spawns via `go reconciler.Run(ctx)`, calling RolloutDriver's exported
// Go methods directly — no gRPC, no HTTP, no listener, nothing any
// CiliumNetworkPolicy could even name. There is no code path from
// outside this process into AutoAdvance/AutoRollback at all; the only
// way either ever fires is this loop's own observation of a real
// AnalysisRun's status.
//
// Deliberately POLLING, not a Kubernetes watch/informer: every tick,
// reconcileAll re-lists EVERY currently-running rollout from the
// database (rollout.Service.ListRunningRollouts) and, for each, checks
// current AnalysisRun state fresh from the Kubernetes API — never from
// any in-memory list carried across ticks or process restarts. This is
// what makes restart re-attach a property of the loop's own normal
// operation rather than a separate "recovery" code path: the very first
// reconcileAll call after a fresh boot does exactly the same thing every
// subsequent tick does, so a rollout stuck at canary_10 when control-
// plane restarts is discovered, and resumed, on the very next tick after
// the new process comes up — never orphaned waiting for a watch stream
// that was never re-established.
type Reconciler struct {
	driver     RolloutDriver
	runs       Client
	log        *slog.Logger
	interval   time.Duration
	stageGrace time.Duration
}

// stageGracePeriod is how long a stage must have been active (Rollout.
// StageEnteredAt, data/migrations/0021) before the reconciler will
// create its AnalysisRun. Live-caught during Step O's healthy-canary
// proof: with no grace period, the AnalysisRun's own single measurement
// fired within ~1 second of the stage starting — before any canary-
// labeled request could possibly exist yet, making the query a
// deterministic 0/0 (NaN) regardless of the canary's true health, every
// time, for every stage. 30s covers at least two Prometheus scrape
// intervals (15s default) plus buffer for real traffic to land, so the
// eventual single measurement reflects genuine canary behavior instead
// of a structural race.
const stageGracePeriod = 30 * time.Second

func NewReconciler(driver RolloutDriver, runs Client, log *slog.Logger) *Reconciler {
	return &Reconciler{driver: driver, runs: runs, log: log, interval: 5 * time.Second, stageGrace: stageGracePeriod}
}

// Run blocks until ctx is cancelled. Calls reconcileAll immediately, on
// entry, before ever waiting on the ticker — this is the restart-
// re-attach guarantee made concrete: a rollout already in flight when
// this process starts gets its first reconcile pass within the same
// call, not up to `interval` later.
func (r *Reconciler) Run(ctx context.Context) {
	r.log.Info("reconciler: starting", "interval", r.interval)
	r.reconcileAll(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.log.Info("reconciler: stopping")
			return
		case <-ticker.C:
			r.reconcileAll(ctx)
		}
	}
}

func (r *Reconciler) reconcileAll(ctx context.Context) {
	running, err := r.driver.ListRunningRollouts(ctx)
	if err != nil {
		r.log.Error("reconciler: failed to list running rollouts", "error", err)
		return
	}
	for _, ro := range running {
		r.reconcileOne(ctx, ro)
	}
}

// reconcileOne is idempotent and side-effect-free when there is nothing
// to do — safe to call every tick for every running rollout, forever,
// which is exactly what it is called as.
func (r *Reconciler) reconcileOne(ctx context.Context, ro rollout.Rollout) {
	if ro.Stage == "stable" {
		// Defensive only — reaching "stable" always sets status="promoted"
		// in the same write (rollout.go's own advanceStage), so
		// ListRunningRollouts (status="running") should never return one
		// of these. Not treated as an error, just skipped.
		return
	}

	run, err := r.runs.FindForStage(ctx, ro.RolloutID, ro.Stage)
	if err != nil {
		r.log.Error("reconciler: failed to look up AnalysisRun", "rollout_id", ro.RolloutID, "stage", ro.Stage, "error", err)
		return
	}

	if run == nil {
		// Withhold creation until the stage has been active for at least
		// stageGrace (data/migrations/0021's stage_entered_at) — see the
		// constant's own doc comment. Not an error, just not yet time;
		// the next tick re-checks the same DB-persisted timestamp, so
		// this survives a restart exactly like everything else here.
		if time.Since(ro.StageEnteredAt) < r.stageGrace {
			return
		}
		// No analysis in flight for this stage. Covers three cases with
		// the SAME code path, deliberately: a rollout that just entered
		// this stage and has no AnalysisRun yet; a previous tick that
		// created one but crashed/errored before this tick could see it
		// (CreateForStage is safe to retry — a stray duplicate is
		// harmless, FindForStage already tolerates it); and a rollout
		// this process is seeing for the very first time after a restart
		// mid-canary. There is no separate "am I recovering from a
		// restart" branch — this IS what recovering looks like, because
		// reconcileAll always starts from ListRunningRollouts's own
		// fresh database read, never a resumed in-memory list.
		if err := r.runs.CreateForStage(ctx, ro.RolloutID, ro.ModelRef, ro.Stage, rollout.StagePercent(ro.Stage)); err != nil {
			r.log.Error("reconciler: failed to create AnalysisRun", "rollout_id", ro.RolloutID, "stage", ro.Stage, "error", err)
			return
		}
		r.log.Info("reconciler: created AnalysisRun", "rollout_id", ro.RolloutID, "model_ref", ro.ModelRef, "stage", ro.Stage)
		return
	}

	switch run.Phase {
	case PhaseSuccessful:
		newStage, err := r.driver.AutoAdvance(ctx, ro.RolloutID)
		if err != nil {
			r.log.Error("reconciler: AutoAdvance failed", "rollout_id", ro.RolloutID, "stage", ro.Stage, "error", err)
			return
		}
		r.log.Info("reconciler: advanced rollout automatically", "rollout_id", ro.RolloutID,
			"model_ref", ro.ModelRef, "from_stage", ro.Stage, "new_stage", newStage, "analysis_run", run.Name)
		// The next stage's own AnalysisRun is created on the NEXT
		// reconcileAll tick (the run==nil branch above, now reading the
		// just-updated stage) — no recursion needed here.

	case PhaseFailed, PhaseError, PhaseInconclusive:
		// Inconclusive shares AutoRollback with Failed/Error rather than
		// "still waiting," deliberately: with this package's own count:1
		// AnalysisRun spec, Inconclusive is Argo Rollouts' own TERMINAL
		// verdict (status.completedAt is already set) — the same object
		// never re-evaluates and never transitions to Successful on its
		// own. Grouping it with Pending/Running below would silently
		// orphan the rollout at this stage forever (live-caught, not
		// hypothetical): a thin/zero-traffic canary window makes the
		// AnalysisTemplate's ratio a genuine 0/0 (no canary requests
		// landed during the query's rate() window yet), which is NaN —
		// neither successCondition nor failureCondition matches NaN, so
		// Argo terminalizes Inconclusive instead of Successful/Failed. An
		// ambiguous verdict gets the same fail-safe treatment the
		// consecutiveErrorLimit fix already established for a query that
		// can't get data at all: don't promote an unvetted canary to more
		// live traffic on an uncertain signal, and don't hang forever
		// either — revert to known-good stable.
		if err := r.driver.AutoRollback(ctx, ro.RolloutID); err != nil {
			r.log.Error("reconciler: AutoRollback failed", "rollout_id", ro.RolloutID, "stage", ro.Stage, "error", err)
			return
		}
		r.log.Warn("reconciler: rolled back rollout automatically", "rollout_id", ro.RolloutID,
			"model_ref", ro.ModelRef, "stage", ro.Stage, "analysis_run", run.Name, "analysis_phase", run.Phase)

	case PhasePending, PhaseRunning:
		// Still waiting — nothing to do until the next tick observes a
		// terminal phase.
	}
}
