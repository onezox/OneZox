// Package analysis is control-plane's own Kubernetes-facing boundary for
// Argo Rollouts' AnalysisRun custom resource (Phase-05 Step M) — a
// standalone, non-Rollout-owned usage of AnalysisRun: this project's
// canary traffic-shifting is Step K's own etcd envelope + weighted
// resolve(), not Argo Rollouts' native pod-traffic mechanism (there is no
// per-model-version Kubernetes workload to shift traffic toward — a
// "model" is a config row, not a Deployment). Argo Rollouts is used here
// purely for what AnalysisRun already does well on its own: run a
// Prometheus query on a schedule and report a structured Successful/
// Failed/Inconclusive verdict. `kubectl create -f analysisrun.yaml`
// against a bare AnalysisRun (no owning Rollout) is a real, documented
// Argo Rollouts usage pattern, not a misuse.
//
// Client is a narrow interface (Find/Create) — the same "fake for tests,
// real client behind a boundary" discipline this project has used for
// every other external dependency (etcd, Vault, CockroachDB).
package analysis

import (
	"context"
	"fmt"
)

// Phase mirrors Argo Rollouts' own AnalysisRun status.phase values
// verbatim — not re-interpreted or renamed, so a reader cross-referencing
// `kubectl get analysisrun` output against this code sees the same words.
type Phase string

const (
	PhasePending      Phase = "Pending"
	PhaseRunning      Phase = "Running"
	PhaseSuccessful   Phase = "Successful"
	PhaseFailed       Phase = "Failed"
	PhaseError        Phase = "Error"
	PhaseInconclusive Phase = "Inconclusive"
)

// Run is what the reconciler needs to know about one AnalysisRun — not
// the full Kubernetes object.
type Run struct {
	Name  string
	Phase Phase
}

// Client is control-plane's own AnalysisRun boundary. FindForStage/
// CreateForStage key off the SAME (rollout-id, stage) label pair the
// reconciler's own reconcile-everything pass uses — see reconciler.go.
type Client interface {
	// FindForStage returns the AnalysisRun currently associated with
	// (rolloutID, stage), or nil if none exists — at most one should
	// ever exist per (rolloutID, stage) by this package's own labeling
	// convention (CreateForStage always sets both labels, and nothing
	// else in this codebase creates AnalysisRun objects).
	FindForStage(ctx context.Context, rolloutID, stage string) (*Run, error)

	// CreateForStage starts a new AnalysisRun checking modelRef's CANARY
	// population's error rate at the given stage. The Prometheus query
	// targets the metric contract Step N's data-plane change provides:
	// a counter named data_plane_submit_total, labeled model_ref/canary/
	// status — see queryFor's own comment. Until Step N ships that
	// metric, a real AnalysisRun created here has no data to query
	// against (Argo Rollouts' own documented no-data handling applies,
	// not a bug in this package) — Step M's own live proof exercises the
	// reconciler's REACTIVE half directly (hand-created test AnalysisRuns
	// with trivial pass/fail conditions), independent of whether this
	// query is yet meaningful; O/P are what exercise this real query for
	// real, once N lands.
	CreateForStage(ctx context.Context, rolloutID, modelRef, stage string, canaryPercent int32) error
}

// queryFor is the PromQL contract Step N's metric must satisfy: canary-
// population error rate over a 2-minute window. sum(rate(...)) twice
// (errors / all) rather than a single ratio expression — Prometheus's
// own rate() needs to be applied to each counter separately before
// dividing, dividing raw counters directly would be wrong once either
// side resets (a pod restart zeroes a counter, rate() handles that
// correctly, a raw division would not).
func queryFor(modelRef string) string {
	return fmt.Sprintf(
		`sum(rate(data_plane_submit_total{model_ref=%q,canary="true",status="error"}[2m])) `+
			`/ sum(rate(data_plane_submit_total{model_ref=%q,canary="true"}[2m]))`,
		modelRef, modelRef,
	)
}
