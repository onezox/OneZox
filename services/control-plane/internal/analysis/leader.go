package analysis

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/rest"
)

// Post-M2 CRITICAL fix 2/2 — the singleton guard for the reconciler.
//
// control-plane runs 2 replicas, and every replica used to start the
// canary reconciler unconditionally (`go reconciler.Run(ctx)`), so two
// independent loops watched the same rollouts and wrote the same rows on
// the same 5-second tick. The compare-and-swap added in the previous
// commit makes the DB arbitrate stage transitions, so correctness no
// longer DEPENDS on there being one reconciler — but CAS cannot fix
// everything two loops do:
//
//   - DUPLICATE AnalysisRuns. Both replicas call CreateForStage for the
//     same stage, producing two AnalysisRun objects that query Prometheus
//     independently and can reach OPPOSITE verdicts. Whichever one
//     FindForStage happens to return next tick then decides the rollout's
//     fate. CAS cannot help: both writes are legitimate, they just
//     disagree. This is the race only leader election closes.
//   - Duplicate etcd canary-state publishes and duplicate log/audit noise.
//   - Twice the Kubernetes and Prometheus API load, for nothing.
//
// So: CAS is the correctness FLOOR (safe even if two reconcilers somehow
// run — during a lease handover, say), and leader election is the
// PRIMARY guard (only one runs at a time). Neither is redundant.
//
// WHY A KUBERNETES LEASE rather than etcd, which control-plane already
// talks to. etcd here is the model-registry distribution channel with a
// specific key layout (/onezox/manifests, /onezox/active) that data-plane
// and edge-gateway both watch; putting controller-coordination keys in
// the same store would mix two unrelated concerns in one namespace and
// give the registry watchers keys they must learn to ignore. A
// coordination.k8s.io Lease is the platform's own answer for exactly
// this, client-go implements the whole protocol including clock-skew
// handling, and control-plane already holds an in-cluster credential and
// a Kubernetes client for AnalysisRuns. No new dependency, no new
// failure domain.

const (
	// leaseName is stable across replicas and restarts — it IS the
	// identity of "the one reconciler", not of any pod.
	leaseName = "control-plane-reconciler"

	// Standard client-go ratios: LeaseDuration > RenewDeadline >
	// RetryPeriod. A leader that cannot renew within RenewDeadline stops
	// LEADING before its lease expires, so the next leader never starts
	// while the old one still believes it holds the lock — that ordering
	// is what makes "only one reconciler" true across a handover, not
	// merely likely.
	//
	// 15s/10s/2s means a crashed leader is replaced within ~15s. The
	// reconciler ticks every 5s and every action is idempotent-by-CAS, so
	// a gap of a few ticks delays a canary stage slightly and breaks
	// nothing — worth far more than a tighter window that would make
	// spurious failovers likely on a laptop-grade cluster.
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second
)

// RunWithLeaderElection blocks until ctx is done, running `run` only
// while this process holds the lease.
//
// `run` is expected to block until ITS ctx is cancelled (Reconciler.Run
// does). On losing leadership the ctx passed to `run` is cancelled, so a
// demoted replica stops reconciling promptly rather than racing the new
// leader.
func RunWithLeaderElection(
	ctx context.Context,
	cfg *rest.Config,
	namespace string,
	log *slog.Logger,
	run func(context.Context),
) error {
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("creating kubernetes clientset for leader election: %w", err)
	}
	return runWithLock(ctx, newLeaseLock(client, namespace), log, run)
}

// identity is what appears in the Lease's holderIdentity — the pod name,
// so `kubectl get lease control-plane-reconciler` names the pod that is
// actually driving rollouts. Falls back to a hostname only if the
// downward-API env var is missing; an empty identity would make the lock
// meaningless (two holders would look identical to each other).
func identity() string {
	if p := os.Getenv("POD_NAME"); p != "" {
		return p
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown-" + fmt.Sprint(os.Getpid())
}

func newLeaseLock(client kubernetes.Interface, namespace string) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity()},
	}
}

// runWithLock is split out so the election wiring can be unit-tested
// against a fake lock without a cluster.
func runWithLock(ctx context.Context, lock resourcelock.Interface, log *slog.Logger, run func(context.Context)) error {
	me := lock.Identity()
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		// ReleaseOnCancel makes a clean shutdown hand over in ~seconds
		// instead of waiting out the full lease — a rolling restart of
		// control-plane should not stall an in-flight canary.
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				log.Info("reconciler: acquired leadership, starting", "identity", me, "lease", leaseName)
				run(c)
			},
			OnStoppedLeading: func() {
				// client-go has already stopped renewing and cancelled the
				// context handed to run, so the reconcile loop is winding
				// down as this logs. Deliberately NOT fatal: this replica
				// keeps serving gRPC, it simply is not the one driving
				// rollouts, and it stays a candidate to take over later.
				log.Warn("reconciler: lost leadership, stopped reconciling", "identity", me, "lease", leaseName)
			},
			OnNewLeader: func(who string) {
				if who == me {
					return
				}
				log.Info("reconciler: standing by, another replica holds the lease",
					"identity", me, "leader", who, "lease", leaseName)
			},
		},
	})
	return ctx.Err()
}
