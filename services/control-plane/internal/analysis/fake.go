package analysis

import (
	"context"
	"sync"

	"github.com/onezox/OneZox/services/control-plane/internal/rollout"
)

// FakeClient is an in-memory Client for unit tests — no real Kubernetes
// API needed. A test sets a Run's phase directly (SetPhase) to simulate
// Argo Rollouts itself completing an analysis, then asserts the
// reconciler reacted correctly on its next tick — this is what makes the
// automatic-trigger tests genuine: the test controls ONLY the AnalysisRun
// status, exactly like Argo Rollouts would, and observes whether
// RolloutDriver's methods actually got called, not just whether the
// reconciler logged something.
type FakeClient struct {
	mu   sync.Mutex
	runs map[string]*Run // key: rolloutID+"/"+stage
	// CreateCalls records every CreateForStage call, in order — lets a
	// test assert exactly which (rollout, stage) pairs got an
	// AnalysisRun, including the CHAINING behavior (each stage gets its
	// own, one at a time).
	CreateCalls []CreateCall
	CreateErr   error
	FindErr     error
}

type CreateCall struct {
	RolloutID     string
	ModelRef      string
	Stage         string
	CanaryPercent int32
}

func NewFakeClient() *FakeClient {
	return &FakeClient{runs: make(map[string]*Run)}
}

func key(rolloutID, stage string) string { return rolloutID + "/" + stage }

func (f *FakeClient) FindForStage(ctx context.Context, rolloutID, stage string) (*Run, error) {
	if f.FindErr != nil {
		return nil, f.FindErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[key(rolloutID, stage)]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *FakeClient) CreateForStage(ctx context.Context, rolloutID, modelRef, stage string, canaryPercent int32) error {
	if f.CreateErr != nil {
		return f.CreateErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CreateCalls = append(f.CreateCalls, CreateCall{RolloutID: rolloutID, ModelRef: modelRef, Stage: stage, CanaryPercent: canaryPercent})
	f.runs[key(rolloutID, stage)] = &Run{Name: "fake-run-" + key(rolloutID, stage), Phase: PhasePending}
	return nil
}

// SetPhase simulates Argo Rollouts itself completing (or progressing)
// the AnalysisRun for (rolloutID, stage) — the ONLY way a test drives the
// reconciler's automatic reaction, mirroring exactly how the real
// Kubernetes API would report a real AnalysisRun's own status change.
func (f *FakeClient) SetPhase(rolloutID, stage string, phase Phase) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.runs[key(rolloutID, stage)]; ok {
		r.Phase = phase
	}
}

// FakeDriver is an in-memory RolloutDriver for unit tests — wraps a real
// rollout.Service backed by rollout's own FakeStore/FakePublisher/
// FakeRegistry, so these tests exercise the REAL advanceStage/
// revertCanary logic (Step L, already thoroughly tested on its own
// terms) through the reconciler's own automatic entry points, not a
// second, parallel simulation of what those functions do.
type FakeDriver struct {
	svc *rollout.Service
	// AutoAdvanceCalls/AutoRollbackCalls record every call the reconciler
	// made — lets a test assert the TRIGGER fired, not just that the
	// underlying state ended up correct (which svc's own Step L tests
	// already cover exhaustively).
	mu                sync.Mutex
	AutoAdvanceCalls  []string
	AutoRollbackCalls []string
}

func NewFakeDriver(svc *rollout.Service) *FakeDriver {
	return &FakeDriver{svc: svc}
}

func (f *FakeDriver) ListRunningRollouts(ctx context.Context) ([]rollout.Rollout, error) {
	return f.svc.ListRunningRollouts(ctx)
}

func (f *FakeDriver) AutoAdvance(ctx context.Context, rolloutID, fromStage string) (string, error) {
	f.mu.Lock()
	f.AutoAdvanceCalls = append(f.AutoAdvanceCalls, rolloutID)
	f.mu.Unlock()
	return f.svc.AutoAdvance(ctx, rolloutID, fromStage)
}

func (f *FakeDriver) AutoRollback(ctx context.Context, rolloutID, fromStage string) error {
	f.mu.Lock()
	f.AutoRollbackCalls = append(f.AutoRollbackCalls, rolloutID)
	f.mu.Unlock()
	return f.svc.AutoRollback(ctx, rolloutID, fromStage)
}
