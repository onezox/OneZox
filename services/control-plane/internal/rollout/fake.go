package rollout

import (
	"context"
	"sync"
	"time"

	"github.com/onezox/OneZox/services/control-plane/internal/registry"
)

// FakeStore is an in-memory Store for unit tests — no CockroachDB needed.
type FakeStore struct {
	mu       sync.Mutex
	rollouts map[string]*Rollout
}

func NewFakeStore() *FakeStore {
	return &FakeStore{rollouts: make(map[string]*Rollout)}
}

func (f *FakeStore) InsertRollout(ctx context.Context, r Rollout) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := r
	f.rollouts[r.RolloutID] = &cp
	return nil
}

func (f *FakeStore) GetRollout(ctx context.Context, rolloutID string) (*Rollout, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rollouts[rolloutID]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *FakeStore) GetRunningRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rollouts {
		if r.ModelRef == modelRef && r.Status == "running" {
			cp := *r
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *FakeStore) GetMostRecentRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var most *Rollout
	for _, r := range f.rollouts {
		if r.ModelRef != modelRef {
			continue
		}
		if most == nil || r.StartedAt.After(most.StartedAt) {
			most = r
		}
	}
	if most == nil {
		return nil, nil
	}
	cp := *most
	return &cp, nil
}

func (f *FakeStore) UpdateRollout(ctx context.Context, rolloutID, stage, status string, endedAt *time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rollouts[rolloutID]
	if !ok {
		return nil
	}
	r.Stage = stage
	r.Status = status
	r.EndedAt = endedAt
	return nil
}

// FakePublisher is an in-memory CanaryPublisher for unit tests — records
// every write so a test can assert exactly what was published, without a
// real etcd.
type FakePublisher struct {
	mu    sync.Mutex
	Calls []CanaryWrite
	Err   error
}

type CanaryWrite struct {
	ModelRef        string
	StableVersionID string
	CanaryVersionID string
	Percent         int
}

func NewFakePublisher() *FakePublisher {
	return &FakePublisher{}
}

func (f *FakePublisher) PublishCanaryState(ctx context.Context, modelRef, stableVersionID, canaryVersionID string, percent int) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, CanaryWrite{
		ModelRef: modelRef, StableVersionID: stableVersionID, CanaryVersionID: canaryVersionID, Percent: percent,
	})
	return nil
}

// LastCall returns the most recent write, or the zero value if none
// happened — convenience for tests that only care about the latest state.
func (f *FakePublisher) LastCall() CanaryWrite {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Calls) == 0 {
		return CanaryWrite{}
	}
	return f.Calls[len(f.Calls)-1]
}

// FakeRegistry is an in-memory Registry for unit tests — a plain map of
// (model_ref, version_id) -> Manifest, plus a model_ref -> active
// version_id pointer that ActivateVersion mutates, letting a test assert
// promotion actually happened without depending on the real registry
// package's own store/signer machinery (already thoroughly tested on its
// own terms, Phase-04).
type FakeRegistry struct {
	mu          sync.Mutex
	manifests   map[string]*registry.Manifest // key: model_ref+"/"+version_id
	active      map[string]string             // model_ref -> version_id
	ActivateErr error
}

func NewFakeRegistry() *FakeRegistry {
	return &FakeRegistry{
		manifests: make(map[string]*registry.Manifest),
		active:    make(map[string]string),
	}
}

func (f *FakeRegistry) Seed(modelRef, versionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.manifests[modelRef+"/"+versionID] = &registry.Manifest{ModelRef: modelRef, VersionID: versionID}
}

func (f *FakeRegistry) SeedActive(modelRef, versionID string) {
	f.Seed(modelRef, versionID)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active[modelRef] = versionID
}

func (f *FakeRegistry) GetModelManifest(ctx context.Context, modelRef, versionID string) (*registry.Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if versionID == "" {
		versionID = f.active[modelRef]
		if versionID == "" {
			return nil, registry.ErrNotFound
		}
	}
	m, ok := f.manifests[modelRef+"/"+versionID]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (f *FakeRegistry) ActivateVersion(ctx context.Context, modelRef, versionID string) error {
	if f.ActivateErr != nil {
		return f.ActivateErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active[modelRef] = versionID
	return nil
}

func (f *FakeRegistry) CurrentActive(modelRef string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active[modelRef]
}
