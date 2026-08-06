package registry

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync"
	"time"
)

// FakeStore is an in-memory Store for unit tests — no CockroachDB needed.
type FakeStore struct {
	mu        sync.Mutex
	manifests map[string]*Manifest // version_id -> manifest
	active    map[string]string    // model_ref -> version_id
}

func NewFakeStore() *FakeStore {
	return &FakeStore{
		manifests: make(map[string]*Manifest),
		active:    make(map[string]string),
	}
}

func (f *FakeStore) InsertManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy string, createdAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.manifests[versionID]; exists {
		return fmt.Errorf("version_id collision: %s", versionID)
	}
	f.manifests[versionID] = &Manifest{
		VersionID: versionID,
		ModelRef:  modelRef,
		SpecJSON:  specJSON,
		Signature: signature,
		CreatedBy: createdBy,
		CreatedAt: createdAt,
		Status:    "published",
	}
	return nil
}

func (f *FakeStore) SetActive(ctx context.Context, modelRef, versionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active[modelRef] = versionID
	return nil
}

func (f *FakeStore) GetManifestByVersion(ctx context.Context, versionID string) (*Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.manifests[versionID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (f *FakeStore) GetActiveManifest(ctx context.Context, modelRef string) (*Manifest, error) {
	f.mu.Lock()
	versionID, ok := f.active[modelRef]
	f.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return f.GetManifestByVersion(ctx, versionID)
}

func (f *FakeStore) ListActive(ctx context.Context) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries := make([]Entry, 0, len(f.active))
	for modelRef, versionID := range f.active {
		entries = append(entries, Entry{ModelRef: modelRef, ActiveVersionID: versionID})
	}
	return entries, nil
}

// TamperManifest directly mutates a stored manifest's fields, bypassing
// InsertManifest entirely — simulates a direct-SQL bypass of the
// app-level "always sign at RegisterModelManifest" path, for adversarial
// signature tests (Step G): a row that exists with a missing or altered
// signature/content, the way a direct SQL INSERT against model_manifest
// (still permitted — only UPDATE/DELETE are withheld) could produce.
func (f *FakeStore) TamperManifest(versionID string, mutate func(*Manifest)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.manifests[versionID]; ok {
		mutate(m)
	}
}

// FakeSigner is a real HMAC-based Signer for unit tests, not a stub that
// always answers true — Verify genuinely recomputes and compares, so a
// tampering test exercises real rejection logic instead of a canned
// response. Not a real secret: the "key" is derived deterministically
// from keyName, test-only.
type FakeSigner struct {
	mu   sync.Mutex
	keys map[string][]byte
}

func NewFakeSigner() *FakeSigner {
	return &FakeSigner{keys: make(map[string][]byte)}
}

func (f *FakeSigner) keyFor(keyName string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[keyName]
	if !ok {
		k = []byte("fake-test-signing-key-" + keyName)
		f.keys[keyName] = k
	}
	return k
}

func (f *FakeSigner) Sign(ctx context.Context, keyName string, input []byte) (string, error) {
	mac := hmac.New(sha256.New, f.keyFor(keyName))
	mac.Write(input)
	return "fake:v1:" + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (f *FakeSigner) Verify(ctx context.Context, keyName string, input []byte, signature string) (bool, error) {
	expected, err := f.Sign(ctx, keyName, input)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(expected), []byte(signature)), nil
}

// PublishedManifest is what FakePublisher records — enough to assert on
// in tests (e.g. "was this exact version_id ever published, with this
// exact signature") without needing a real etcd.
type PublishedManifest struct {
	VersionID string
	ModelRef  string
	SpecJSON  string
	Signature string
	CreatedBy string
	CreatedAt string
	Status    string
}

// FakePublisher is an in-memory Publisher for unit tests — no etcd
// needed. Err, if set, makes both Publish methods fail, for testing
// RegisterModelManifest's own "etcd publish failure fails the whole call"
// behavior.
type FakePublisher struct {
	mu        sync.Mutex
	Manifests []PublishedManifest
	Active    map[string]string // model_ref -> version_id
	Err       error
}

func NewFakePublisher() *FakePublisher {
	return &FakePublisher{Active: make(map[string]string)}
}

func (f *FakePublisher) PublishManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy, createdAt, status string) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Manifests = append(f.Manifests, PublishedManifest{
		VersionID: versionID, ModelRef: modelRef, SpecJSON: specJSON,
		Signature: signature, CreatedBy: createdBy, CreatedAt: createdAt, Status: status,
	})
	return nil
}

func (f *FakePublisher) PublishActive(ctx context.Context, modelRef, versionID string) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Active[modelRef] = versionID
	return nil
}
