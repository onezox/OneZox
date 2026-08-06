// Package registry implements the model manifest registry: signed,
// versioned, immutable manifests (RegisterModelManifest, GetModelManifest,
// ListModels), backed by model_manifest/model_active
// (data/migrations/0008-0009).
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/onezox/OneZox/services/control-plane/internal/vaultclient"
)

// SigningKeyName is the Vault Transit key every manifest is signed and
// verified with — must match scripts/vault-setup-control-plane.sh's own
// SIGNING_KEY.
const SigningKeyName = "model-manifest-signing"

var (
	ErrNotFound         = errors.New("model manifest not found")
	ErrInvalidSignature = errors.New("model manifest signature verification failed")
)

type Manifest struct {
	VersionID string
	ModelRef  string
	SpecJSON  string
	Signature string
	CreatedBy string
	CreatedAt time.Time
	Status    string
}

type Entry struct {
	ModelRef        string
	ActiveVersionID string
}

// Store is model_manifest/model_active's own persistence boundary —
// CockroachStore (cockroach_store.go) is the real CockroachDB-backed
// implementation; FakeStore (fake.go) is an in-memory one for unit tests.
// InsertManifest takes an already-generated version_id (not DB-assigned)
// because the version_id must be part of what gets signed, before the row
// exists.
type Store interface {
	InsertManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy string) error
	SetActive(ctx context.Context, modelRef, versionID string) error
	GetManifestByVersion(ctx context.Context, versionID string) (*Manifest, error)
	GetActiveManifest(ctx context.Context, modelRef string) (*Manifest, error)
	ListActive(ctx context.Context) ([]Entry, error)
}

type Service struct {
	store  Store
	signer vaultclient.Signer
	log    *slog.Logger
}

func NewService(store Store, signer vaultclient.Signer, log *slog.Logger) *Service {
	return &Service{store: store, signer: signer, log: log}
}

// signedPayload is what actually gets signed/verified — binding
// version_id and model_ref into the payload (not signing spec_json alone)
// stops a valid signature for one manifest being replayed onto a
// different version_id/model_ref that happens to share the same
// spec_json bytes.
func signedPayload(versionID, modelRef, specJSON string) []byte {
	return []byte(versionID + "|" + modelRef + "|" + specJSON)
}

// RegisterModelManifest signs spec_json (Step E), writes the immutable
// model_manifest row, and activates it (model_active) — matching Phase-04.txt's
// own INTERNAL COMMUNICATION FLOW: "validate + sign -> write model_manifest
// (immutable) -> set model_active -> publish to etcd" (etcd publish is
// Step R, not here). Every registration activates immediately: Phase-04
// has no staged/canary "publish but don't activate" concept — that's
// Phase-05's rollout UX, out of this phase's scope.
func (s *Service) RegisterModelManifest(ctx context.Context, modelRef, specJSON, createdBy string) (string, error) {
	// spec_json is a plain STRING column (data/migrations/0013), not JSONB
	// — CockroachDB's JSONB type reformats input text on storage (e.g.
	// adds a space after ":"), which would silently break byte-for-byte
	// signature verification on read. STRING preserves exactly what's
	// signed, but that means the database no longer enforces "this is
	// valid JSON" the way JSONB did — validated here instead, at the
	// boundary, to make up for it.
	if !json.Valid([]byte(specJSON)) {
		return "", fmt.Errorf("spec_json is not valid JSON")
	}

	versionID := uuid.NewString()

	signature, err := s.signer.Sign(ctx, SigningKeyName, signedPayload(versionID, modelRef, specJSON))
	if err != nil {
		return "", fmt.Errorf("signing manifest: %w", err)
	}

	if err := s.store.InsertManifest(ctx, versionID, modelRef, specJSON, signature, createdBy); err != nil {
		return "", fmt.Errorf("inserting manifest: %w", err)
	}

	if err := s.store.SetActive(ctx, modelRef, versionID); err != nil {
		return "", fmt.Errorf("setting active version: %w", err)
	}

	s.log.Info("registered model manifest", "model_ref", modelRef, "version_id", versionID, "created_by", createdBy)
	return versionID, nil
}

// GetModelManifest resolves a manifest (a specific version_id, or the
// active one when version_id is empty) and VERIFIES its signature before
// returning it — every read re-verifies, not just RegisterModelManifest's
// own write path, since a direct-SQL bypass could insert a row with a
// missing or tampered signature that the app-level "always sign at
// RegisterModelManifest" guarantee alone wouldn't catch (control_plane's
// own DB role still has INSERT on model_manifest, by design — only
// UPDATE/DELETE are withheld, data/migrations/0012). Step G's adversarial
// tests exercise exactly this rejection path.
func (s *Service) GetModelManifest(ctx context.Context, modelRef, versionID string) (*Manifest, error) {
	var (
		m   *Manifest
		err error
	)
	if versionID == "" {
		m, err = s.store.GetActiveManifest(ctx, modelRef)
	} else {
		m, err = s.store.GetManifestByVersion(ctx, versionID)
	}
	if err != nil {
		return nil, err
	}

	valid, err := s.signer.Verify(ctx, SigningKeyName, signedPayload(m.VersionID, m.ModelRef, m.SpecJSON), m.Signature)
	if err != nil {
		return nil, fmt.Errorf("verifying manifest signature: %w", err)
	}
	if !valid {
		s.log.Warn("manifest signature verification failed, refusing to serve",
			"model_ref", m.ModelRef, "version_id", m.VersionID)
		return nil, ErrInvalidSignature
	}

	return m, nil
}

// ListModels backs GET /v1/models (Phase-04.txt APIS CREATED). Signatures
// are NOT re-verified here — this is a listing of (model_ref,
// active_version_id) pairs, not manifest content; content verification
// happens in GetModelManifest, the RPC that actually returns spec_json.
func (s *Service) ListModels(ctx context.Context) ([]Entry, error) {
	return s.store.ListActive(ctx)
}
