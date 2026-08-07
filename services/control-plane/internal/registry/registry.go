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
	InsertManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy string, createdAt time.Time) error
	SetActive(ctx context.Context, modelRef, versionID string) error
	GetManifestByVersion(ctx context.Context, versionID string) (*Manifest, error)
	GetActiveManifest(ctx context.Context, modelRef string) (*Manifest, error)
	ListActive(ctx context.Context) ([]Entry, error)
	// HasActiveVersion reports whether model_ref already has ANY active
	// version — Phase-05's own bootstrap-vs-rollout distinction (see
	// RegisterModelManifest's doc comment). Deliberately not
	// GetActiveManifest reused for this: that method also re-verifies a
	// signature, work this existence-only check has no reason to do or
	// depend on succeeding.
	HasActiveVersion(ctx context.Context, modelRef string) (bool, error)
}

// Publisher distributes a registered manifest to etcd — the mechanism
// edge-gateway/data-plane's own caches watch (Step Q/R). CockroachDB
// (Store) is the source of truth; etcd is a distribution layer, so a
// publish failure here fails RegisterModelManifest outright (below)
// rather than silently leaving a "registered" manifest nothing ever
// actually distributes — same "fail loud, not silently incomplete"
// discipline as this codebase's other boot-time/write-path guards.
type Publisher interface {
	PublishManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy, createdAt, status string) error
	PublishActive(ctx context.Context, modelRef, versionID string) error
}

type Service struct {
	store     Store
	signer    vaultclient.Signer
	publisher Publisher
	log       *slog.Logger
}

func NewService(store Store, signer vaultclient.Signer, publisher Publisher, log *slog.Logger) *Service {
	return &Service{store: store, signer: signer, publisher: publisher, log: log}
}

// signedPayload is what actually gets signed/verified — binding
// version_id and model_ref into the payload (not signing spec_json alone)
// stops a valid signature for one manifest being replayed onto a
// different version_id/model_ref that happens to share the same
// spec_json bytes.
func signedPayload(versionID, modelRef, specJSON string) []byte {
	return []byte(versionID + "|" + modelRef + "|" + specJSON)
}

// manifestStatus is the only status value Phase-04 defines — no status
// transitions exist yet, so this is a literal, not read back from the DB
// default (data/migrations/0008's own DEFAULT 'published'). Kept in sync
// with that default manually; a future phase adding real status
// transitions needs to revisit both.
const manifestStatus = "published"

// RegisterModelManifest signs spec_json (Step E), writes the immutable
// model_manifest row, and publishes it to etcd (Step Q) — matching
// Phase-04.txt's own INTERNAL COMMUNICATION FLOW: "validate + sign ->
// write model_manifest (immutable) -> ... -> publish to etcd -> edge +
// data plane update cached manifest".
//
// Activation (model_active) is now conditional — the Phase-05 change this
// method's own P04-era comment anticipated ("Phase-04 has no staged/
// canary 'publish but don't activate' concept — that's Phase-05's rollout
// UX"): a model_ref with NO existing active version activates immediately
// on registration (the bootstrap case — there is no live traffic to
// protect, exactly what registering each of the 5 real providers for the
// first time needed and got in Phase-04's own Step T). A model_ref that
// ALREADY has an active version does NOT activate on publish anymore —
// the new version is signed, stored, and independently fetchable by its
// own version_id, but stays inert until Phase-05's rollout module (Step
// L) promotes it. This is what makes EC4's "no path exists to mutate a
// live model outside signed manifests + rollout" literally true: from
// this phase forward, publishing alone never changes what's live for an
// already-active model_ref — only a real rollout's own promotion does.
//
// version_id AND created_at are both generated here in application code,
// not left to the DB's own defaults (gen_random_uuid()/now()) — both need
// to be known values before the etcd envelope can be published without an
// extra read-back round-trip, and created_at in particular must match
// EXACTLY between the CockroachDB row and what data-plane's independent
// verifier sees over etcd (any drift would just be cosmetic here since
// created_at isn't part of the signed payload, but keeping the two
// genuinely identical avoids a confusing "same version_id, different
// created_at" support question later).
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
	createdAt := time.Now().UTC()

	signature, err := s.signer.Sign(ctx, SigningKeyName, signedPayload(versionID, modelRef, specJSON))
	if err != nil {
		return "", fmt.Errorf("signing manifest: %w", err)
	}

	if err := s.store.InsertManifest(ctx, versionID, modelRef, specJSON, signature, createdBy, createdAt); err != nil {
		return "", fmt.Errorf("inserting manifest: %w", err)
	}

	// etcd publish failure fails the whole call — see Publisher's own doc
	// comment for why (a "registered" manifest nothing distributes is
	// silently broken, not merely incomplete).
	createdAtStr := createdAt.Format(time.RFC3339)
	if err := s.publisher.PublishManifest(ctx, versionID, modelRef, specJSON, signature, createdBy, createdAtStr, manifestStatus); err != nil {
		return "", fmt.Errorf("publishing manifest to etcd: %w", err)
	}

	hasActive, err := s.store.HasActiveVersion(ctx, modelRef)
	if err != nil {
		return "", fmt.Errorf("checking existing active version: %w", err)
	}
	if hasActive {
		s.log.Info("registered model manifest (not activated: model_ref already has a live version; a rollout must promote it)",
			"model_ref", modelRef, "version_id", versionID, "created_by", createdBy)
		return versionID, nil
	}

	if err := s.ActivateVersion(ctx, modelRef, versionID); err != nil {
		return "", err
	}

	s.log.Info("registered model manifest (activated: first version for this model_ref)",
		"model_ref", modelRef, "version_id", versionID, "created_by", createdBy)
	return versionID, nil
}

// ActivateVersion sets modelRef's stable pointer to versionID — both in
// CockroachDB (model_active) and etcd (the active-pointer key,
// data/migrations/0016's own JSON envelope, K's own canary/percent fields
// implicitly cleared since PublishActive writes stable-only). Two callers
// share this exact logic, deliberately not duplicated: RegisterModelManifest's
// own bootstrap-activation branch above (a model_ref's first-ever version),
// and Step L's rollout module's own promotion transition (a canary that
// reached its final stage becomes the new stable) — both are "make this
// version THE live one," the same operation regardless of which path led
// there.
func (s *Service) ActivateVersion(ctx context.Context, modelRef, versionID string) error {
	if err := s.store.SetActive(ctx, modelRef, versionID); err != nil {
		return fmt.Errorf("setting active version: %w", err)
	}
	if err := s.publisher.PublishActive(ctx, modelRef, versionID); err != nil {
		return fmt.Errorf("publishing active pointer to etcd: %w", err)
	}
	return nil
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
