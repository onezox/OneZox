package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func testService() (*Service, *FakeStore) {
	store := NewFakeStore()
	signer := NewFakeSigner()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(store, signer, log), store
}

func TestRegisterAndGetByActiveVersion(t *testing.T) {
	ctx := context.Background()
	svc, _ := testService()

	versionID, err := svc.RegisterModelManifest(ctx, "openai", `{"provider":"openai"}`, "test-runner")
	if err != nil {
		t.Fatalf("RegisterModelManifest: %v", err)
	}
	if versionID == "" {
		t.Fatal("expected a non-empty version_id")
	}

	got, err := svc.GetModelManifest(ctx, "openai", "")
	if err != nil {
		t.Fatalf("GetModelManifest(active): %v", err)
	}
	if got.VersionID != versionID {
		t.Errorf("version_id = %q, want %q", got.VersionID, versionID)
	}
	if got.ModelRef != "openai" {
		t.Errorf("model_ref = %q, want openai", got.ModelRef)
	}
	if got.SpecJSON != `{"provider":"openai"}` {
		t.Errorf("spec_json = %q", got.SpecJSON)
	}
}

func TestGetByExplicitVersion(t *testing.T) {
	ctx := context.Background()
	svc, _ := testService()

	v1, err := svc.RegisterModelManifest(ctx, "anthropic", `{"v":1}`, "test-runner")
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	v2, err := svc.RegisterModelManifest(ctx, "anthropic", `{"v":2}`, "test-runner")
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}
	if v1 == v2 {
		t.Fatal("expected distinct version_id per registration, got the same")
	}

	// Active should be v2 (most recent registration activates immediately).
	active, err := svc.GetModelManifest(ctx, "anthropic", "")
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.VersionID != v2 {
		t.Errorf("active version = %q, want %q (most recent)", active.VersionID, v2)
	}

	// v1 must still be independently fetchable by its own version_id — a
	// new version never overwrites or hides the old one, it's a new row.
	old, err := svc.GetModelManifest(ctx, "anthropic", v1)
	if err != nil {
		t.Fatalf("get v1 explicitly: %v", err)
	}
	if old.SpecJSON != `{"v":1}` {
		t.Errorf("v1 spec_json = %q, want {\"v\":1}", old.SpecJSON)
	}
}

func TestListModels(t *testing.T) {
	ctx := context.Background()
	svc, _ := testService()

	for _, ref := range []string{"openai", "anthropic", "grok"} {
		if _, err := svc.RegisterModelManifest(ctx, ref, `{}`, "test-runner"); err != nil {
			t.Fatalf("register %s: %v", ref, err)
		}
	}

	entries, err := svc.ListModels(ctx)
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
}

// TestRegisterModelManifestInvalidJSONRejected: spec_json is a plain
// STRING column (data/migrations/0013, fixed from JSONB after a live
// signature-verification bug), which no longer has the database's own
// "this is valid JSON" enforcement — must be validated in application
// code instead.
func TestRegisterModelManifestInvalidJSONRejected(t *testing.T) {
	ctx := context.Background()
	svc, _ := testService()

	_, err := svc.RegisterModelManifest(ctx, "openai", `{not valid json`, "test-runner")
	if err == nil {
		t.Fatal("expected an error for invalid spec_json, got nil")
	}
}

func TestGetModelManifestNotFound(t *testing.T) {
	ctx := context.Background()
	svc, _ := testService()

	if _, err := svc.GetModelManifest(ctx, "does-not-exist", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestTamperedSpecJSONRejected is the adversarial case Step G's plan
// calls for at the registry's own serving path: simulate a direct-SQL
// bypass altering spec_json after the fact (control_plane's DB role can't
// UPDATE model_manifest, data/migrations/0012 — but this proves the
// APPLICATION would refuse to serve it even if some other path managed to
// change it, e.g. a bug, a different role, a restore from an old backup
// with different content under the same row). A tampered row must FAIL
// verification, not silently load.
func TestTamperedSpecJSONRejected(t *testing.T) {
	ctx := context.Background()
	svc, store := testService()

	versionID, err := svc.RegisterModelManifest(ctx, "openai", `{"provider":"openai"}`, "test-runner")
	if err != nil {
		t.Fatalf("RegisterModelManifest: %v", err)
	}

	// Positive control first: unmodified, it must still verify — this is
	// what proves the negative result below is really about tampering,
	// not a broken verifier that rejects everything.
	if _, err := svc.GetModelManifest(ctx, "openai", versionID); err != nil {
		t.Fatalf("expected untampered manifest to verify, got: %v", err)
	}

	store.TamperManifest(versionID, func(m *Manifest) {
		m.SpecJSON = `{"provider":"openai","backdoor":true}`
	})

	_, err = svc.GetModelManifest(ctx, "openai", versionID)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("tampered manifest: err = %v, want ErrInvalidSignature", err)
	}
}

// TestUnsignedManifestRejected: a row inserted with an empty/garbage
// signature (what a direct SQL INSERT bypassing RegisterModelManifest
// would produce — INSERT is still permitted for control_plane, only
// UPDATE/DELETE are withheld) must be refused on read, not served as if
// it were legitimately signed.
func TestUnsignedManifestRejected(t *testing.T) {
	ctx := context.Background()
	svc, store := testService()

	if err := store.InsertManifest(ctx, "unsigned-version", "openai", `{"provider":"openai"}`, "", "direct-sql-bypass"); err != nil {
		t.Fatalf("InsertManifest (unsigned): %v", err)
	}

	_, err := svc.GetModelManifest(ctx, "openai", "unsigned-version")
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("unsigned manifest: err = %v, want ErrInvalidSignature", err)
	}
}

// TestGarbageSignatureRejected: a row with a plausible-looking but wrong
// signature (not empty, not derived from this content) must also be
// refused — proves rejection isn't just an empty-string special case.
func TestGarbageSignatureRejected(t *testing.T) {
	ctx := context.Background()
	svc, store := testService()

	if err := store.InsertManifest(ctx, "garbage-sig-version", "openai", `{"provider":"openai"}`, "fake:v1:not-a-real-signature", "direct-sql-bypass"); err != nil {
		t.Fatalf("InsertManifest (garbage signature): %v", err)
	}

	_, err := svc.GetModelManifest(ctx, "openai", "garbage-sig-version")
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("garbage-signature manifest: err = %v, want ErrInvalidSignature", err)
	}
}
