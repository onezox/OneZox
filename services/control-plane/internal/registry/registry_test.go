package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func testService() (*Service, *FakeStore) {
	svc, store, _ := testServiceWithPublisher()
	return svc, store
}

func testServiceWithPublisher() (*Service, *FakeStore, *FakePublisher) {
	store := NewFakeStore()
	signer := NewFakeSigner()
	publisher := NewFakePublisher()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(store, signer, publisher, log), store, publisher
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

	// Active stays v1 — Phase-05's own bootstrap-vs-rollout rule
	// (registry.go's RegisterModelManifest doc comment): only a model_ref's
	// FIRST registration auto-activates. v2 does not, even though it's the
	// most recent — a real rollout (Step L) is the only thing that could
	// promote it from here on. See TestSecondRegistrationDoesNotActivate
	// for this property's own dedicated, explicit test.
	active, err := svc.GetModelManifest(ctx, "anthropic", "")
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.VersionID != v1 {
		t.Errorf("active version = %q, want %q (first registration, unchanged by v2)", active.VersionID, v1)
	}

	// v2 must still be independently fetchable by its own version_id even
	// though it never activated — a new version never overwrites or hides
	// the old one, it's a new row, signed and stored regardless of
	// activation state.
	got2, err := svc.GetModelManifest(ctx, "anthropic", v2)
	if err != nil {
		t.Fatalf("get v2 explicitly: %v", err)
	}
	if got2.SpecJSON != `{"v":2}` {
		t.Errorf("v2 spec_json = %q, want {\"v\":2}", got2.SpecJSON)
	}
}

// TestSecondRegistrationDoesNotActivate is the EC4-relevant property this
// step's own investigation surfaced, tested directly and explicitly (not
// just as a side observation inside TestGetByExplicitVersion): once a
// model_ref has a live version, publishing a new one must NOT change what
// real traffic resolves to. Only a rollout's own promotion may do that
// (Step L) — this is what makes "no path exists to mutate a live model
// outside signed manifests + rollout" true starting at the publish path
// itself, not just asserted at the rollout layer.
func TestSecondRegistrationDoesNotActivate(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher := testServiceWithPublisher()

	v1, err := svc.RegisterModelManifest(ctx, "openai", `{"v":1}`, "test-runner")
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if publisher.Active["openai"] != v1 {
		t.Fatalf("bootstrap registration must activate: publisher.Active[openai] = %q, want %q", publisher.Active["openai"], v1)
	}

	if _, err := svc.RegisterModelManifest(ctx, "openai", `{"v":2}`, "test-runner"); err != nil {
		t.Fatalf("register v2: %v", err)
	}

	// The etcd active pointer (what data-plane/edge-gateway's own caches
	// actually resolve requests against, Phase-04 Step Q/R) must be
	// UNCHANGED — still v1, never touched by v2's own publish.
	if publisher.Active["openai"] != v1 {
		t.Errorf("published active[openai] changed to %q after a second, non-bootstrap registration — want unchanged %q", publisher.Active["openai"], v1)
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

// TestRegisterModelManifestPublishesToEtcd: a successful registration
// must publish both the manifest and the active pointer — this is what
// data-plane/edge-gateway's own caches (Step Q/R) actually watch, not
// CockroachDB directly.
func TestRegisterModelManifestPublishesToEtcd(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher := testServiceWithPublisher()

	versionID, err := svc.RegisterModelManifest(ctx, "openai", `{"provider":"openai"}`, "test-runner")
	if err != nil {
		t.Fatalf("RegisterModelManifest: %v", err)
	}

	if len(publisher.Manifests) != 1 {
		t.Fatalf("got %d published manifests, want 1", len(publisher.Manifests))
	}
	pub := publisher.Manifests[0]
	if pub.VersionID != versionID || pub.ModelRef != "openai" || pub.SpecJSON != `{"provider":"openai"}` {
		t.Errorf("published manifest = %+v, want matching version_id/model_ref/spec_json", pub)
	}
	if publisher.Active["openai"] != versionID {
		t.Errorf("published active[openai] = %q, want %q", publisher.Active["openai"], versionID)
	}
}

// TestRegisterModelManifestEtcdFailureFailsCall: CockroachDB is the
// source of truth, but a manifest nothing ever distributes is silently
// broken — RegisterModelManifest must fail loud, not return success while
// leaving etcd stale.
func TestRegisterModelManifestEtcdFailureFailsCall(t *testing.T) {
	ctx := context.Background()
	svc, _, publisher := testServiceWithPublisher()
	publisher.Err = errors.New("etcd unreachable")

	if _, err := svc.RegisterModelManifest(ctx, "openai", `{"provider":"openai"}`, "test-runner"); err == nil {
		t.Fatal("expected an error when etcd publish fails, got nil")
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

	if err := store.InsertManifest(ctx, "unsigned-version", "openai", `{"provider":"openai"}`, "", "direct-sql-bypass", time.Now()); err != nil {
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

	if err := store.InsertManifest(ctx, "garbage-sig-version", "openai", `{"provider":"openai"}`, "fake:v1:not-a-real-signature", "direct-sql-bypass", time.Now()); err != nil {
		t.Fatalf("InsertManifest (garbage signature): %v", err)
	}

	_, err := svc.GetModelManifest(ctx, "openai", "garbage-sig-version")
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("garbage-signature manifest: err = %v, want ErrInvalidSignature", err)
	}
}
