package graph

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
	providerpb "github.com/onezox/OneZox/services/admin-api/internal/pb/provider/v1"
	"github.com/onezox/OneZox/services/admin-api/internal/promclient"
)

// fakeControlReader implements the narrow read slice — note it has no
// mutating method at all, which is the point of controlReader being
// separate from server.go's controlPublisher.
type fakeControlReader struct {
	models        *controlpb.ListModelsResponse
	modelsErr     error
	manifest      *controlpb.GetModelManifestResponse
	manifestErr   error
	lastManifest  *controlpb.GetModelManifestRequest
	rolloutStatus *controlpb.GetRolloutStatusResponse
	rolloutErr    error
	lastRollout   *controlpb.GetRolloutStatusRequest
	rollouts      *controlpb.ListRolloutsResponse
	rolloutsErr   error
}

func (f *fakeControlReader) ListModels(ctx context.Context, in *controlpb.ListModelsRequest, opts ...grpc.CallOption) (*controlpb.ListModelsResponse, error) {
	if f.modelsErr != nil {
		return nil, f.modelsErr
	}
	if f.models == nil {
		return &controlpb.ListModelsResponse{}, nil
	}
	return f.models, nil
}

func (f *fakeControlReader) GetModelManifest(ctx context.Context, in *controlpb.GetModelManifestRequest, opts ...grpc.CallOption) (*controlpb.GetModelManifestResponse, error) {
	f.lastManifest = in
	if f.manifestErr != nil {
		return nil, f.manifestErr
	}
	return f.manifest, nil
}

func (f *fakeControlReader) GetRolloutStatus(ctx context.Context, in *controlpb.GetRolloutStatusRequest, opts ...grpc.CallOption) (*controlpb.GetRolloutStatusResponse, error) {
	f.lastRollout = in
	if f.rolloutErr != nil {
		return nil, f.rolloutErr
	}
	return f.rolloutStatus, nil
}

func (f *fakeControlReader) ListRollouts(ctx context.Context, in *controlpb.ListRolloutsRequest, opts ...grpc.CallOption) (*controlpb.ListRolloutsResponse, error) {
	if f.rolloutsErr != nil {
		return nil, f.rolloutsErr
	}
	if f.rollouts == nil {
		return &controlpb.ListRolloutsResponse{}, nil
	}
	return f.rollouts, nil
}

type fakeProviderReader struct {
	resp *providerpb.ProviderHealthResponse
	err  error
}

func (f *fakeProviderReader) ProviderHealth(ctx context.Context, in *providerpb.ProviderHealthRequest, opts ...grpc.CallOption) (*providerpb.ProviderHealthResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func testResolver() (*Resolver, *apikeys.FakeStore, *fakeControlReader, *fakeProviderReader, *audit.FakeReader, *promclient.FakeQuerier) {
	keys := apikeys.NewFakeStore()
	control := &fakeControlReader{}
	providers := &fakeProviderReader{resp: &providerpb.ProviderHealthResponse{}}
	auditR := &audit.FakeReader{}
	metrics := promclient.NewFakeQuerier()
	r := &Resolver{
		Keys:      keys,
		Control:   control,
		Providers: providers,
		Audit:     auditR,
		Metrics:   metrics,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r, keys, control, providers, auditR, metrics
}

func adminCtx() context.Context {
	return authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u1", OrgID: "o1", Role: "admin"})
}

func TestMeReturnsTheVerifiedIdentity(t *testing.T) {
	r, _, _, _, _, _ := testResolver()

	me, err := r.Query().Me(adminCtx())
	if err != nil {
		t.Fatalf("Me: %v", err)
	}
	if me.ID != "u1" || me.OrgID != "o1" || me.Role != "admin" {
		t.Fatalf("me = %+v, want the identity authn attached", me)
	}
}

func TestMeFailsClosedWithoutIdentity(t *testing.T) {
	r, _, _, _, _, _ := testResolver()

	if _, err := r.Query().Me(context.Background()); err == nil {
		t.Fatal("Me: want an error with no verified identity, got nil")
	}
}

func TestAPIKeysReturnsOnlySafeMetadata(t *testing.T) {
	r, keys, _, _, _, _ := testResolver()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	if _, err := keys.Create(context.Background(), "org-1", "some-hash-value", []string{"chat.completions"}); err != nil {
		t.Fatalf("setup Create: %v", err)
	}

	got, err := r.Query().APIKeys(context.Background())
	if err != nil {
		t.Fatalf("APIKeys: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}

	entry := got[0]
	if entry.KeyID == "" || entry.OrgID != "org-1" || entry.CreatedAt == "" {
		t.Errorf("unexpected entry: %+v", entry)
	}
	if !reflect.DeepEqual(entry.Scopes, []string{"chat.completions"}) {
		t.Errorf("scopes = %v, want [chat.completions]", entry.Scopes)
	}
	if entry.RevokedAt != nil {
		t.Errorf("revokedAt = %v, want nil for a never-revoked key", entry.RevokedAt)
	}

	// The structural proof, not just "this test didn't print a hash":
	// APIKeySummary (models_gen.go, generated straight from
	// admin.graphql) has no field that COULD hold a hash or raw key —
	// reflect over the struct's own fields and confirm neither name
	// appears, so this test breaks loudly if a future schema change
	// ever adds one back.
	typ := reflect.TypeOf(*entry)
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if name == "Hash" || name == "RawKey" {
			t.Fatalf("APIKeySummary has a %s field — a hash or raw key could leak through listApiKeys", name)
		}
	}
}

func TestAPIKeysShowsRevokedKeysWithTheirStatus(t *testing.T) {
	r, keys, _, _, _, _ := testResolver()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	keyID, err := keys.Create(context.Background(), "org-1", "some-hash-value", nil)
	if err != nil {
		t.Fatalf("setup Create: %v", err)
	}
	if _, err := keys.Revoke(context.Background(), keyID); err != nil {
		t.Fatalf("setup Revoke: %v", err)
	}

	got, err := r.Query().APIKeys(context.Background())
	if err != nil {
		t.Fatalf("APIKeys: %v", err)
	}
	if len(got) != 1 || got[0].RevokedAt == nil {
		t.Fatalf("got %+v, want exactly one entry with a non-nil revokedAt", got)
	}
}

func TestAPIKeysPropagatesStoreError(t *testing.T) {
	r, keys, _, _, _, _ := testResolver()
	keys.ListErr = errors.New("connection refused")

	if _, err := r.Query().APIKeys(context.Background()); err == nil {
		t.Fatal("APIKeys: want an error when the store fails, got nil")
	}
}

func TestModelsMapsControlPlaneResponse(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.models = &controlpb.ListModelsResponse{Models: []*controlpb.ListModelsResponse_Entry{
		{ModelRef: "openai", ActiveVersionId: "v-1"},
		{ModelRef: "anthropic", ActiveVersionId: "v-2"},
	}}

	got, err := r.Query().Models(context.Background())
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if len(got) != 2 || got[0].ModelRef != "openai" || got[0].ActiveVersionID != "v-1" {
		t.Fatalf("models = %+v, want the control-plane entries mapped through", got)
	}
}

// TestModelDraftRequestsTheActiveVersion — modelDraft is the Model
// Studio editor's starting point, which by definition means the
// CURRENTLY ACTIVE manifest: an empty version_id is what control-plane
// resolves as "active" (control.proto). Asserting the outgoing request
// is the only way to prove that, since the response shape is identical
// either way.
func TestModelDraftRequestsTheActiveVersion(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.manifest = &controlpb.GetModelManifestResponse{
		VersionId: "v-active", ModelRef: "openai", SpecJson: `{"worker_ref":"openai:gpt-4o-mini"}`,
		Signature: "sig", CreatedBy: "u1", CreatedAt: "2026-01-01T00:00:00Z", Status: "published",
	}

	got, err := r.Query().ModelDraft(context.Background(), "openai")
	if err != nil {
		t.Fatalf("ModelDraft: %v", err)
	}
	if control.lastManifest.GetVersionId() != "" {
		t.Errorf("version_id sent = %q, want empty (control-plane resolves that as the active version)", control.lastManifest.GetVersionId())
	}
	if control.lastManifest.GetModelRef() != "openai" {
		t.Errorf("model_ref sent = %q, want openai", control.lastManifest.GetModelRef())
	}
	if got.VersionID != "v-active" || got.SpecJSON == "" {
		t.Errorf("manifest = %+v, want the control-plane manifest mapped through", got)
	}
}

// TestModelVersionRequestsTheNamedVersion is the other half of the
// Model Studio diff view: the same resolver, a specific version.
func TestModelVersionRequestsTheNamedVersion(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.manifest = &controlpb.GetModelManifestResponse{VersionId: "v-old", ModelRef: "openai"}
	want := "v-old"

	if _, err := r.Query().ModelVersion(context.Background(), "openai", &want); err != nil {
		t.Fatalf("ModelVersion: %v", err)
	}
	if control.lastManifest.GetVersionId() != "v-old" {
		t.Errorf("version_id sent = %q, want v-old", control.lastManifest.GetVersionId())
	}
}

// TestRolloutNotFoundIsNullNotAnError — the schema types rollout as a
// NULLABLE Rollout precisely so "this model has never had a rollout" is
// an ordinary empty result the panel renders as such, not an error
// banner. A model that has never been canaried is the NORMAL case for
// most models, so getting this wrong would make the panel look broken.
func TestRolloutNotFoundIsNullNotAnError(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.rolloutErr = status.Error(codes.NotFound, "rollout not found")

	got, err := r.Query().RolloutByModel(context.Background(), "never-canaried")
	if err != nil {
		t.Fatalf("RolloutByModel: want nil error for NotFound, got %v", err)
	}
	if got != nil {
		t.Fatalf("rollout = %+v, want nil", got)
	}
}

// ...but a genuine failure must still surface as an error, or the panel
// would silently render "no rollout" during a control-plane outage.
func TestRolloutRealFailureStillErrors(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.rolloutErr = status.Error(codes.Unavailable, "control-plane unreachable")

	if _, err := r.Query().RolloutByModel(context.Background(), "openai"); err == nil {
		t.Fatal("RolloutByModel: want an error for a non-NotFound failure, got nil")
	}
}

func TestRolloutsMapsHistory(t *testing.T) {
	r, _, control, _, _, _ := testResolver()
	control.rollouts = &controlpb.ListRolloutsResponse{Rollouts: []*controlpb.GetRolloutStatusResponse{
		{RolloutId: "r-1", ModelRef: "openai", Stage: "stable", Status: "promoted", CanaryPercent: 100, StartedAt: "t0", EndedAt: "t1"},
		{RolloutId: "r-2", ModelRef: "openai", Stage: "canary_1", Status: "running", CanaryPercent: 1, StartedAt: "t2"},
	}}

	got, err := r.Query().Rollouts(context.Background())
	if err != nil {
		t.Fatalf("Rollouts: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rollouts, want 2", len(got))
	}
	if got[0].EndedAt == nil || *got[0].EndedAt != "t1" {
		t.Errorf("finished rollout endedAt = %v, want t1", got[0].EndedAt)
	}
	// A still-running rollout has no end time — that must be null, not
	// the empty string, or the panel can't tell "in progress" from
	// "ended at an unknown time".
	if got[1].EndedAt != nil {
		t.Errorf("running rollout endedAt = %v, want nil", got[1].EndedAt)
	}
}

// TestDashboardMetricsUsesTheExpectedQueries pins the exact PromQL each
// number is built from. Without this, a typo'd query silently returns 0
// forever and the dashboard looks merely quiet rather than broken —
// the same class of decorative-signal failure Step N had to rule out
// for the canary gate.
func TestDashboardMetricsUsesTheExpectedQueries(t *testing.T) {
	r, _, control, _, _, metrics := testResolver()
	metrics.Results[QueryRequestsPerSecond] = 12.5
	metrics.Results[QueryP95LatencyMs] = 340
	metrics.Results[QueryErrorRate] = 0.02
	control.models = &controlpb.ListModelsResponse{Models: []*controlpb.ListModelsResponse_Entry{
		{ModelRef: "openai"}, {ModelRef: "anthropic"},
	}}
	control.rollouts = &controlpb.ListRolloutsResponse{Rollouts: []*controlpb.GetRolloutStatusResponse{
		{RolloutId: "r-1", Status: "running"},
		{RolloutId: "r-2", Status: "promoted"},
		{RolloutId: "r-3", Status: "rolled_back"},
	}}

	got, err := r.Query().DashboardMetrics(context.Background())
	if err != nil {
		t.Fatalf("DashboardMetrics: %v", err)
	}
	if got.RequestsPerSecond != 12.5 || got.P95LatencyMs != 340 || got.ErrorRate != 0.02 {
		t.Errorf("metrics = %+v, want the values keyed to the exact expected queries", got)
	}
	if got.ActiveModelsCount != 2 {
		t.Errorf("activeModelsCount = %d, want 2", got.ActiveModelsCount)
	}
	// Only "running" counts — a promoted or rolled-back rollout is over.
	if got.ActiveRolloutsCount != 1 {
		t.Errorf("activeRolloutsCount = %d, want 1 (only status=running)", got.ActiveRolloutsCount)
	}
}

// TestDashboardMetricsDegradesOnPrometheusFailure — a metrics-backend
// blip must not blank the whole dashboard, because the model/rollout
// counts come from a different backend entirely and are still good.
func TestDashboardMetricsDegradesOnPrometheusFailure(t *testing.T) {
	r, _, control, _, _, metrics := testResolver()
	metrics.Err = errors.New("prometheus unreachable")
	control.models = &controlpb.ListModelsResponse{Models: []*controlpb.ListModelsResponse_Entry{{ModelRef: "openai"}}}

	got, err := r.Query().DashboardMetrics(context.Background())
	if err != nil {
		t.Fatalf("DashboardMetrics: want graceful degradation, got error %v", err)
	}
	if got.RequestsPerSecond != 0 || got.P95LatencyMs != 0 || got.ErrorRate != 0 {
		t.Errorf("metrics = %+v, want zeros when prometheus fails", got)
	}
	if got.ActiveModelsCount != 1 {
		t.Errorf("activeModelsCount = %d, want 1 — the non-prometheus half must survive", got.ActiveModelsCount)
	}
}

func TestAuditLogAppliesFiltersAndMapsRows(t *testing.T) {
	r, _, _, _, auditR, _ := testResolver()
	after := `{"version_id":"v-1"}`
	auditR.Records = []audit.Record{
		{AuditID: "a-1", Actor: "u1", Action: "publish_model_version", Target: "openai", AfterJSON: &after, TS: "2026-01-01T00:00:00Z"},
	}
	limit := 10
	actor := "u1"
	action := "publish_model_version"

	got, err := r.Query().AuditLog(context.Background(), &limit, &actor, &action)
	if err != nil {
		t.Fatalf("AuditLog: %v", err)
	}
	if auditR.LastLimit != 10 || auditR.LastActor != "u1" || auditR.LastAction != "publish_model_version" {
		t.Errorf("filters passed through = limit %d actor %q action %q, want 10/u1/publish_model_version",
			auditR.LastLimit, auditR.LastActor, auditR.LastAction)
	}
	if len(got) != 1 || got[0].AuditID != "a-1" || got[0].BeforeJSON != nil || got[0].AfterJSON == nil {
		t.Fatalf("entry = %+v, want the record mapped with a null before and a non-null after", got)
	}
}

func TestAuditLogNilArgsMeanNoFilter(t *testing.T) {
	r, _, _, _, auditR, _ := testResolver()

	if _, err := r.Query().AuditLog(context.Background(), nil, nil, nil); err != nil {
		t.Fatalf("AuditLog: %v", err)
	}
	if auditR.LastActor != "" || auditR.LastAction != "" {
		t.Errorf("nil filters became %q/%q, want empty strings (the reader's own no-filter sentinel)", auditR.LastActor, auditR.LastAction)
	}
	if auditR.LastLimit != 0 {
		t.Errorf("nil limit became %d, want 0 so the reader applies its own default", auditR.LastLimit)
	}
}

// TestProviderHealthDerivesHealthyFromBreakerState — provider.proto has
// no `healthy` field; a provider is healthy exactly when its breaker is
// closed. Deriving that in one place keeps every panel view agreeing.
func TestProviderHealthDerivesHealthyFromBreakerState(t *testing.T) {
	r, _, _, providers, _, _ := testResolver()
	providers.resp = &providerpb.ProviderHealthResponse{Statuses: []*providerpb.ProviderStatus{
		{Provider: "openai", BreakerState: providerpb.BreakerState_BREAKER_STATE_CLOSED, QuotaHeadroom: 0.9},
		{Provider: "anthropic", BreakerState: providerpb.BreakerState_BREAKER_STATE_OPEN, QuotaHeadroom: 0.1},
		{Provider: "grok", BreakerState: providerpb.BreakerState_BREAKER_STATE_HALF_OPEN, QuotaHeadroom: 0.5},
	}}

	got, err := r.Query().ProviderHealth(context.Background())
	if err != nil {
		t.Fatalf("ProviderHealth: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d providers, want 3", len(got))
	}
	if !got[0].Healthy || got[0].BreakerState != "closed" {
		t.Errorf("closed breaker = %+v, want healthy/closed", got[0])
	}
	if got[1].Healthy || got[1].BreakerState != "open" {
		t.Errorf("open breaker = %+v, want unhealthy/open", got[1])
	}
	// half-open is NOT healthy — it is a probationary state, and calling
	// it healthy would hide a provider that is still failing probes.
	if got[2].Healthy || got[2].BreakerState != "half_open" {
		t.Errorf("half-open breaker = %+v, want unhealthy/half_open", got[2])
	}
}
