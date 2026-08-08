package graph

// THIS CODE WILL BE UPDATED WITH SCHEMA CHANGES. PREVIOUS IMPLEMENTATION FOR SCHEMA CHANGES WILL BE KEPT IN THE COMMENT SECTION. IMPLEMENTATION FOR UNCHANGED SCHEMA WILL BE KEPT.

import (
	"context"
	"fmt"
	"log/slog"

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

// controlReader is the READ slice of control-plane's ControlService this
// package depends on — deliberately separate from server.go's own
// controlPublisher (the command slice). Same commands-vs-queries split
// admin.proto and admin.graphql are built around, carried into the Go
// interfaces so a resolver structurally cannot invoke a mutation: there
// is no RegisterModelManifest/CreateRollout/PromoteRollout/AbortRollout
// method on this interface to call.
type controlReader interface {
	ListModels(ctx context.Context, in *controlpb.ListModelsRequest, opts ...grpc.CallOption) (*controlpb.ListModelsResponse, error)
	GetModelManifest(ctx context.Context, in *controlpb.GetModelManifestRequest, opts ...grpc.CallOption) (*controlpb.GetModelManifestResponse, error)
	GetRolloutStatus(ctx context.Context, in *controlpb.GetRolloutStatusRequest, opts ...grpc.CallOption) (*controlpb.GetRolloutStatusResponse, error)
	ListRollouts(ctx context.Context, in *controlpb.ListRolloutsRequest, opts ...grpc.CallOption) (*controlpb.ListRolloutsResponse, error)
}

// providerHealthReader is ProviderHealth and nothing else — admin-api
// never calls Invoke/InvokeEmbedding, and this interface is what makes
// that structural rather than a matter of discipline (the generated
// client has those methods; nothing here can reach them). Same reason
// provider-gateway's own credentials.TokenFetcher stayed one method.
type providerHealthReader interface {
	ProviderHealth(ctx context.Context, in *providerpb.ProviderHealthRequest, opts ...grpc.CallOption) (*providerpb.ProviderHealthResponse, error)
}

// Resolver holds one narrow dependency per data source the query schema
// actually reads. Every field is an interface so the whole read surface
// is unit-testable without a cluster.
type Resolver struct {
	Keys      apikeys.Store
	Control   controlReader
	Providers providerHealthReader
	Audit     audit.Reader
	Metrics   promclient.Querier
	Log       *slog.Logger
}

// Me is the resolver for the me field. Reads the identity authn already
// verified and attached (Step F) — never re-derives or re-checks it,
// the same rule server.go's own handlers follow.
func (r *queryResolver) Me(ctx context.Context) (*AdminUser, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		// Unreachable in practice: authn.HTTPMiddleware wraps /graphql
		// unconditionally (main.go), so no resolver runs without one.
		// Fails closed regardless.
		return nil, fmt.Errorf("no verified identity")
	}
	return &AdminUser{ID: id.UserID, OrgID: id.OrgID, Role: id.Role}, nil
}

// Models is the resolver for the models field.
func (r *queryResolver) Models(ctx context.Context) ([]*ModelSummary, error) {
	resp, err := r.Control.ListModels(ctx, &controlpb.ListModelsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]*ModelSummary, 0, len(resp.GetModels()))
	for _, m := range resp.GetModels() {
		out = append(out, &ModelSummary{ModelRef: m.GetModelRef(), ActiveVersionID: m.GetActiveVersionId()})
	}
	return out, nil
}

// ModelDraft is the resolver for the modelDraft field — the Model
// Studio editor's starting point. A READ: it returns the currently
// active manifest and persists nothing, which is exactly why
// Phase-05.txt's "createModelDraft" is a query here and not a command
// (see admin.proto's own header).
func (r *queryResolver) ModelDraft(ctx context.Context, modelRef string) (*ModelManifest, error) {
	return r.fetchManifest(ctx, modelRef, "")
}

// ModelVersion is the resolver for the modelVersion field. An empty
// versionID resolves the active version — which is what makes the Model
// Studio diff view (a chosen past version vs. current active) two calls
// to this one resolver rather than a special-cased endpoint.
func (r *queryResolver) ModelVersion(ctx context.Context, modelRef string, versionID *string) (*ModelManifest, error) {
	var v string
	if versionID != nil {
		v = *versionID
	}
	return r.fetchManifest(ctx, modelRef, v)
}

func (r *queryResolver) fetchManifest(ctx context.Context, modelRef, versionID string) (*ModelManifest, error) {
	resp, err := r.Control.GetModelManifest(ctx, &controlpb.GetModelManifestRequest{
		ModelRef: modelRef, VersionId: versionID,
	})
	if err != nil {
		return nil, err
	}
	return &ModelManifest{
		VersionID: resp.GetVersionId(),
		ModelRef:  resp.GetModelRef(),
		SpecJSON:  resp.GetSpecJson(),
		Signature: resp.GetSignature(),
		CreatedBy: resp.GetCreatedBy(),
		CreatedAt: resp.GetCreatedAt(),
		Status:    resp.GetStatus(),
	}, nil
}

// Rollout is the resolver for the rollout field. Returns nil (not an
// error) when the id matches nothing — the schema types this as a
// nullable Rollout precisely so "no such rollout" is an ordinary empty
// result for the panel to render, not an error banner.
func (r *queryResolver) Rollout(ctx context.Context, rolloutID string) (*Rollout, error) {
	return r.fetchRollout(ctx, &controlpb.GetRolloutStatusRequest{RolloutId: rolloutID})
}

// RolloutByModel is the resolver for the rolloutByModel field — the
// current/most recent rollout for a model, which is what Model Studio
// shows beside a model's active version.
func (r *queryResolver) RolloutByModel(ctx context.Context, modelRef string) (*Rollout, error) {
	return r.fetchRollout(ctx, &controlpb.GetRolloutStatusRequest{ModelRef: modelRef})
}

func (r *queryResolver) fetchRollout(ctx context.Context, req *controlpb.GetRolloutStatusRequest) (*Rollout, error) {
	resp, err := r.Control.GetRolloutStatus(ctx, req)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return rolloutFromProto(resp), nil
}

// Rollouts is the resolver for the rollouts field — full history, most
// recent first. Backed by control-plane's ListRollouts RPC (Step U1a),
// NOT by a direct rollout-table read: admin-api has no DB grant there
// (migration 0020), which Step T proved adversarially.
func (r *queryResolver) Rollouts(ctx context.Context) ([]*Rollout, error) {
	resp, err := r.Control.ListRollouts(ctx, &controlpb.ListRolloutsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]*Rollout, 0, len(resp.GetRollouts()))
	for _, ro := range resp.GetRollouts() {
		out = append(out, rolloutFromProto(ro))
	}
	return out, nil
}

// isNotFound distinguishes control-plane's "that rollout doesn't exist"
// from a genuine transport/server failure. Uses the gRPC status code,
// not string matching on the message — control-plane's own handler maps
// rollout.ErrNotFound to codes.NotFound precisely so callers can tell
// these apart structurally.
func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

func rolloutFromProto(p *controlpb.GetRolloutStatusResponse) *Rollout {
	out := &Rollout{
		RolloutID:     p.GetRolloutId(),
		ModelRef:      p.GetModelRef(),
		VersionID:     p.GetVersionId(),
		StrategyJSON:  "",
		Stage:         p.GetStage(),
		Status:        p.GetStatus(),
		CanaryPercent: int(p.GetCanaryPercent()),
		StartedAt:     p.GetStartedAt(),
	}
	if e := p.GetEndedAt(); e != "" {
		out.EndedAt = &e
	}
	return out
}

// APIKeys is the resolver for the apiKeys field — Step S. Backed by
// apikeys.Store.List, whose own SELECT never names the hash column at
// all (apikeys.go's own doc comment) — this resolver has no hash value
// available to leak even by mistake, and APIKeySummary (models_gen.go)
// has no Hash/RawKey field to put one in regardless. Two independent
// layers (the SQL query itself, and the GraphQL type shape) both make
// "listApiKeys leaks a hash" structurally impossible, not just avoided
// by this function's own care.
func (r *queryResolver) APIKeys(ctx context.Context) ([]*APIKeySummary, error) {
	summaries, err := r.Keys.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*APIKeySummary, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, &APIKeySummary{
			KeyID:     s.KeyID,
			OrgID:     s.OrgID,
			Scopes:    s.Scopes,
			CreatedAt: s.CreatedAt,
			RevokedAt: s.RevokedAt,
		})
	}
	return out, nil
}

// The three Prometheus expressions behind the dashboard's SLO numbers.
// edge-gateway, not data-plane: it is the actual front door, so its
// request count and latency are what an operator means by "RPS" and
// "p95". Exported as consts so the resolver test asserts the EXACT
// query text rather than re-deriving it and agreeing with itself.
const (
	QueryRequestsPerSecond = `sum(rate(edge_gateway_requests_total[5m]))`
	QueryP95LatencyMs      = `histogram_quantile(0.95, sum by (le) (rate(edge_gateway_request_duration_seconds_bucket[5m]))) * 1000`
	QueryErrorRate         = `sum(rate(edge_gateway_requests_total{status=~"5.."}[5m])) / sum(rate(edge_gateway_requests_total[5m]))`
)

// DashboardMetrics is the resolver for the dashboardMetrics field.
// Three numbers from Prometheus, two counts from control-plane — all
// real, none simulated. Part R's fuller vision (ClickHouse near-real-
// time + Redpanda WS live tail) needs F6/P13; this is the Phase-05
// plan's own "modest but genuinely real this phase" scope.
//
// A Prometheus failure degrades that number to 0 and logs, rather than
// failing the whole query: the model/rollout counts come from a
// different backend entirely and are still perfectly good, so taking
// the entire dashboard down because one metrics backend blipped would
// lose real information for no safety gain (nothing is being decided
// here — see promclient's own QueryScalar comment on why this
// degradation is right HERE and wrong for the canary SLO gate).
func (r *queryResolver) DashboardMetrics(ctx context.Context) (*DashboardMetrics, error) {
	out := &DashboardMetrics{}

	out.RequestsPerSecond = r.scalarOrZero(ctx, "requests_per_second", QueryRequestsPerSecond)
	out.P95LatencyMs = r.scalarOrZero(ctx, "p95_latency_ms", QueryP95LatencyMs)
	out.ErrorRate = r.scalarOrZero(ctx, "error_rate", QueryErrorRate)

	models, err := r.Control.ListModels(ctx, &controlpb.ListModelsRequest{})
	if err != nil {
		return nil, err
	}
	out.ActiveModelsCount = len(models.GetModels())

	rollouts, err := r.Control.ListRollouts(ctx, &controlpb.ListRolloutsRequest{})
	if err != nil {
		return nil, err
	}
	for _, ro := range rollouts.GetRollouts() {
		if ro.GetStatus() == "running" {
			out.ActiveRolloutsCount++
		}
	}
	return out, nil
}

func (r *queryResolver) scalarOrZero(ctx context.Context, name, promQL string) float64 {
	v, err := r.Metrics.QueryScalar(ctx, promQL)
	if err != nil {
		r.Log.Error("dashboardMetrics: prometheus query failed, reporting 0", "metric", name, "error", err)
		return 0
	}
	return v
}

// AuditLog is the resolver for the auditLog field. Direct CockroachDB
// read — audit_log is one of the four tables admin_api genuinely has a
// grant on (SELECT+INSERT, migration 0018), so unlike rollout this
// needs no control-plane round trip.
func (r *queryResolver) AuditLog(ctx context.Context, limit *int, actor *string, action *string) ([]*AuditEntry, error) {
	l := 0
	if limit != nil {
		l = *limit
	}
	var a, act string
	if actor != nil {
		a = *actor
	}
	if action != nil {
		act = *action
	}

	records, err := r.Audit.List(ctx, l, a, act)
	if err != nil {
		return nil, err
	}
	out := make([]*AuditEntry, 0, len(records))
	for _, rec := range records {
		out = append(out, &AuditEntry{
			AuditID:    rec.AuditID,
			Actor:      rec.Actor,
			Action:     rec.Action,
			Target:     rec.Target,
			BeforeJSON: rec.BeforeJSON,
			AfterJSON:  rec.AfterJSON,
			Ts:         rec.TS,
		})
	}
	return out, nil
}

// ProviderHealth is the resolver for the providerHealth field — the
// Provider Console (Part R). Backed by provider-gateway's own existing
// ProviderHealth RPC (Phase-02, unchanged), reached over the mesh rule
// added this same step.
//
// healthy is DERIVED here rather than read from a field, because
// provider.proto has no such field — a provider is healthy exactly when
// its circuit breaker is closed. Deriving it in one place keeps the
// panel from re-implementing that judgement per view.
func (r *queryResolver) ProviderHealth(ctx context.Context) ([]*ProviderHealth, error) {
	resp, err := r.Providers.ProviderHealth(ctx, &providerpb.ProviderHealthRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]*ProviderHealth, 0, len(resp.GetStatuses()))
	for _, s := range resp.GetStatuses() {
		out = append(out, &ProviderHealth{
			Provider:      s.GetProvider(),
			Healthy:       s.GetBreakerState() == providerpb.BreakerState_BREAKER_STATE_CLOSED,
			QuotaHeadroom: float64(s.GetQuotaHeadroom()),
			BreakerState:  breakerStateName(s.GetBreakerState()),
		})
	}
	return out, nil
}

// breakerStateName maps the enum to the same lowercase words
// provider-gateway's own breaker package uses, rather than leaking
// protobuf's BREAKER_STATE_ prefix into the panel's UI text.
func breakerStateName(s providerpb.BreakerState) string {
	switch s {
	case providerpb.BreakerState_BREAKER_STATE_CLOSED:
		return "closed"
	case providerpb.BreakerState_BREAKER_STATE_OPEN:
		return "open"
	case providerpb.BreakerState_BREAKER_STATE_HALF_OPEN:
		return "half_open"
	default:
		return "unspecified"
	}
}

// Query returns QueryResolver implementation.
func (r *Resolver) Query() QueryResolver { return &queryResolver{r} }

type queryResolver struct{ *Resolver }
