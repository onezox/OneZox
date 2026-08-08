package graph

// THIS CODE WILL BE UPDATED WITH SCHEMA CHANGES. PREVIOUS IMPLEMENTATION FOR SCHEMA CHANGES WILL BE KEPT IN THE COMMENT SECTION. IMPLEMENTATION FOR UNCHANGED SCHEMA WILL BE KEPT.

import (
	"context"

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
)

// Resolver's fields are added to as each query lands its own real
// implementation (Step S: Keys) — every other field here stays a
// gqlgen panic("not implemented") stub until its own step.
type Resolver struct {
	Keys apikeys.Store
}

// Me is the resolver for the me field.
func (r *queryResolver) Me(ctx context.Context) (*AdminUser, error) {
	panic("not implemented")
}

// Models is the resolver for the models field.
func (r *queryResolver) Models(ctx context.Context) ([]*ModelSummary, error) {
	panic("not implemented")
}

// ModelDraft is the resolver for the modelDraft field.
func (r *queryResolver) ModelDraft(ctx context.Context, modelRef string) (*ModelManifest, error) {
	panic("not implemented")
}

// ModelVersion is the resolver for the modelVersion field.
func (r *queryResolver) ModelVersion(ctx context.Context, modelRef string, versionID *string) (*ModelManifest, error) {
	panic("not implemented")
}

// Rollout is the resolver for the rollout field.
func (r *queryResolver) Rollout(ctx context.Context, rolloutID string) (*Rollout, error) {
	panic("not implemented")
}

// RolloutByModel is the resolver for the rolloutByModel field.
func (r *queryResolver) RolloutByModel(ctx context.Context, modelRef string) (*Rollout, error) {
	panic("not implemented")
}

// Rollouts is the resolver for the rollouts field.
func (r *queryResolver) Rollouts(ctx context.Context) ([]*Rollout, error) {
	panic("not implemented")
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

// DashboardMetrics is the resolver for the dashboardMetrics field.
func (r *queryResolver) DashboardMetrics(ctx context.Context) (*DashboardMetrics, error) {
	panic("not implemented")
}

// AuditLog is the resolver for the auditLog field.
func (r *queryResolver) AuditLog(ctx context.Context, limit *int, actor *string, action *string) ([]*AuditEntry, error) {
	panic("not implemented")
}

// ProviderHealth is the resolver for the providerHealth field.
func (r *queryResolver) ProviderHealth(ctx context.Context) ([]*ProviderHealth, error) {
	panic("not implemented")
}

// Query returns QueryResolver implementation.
func (r *Resolver) Query() QueryResolver { return &queryResolver{r} }

type queryResolver struct{ *Resolver }

// !!! WARNING !!!
// The code below was going to be deleted when updating resolvers. It has been copied here so you have
// one last chance to move it out of harms way if you want. There are two reasons this happens:
//  - When renaming or deleting a resolver the old code will be put in here. You can safely delete
//    it when you're done.
//  - You have helper methods in this file. Move them out to keep these resolver files clean.
/*
	type Resolver struct{}
*/
