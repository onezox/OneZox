package graph

// THIS CODE WILL BE UPDATED WITH SCHEMA CHANGES. PREVIOUS IMPLEMENTATION FOR SCHEMA CHANGES WILL BE KEPT IN THE COMMENT SECTION. IMPLEMENTATION FOR UNCHANGED SCHEMA WILL BE KEPT.

import (
	"context"
)

type Resolver struct{}

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

// APIKeys is the resolver for the apiKeys field.
func (r *queryResolver) APIKeys(ctx context.Context) ([]*APIKeySummary, error) {
	panic("not implemented")
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
