package credentials

import (
	"context"
	"time"

	controlv1 "github.com/onezox/OneZox/services/provider-gateway/internal/pb/control/v1"
)

// GRPCFetcher wraps control-plane's ControlService client — the real
// TokenFetcher. Plain/insecure gRPC channel credentials at the app layer
// (main.go dials it), matching this repo's established cross-service
// pattern (data-plane's own provider-gateway client dial): Cilium's
// SPIFFE/SPIRE-backed mTLS enforcement (control-plane-mtls,
// authentication: required, Step K) happens transparently at the network
// layer, not something application code manages.
type GRPCFetcher struct {
	client controlv1.ControlServiceClient
}

func NewGRPCFetcher(client controlv1.ControlServiceClient) *GRPCFetcher {
	return &GRPCFetcher{client: client}
}

func (f *GRPCFetcher) FetchToken(ctx context.Context, provider, scope string) (string, time.Duration, error) {
	resp, err := f.client.IssueProviderToken(ctx, &controlv1.IssueProviderTokenRequest{
		Provider: provider,
		Scope:    scope,
	})
	if err != nil {
		return "", 0, err
	}
	return resp.GetToken(), time.Duration(resp.GetTtlSeconds()) * time.Second, nil
}
