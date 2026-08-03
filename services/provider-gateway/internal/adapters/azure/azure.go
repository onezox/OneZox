// Package azure is a Step J7 SCAFFOLD ONLY — Phase-02.txt's own folder
// structure lists adapters/{openai,anthropic,google,azure,bedrock,vllm},
// and its FEATURES IMPLEMENTED line is explicit: "Azure/Bedrock/self-host
// adapters scaffolded" (self-host wired in Phase-11; Azure/Bedrock have no
// wire-in phase assigned anywhere in the roadmap yet). This package
// satisfies adapters.Adapter so the shape exists and compiles, but is
// deliberately NOT registered in main.go's registry — worker_ref
// "azure:*" resolves to ErrUnknownProvider exactly like any other
// unregistered name, not to this adapter. No HTTP client, no request/
// response normalization: that's real implementation work for whichever
// future phase actually wires this in, per CLAUDE.md's "don't build
// components that belong to a later phase."
package azure

import (
	"context"
	"errors"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

const ProviderName = "azure"

// ErrNotImplemented is returned by Invoke unconditionally — a scaffold
// adapter must fail loudly and typed if it's ever accidentally wired in
// before real implementation lands, not silently accept traffic.
var ErrNotImplemented = errors.New("azure adapter: scaffolded in Phase-02 Step J7, not implemented")

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return ProviderName }

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	return nil, ErrNotImplemented
}
