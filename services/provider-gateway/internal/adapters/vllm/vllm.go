// Package vllm is a Step J7 SCAFFOLD ONLY, for the self-hosted-model
// adapter Phase-02.txt names explicitly: "Azure/Bedrock/self-host
// adapters scaffolded; self-host WIRED IN PHASE-11" — the one adapter in
// this trio with a concrete future phase attached (Phase-11: Secure
// Execution, real GPU inference pools, Firecracker sandbox). Directory
// name "vllm" matches Phase-02.txt's own folder-structure line
// (adapters/{...,vllm}), vLLM being the serving stack self-hosted
// inference is expected to run behind. Satisfies adapters.Adapter so the
// shape exists and compiles; not registered in main.go's registry — real
// implementation (prefix-cache-aware KV reuse per Part P, GPU pool
// integration) is explicitly Phase-11 scope, not this phase's.
package vllm

import (
	"context"
	"errors"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

const ProviderName = "vllm"

var ErrNotImplemented = errors.New("vllm adapter: scaffolded in Phase-02 Step J7, wired in Phase-11")

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return ProviderName }

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	return nil, ErrNotImplemented
}
