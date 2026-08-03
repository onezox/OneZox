// Package bedrock is a Step J7 SCAFFOLD ONLY — same status as the azure
// package's own doc comment: listed in Phase-02.txt's folder structure
// and FEATURES IMPLEMENTED ("Azure/Bedrock/self-host adapters
// scaffolded"), no wire-in phase assigned in the roadmap yet, not
// registered in main.go's registry. Satisfies adapters.Adapter so the
// shape exists and compiles; real AWS SigV4 auth, request/response
// normalization, and wiring are future-phase work.
package bedrock

import (
	"context"
	"errors"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

const ProviderName = "bedrock"

var ErrNotImplemented = errors.New("bedrock adapter: scaffolded in Phase-02 Step J7, not implemented")

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return ProviderName }

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	return nil, ErrNotImplemented
}
