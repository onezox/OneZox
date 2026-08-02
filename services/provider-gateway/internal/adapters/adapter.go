// Package adapters defines the internal contract every provider adapter
// (openai, anthropic, google, and the test-only fake) implements, and the
// registry provider-gateway's Invoke handler dispatches through. Part G.1:
// "Provider adapters behind one internal contract."
package adapters

import (
	"context"
	"fmt"
	"strings"
)

// Delta is one item of a provider's streamed response, already normalized
// to provider-gateway's own internal shape — independent of whichever
// wire format (SSE, chunked JSON, gRPC) the specific adapter's upstream
// actually speaks.
type Delta struct {
	Content           *string
	FinishReason      *string
	IsFinal           bool
	PrefixCacheHandle *string
}

// Stream is deliberately pull-based (Recv, not a pushed channel): the
// caller (provider-gateway's own gRPC Invoke handler) only calls Recv
// again once it has finished forwarding the previous Delta to its own
// caller. That sequencing is what gives the whole pipeline backpressure
// for free (Step H) — an adapter's upstream HTTP body read naturally
// pauses whenever the outer gRPC Send blocks on a slow client, with no
// manual buffering anywhere in between, the same discipline Phase-01's
// edge-gateway SSE relay used.
type Stream interface {
	// Recv returns the next Delta, io.EOF once the stream is genuinely
	// done, or any other error if the adapter call failed.
	Recv() (Delta, error)
}

// Adapter is the one internal contract every provider (real or fake) is
// normalized behind (Part G.1). Name identifies the provider for Redis
// key prefixes (provider:{name}:quota:{window}, provider:{name}:breaker)
// and registry lookup — not necessarily the same string as worker_ref's
// own "<provider>:<model>" prefix, though today it is.
type Adapter interface {
	Name() string
	Invoke(ctx context.Context, req InvokeRequest) (Stream, error)
}

// InvokeRequest is the adapter-facing view of proto/provider's
// InvokeRequest — plain Go types, not generated protobuf types, so
// adapters don't need to import the pb package or worry about oneof
// plumbing. The gRPC handler (Step D5) is the only place that translates
// between the two.
type InvokeRequest struct {
	RequestID   string
	Model       string // worker_ref with the "<provider>:" prefix stripped
	Messages    []Message
	MaxTokens   *int32
	Temperature *float32
}

type Message struct {
	Role    string
	Content string
}

// Registry looks up an Adapter by provider name, parsed from a
// worker_ref's "<provider>:<model>" convention (Step A1's proto doc
// comment). Phase-02 has no scheduler or model registry yet
// (Dependencies.txt F10) — this parsing IS the routing.
type Registry struct {
	adapters map[string]Adapter
}

func NewRegistry(adapters ...Adapter) *Registry {
	r := &Registry{adapters: make(map[string]Adapter, len(adapters))}
	for _, a := range adapters {
		r.adapters[a.Name()] = a
	}
	return r
}

// ErrUnknownProvider is returned by Lookup when worker_ref's provider
// prefix doesn't match any registered adapter.
type ErrUnknownProvider struct {
	Provider string
}

func (e ErrUnknownProvider) Error() string {
	return fmt.Sprintf("no adapter registered for provider %q", e.Provider)
}

func (r *Registry) Lookup(provider string) (Adapter, error) {
	a, ok := r.adapters[provider]
	if !ok {
		return nil, ErrUnknownProvider{Provider: provider}
	}
	return a, nil
}

// ParseWorkerRef splits worker_ref's "<provider>:<model>" convention
// (Step A1's proto.go doc comment) into its two parts. Model may itself
// contain colons (e.g. "fake:fail:503" for the fake adapter's per-call
// mode+status control) — only the first colon is a delimiter.
func ParseWorkerRef(workerRef string) (provider, model string, err error) {
	provider, model, ok := strings.Cut(workerRef, ":")
	if !ok || provider == "" {
		return "", "", fmt.Errorf("worker_ref %q is not in \"<provider>:<model>\" form", workerRef)
	}
	return provider, model, nil
}
