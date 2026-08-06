// Package credentials implements Phase-04's F9 cutover: for each real
// provider, resolve its API key via control-plane's IssueProviderToken
// RPC (Step J).
//
// Step M deployed this dual-path (Vault-first, K8s-Secret env var
// fallback) and Step N positively confirmed — via Vault's own audit log,
// not just this application's self-report — every pod was genuinely
// reading from Vault, with zero fallback firings. Step O removes the
// fallback entirely: this is now Vault-only, by design, not merely
// unused. A pod that cannot reach Vault no longer has any other source
// for a provider's credential — that absence is the point (Step P proves
// it by poisoning the now-vestigial K8s Secret and confirming nothing
// changes).
//
// A resolved credential is pushed into the already-registered adapter via
// SetAPIKey (openai/anthropic, both atomic.Pointer-backed for exactly
// this) and periodically refreshed well before Vault's own short TTL
// expires — a credential fetched once at startup and never revisited
// would make Step P's "poison the old Secret, prove nothing breaks"
// meaningless on a running pod.
package credentials

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type Source string

const (
	SourceVault Source = "vault"
	sourceNone  Source = ""
)

// refreshBuffer mirrors vaultclient.Client's own renewal-buffer reasoning
// on the control-plane side: refresh before the TTL expires, not exactly
// at it.
const refreshBuffer = 30 * time.Second

// errorRetryInterval is the retry cadence after a failed refresh (Vault
// unreachable, no credential resolved).
const errorRetryInterval = 30 * time.Second

// TokenFetcher is control-plane's IssueProviderToken, abstracted for
// testability — grpcFetcher (grpc_fetcher.go) is the real implementation.
type TokenFetcher interface {
	FetchToken(ctx context.Context, provider, scope string) (token string, ttl time.Duration, err error)
}

// KeySetter is what an adapter must support to receive a refreshed key —
// openai.Adapter and anthropic.Adapter both implement this.
type KeySetter interface {
	SetAPIKey(key string)
}

// Resolver owns one provider's credential lifecycle: Vault resolution
// plus periodic background refresh.
type Resolver struct {
	provider string
	fetcher  TokenFetcher
	adapter  KeySetter
	log      *slog.Logger

	mu     sync.Mutex
	source Source
}

func NewResolver(provider string, fetcher TokenFetcher, adapter KeySetter, log *slog.Logger) *Resolver {
	return &Resolver{
		provider: provider,
		fetcher:  fetcher,
		adapter:  adapter,
		log:      log,
	}
}

func (r *Resolver) setSource(s Source) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.source = s
}

// CurrentSource reports whether a credential is currently resolved — ""
// if not. Used by /readyz.
func (r *Resolver) CurrentSource() Source {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.source
}

// Provider returns which provider this resolver is for — used by
// /readyz's own log line when a credential isn't currently resolved.
func (r *Resolver) Provider() string {
	return r.provider
}

// Refresh resolves the current credential from Vault (via control-plane),
// pushes it into the adapter, and returns how long to wait before the
// next refresh. Returns an error if Vault is unreachable or has no
// secret for this provider — "proceed with what's available" still
// holds: a caller that gets an error should not register/keep this
// provider's adapter at all.
func (r *Resolver) Refresh(ctx context.Context) (time.Duration, error) {
	token, ttl, err := r.fetcher.FetchToken(ctx, r.provider, "invoke")
	if err != nil || token == "" {
		r.setSource(sourceNone)
		return 0, fmt.Errorf("no credential available for %s: vault fetch failed: %w", r.provider, err)
	}

	r.adapter.SetAPIKey(token)
	r.setSource(SourceVault)
	r.log.Info("provider credential refreshed",
		"provider", r.provider, "credential_source", string(SourceVault), "ttl_seconds", int(ttl.Seconds()))

	next := ttl - refreshBuffer
	if next <= 0 {
		next = ttl
	}
	if next <= 0 {
		next = errorRetryInterval
	}
	return next, nil
}

// Run refreshes in a loop until ctx is done — call as a background
// goroutine after the initial synchronous Refresh (in main) has already
// succeeded once. initialWait is the interval that first Refresh call
// already returned — Run's own loop waits it out BEFORE refreshing again,
// rather than immediately re-refreshing as its first action (which would
// otherwise double the Vault calls made at pod startup: one from the
// caller's synchronous Refresh, a second almost instantly from here).
func (r *Resolver) Run(ctx context.Context, initialWait time.Duration) {
	wait := initialWait
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		var err error
		wait, err = r.Refresh(ctx)
		if err != nil {
			r.log.Error("provider credential refresh failed, no usable credential", "provider", r.provider, "error", err)
			wait = errorRetryInterval
		}
	}
}
