// Package credentials implements Phase-04 Step M's F9 cutover: for each
// real provider, resolve its API key VAULT-FIRST (via control-plane's
// IssueProviderToken RPC, Step J) with a K8s-Secret env var FALLBACK — the
// gap-free sequencing the whole cutover depends on: this dual-path code
// deploys and proves itself BEFORE the fallback is ever removed (Step O)
// or the underlying Secret deleted (Step P).
//
// A resolved credential is pushed into the already-registered adapter via
// SetAPIKey (openai/anthropic, both now atomic.Pointer-backed for exactly
// this) and periodically refreshed well before Vault's own short TTL
// expires — a credential fetched once at startup and never revisited
// would make Step N's "prove pods are genuinely using Vault" and Step P's
// "poison the old Secret and prove nothing breaks" both meaningless,
// since neither Vault nor the Secret would ever be re-read on a live pod.
//
// credential_source is logged on every refresh (vault vs
// k8s-secret-fallback) — this is the exact signal Step N's live
// verification reads to positively confirm Vault usage, not infer it from
// "the pod is healthy."
package credentials

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

type Source string

const (
	SourceVault             Source = "vault"
	SourceK8sSecretFallback Source = "k8s-secret-fallback"
	sourceNone              Source = ""
)

// refreshBuffer mirrors vaultclient.Client's own renewal-buffer reasoning
// on the control-plane side: refresh before the TTL expires, not exactly
// at it.
const refreshBuffer = 30 * time.Second

// fallbackRetryInterval governs both how soon a fallback-sourced
// credential gets re-attempted against Vault (so a later-recovering Vault
// is picked up promptly, not stuck on fallback indefinitely) and the
// retry cadence after a hard failure (neither path worked).
const fallbackRetryInterval = 30 * time.Second

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

// Resolver owns one provider's credential lifecycle: dual-path resolution
// plus periodic background refresh.
type Resolver struct {
	provider   string
	envVarName string
	fetcher    TokenFetcher
	adapter    KeySetter
	log        *slog.Logger

	mu     sync.Mutex
	source Source
}

func NewResolver(provider, envVarName string, fetcher TokenFetcher, adapter KeySetter, log *slog.Logger) *Resolver {
	return &Resolver{
		provider:   provider,
		envVarName: envVarName,
		fetcher:    fetcher,
		adapter:    adapter,
		log:        log,
	}
}

func (r *Resolver) setSource(s Source) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.source = s
}

// CurrentSource reports which path last successfully supplied a
// credential — "" if neither ever has. Used by /readyz (Step M) and by
// Step N's own live verification.
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

// Refresh resolves the current credential (Vault-first, K8s-Secret
// fallback), pushes it into the adapter, and returns how long to wait
// before the next refresh. Returns an error only when BOTH paths fail —
// same "proceed with what's available" discipline as the rest of this
// service; a caller that gets an error should not register/keep this
// provider's adapter at all.
func (r *Resolver) Refresh(ctx context.Context) (time.Duration, error) {
	token, ttl, err := r.fetcher.FetchToken(ctx, r.provider, "invoke")
	if err == nil && token != "" {
		r.adapter.SetAPIKey(token)
		r.setSource(SourceVault)
		r.log.Info("provider credential refreshed",
			"provider", r.provider, "credential_source", string(SourceVault), "ttl_seconds", int(ttl.Seconds()))

		next := ttl - refreshBuffer
		if next <= 0 {
			next = ttl
		}
		if next <= 0 {
			next = fallbackRetryInterval
		}
		return next, nil
	}

	fallbackKey := os.Getenv(r.envVarName)
	if fallbackKey == "" {
		r.setSource(sourceNone)
		return 0, fmt.Errorf("no credential available for %s: vault fetch failed (%v) and %s is unset", r.provider, err, r.envVarName)
	}

	r.adapter.SetAPIKey(fallbackKey)
	r.setSource(SourceK8sSecretFallback)
	// Warn, not Error: a live system correctly degrading to its fallback
	// is not itself a system failure — but it IS the exact condition Step
	// N's own live check must be able to positively rule out, hence
	// logging credential_source on every refresh regardless of outcome,
	// not only on the vault path.
	r.log.Warn("provider credential using K8s-Secret fallback, vault unavailable",
		"provider", r.provider, "credential_source", string(SourceK8sSecretFallback), "vault_error", err)
	return fallbackRetryInterval, nil
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
			wait = fallbackRetryInterval
		}
	}
}
