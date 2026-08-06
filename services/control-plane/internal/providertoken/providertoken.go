// Package providertoken implements IssueProviderToken (Phase-04.txt's own
// "Vault-backed; gateway only" RPC) — control-plane reads a provider's
// real API key from Vault (secret/provider/{name}, Step I) and hands it
// back with a short TTL.
//
// Design note, stated explicitly rather than assumed: OpenAI/Anthropic/
// Grok/GLM/Kimi are static third-party API keys, not Vault dynamic/leased
// secrets — Vault has no plugin that mints a fresh, genuinely-expiring
// credential for any of them. "Short-lived token" here means the VALUE
// this call returns must be treated by the caller as expiring after
// ttl_seconds and re-fetched, not cached indefinitely — that's what
// actually bounds exposure and gives centralized revocation reach
// (rotating the key in Vault takes effect on the caller's next refresh,
// no redeploy needed), not a literal new secret minted per call.
package providertoken

import (
	"context"
	"log/slog"
	"time"
)

// SecretReader is the Vault dependency this package needs — vaultclient.Client
// satisfies it directly; tests use a fake.
type SecretReader interface {
	ReadProviderSecret(ctx context.Context, provider string) (apiKey string, err error)
}

type Service struct {
	reader SecretReader
	ttl    time.Duration
	log    *slog.Logger
}

func NewService(reader SecretReader, ttl time.Duration, log *slog.Logger) *Service {
	return &Service{reader: reader, ttl: ttl, log: log}
}

// IssueToken returns the requested provider's current API key and the TTL
// the caller must honor. scope is accepted and logged but not yet
// validated against a fixed set of values — reserved for finer-grained
// scoping if a future need arises; inventing meaning for it now would be
// scope Phase-04.txt doesn't call for.
func (s *Service) IssueToken(ctx context.Context, provider, scope string) (token string, ttlSeconds int64, err error) {
	apiKey, err := s.reader.ReadProviderSecret(ctx, provider)
	if err != nil {
		return "", 0, err
	}

	ttl := int64(s.ttl.Seconds())
	// Never logs apiKey itself — only that a token for this provider/scope
	// was issued, same "log presence, never the value" discipline
	// provider-gateway's own credential handling already established.
	s.log.Info("issued provider token", "provider", provider, "scope", scope, "ttl_seconds", ttl)
	return apiKey, ttl, nil
}
