package providertoken

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func testService(secrets map[string]string) *Service {
	reader := NewFakeSecretReader()
	reader.Secrets = secrets
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(reader, 5*time.Minute, log)
}

func TestIssueTokenReturnsScopedValueAndShortTTL(t *testing.T) {
	ctx := context.Background()
	svc := testService(map[string]string{
		"openai":    "sk-openai-fake",
		"anthropic": "sk-anthropic-fake",
	})

	token, ttl, err := svc.IssueToken(ctx, "openai", "invoke")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if token != "sk-openai-fake" {
		t.Errorf("token = %q, want the openai secret only, not any other provider's", token)
	}
	if ttl != 300 {
		t.Errorf("ttl_seconds = %d, want 300 (5m)", ttl)
	}
}

// TestIssueTokenScopedToRequestedProviderOnly: asking for one provider
// must never leak another's value — the whole point of "scoped to only
// the provider secrets requested," not a bundle of everything
// control-plane's own Vault policy happens to have access to.
func TestIssueTokenScopedToRequestedProviderOnly(t *testing.T) {
	ctx := context.Background()
	svc := testService(map[string]string{
		"openai":    "sk-openai-fake",
		"anthropic": "sk-anthropic-fake",
	})

	token, _, err := svc.IssueToken(ctx, "anthropic", "invoke")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if token == "sk-openai-fake" {
		t.Fatal("got openai's secret when anthropic was requested")
	}
	if token != "sk-anthropic-fake" {
		t.Errorf("token = %q, want sk-anthropic-fake", token)
	}
}

func TestIssueTokenUnknownProvider(t *testing.T) {
	ctx := context.Background()
	svc := testService(map[string]string{"openai": "sk-openai-fake"})

	if _, _, err := svc.IssueToken(ctx, "does-not-exist", "invoke"); err == nil {
		t.Fatal("expected an error for an unknown provider, got nil")
	}
}
