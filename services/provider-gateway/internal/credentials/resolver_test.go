package credentials

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRefreshVaultSucceedsSourceIsVault(t *testing.T) {
	fetcher := NewFakeFetcher()
	fetcher.Tokens["openai"] = "sk-from-vault"
	adapter := &FakeAdapter{}
	r := NewResolver("openai", "OPENAI_API_KEY", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if adapter.Key != "sk-from-vault" {
		t.Errorf("adapter key = %q, want sk-from-vault", adapter.Key)
	}
	if r.CurrentSource() != SourceVault {
		t.Errorf("source = %q, want %q", r.CurrentSource(), SourceVault)
	}
}

// TestRefreshVaultUnavailableFallsBackToEnv: the core dual-path property
// — when control-plane/Vault is unreachable, the resolver must fall back
// to the K8s Secret env var, not fail outright, and must record the
// fallback as the source (not silently claim vault).
func TestRefreshVaultUnavailableFallsBackToEnv(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "sk-from-k8s-secret")

	fetcher := NewFakeFetcher()
	fetcher.Err = errors.New("control-plane unreachable")
	adapter := &FakeAdapter{}
	r := NewResolver("openai", "OPENAI_API_KEY", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if adapter.Key != "sk-from-k8s-secret" {
		t.Errorf("adapter key = %q, want sk-from-k8s-secret", adapter.Key)
	}
	if r.CurrentSource() != SourceK8sSecretFallback {
		t.Errorf("source = %q, want %q", r.CurrentSource(), SourceK8sSecretFallback)
	}
}

// TestRefreshBothPathsFailReturnsError: neither Vault nor the env var
// available must be a hard error, not a silently-empty credential pushed
// into the adapter — matches "proceed with what's available": the caller
// (main.go) must not register this provider's adapter at all in this
// case.
func TestRefreshBothPathsFailReturnsError(t *testing.T) {
	os.Unsetenv("ANTHROPIC_API_KEY_TEST_UNSET")
	fetcher := NewFakeFetcher()
	fetcher.Err = errors.New("control-plane unreachable")
	adapter := &FakeAdapter{Key: "should-not-change"}
	r := NewResolver("anthropic", "ANTHROPIC_API_KEY_TEST_UNSET", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expected an error when both vault and fallback fail, got nil")
	}
	if adapter.Key != "should-not-change" {
		t.Errorf("adapter key was mutated on total failure: %q", adapter.Key)
	}
	if r.CurrentSource() != sourceNone {
		t.Errorf("source = %q, want empty (no credential resolved)", r.CurrentSource())
	}
}

// TestRefreshRecoversFromFallbackToVault: once Vault becomes reachable
// again, the NEXT refresh must switch back to vault as the source — the
// fallback is not sticky.
func TestRefreshRecoversFromFallbackToVault(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "sk-from-k8s-secret")

	fetcher := NewFakeFetcher()
	fetcher.Tokens["openai"] = "sk-from-vault"
	fetcher.Err = errors.New("control-plane temporarily unreachable")
	adapter := &FakeAdapter{}
	r := NewResolver("openai", "OPENAI_API_KEY", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if r.CurrentSource() != SourceK8sSecretFallback {
		t.Fatalf("expected fallback on first refresh, got %q", r.CurrentSource())
	}

	fetcher.Err = nil // vault recovers
	if _, err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	if r.CurrentSource() != SourceVault {
		t.Errorf("source after recovery = %q, want %q", r.CurrentSource(), SourceVault)
	}
	if adapter.Key != "sk-from-vault" {
		t.Errorf("adapter key after recovery = %q, want sk-from-vault", adapter.Key)
	}
}
