package credentials

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRefreshVaultSucceedsSourceIsVault(t *testing.T) {
	fetcher := NewFakeFetcher()
	fetcher.Tokens["openai"] = "sk-from-vault"
	adapter := &FakeAdapter{}
	r := NewResolver("openai", fetcher, adapter, testLogger())

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

// TestRefreshVaultUnavailableReturnsError: Step O removed the K8s-Secret
// fallback entirely — Vault unreachable must now be a hard error, not a
// silent degrade to anything else, and must never mutate the adapter's
// existing key.
func TestRefreshVaultUnavailableReturnsError(t *testing.T) {
	fetcher := NewFakeFetcher()
	fetcher.Err = errors.New("control-plane unreachable")
	adapter := &FakeAdapter{Key: "should-not-change"}
	r := NewResolver("openai", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expected an error when vault is unreachable, got nil")
	}
	if adapter.Key != "should-not-change" {
		t.Errorf("adapter key was mutated on vault failure: %q", adapter.Key)
	}
	if r.CurrentSource() != sourceNone {
		t.Errorf("source = %q, want empty (no credential resolved)", r.CurrentSource())
	}
}

// TestRefreshRecoversAfterVaultOutage: once Vault becomes reachable
// again, the next refresh must succeed and update the adapter — a
// transient outage doesn't permanently wedge the resolver.
func TestRefreshRecoversAfterVaultOutage(t *testing.T) {
	fetcher := NewFakeFetcher()
	fetcher.Tokens["openai"] = "sk-from-vault"
	fetcher.Err = errors.New("control-plane temporarily unreachable")
	adapter := &FakeAdapter{}
	r := NewResolver("openai", fetcher, adapter, testLogger())

	if _, err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expected an error during the outage, got nil")
	}

	fetcher.Err = nil // vault recovers
	if _, err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh after recovery: %v", err)
	}
	if r.CurrentSource() != SourceVault {
		t.Errorf("source after recovery = %q, want %q", r.CurrentSource(), SourceVault)
	}
	if adapter.Key != "sk-from-vault" {
		t.Errorf("adapter key after recovery = %q, want sk-from-vault", adapter.Key)
	}
}
