package pricing

import (
	"context"
	"errors"
	"testing"
)

func TestSetAndGetCurrentPricing(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	costs := UnitCosts{InputPerMillionTokens: 0.15, OutputPerMillionTokens: 0.60, Currency: "USD"}
	if err := svc.SetPricing(ctx, "openai", costs); err != nil {
		t.Fatalf("SetPricing: %v", err)
	}

	got, err := svc.GetCurrentPricing(ctx, "openai")
	if err != nil {
		t.Fatalf("GetCurrentPricing: %v", err)
	}
	if got.UnitCosts != costs {
		t.Errorf("UnitCosts = %+v, want %+v", got.UnitCosts, costs)
	}
}

// TestLatestPricingWins: a second SetPricing call for the same model_ref
// must become "current" — pricing changes over time via new rows, never
// an in-place update (no immutability enforcement requires this, but the
// application semantics do: effective_at is what "current" means).
func TestLatestPricingWins(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	old := UnitCosts{InputPerMillionTokens: 1.0, OutputPerMillionTokens: 2.0, Currency: "USD"}
	newer := UnitCosts{InputPerMillionTokens: 0.5, OutputPerMillionTokens: 1.0, Currency: "USD"}

	if err := svc.SetPricing(ctx, "anthropic", old); err != nil {
		t.Fatalf("SetPricing(old): %v", err)
	}
	if err := svc.SetPricing(ctx, "anthropic", newer); err != nil {
		t.Fatalf("SetPricing(newer): %v", err)
	}

	got, err := svc.GetCurrentPricing(ctx, "anthropic")
	if err != nil {
		t.Fatalf("GetCurrentPricing: %v", err)
	}
	if got.UnitCosts != newer {
		t.Errorf("current pricing = %+v, want the most recently set %+v", got.UnitCosts, newer)
	}
}

func TestGetCurrentPricingNotFound(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	if _, err := svc.GetCurrentPricing(ctx, "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestListCurrentPricing(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	for _, ref := range []string{"openai", "anthropic", "grok", "glm", "kimi"} {
		if err := svc.SetPricing(ctx, ref, UnitCosts{InputPerMillionTokens: 1, OutputPerMillionTokens: 2, Currency: "USD"}); err != nil {
			t.Fatalf("SetPricing(%s): %v", ref, err)
		}
	}

	entries, err := svc.ListCurrentPricing(ctx)
	if err != nil {
		t.Fatalf("ListCurrentPricing: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(entries))
	}
}
