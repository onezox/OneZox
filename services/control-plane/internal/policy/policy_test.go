package policy

import (
	"context"
	"errors"
	"testing"
)

func TestSetAndGetCurrentPolicy(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	if err := svc.SetPolicy(ctx, "org-1", `{"max_rpm":100}`); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}

	got, err := svc.GetCurrentPolicy(ctx, "org-1")
	if err != nil {
		t.Fatalf("GetCurrentPolicy: %v", err)
	}
	if got.RulesJSON != `{"max_rpm":100}` {
		t.Errorf("RulesJSON = %q", got.RulesJSON)
	}
}

func TestLatestPolicyWins(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	if err := svc.SetPolicy(ctx, "org-1", `{"max_rpm":100}`); err != nil {
		t.Fatalf("SetPolicy(1): %v", err)
	}
	if err := svc.SetPolicy(ctx, "org-1", `{"max_rpm":200}`); err != nil {
		t.Fatalf("SetPolicy(2): %v", err)
	}

	got, err := svc.GetCurrentPolicy(ctx, "org-1")
	if err != nil {
		t.Fatalf("GetCurrentPolicy: %v", err)
	}
	if got.RulesJSON != `{"max_rpm":200}` {
		t.Errorf("current policy = %q, want the most recently set value", got.RulesJSON)
	}
}

func TestGetCurrentPolicyNotFound(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewFakeStore())

	if _, err := svc.GetCurrentPolicy(ctx, "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
