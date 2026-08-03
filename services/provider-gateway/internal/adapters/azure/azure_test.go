package azure

import (
	"context"
	"errors"
	"testing"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

func TestInvokeReturnsNotImplemented(t *testing.T) {
	a := New()
	if a.Name() != ProviderName {
		t.Errorf("Name() = %q, want %q", a.Name(), ProviderName)
	}
	_, err := a.Invoke(context.Background(), adapters.InvokeRequest{})
	if !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Invoke() error = %v, want ErrNotImplemented", err)
	}
}
