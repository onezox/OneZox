package credentials

import (
	"context"
	"fmt"
	"time"
)

// FakeFetcher is an in-memory TokenFetcher for unit tests — no
// control-plane/Vault needed. Set Err to simulate control-plane being
// unreachable (the condition that must trigger fallback).
type FakeFetcher struct {
	Tokens map[string]string
	TTL    time.Duration
	Err    error
}

func NewFakeFetcher() *FakeFetcher {
	return &FakeFetcher{Tokens: make(map[string]string), TTL: 5 * time.Minute}
}

func (f *FakeFetcher) FetchToken(ctx context.Context, provider, scope string) (string, time.Duration, error) {
	if f.Err != nil {
		return "", 0, f.Err
	}
	token, ok := f.Tokens[provider]
	if !ok {
		return "", 0, fmt.Errorf("no fake token for provider: %s", provider)
	}
	return token, f.TTL, nil
}

// FakeAdapter is an in-memory KeySetter for unit tests.
type FakeAdapter struct {
	Key string
}

func (f *FakeAdapter) SetAPIKey(key string) {
	f.Key = key
}
