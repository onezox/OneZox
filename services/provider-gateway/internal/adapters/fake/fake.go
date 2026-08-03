// Package fake is the test-only adapter that talks to the provider-fake
// harness (Step D1) instead of a real provider. Registered under the
// provider name "fake" so its Redis quota/breaker keys
// (provider:fake:quota:*, provider:fake:breaker) never overlap with any
// real provider's fleet state — induced failures here can never affect
// real-provider resilience accounting.
//
// worker_ref's model portion (after the "fake:" prefix) selects
// provider-fake's per-request mode directly: "normal", "slow", "fail", or
// "fail:<status>" for a specific induced status code (default 503 if
// omitted). This is deliberately NOT part of proto/provider's public
// contract — it's a test-harness-only convention layered onto the
// existing worker_ref field, the same way an interim measure never leaks
// into a public contract (Dependencies.txt's forward-reference rule,
// applied here by analogy even though this isn't a forward reference).
package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

const ProviderName = "fake"

type Adapter struct {
	baseURL    string
	httpClient *http.Client
}

func New(baseURL string) *Adapter {
	return &Adapter{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *Adapter) Name() string { return ProviderName }

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type wireRequest struct {
	RequestID  string        `json:"request_id"`
	Messages   []wireMessage `json:"messages"`
	Mode       string        `json:"mode"`
	FailStatus int           `json:"fail_status,omitempty"`
}

type wireChunk struct {
	RequestID         string  `json:"request_id"`
	Content           *string `json:"content,omitempty"`
	FinishReason      *string `json:"finish_reason,omitempty"`
	IsFinal           bool    `json:"is_final"`
	PrefixCacheHandle *string `json:"prefix_cache_handle,omitempty"`
	InputTokens       *int32  `json:"input_tokens,omitempty"`
	OutputTokens      *int32  `json:"output_tokens,omitempty"`
}

// parseMode turns worker_ref's model portion ("normal" | "slow" | "fail" |
// "fail:<status>" | "fail_mid_stream") into provider-fake's own wire
// fields.
func parseMode(model string) (mode string, failStatus int, err error) {
	provider, rest, _ := strings.Cut(model, ":")
	switch provider {
	case "normal", "slow", "fail_mid_stream":
		return provider, 0, nil
	case "fail":
		if rest == "" {
			return "fail", 0, nil
		}
		status, err := strconv.Atoi(rest)
		if err != nil {
			return "", 0, fmt.Errorf("fake adapter: invalid fail status %q: %w", rest, err)
		}
		return "fail", status, nil
	default:
		return "", 0, fmt.Errorf("fake adapter: unknown mode %q (want normal, slow, fail, fail:<status>, or fail_mid_stream)", model)
	}
}

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	mode, failStatus, err := parseMode(req.Model)
	if err != nil {
		return nil, err
	}

	messages := make([]wireMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = wireMessage{Role: m.Role, Content: m.Content}
	}

	body, err := json.Marshal(wireRequest{
		RequestID:  req.RequestID,
		Messages:   messages,
		Mode:       mode,
		FailStatus: failStatus,
	})
	if err != nil {
		return nil, fmt.Errorf("fake adapter: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/fake-complete", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("fake adapter: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("fake adapter: request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		return nil, &UpstreamError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}

	return &stream{decoder: json.NewDecoder(resp.Body), body: resp.Body}, nil
}

// UpstreamError carries the fake's induced HTTP status back to the
// caller (Step F's breaker logic distinguishes this from a transport-
// level error) — a typed error, not a bare fmt.Errorf, so breaker.go
// doesn't have to string-match to tell "provider said no" apart from
// "couldn't even reach it".
type UpstreamError struct {
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("fake adapter: upstream returned status %d: %s", e.StatusCode, e.Body)
}

type stream struct {
	decoder *json.Decoder
	body    io.ReadCloser
}

func (s *stream) Recv() (adapters.Delta, error) {
	var chunk wireChunk
	if err := s.decoder.Decode(&chunk); err != nil {
		_ = s.body.Close()
		if err == io.EOF {
			return adapters.Delta{}, io.EOF
		}
		return adapters.Delta{}, fmt.Errorf("fake adapter: decode chunk: %w", err)
	}
	if chunk.IsFinal {
		_ = s.body.Close()
	}
	return adapters.Delta{
		Content:           chunk.Content,
		FinishReason:      chunk.FinishReason,
		IsFinal:           chunk.IsFinal,
		PrefixCacheHandle: chunk.PrefixCacheHandle,
		InputTokens:       chunk.InputTokens,
		OutputTokens:      chunk.OutputTokens,
	}, nil
}
