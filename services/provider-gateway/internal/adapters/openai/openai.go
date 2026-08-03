// Package openai adapts OpenAI's Chat Completions streaming API to
// provider-gateway's internal adapters.Adapter contract (Part G.1).
// Wire format: POST /v1/chat/completions with "stream": true returns
// Server-Sent Events, each `data:` line a JSON chunk, terminated by a
// literal `data: [DONE]` frame. This adapter stops (IsFinal) as soon as a
// chunk carries a non-null finish_reason — it never needs to see [DONE],
// since Relay (internal/stream) never calls Recv again once IsFinal is
// set (Step H's design).
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/sse"
)

const ProviderName = "openai"

const defaultBaseURL = "https://api.openai.com"

type Adapter struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// New builds an OpenAI adapter. baseURL defaults to the real API if empty
// — a param, not hardcoded, purely so tests can point it at an
// httptest.Server without touching the real endpoint.
func New(apiKey, baseURL string) *Adapter {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Adapter{
		apiKey:     apiKey,
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (a *Adapter) Name() string { return ProviderName }

type wireReqMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type wireRequest struct {
	Model       string           `json:"model"`
	Messages    []wireReqMessage `json:"messages"`
	Stream      bool             `json:"stream"`
	MaxTokens   *int32           `json:"max_tokens,omitempty"`
	Temperature *float32         `json:"temperature,omitempty"`
}

type wireDelta struct {
	Content *string `json:"content"`
}

type wireChoice struct {
	Delta        wireDelta `json:"delta"`
	FinishReason *string   `json:"finish_reason"`
}

type wireChunk struct {
	Choices []wireChoice `json:"choices"`
	Error   *wireError   `json:"error"`
}

type wireError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// UpstreamError carries OpenAI's own HTTP-level failure (auth, rate
// limit, bad request) back to the caller, distinctly from a transport
// error — same typed-error discipline fake.UpstreamError established
// (Step D1) so breaker.go never has to string-match to classify "provider
// said no."
type UpstreamError struct {
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("openai adapter: upstream returned status %d: %s", e.StatusCode, e.Body)
}

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	messages := make([]wireReqMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = wireReqMessage{Role: m.Role, Content: m.Content}
	}

	body, err := json.Marshal(wireRequest{
		Model:       req.Model,
		Messages:    messages,
		Stream:      true,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	})
	if err != nil {
		return nil, fmt.Errorf("openai adapter: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openai adapter: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai adapter: request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		return nil, &UpstreamError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}

	return &stream{scanner: sse.NewScanner(resp.Body), body: resp.Body}, nil
}

type stream struct {
	scanner *sse.Scanner
	body    io.ReadCloser
}

func (s *stream) Recv() (adapters.Delta, error) {
	for {
		ev, err := s.scanner.Next()
		if err != nil {
			_ = s.body.Close()
			if err == io.EOF {
				return adapters.Delta{}, io.EOF
			}
			return adapters.Delta{}, fmt.Errorf("openai adapter: read stream: %w", err)
		}
		if ev.Data == "[DONE]" {
			// Reached without ever seeing a finish_reason — a
			// malformed/truncated stream. Close and report as
			// incomplete (a plain EOF here, like an interrupted
			// body, correctly reads as a breaker failure upstream —
			// Relay/reportingStream in main.go treat "Recv returned
			// an error without ever setting IsFinal" as a failed
			// call).
			_ = s.body.Close()
			return adapters.Delta{}, io.EOF
		}

		var chunk wireChunk
		if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
			_ = s.body.Close()
			return adapters.Delta{}, fmt.Errorf("openai adapter: decode chunk: %w", err)
		}
		if chunk.Error != nil {
			_ = s.body.Close()
			return adapters.Delta{}, fmt.Errorf("openai adapter: stream error: %s", chunk.Error.Message)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		if choice.FinishReason != nil {
			_ = s.body.Close()
			return adapters.Delta{
				Content:      choice.Delta.Content,
				FinishReason: choice.FinishReason,
				IsFinal:      true,
			}, nil
		}
		if choice.Delta.Content == nil {
			// Role-only chunk (the first event sets delta.role with
			// no content) — nothing to forward yet, keep reading.
			continue
		}
		return adapters.Delta{Content: choice.Delta.Content}, nil
	}
}
