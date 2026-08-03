// Package anthropic adapts Anthropic's Messages API streaming format to
// provider-gateway's internal adapters.Adapter contract (Part G.1). Wire
// format is genuinely different from OpenAI's: named SSE events
// (message_start, content_block_delta, message_delta, message_stop, ...)
// rather than one uniform chunk shape, auth via x-api-key + a required
// anthropic-version header (not Authorization: Bearer), and no [DONE]
// sentinel — message_delta carries the stop_reason and is where this
// adapter stops (IsFinal), same "don't wait for the trailing event"
// discipline the openai adapter uses.
package anthropic

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

const ProviderName = "anthropic"

const (
	defaultBaseURL = "https://api.anthropic.com"
	apiVersion     = "2023-06-01"
	// Anthropic requires max_tokens on every request (unlike OpenAI,
	// where it's optional) — this is the fallback when the caller
	// didn't set one, not a tuned production value.
	defaultMaxTokens = 256
)

type Adapter struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

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
	System      string           `json:"system,omitempty"`
	Messages    []wireReqMessage `json:"messages"`
	Stream      bool             `json:"stream"`
	MaxTokens   int32            `json:"max_tokens"`
	Temperature *float32         `json:"temperature,omitempty"`
}

// content_block_delta's data payload.
type wireContentBlockDelta struct {
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

// message_delta's data payload — carries the stop reason, not text.
type wireMessageDelta struct {
	Delta struct {
		StopReason *string `json:"stop_reason"`
	} `json:"delta"`
}

type wireErrorEvent struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

type UpstreamError struct {
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("anthropic adapter: upstream returned status %d: %s", e.StatusCode, e.Body)
}

// splitSystem pulls any "system"-role messages out into Anthropic's
// separate top-level system field (its Messages API doesn't accept
// "system" inside the messages array the way OpenAI/Google-with-mapping
// do) — joined in order, same normalization duty every adapter here owns.
func splitSystem(messages []adapters.Message) (system string, rest []wireReqMessage) {
	var systemParts []string
	for _, m := range messages {
		if m.Role == "system" {
			systemParts = append(systemParts, m.Content)
			continue
		}
		rest = append(rest, wireReqMessage{Role: m.Role, Content: m.Content})
	}
	if len(systemParts) == 0 {
		return "", rest
	}
	system = systemParts[0]
	for _, p := range systemParts[1:] {
		system += "\n" + p
	}
	return system, rest
}

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	system, messages := splitSystem(req.Messages)

	maxTokens := int32(defaultMaxTokens)
	if req.MaxTokens != nil {
		maxTokens = *req.MaxTokens
	}

	body, err := json.Marshal(wireRequest{
		Model:       req.Model,
		System:      system,
		Messages:    messages,
		Stream:      true,
		MaxTokens:   maxTokens,
		Temperature: req.Temperature,
	})
	if err != nil {
		return nil, fmt.Errorf("anthropic adapter: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic adapter: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", apiVersion)

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic adapter: request failed: %w", err)
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
			return adapters.Delta{}, fmt.Errorf("anthropic adapter: read stream: %w", err)
		}

		switch ev.Name {
		case "content_block_delta":
			var d wireContentBlockDelta
			if err := json.Unmarshal([]byte(ev.Data), &d); err != nil {
				_ = s.body.Close()
				return adapters.Delta{}, fmt.Errorf("anthropic adapter: decode content_block_delta: %w", err)
			}
			if d.Delta.Type != "text_delta" || d.Delta.Text == "" {
				continue
			}
			text := d.Delta.Text
			return adapters.Delta{Content: &text}, nil

		case "message_delta":
			var d wireMessageDelta
			if err := json.Unmarshal([]byte(ev.Data), &d); err != nil {
				_ = s.body.Close()
				return adapters.Delta{}, fmt.Errorf("anthropic adapter: decode message_delta: %w", err)
			}
			_ = s.body.Close()
			return adapters.Delta{FinishReason: d.Delta.StopReason, IsFinal: true}, nil

		case "error":
			var e wireErrorEvent
			_ = json.Unmarshal([]byte(ev.Data), &e)
			_ = s.body.Close()
			return adapters.Delta{}, fmt.Errorf("anthropic adapter: stream error: %s", e.Error.Message)

		default:
			// message_start, content_block_start, content_block_stop,
			// message_stop, ping — no data this adapter needs to
			// surface. Keep reading.
			continue
		}
	}
}
