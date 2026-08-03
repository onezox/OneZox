// Package google adapts Google's Gemini streamGenerateContent API to
// provider-gateway's internal adapters.Adapter contract (Part G.1). Wire
// format is the third genuinely different shape: the REST endpoint's
// default response is a single JSON array delivered over chunked
// transfer, not real SSE — the documented ?alt=sse query parameter is
// what switches it to `data:` frames, chosen here specifically so this
// adapter can reuse the same sse.Scanner the other two use instead of a
// third parsing strategy. Auth is a header (x-goog-api-key), not a query
// param, so the key never lands in a URL any proxy/access-log might
// record — same "never logged" discipline Phase-02.txt's SECURITY
// IMPLEMENTATION section requires. Role naming and message shape also
// differ from OpenAI/Anthropic: "contents"/"parts" instead of "messages",
// role "model" instead of "assistant", and system text goes in a separate
// systemInstruction field, not a message role.
//
// Step A5 (Phase-03): real token usage. Gemini's usageMetadata is a
// top-level chunk field (not nested under candidates), and — unlike
// OpenAI's dedicated final chunk or Anthropic's two-event split — it's
// present on every chunk as a running cumulative count, simplest of the
// three: just read it off the same chunk that carries finishReason. Note
// this adapter is not exercised against the real API this phase (F13:
// the available key has generate_content quota=0 on the free tier; the
// EC1 real-model proof routes through OpenAI/Anthropic instead) — usage
// parsing here is verified against a canned real-shaped payload only,
// same as F13 already documents for the rest of this adapter.
package google

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

const ProviderName = "google"

const defaultBaseURL = "https://generativelanguage.googleapis.com"

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

type wirePart struct {
	Text string `json:"text"`
}

type wireContent struct {
	Role  string     `json:"role"`
	Parts []wirePart `json:"parts"`
}

type wireGenerationConfig struct {
	MaxOutputTokens *int32   `json:"maxOutputTokens,omitempty"`
	Temperature     *float32 `json:"temperature,omitempty"`
}

type wireRequest struct {
	Contents          []wireContent         `json:"contents"`
	SystemInstruction *wireContent          `json:"systemInstruction,omitempty"`
	GenerationConfig  *wireGenerationConfig `json:"generationConfig,omitempty"`
}

type wireCandidate struct {
	Content      wireContent `json:"content"`
	FinishReason *string     `json:"finishReason"`
}

// A top-level chunk field (not nested under candidates), present on every
// chunk as a running cumulative count — unlike OpenAI/Anthropic, Gemini
// doesn't withhold usage for a dedicated final event.
type wireUsageMetadata struct {
	PromptTokenCount     int32 `json:"promptTokenCount"`
	CandidatesTokenCount int32 `json:"candidatesTokenCount"`
}

type wireChunk struct {
	Candidates    []wireCandidate    `json:"candidates"`
	UsageMetadata *wireUsageMetadata `json:"usageMetadata"`
}

type UpstreamError struct {
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("google adapter: upstream returned status %d: %s", e.StatusCode, e.Body)
}

// mapRole translates the internal contract's OpenAI-shaped role naming
// ("assistant") to Gemini's own ("model"); "user" is already identical.
func mapRole(role string) string {
	if role == "assistant" {
		return "model"
	}
	return role
}

// splitSystem mirrors the anthropic adapter's own normalization: Gemini
// has no "system" content role either — system text is a distinct
// top-level field.
func splitSystem(messages []adapters.Message) (system *wireContent, rest []wireContent) {
	var systemParts []string
	for _, m := range messages {
		if m.Role == "system" {
			systemParts = append(systemParts, m.Content)
			continue
		}
		rest = append(rest, wireContent{Role: mapRole(m.Role), Parts: []wirePart{{Text: m.Content}}})
	}
	if len(systemParts) == 0 {
		return nil, rest
	}
	joined := systemParts[0]
	for _, p := range systemParts[1:] {
		joined += "\n" + p
	}
	return &wireContent{Parts: []wirePart{{Text: joined}}}, rest
}

func (a *Adapter) Invoke(ctx context.Context, req adapters.InvokeRequest) (adapters.Stream, error) {
	systemInstruction, contents := splitSystem(req.Messages)

	var genConfig *wireGenerationConfig
	if req.MaxTokens != nil || req.Temperature != nil {
		genConfig = &wireGenerationConfig{MaxOutputTokens: req.MaxTokens, Temperature: req.Temperature}
	}

	body, err := json.Marshal(wireRequest{
		Contents:          contents,
		SystemInstruction: systemInstruction,
		GenerationConfig:  genConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("google adapter: marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/v1beta/models/%s:streamGenerateContent?alt=sse", a.baseURL, req.Model)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("google adapter: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-goog-api-key", a.apiKey)

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("google adapter: request failed: %w", err)
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
			return adapters.Delta{}, fmt.Errorf("google adapter: read stream: %w", err)
		}

		var chunk wireChunk
		if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
			_ = s.body.Close()
			return adapters.Delta{}, fmt.Errorf("google adapter: decode chunk: %w", err)
		}
		if len(chunk.Candidates) == 0 {
			continue
		}
		candidate := chunk.Candidates[0]

		var text string
		if len(candidate.Content.Parts) > 0 {
			text = candidate.Content.Parts[0].Text
		}

		if candidate.FinishReason != nil {
			_ = s.body.Close()
			var content *string
			if text != "" {
				content = &text
			}
			delta := adapters.Delta{
				Content:      content,
				FinishReason: candidate.FinishReason,
				IsFinal:      true,
			}
			if chunk.UsageMetadata != nil {
				inputTokens := chunk.UsageMetadata.PromptTokenCount
				outputTokens := chunk.UsageMetadata.CandidatesTokenCount
				delta.InputTokens = &inputTokens
				delta.OutputTokens = &outputTokens
			}
			return delta, nil
		}
		if text == "" {
			continue
		}
		return adapters.Delta{Content: &text}, nil
	}
}
