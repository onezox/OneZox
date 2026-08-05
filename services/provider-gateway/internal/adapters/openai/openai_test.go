package openai

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

// A canned, real-shaped OpenAI chat.completions.chunk stream: a role-only
// first chunk, two content chunks, a finish_reason chunk, the dedicated
// usage chunk (stream_options.include_usage — empty choices, populated
// usage, arrives AFTER the finish_reason chunk, not on it), then [DONE].
const cannedStream = "" +
	"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2}}\n\n" +
	"data: [DONE]\n\n"

// A canned stream WITHOUT the usage chunk (as if stream_options were
// ignored) — confirms the [DONE] fallback still returns the deferred
// finish_reason rather than losing it.
const cannedStreamNoUsage = "" +
	"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: [DONE]\n\n"

// GLM's (z.ai) real shape, captured live via the Between-Phase provider
// task: usage arrives INLINE on the SAME chunk as finish_reason, not as
// a separate trailing chunk the way OpenAI itself sends it. No dedicated
// usage-only chunk ever follows.
const cannedStreamInlineUsage = "" +
	"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\" there\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":9}}\n\n" +
	"data: [DONE]\n\n"

func TestInvokeParsesCannedStreamCorrectly(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(cannedStream))
	}))
	defer srv.Close()

	a := New("test-key", srv.URL)
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model:    "gpt-4o-mini",
		Messages: []adapters.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer test-key")
	}
	if !strings.Contains(gotBody, `"stream":true`) {
		t.Errorf("request body missing stream:true: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"stream_options":{"include_usage":true}`) {
		t.Errorf("request body missing stream_options.include_usage: %s", gotBody)
	}

	var content string
	var gotFinal bool
	for {
		d, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if d.Content != nil {
			content += *d.Content
		}
		if d.IsFinal {
			gotFinal = true
			if d.FinishReason == nil || *d.FinishReason != "stop" {
				t.Errorf("finish reason = %v, want \"stop\"", d.FinishReason)
			}
			if d.InputTokens == nil || *d.InputTokens != 7 {
				t.Errorf("input tokens = %v, want 7 (present)", d.InputTokens)
			}
			if d.OutputTokens == nil || *d.OutputTokens != 2 {
				t.Errorf("output tokens = %v, want 2 (present)", d.OutputTokens)
			}
			break
		}
	}

	if content != "Hello world" {
		t.Errorf("assembled content = %q, want %q", content, "Hello world")
	}
	if !gotFinal {
		t.Error("stream never produced a final delta")
	}
}

func TestInvokeReturnsFinishReasonWithUnsetUsageWhenNoUsageChunkArrives(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(cannedStreamNoUsage))
	}))
	defer srv.Close()

	a := New("test-key", srv.URL)
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model:    "gpt-4o-mini",
		Messages: []adapters.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var gotFinal bool
	for {
		d, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if d.IsFinal {
			gotFinal = true
			if d.FinishReason == nil || *d.FinishReason != "stop" {
				t.Errorf("finish reason = %v, want \"stop\" (must not be lost even without a usage chunk)", d.FinishReason)
			}
			if d.InputTokens != nil || d.OutputTokens != nil {
				t.Errorf("usage = input:%v output:%v, want both unset (never coerced to zero)", d.InputTokens, d.OutputTokens)
			}
			break
		}
	}
	if !gotFinal {
		t.Error("stream never produced a final delta")
	}
}

// GLM's (z.ai) real shape, found live via the Between-Phase provider
// task: this is the regression test proving the fix. Before it, this
// exact canned stream produced FinishReason="stop" with BOTH usage
// fields nil — a silent billing hole, not an error, indistinguishable
// from a genuinely un-meterable provider until someone checked
// usage_event specifically. usage must now be read off the finish_reason
// chunk itself, not deferred waiting for a separate chunk that GLM never
// sends.
func TestInvokeReadsUsageInlineOnTheFinishReasonChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(cannedStreamInlineUsage))
	}))
	defer srv.Close()

	a := NewNamedWithPath("glm", "test-key", srv.URL, "/api/paas/v4/chat/completions")
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model:    "glm-4.5-flash",
		Messages: []adapters.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var gotFinal bool
	for {
		d, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if d.IsFinal {
			gotFinal = true
			if d.FinishReason == nil || *d.FinishReason != "stop" {
				t.Errorf("finish reason = %v, want \"stop\"", d.FinishReason)
			}
			if d.InputTokens == nil || *d.InputTokens != 13 {
				t.Errorf("InputTokens = %v, want 13", d.InputTokens)
			}
			if d.OutputTokens == nil || *d.OutputTokens != 9 {
				t.Errorf("OutputTokens = %v, want 9", d.OutputTokens)
			}
			break
		}
	}
	if !gotFinal {
		t.Error("stream never produced a final delta")
	}
}

func TestInvokeReturnsUpstreamErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"invalid api key"}}`))
	}))
	defer srv.Close()

	a := New("bad-key", srv.URL)
	_, err := a.Invoke(context.Background(), adapters.InvokeRequest{Model: "gpt-4o-mini"})

	var upstreamErr *UpstreamError
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if ue, ok := err.(*UpstreamError); ok {
		upstreamErr = ue
	}
	if upstreamErr == nil || upstreamErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("err = %v, want *UpstreamError with status 401", err)
	}
}

// Between-Phase provider task: New() must keep reporting "openai" exactly
// as before — this is the regression check proving NewNamed's addition
// didn't change the real OpenAI adapter's own identity in the Registry.
func TestNewReportsTheFixedOpenAIProviderName(t *testing.T) {
	a := New("test-key", "")
	if got := a.Name(); got != "openai" {
		t.Errorf("New(...).Name() = %q, want %q", got, "openai")
	}
}

// NewNamed is what lets an OpenAI-compatible provider (Grok/GLM/Kimi)
// register under its own name instead of colliding with "openai" (or
// each other) in the Registry's Name()-keyed map.
func TestNewNamedReportsItsOwnProviderName(t *testing.T) {
	a := NewNamed("grok", "test-key", "")
	if got := a.Name(); got != "grok" {
		t.Errorf("NewNamed(\"grok\", ...).Name() = %q, want %q", got, "grok")
	}
}

// Two NewNamed instances (or one NewNamed + one New) must report distinct
// names — the exact collision this change exists to prevent.
func TestDistinctNewNamedInstancesDoNotCollide(t *testing.T) {
	openaiAdapter := New("k1", "")
	grokAdapter := NewNamed("grok", "k2", "")
	glmAdapter := NewNamed("glm", "k3", "")

	names := map[string]bool{
		openaiAdapter.Name(): true,
		grokAdapter.Name():   true,
		glmAdapter.Name():    true,
	}
	if len(names) != 3 {
		t.Errorf("expected 3 distinct provider names, got %d: %v", len(names), names)
	}
}

// GLM's real path (https://api.z.ai/api/paas/v4/chat/completions) has no
// /v1 segment at all — NewNamedWithPath's whole reason to exist. Proves
// the constructed request URL is exactly what a real z.ai account would
// need, not baseURL+the OpenAI-shaped default suffix.
func TestNewNamedWithPathBuildsTheOverriddenURL(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer srv.Close()

	a := NewNamedWithPath("glm", "test-key", srv.URL, "/api/paas/v4/chat/completions")
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{Model: "glm-4.6"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	_, _ = s.Recv()

	if gotPath != "/api/paas/v4/chat/completions" {
		t.Errorf("request path = %q, want %q", gotPath, "/api/paas/v4/chat/completions")
	}
}

// An empty path override must fall back to the default, exactly like an
// empty baseURL falls back to defaultBaseURL — same "explicit empty
// means use the default" convention this package already established.
func TestNewNamedWithPathDefaultsWhenPathIsEmpty(t *testing.T) {
	a := NewNamedWithPath("kimi", "test-key", "", "")
	if a.completionsPath != defaultCompletionsPath {
		t.Errorf("completionsPath = %q, want default %q", a.completionsPath, defaultCompletionsPath)
	}
}
