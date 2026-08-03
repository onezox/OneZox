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
