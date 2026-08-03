package anthropic

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

// A canned, real-shaped Anthropic Messages API stream: named SSE events,
// text deltas, then message_delta carrying stop_reason, then message_stop.
// message_start carries usage.input_tokens; message_delta carries
// usage.output_tokens — the two halves this adapter has to reunite onto
// one final delta.
const cannedStream = "" +
	"event: message_start\n" +
	"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":9}}}\n\n" +
	"event: content_block_start\n" +
	"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_delta\n" +
	"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n" +
	"event: content_block_delta\n" +
	"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n" +
	"event: content_block_stop\n" +
	"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: message_delta\n" +
	"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n" +
	"event: message_stop\n" +
	"data: {\"type\":\"message_stop\"}\n\n"

func TestInvokeParsesCannedStreamCorrectly(t *testing.T) {
	var gotAPIKey, gotVersion, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(cannedStream))
	}))
	defer srv.Close()

	a := New("test-key", srv.URL)
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model: "claude-3-5-haiku-20241022",
		Messages: []adapters.Message{
			{Role: "system", Content: "be terse"},
			{Role: "user", Content: "hi"},
		},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	if gotAPIKey != "test-key" {
		t.Errorf("x-api-key = %q, want %q", gotAPIKey, "test-key")
	}
	if gotVersion != apiVersion {
		t.Errorf("anthropic-version = %q, want %q", gotVersion, apiVersion)
	}
	if !strings.Contains(gotBody, `"system":"be terse"`) {
		t.Errorf("request body missing hoisted system field: %s", gotBody)
	}
	if strings.Contains(gotBody, `"role":"system"`) {
		t.Errorf("system message leaked into messages array: %s", gotBody)
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
			if d.FinishReason == nil || *d.FinishReason != "end_turn" {
				t.Errorf("finish reason = %v, want \"end_turn\"", d.FinishReason)
			}
			if d.InputTokens == nil || *d.InputTokens != 9 {
				t.Errorf("input tokens = %v, want 9 (present, from message_start)", d.InputTokens)
			}
			if d.OutputTokens == nil || *d.OutputTokens != 5 {
				t.Errorf("output tokens = %v, want 5 (present, from message_delta)", d.OutputTokens)
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

func TestInputTokensStaysUnsetWithoutMessageStart(t *testing.T) {
	// A malformed/truncated stream missing message_start entirely —
	// output_tokens still arrives on message_delta, but input_tokens must
	// stay genuinely nil (unknown), never silently 0.
	const streamNoStart = "" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n" +
		"event: message_delta\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(streamNoStart))
	}))
	defer srv.Close()

	a := New("test-key", srv.URL)
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model:    "claude-3-5-haiku-20241022",
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
			if d.InputTokens != nil {
				t.Errorf("input tokens = %v, want nil (message_start never arrived)", *d.InputTokens)
			}
			if d.OutputTokens == nil || *d.OutputTokens != 3 {
				t.Errorf("output tokens = %v, want 3 (present, from message_delta)", d.OutputTokens)
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
		_, _ = w.Write([]byte(`{"error":{"message":"invalid x-api-key"}}`))
	}))
	defer srv.Close()

	a := New("bad-key", srv.URL)
	_, err := a.Invoke(context.Background(), adapters.InvokeRequest{Model: "claude-3-5-haiku-20241022"})

	ue, ok := err.(*UpstreamError)
	if !ok || ue.StatusCode != http.StatusUnauthorized {
		t.Errorf("err = %v, want *UpstreamError with status 401", err)
	}
}
