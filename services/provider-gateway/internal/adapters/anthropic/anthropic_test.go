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
const cannedStream = "" +
	"event: message_start\n" +
	"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n" +
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
