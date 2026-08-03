package google

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

// A canned, real-shaped Gemini streamGenerateContent(alt=sse) stream: two
// content chunks, then a final chunk carrying finishReason.
const cannedStream = "" +
	"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello\"}],\"role\":\"model\"},\"index\":0}]}\n\n" +
	"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" world\"}],\"role\":\"model\"},\"index\":0}]}\n\n" +
	"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}]}\n\n"

func TestInvokeParsesCannedStreamCorrectly(t *testing.T) {
	var gotAPIKey, gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("x-goog-api-key")
		gotPath = r.URL.Path + "?" + r.URL.RawQuery
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(cannedStream))
	}))
	defer srv.Close()

	a := New("test-key", srv.URL)
	s, err := a.Invoke(context.Background(), adapters.InvokeRequest{
		Model: "gemini-1.5-flash",
		Messages: []adapters.Message{
			{Role: "system", Content: "be terse"},
			{Role: "user", Content: "hi"},
			{Role: "assistant", Content: "prior reply"},
		},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	if gotAPIKey != "test-key" {
		t.Errorf("x-goog-api-key = %q, want %q (must be a header, never a URL param, so it never lands in a proxy/access log)", gotAPIKey, "test-key")
	}
	if !strings.Contains(gotPath, "/v1beta/models/gemini-1.5-flash:streamGenerateContent") || !strings.Contains(gotPath, "alt=sse") {
		t.Errorf("request path = %q, want streamGenerateContent with alt=sse", gotPath)
	}
	if !strings.Contains(gotBody, `"systemInstruction"`) {
		t.Errorf("request body missing systemInstruction: %s", gotBody)
	}
	if strings.Contains(gotBody, `"role":"assistant"`) {
		t.Errorf("assistant role not mapped to \"model\": %s", gotBody)
	}
	if !strings.Contains(gotBody, `"role":"model"`) {
		t.Errorf("expected mapped role \"model\" in body: %s", gotBody)
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
			if d.FinishReason == nil || *d.FinishReason != "STOP" {
				t.Errorf("finish reason = %v, want \"STOP\"", d.FinishReason)
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
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"API key invalid"}}`))
	}))
	defer srv.Close()

	a := New("bad-key", srv.URL)
	_, err := a.Invoke(context.Background(), adapters.InvokeRequest{Model: "gemini-1.5-flash"})

	ue, ok := err.(*UpstreamError)
	if !ok || ue.StatusCode != http.StatusForbidden {
		t.Errorf("err = %v, want *UpstreamError with status 403", err)
	}
}
