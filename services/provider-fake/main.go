// provider-fake — Phase-02 Step D: a controllable stand-in for a real
// provider's HTTP API, deployed alongside provider-gateway so its own
// resilience logic (breaker, quota, coalescing, backpressure, fan-out cap)
// can be exercised on command. A real provider can't be made to fail or
// exhaust quota on demand, and hammering one to find out costs money —
// this is the fake provider-gateway's own `adapters/fake` adapter talks
// to instead, registered as its own provider identity ("fake") so induced
// failures never touch real providers' fleet-wide quota/breaker state.
//
// Behavior is controlled PER REQUEST (a field in the request body), not
// shared mutable state toggled out-of-band by a separate admin call: two
// concurrent tests picking different modes must never race each other's
// state. The one thing this service is NOT responsible for is enforcing
// its own rate limit — "quota exhaustion throttles rather than errors"
// (Phase-02.txt) describes provider-gateway's OWN fleet-wide Redis-backed
// governor rejecting a request before it ever reaches here, not an
// upstream 429; this fake has no need to reimplement that on its own side.
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const serviceName = "provider-fake"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type fakeCompleteRequest struct {
	RequestID  string        `json:"request_id"`
	Messages   []chatMessage `json:"messages"`
	Mode       string        `json:"mode"`        // "normal" | "fail" | "slow"
	FailStatus int           `json:"fail_status"` // used only when mode == "fail"; default 503
}

type fakeCompleteChunk struct {
	RequestID         string  `json:"request_id"`
	Content           *string `json:"content,omitempty"`
	FinishReason      *string `json:"finish_reason,omitempty"`
	IsFinal           bool    `json:"is_final"`
	PrefixCacheHandle *string `json:"prefix_cache_handle,omitempty"`
}

// The canned response body, split into chunks — same "prove it's genuinely
// this service answering, not a fluke" convention dataplane-stub's own
// Submit shim used in Phase-01 (its content literally names itself).
var cannedChunks = []string{"Hello ", "from ", "the ", "Phase-02 ", "provider-fake ", "harness. "}

func strPtr(s string) *string { return &s }

func writeChunk(w http.ResponseWriter, flusher http.Flusher, chunk fakeCompleteChunk) {
	_ = json.NewEncoder(w).Encode(chunk)
	flusher.Flush()
}

func handleFakeComplete(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req fakeCompleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}
		log = log.With("request_id", req.RequestID, "mode", req.Mode)

		switch req.Mode {
		case "fail":
			status := req.FailStatus
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			log.Info("fake: induced failure", "status", status)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "provider-fake: induced failure"})
			return

		case "normal", "slow":
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)

			delay := 0 * time.Millisecond
			if req.Mode == "slow" {
				delay = 700 * time.Millisecond
			}

			for _, text := range cannedChunks {
				if delay > 0 {
					time.Sleep(delay)
				}
				writeChunk(w, flusher, fakeCompleteChunk{RequestID: req.RequestID, Content: strPtr(text)})
			}
			// A canned cache-handle value on the final chunk (Part P), the
			// same place real providers surface a prompt-cache token
			// alongside completion metadata — derived from request_id so a
			// live check can confirm it's genuinely THIS request's value
			// passed through, not a coincidental static constant. Real KV
			// reuse arrives with self-host in Phase-11; this is plumbing
			// only, matching meter.rs's placeholder-field precedent from
			// Phase-01.
			cacheHandle := "fake-cache-" + req.RequestID
			writeChunk(w, flusher, fakeCompleteChunk{
				RequestID:         req.RequestID,
				FinishReason:      strPtr("stop"),
				IsFinal:           true,
				PrefixCacheHandle: &cacheHandle,
			})
			log.Info("fake: stream complete")
			return

		default:
			http.Error(w, `{"error":"mode must be one of: normal, fail, slow"}`, http.StatusBadRequest)
			return
		}
	}
}

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(handler).With("service", serviceName)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/fake-complete", handleFakeComplete(log))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	port := envOr("PORT", "8080")
	log.Info("provider-fake listening", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Error("server exited", "error", err)
		os.Exit(1)
	}
}
