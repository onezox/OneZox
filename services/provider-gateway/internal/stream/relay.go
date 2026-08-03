// Package stream relays a (possibly coalesced) adapters.Stream out over a
// gRPC server-streaming response, translating deltas to proto and control
// signals (fallback) to a typed FallbackSignal — the last stage of
// Phase-02.txt's own flow ("coalesce check -> quota governor -> breaker
// check -> adapter"). No manual buffering: Relay only calls Recv again
// after Send has completed, so a slow gRPC client naturally paces the
// upstream read, the same backpressure discipline Phase-01's edge-gateway
// SSE relay used (Step D5 already established this for the uncoalesced
// path; this package just gives it Phase-02.txt's own named module rather
// than leaving it inline in main.go).
package stream

import (
	"errors"
	"io"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
	pb "github.com/onezox/OneZox/services/provider-gateway/internal/pb/provider/v1"
)

// FallbackError lets a Stream signal "the system proactively declined
// this call" (quota exhausted, breaker open) distinctly from a genuine
// upstream failure — Relay translates it into a typed FallbackSignal
// instead of a transport error.
type FallbackError struct {
	Reason   pb.FallbackReason
	Provider string
}

func (e *FallbackError) Error() string { return "fallback: " + e.Reason.String() }

// UpstreamError marks an error as having come from src.Recv (the adapter
// or coalesced call itself), distinctly from send failing (the caller
// went away). Relay's caller needs this distinction: a genuine upstream
// failure should become a gRPC error status, but a send failure means the
// caller already disconnected and should propagate as-is, not be
// re-wrapped as if the provider were the one at fault.
type UpstreamError struct {
	Err error
}

func (e *UpstreamError) Error() string { return e.Err.Error() }
func (e *UpstreamError) Unwrap() error { return e.Err }

func toPbDelta(requestID string, d adapters.Delta) *pb.InvokeResponse {
	delta := &pb.Delta{
		RequestId: requestID,
		IsFinal:   d.IsFinal,
	}
	if d.Content != nil {
		delta.Content = d.Content
	}
	if d.FinishReason != nil {
		delta.FinishReason = d.FinishReason
	}
	if d.PrefixCacheHandle != nil {
		delta.PrefixCacheHandle = d.PrefixCacheHandle
	}
	if d.InputTokens != nil {
		delta.InputTokens = d.InputTokens
	}
	if d.OutputTokens != nil {
		delta.OutputTokens = d.OutputTokens
	}
	return &pb.InvokeResponse{Event: &pb.InvokeResponse_Delta{Delta: delta}}
}

func FallbackResponse(requestID, provider string, reason pb.FallbackReason) *pb.InvokeResponse {
	return &pb.InvokeResponse{
		Event: &pb.InvokeResponse_Fallback{
			Fallback: &pb.FallbackSignal{
				RequestId: requestID,
				Provider:  provider,
				Reason:    reason,
			},
		},
	}
}

// Relay drains src and forwards each item via send, until src reports
// io.EOF (clean end), a final delta, or a FallbackError — the last two
// stop the loop after handling. A src.Recv failure is wrapped in
// UpstreamError so the caller can tell it apart from send failing (the
// caller went away, returned completely unwrapped — Relay has no opinion
// on how its own caller wants to react to that, and it isn't the
// provider's fault).
func Relay(requestID string, src adapters.Stream, send func(*pb.InvokeResponse) error) error {
	for {
		delta, err := src.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		var fbErr *FallbackError
		if errors.As(err, &fbErr) {
			return send(FallbackResponse(requestID, fbErr.Provider, fbErr.Reason))
		}
		if err != nil {
			return &UpstreamError{Err: err}
		}
		if err := send(toPbDelta(requestID, delta)); err != nil {
			return err
		}
		if delta.IsFinal {
			return nil
		}
	}
}
