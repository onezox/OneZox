package stream

import (
	"errors"
	"io"
	"testing"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
	pb "github.com/onezox/OneZox/services/provider-gateway/internal/pb/provider/v1"
)

type sliceStream struct {
	deltas []adapters.Delta
	idx    int
	err    error
}

func (s *sliceStream) Recv() (adapters.Delta, error) {
	if s.idx < len(s.deltas) {
		d := s.deltas[s.idx]
		s.idx++
		return d, nil
	}
	if s.err != nil {
		return adapters.Delta{}, s.err
	}
	return adapters.Delta{}, io.EOF
}

// fallbackStream returns a FallbackError on the very first Recv, as the
// coalesced closure in main.go does for quota/breaker denial.
type fallbackStream struct {
	err error
}

func (f *fallbackStream) Recv() (adapters.Delta, error) { return adapters.Delta{}, f.err }

func strPtr(s string) *string { return &s }

func TestRelayForwardsEveryDeltaInOrder(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{
		{Content: strPtr("a")},
		{Content: strPtr("b")},
		{FinishReason: strPtr("stop"), IsFinal: true},
	}}
	var sent []*pb.InvokeResponse
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent = append(sent, r)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sent) != 3 {
		t.Fatalf("sent %d responses, want 3", len(sent))
	}
	if sent[0].GetDelta().GetContent() != "a" || sent[1].GetDelta().GetContent() != "b" {
		t.Errorf("deltas out of order or wrong content: %v", sent)
	}
	if !sent[2].GetDelta().GetIsFinal() || sent[2].GetDelta().GetFinishReason() != "stop" {
		t.Errorf("final delta wrong: %v", sent[2])
	}
}

func TestRelayStopsAtIsFinalEvenIfMoreWouldFollow(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{
		{Content: strPtr("a"), IsFinal: true},
		{Content: strPtr("should never be sent")},
	}}
	var sent int
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sent != 1 {
		t.Errorf("sent %d responses, want exactly 1 (stopped at IsFinal)", sent)
	}
}

func TestRelayTranslatesFallbackErrorToATypedResponse(t *testing.T) {
	src := &fallbackStream{err: &FallbackError{Reason: pb.FallbackReason_FALLBACK_REASON_BREAKER_OPEN, Provider: "openai"}}
	var sent *pb.InvokeResponse
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent = r
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fb := sent.GetFallback()
	if fb == nil {
		t.Fatal("expected a Fallback response, got a Delta")
	}
	if fb.GetProvider() != "openai" || fb.GetReason() != pb.FallbackReason_FALLBACK_REASON_BREAKER_OPEN {
		t.Errorf("fallback response = %+v, want provider=openai reason=BREAKER_OPEN", fb)
	}
}

func TestRelayWrapsAGenuineUpstreamErrorDistinctlyFromASendFailure(t *testing.T) {
	wantErr := errors.New("upstream exploded")
	src := &sliceStream{err: wantErr}

	err := Relay("req-1", src, func(r *pb.InvokeResponse) error { return nil })

	var upstreamErr *UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("Relay error = %v, want an *UpstreamError wrapping %v", err, wantErr)
	}
	if !errors.Is(upstreamErr, wantErr) {
		t.Errorf("UpstreamError does not unwrap to the original error: %v", upstreamErr.Unwrap())
	}
}

func TestRelayPropagatesASendFailureUnwrapped(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{{Content: strPtr("a")}}}
	sendErr := errors.New("client disconnected")

	err := Relay("req-1", src, func(r *pb.InvokeResponse) error { return sendErr })

	var upstreamErr *UpstreamError
	if errors.As(err, &upstreamErr) {
		t.Fatalf("a send failure got wrapped as an UpstreamError, want it unwrapped: %v", err)
	}
	if !errors.Is(err, sendErr) {
		t.Errorf("Relay error = %v, want exactly the send error %v", err, sendErr)
	}
}

func TestRelayCarriesPrefixCacheHandleThrough(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{
		{Content: strPtr("a"), PrefixCacheHandle: strPtr("handle-123"), IsFinal: true},
	}}
	var sent *pb.InvokeResponse
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent = r
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := sent.GetDelta().GetPrefixCacheHandle(); got != "handle-123" {
		t.Errorf("prefix_cache_handle = %q, want %q", got, "handle-123")
	}
}

func int32Ptr(i int32) *int32 { return &i }

func TestRelayCarriesUsageThroughOnlyWhenSet(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{
		{Content: strPtr("a"), InputTokens: int32Ptr(12), OutputTokens: int32Ptr(34), IsFinal: true},
	}}
	var sent *pb.InvokeResponse
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent = r
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d := sent.GetDelta()
	if d.InputTokens == nil || d.GetInputTokens() != 12 {
		t.Errorf("input_tokens = %v (present=%v), want 12 (present)", d.GetInputTokens(), d.InputTokens != nil)
	}
	if d.OutputTokens == nil || d.GetOutputTokens() != 34 {
		t.Errorf("output_tokens = %v (present=%v), want 34 (present)", d.GetOutputTokens(), d.OutputTokens != nil)
	}
}

func TestRelayLeavesUsageUnsetWhenAdapterDidNotReportIt(t *testing.T) {
	src := &sliceStream{deltas: []adapters.Delta{
		{Content: strPtr("a"), IsFinal: true},
	}}
	var sent *pb.InvokeResponse
	err := Relay("req-1", src, func(r *pb.InvokeResponse) error {
		sent = r
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d := sent.GetDelta()
	if d.InputTokens != nil || d.OutputTokens != nil {
		t.Errorf("expected usage fields unset when adapter reports none, got input=%v(present=%v) output=%v(present=%v)",
			d.GetInputTokens(), d.InputTokens != nil, d.GetOutputTokens(), d.OutputTokens != nil)
	}
}
