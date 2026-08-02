package coalesce

import (
	"io"
	"sync"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

// broadcast fans out one leader call's deltas to however many subscribers
// join while it's in flight. A growing slice + sync.Cond, not channels:
// every subscriber replays from index 0 (whatever already arrived before
// it joined) and then blocks for new items — simpler to reason about
// correctly than coordinating N dynamically-joining channel readers.
//
// Backpressure note (deliberate simplification): once a call is shared
// across subscribers, the leader's upstream Recv is paced by nothing —
// it drains as fast as the adapter produces data, buffered here in
// memory, rather than by any one subscriber's consumption rate. Honoring
// true backpressure from N independently-paced subscribers sharing one
// upstream is not fully satisfiable in general (the fast and slow
// subscribers disagree on pace); Step H's per-caller backpressure
// applies to the UNCOALESCED path. Fine at this phase's scale (a handful
// of short chunks per response, not unbounded streams).
type broadcast struct {
	mu     sync.Mutex
	cond   *sync.Cond
	deltas []adapters.Delta
	done   bool
	err    error
}

func newBroadcast() *broadcast {
	b := &broadcast{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *broadcast) publish(d adapters.Delta) {
	b.mu.Lock()
	b.deltas = append(b.deltas, d)
	b.mu.Unlock()
	b.cond.Broadcast()
}

func (b *broadcast) finish(err error) {
	b.mu.Lock()
	b.done = true
	b.err = err
	b.mu.Unlock()
	b.cond.Broadcast()
}

// subscriber implements adapters.Stream against a broadcast, letting both
// the leader and every follower consume via the exact same Recv-based
// interface the rest of the pipeline already expects.
type subscriber struct {
	b   *broadcast
	idx int
}

func (s *subscriber) Recv() (adapters.Delta, error) {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	for s.idx >= len(s.b.deltas) && !s.b.done {
		s.b.cond.Wait()
	}
	if s.idx < len(s.b.deltas) {
		d := s.b.deltas[s.idx]
		s.idx++
		return d, nil
	}
	if s.b.err != nil {
		return adapters.Delta{}, s.b.err
	}
	return adapters.Delta{}, io.EOF
}
