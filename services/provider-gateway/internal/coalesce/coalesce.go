// Package coalesce dedupes identical in-flight Invoke calls (Part G.1):
// if a call with the same worker_ref/messages/params is already being
// served, a concurrent identical call joins it instead of starting a
// second one. Deliberately per-pod, in-memory — not Redis-backed like
// quota/breaker. Phase-02.txt's own Redis key list (DATABASE TABLES
// REQUIRED) only names provider:{name}:quota:{window} and
// provider:{name}:breaker; no coalesce key. Sharing a live stream across
// pod boundaries would need real distributed pub/sub infrastructure this
// phase doesn't call for — coalescing's value here is avoiding redundant
// upstream calls within one process's concurrent request handling.
//
// Per Phase-02.txt's own flow ("coalesce check -> quota governor ->
// breaker check -> adapter"), the coalesced unit is the WHOLE quota +
// breaker + adapter sequence, not just the adapter call: a follower
// riding an already-in-flight call must not also consume fleet quota or
// be independently breaker-gated for work it isn't actually causing.
// Group.Invoke's `call` closure is the caller's responsibility to build
// as exactly that sequence; Group only decides whether to run it at all.
package coalesce

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
)

type Group struct {
	mu       sync.Mutex
	inFlight map[string]*broadcast
}

func NewGroup() *Group {
	return &Group{inFlight: make(map[string]*broadcast)}
}

// Invoke returns a Stream for `key`. If a call for the same key is
// already in flight, the returned Stream replays it (past and future
// deltas) instead of invoking `call` again. Otherwise this caller becomes
// the leader: `call` runs exactly once, in the background, and its
// result is fanned out to this and every later-joining subscriber.
func (g *Group) Invoke(key string, call func() (adapters.Stream, error)) adapters.Stream {
	g.mu.Lock()
	if b, ok := g.inFlight[key]; ok {
		g.mu.Unlock()
		return &subscriber{b: b}
	}
	b := newBroadcast()
	g.inFlight[key] = b
	g.mu.Unlock()

	go g.lead(key, b, call)

	return &subscriber{b: b}
}

func (g *Group) release(key string) {
	g.mu.Lock()
	delete(g.inFlight, key)
	g.mu.Unlock()
}

func (g *Group) lead(key string, b *broadcast, call func() (adapters.Stream, error)) {
	upstream, err := call()
	if err != nil {
		g.release(key)
		b.finish(err)
		return
	}
	for {
		d, err := upstream.Recv()
		if err != nil {
			g.release(key)
			// A plain io.EOF becomes b.err == nil: subscriber.Recv
			// already returns io.EOF for "done with no error", so there's
			// no need to carry the sentinel value through b.err too.
			if errors.Is(err, io.EOF) {
				b.finish(nil)
			} else {
				b.finish(err)
			}
			return
		}
		b.publish(d)
		if d.IsFinal {
			g.release(key)
			b.finish(nil)
			return
		}
	}
}

// Key builds the coalescing key from the parts of a request that make two
// calls "identical" for dedup purposes: worker_ref, messages, and params.
// request_id is deliberately excluded — two independently-originated
// calls with different IDs but identical content should still coalesce.
func Key(req adapters.InvokeRequest) string {
	h := sha256.New()
	fmt.Fprintf(h, "model=%s\n", req.Model)
	for _, m := range req.Messages {
		fmt.Fprintf(h, "msg=%s:%s\n", m.Role, m.Content)
	}
	if req.MaxTokens != nil {
		fmt.Fprintf(h, "max_tokens=%d\n", *req.MaxTokens)
	}
	if req.Temperature != nil {
		fmt.Fprintf(h, "temperature=%f\n", *req.Temperature)
	}
	return hex.EncodeToString(h.Sum(nil))
}
