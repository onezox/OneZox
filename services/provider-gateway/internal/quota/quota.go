// Package quota implements the fleet-wide quota governor (Part G.1):
// a shared Redis counter per provider+window, not a per-pod limit, so
// every provider-gateway replica coordinates against the same budget
// instead of each pod hammering a provider independently. Fixed-window,
// request-count based — mirrors edge-gateway's Phase-01 ratelimit design
// (SET NX EX + INCR) at the provider level instead of the tenant level.
// Token-aware quota is explicitly deferred (Dependencies.txt F2: KEDA +
// global governor scaling finalizes in Phase-13); this phase only counts
// requests.
package quota

import (
	"context"
	"fmt"
	"time"
)

type Policy struct {
	Limit  int64
	Window time.Duration
}

type Decision int

const (
	Allow Decision = iota
	Throttle
)

func (d Decision) String() string {
	if d == Allow {
		return "allow"
	}
	return "throttle"
}

// Counter is the fleet-wide shared counter store. Implemented by a real
// Redis-backed counter in production and an in-memory fake in tests —
// same shape as edge-gateway's RateLimitCounter trait in Rust.
type Counter interface {
	// Increment returns the counter's new value after incrementing,
	// creating the key with the given TTL if this is the first
	// increment observed for it.
	Increment(ctx context.Context, key string, window time.Duration) (int64, error)
}

// windowKey buckets `now` into a fixed window and builds the Redis key
// exactly as Phase-02.txt/CLAUDE.md specify: provider:{name}:quota:{window}.
func windowKey(provider string, window time.Duration, now time.Time) string {
	bucket := now.Unix() / int64(window.Seconds())
	return fmt.Sprintf("provider:%s:quota:%d", provider, bucket)
}

// Decide is the pure decision: no I/O, given a snapshot of the current
// count and the policy limit.
func Decide(current int64, policy Policy) Decision {
	if current <= policy.Limit {
		return Allow
	}
	return Throttle
}

// Enforce increments the fleet-wide counter for a provider's current
// window and returns the resulting decision. Fails OPEN on a store error
// (same reasoning as edge-gateway's ratelimit.enforce): quota is fleet
// protection against overrunning a provider's own limits, not a security
// boundary, and a Redis outage isn't evidence of real overload.
func Enforce(ctx context.Context, counter Counter, provider string, policy Policy) (Decision, error) {
	key := windowKey(provider, policy.Window, time.Now())
	current, err := counter.Increment(ctx, key, policy.Window)
	if err != nil {
		return Allow, err
	}
	return Decide(current, policy), nil
}
