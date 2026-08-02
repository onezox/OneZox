// Package breaker implements a per-provider circuit breaker (Part G.1),
// fleet-wide via Redis (provider:{name}:breaker), not per-pod — every
// provider-gateway replica sees and contributes to the same trip state.
// Check-before / report-after, mirroring how breaker libraries are
// normally used: Check decides whether to let a call through (and, on the
// transition out of Open, claims the one half-open trial); ReportResult
// records the outcome and updates state accordingly.
//
// State is stored as a single JSON blob per key rather than separate
// fields, read-then-written with plain GET/SET rather than a Lua script
// for atomicity — a known, deliberate simplification appropriate to this
// phase's "basic" scope (a real race window exists between concurrent
// Check calls during the exact instant a breaker transitions out of Open;
// full atomicity would need a Lua EVAL or WATCH/MULTI transaction). The
// unit tests below exercise this sequentially, matching how it's actually
// driven in this phase; true concurrent-safety hardening is not required
// by Phase-02.txt and is left for whenever it's actually needed.
package breaker

import (
	"context"
	"encoding/json"
	"time"
)

type State string

const (
	Closed State = "closed"
	Open   State = "open"
	// HalfOpen is never stored directly — it's the derived state of an
	// Open record whose OpenDuration has elapsed but hasn't yet resolved
	// via a reported trial result (see record.effectiveState).
	HalfOpen      State = "half_open"
	halfOpenTrial State = "half_open_trial"
)

type Decision int

const (
	Allow      Decision = iota
	AllowTrial          // this call IS the one half-open trial; report its result
	Deny
)

func (d Decision) String() string {
	switch d {
	case Allow:
		return "allow"
	case AllowTrial:
		return "allow_trial"
	default:
		return "deny"
	}
}

type Config struct {
	FailureThreshold int
	OpenDuration     time.Duration
}

// Store is the fleet-wide breaker state store. Implemented by a real
// Redis-backed store in production and an in-memory fake in tests.
type Store interface {
	Get(ctx context.Context, key string) (*record, error)
	Set(ctx context.Context, key string, r *record) error
}

type record struct {
	State    State `json:"state"`
	Failures int   `json:"failures"`
	OpenedAt int64 `json:"opened_at"` // unix milliseconds
}

func (r *record) effectiveState(now time.Time, openDuration time.Duration) State {
	if r == nil {
		return Closed
	}
	// Millisecond precision, not Unix()'s whole-second truncation: a
	// unit-test-scale OpenDuration under 1 second would otherwise always
	// compare as already-elapsed (Unix() difference truncates to 0),
	// caught by TestRepeatedFailuresTripToOpen expecting the breaker to
	// stay genuinely Open for a full 50ms, not flip to half-open
	// instantly.
	if r.State == Open && now.UnixMilli()-r.OpenedAt >= openDuration.Milliseconds() {
		return HalfOpen
	}
	return r.State
}

func key(provider string) string {
	return "provider:" + provider + ":breaker"
}

// Check decides whether a call to `provider` should proceed. Fails open
// on a store error (same reasoning as quota/ratelimit/admission
// throughout this project): a Redis outage isn't evidence the provider
// itself is failing.
func Check(ctx context.Context, store Store, provider string, cfg Config) (Decision, error) {
	k := key(provider)
	r, err := store.Get(ctx, k)
	if err != nil {
		return Allow, err
	}

	switch r.effectiveState(time.Now(), cfg.OpenDuration) {
	case Closed:
		return Allow, nil
	case HalfOpen:
		// Claim the trial: mark it in-flight so any other call arriving
		// in this same instant sees Open (Deny), not another trial.
		if err := store.Set(ctx, k, &record{State: halfOpenTrial, Failures: r.Failures, OpenedAt: r.OpenedAt}); err != nil {
			return Allow, err
		}
		return AllowTrial, nil
	default: // Open, or a trial already in flight
		return Deny, nil
	}
}

// ReportResult records the outcome of a call that Check permitted, and
// updates the breaker's state: any success resets fully to Closed; a
// failure either increments the consecutive-failure count (tripping to
// Open once it reaches the threshold) or, if this was the half-open
// trial, reopens immediately.
func ReportResult(ctx context.Context, store Store, provider string, cfg Config, success bool) error {
	k := key(provider)
	r, err := store.Get(ctx, k)
	if err != nil {
		return err
	}

	if success {
		return store.Set(ctx, k, &record{State: Closed, Failures: 0})
	}

	if r != nil && r.State == halfOpenTrial {
		return store.Set(ctx, k, &record{State: Open, Failures: cfg.FailureThreshold, OpenedAt: time.Now().UnixMilli()})
	}

	failures := 1
	if r != nil {
		failures = r.Failures + 1
	}
	if failures >= cfg.FailureThreshold {
		return store.Set(ctx, k, &record{State: Open, Failures: failures, OpenedAt: time.Now().UnixMilli()})
	}
	return store.Set(ctx, k, &record{State: Closed, Failures: failures})
}

// CurrentState reports the breaker's effective state without affecting
// it, for ProviderHealth (Step I/N).
func CurrentState(ctx context.Context, store Store, provider string, cfg Config) (State, error) {
	r, err := store.Get(ctx, key(provider))
	if err != nil {
		return Closed, err
	}
	return r.effectiveState(time.Now(), cfg.OpenDuration), nil
}

func marshal(r *record) ([]byte, error) { return json.Marshal(r) }

func unmarshal(data []byte) (*record, error) {
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
