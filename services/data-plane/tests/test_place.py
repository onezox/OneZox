"""Unit tests for scheduler.place — Step E.

No network here: bind() is pure logic, tested directly against
hand-built ProviderStatus values. The live counterpart — proving
bind_via_provider_health genuinely reacts to a real provider-gateway
signal across all three health states (healthy, breaker-open,
breaker-half-open, plus quota-exhausted) — is done separately against
the deployed cluster (Phase-03-Progress.txt), not here; that's the actual
point of Step E, but it isn't expressible as a hermetic unit test.

These unit tests instead prove the DECISION logic is correct for every
input combination on its own — the necessary complement, not a
substitute, for the live proof: a wrong live result could come from
either bad decision logic OR a live-wiring bug, and only having live
verification wouldn't tell you which.
"""

import pytest
from scheduler.place import (
    BREAKER_STATE_CLOSED,
    BREAKER_STATE_HALF_OPEN,
    BREAKER_STATE_OPEN,
    ProviderStatus,
    ProviderUnavailable,
    bind,
    parse_provider,
)

WORKER_REF = "anthropic:claude-haiku"


def test_breaker_state_constants_match_proto_enum() -> None:
    """Guards against the module's deliberately-decoupled local constants
    silently drifting from the real proto numbering they're documented
    to mirror — same regression guard Step C's test_classifier.py uses
    for RequestKind."""
    from provider.v1 import provider_pb2

    assert BREAKER_STATE_CLOSED == provider_pb2.BREAKER_STATE_CLOSED
    assert BREAKER_STATE_OPEN == provider_pb2.BREAKER_STATE_OPEN
    assert BREAKER_STATE_HALF_OPEN == provider_pb2.BREAKER_STATE_HALF_OPEN


def test_parse_provider() -> None:
    assert parse_provider("anthropic:claude-haiku-4-5-20251001") == "anthropic"
    assert parse_provider("fake:normal") == "fake"


# --- Healthy: binds normally -------------------------------------------


def test_bind_returns_worker_ref_when_closed_and_healthy() -> None:
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_CLOSED, quota_headroom=1.0
    )
    assert bind(WORKER_REF, status) == WORKER_REF


def test_bind_returns_worker_ref_with_partial_but_nonzero_headroom() -> None:
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_CLOSED, quota_headroom=0.01
    )
    assert bind(WORKER_REF, status) == WORKER_REF


# --- Degraded: half-open still binds (lets the trial call through) ------


def test_bind_allows_half_open_through_for_the_trial_call() -> None:
    # This is the case that actually proves the reaction isn't just
    # "refuse anything not perfectly healthy" — half-open must still
    # bind, or the breaker could never recover (no trial call would ever
    # reach provider-gateway's own breaker.Check).
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_HALF_OPEN, quota_headroom=1.0
    )
    assert bind(WORKER_REF, status) == WORKER_REF


# --- Exhausted: binding changes correctly (typed refusal) ---------------


def test_bind_raises_when_breaker_open() -> None:
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_OPEN, quota_headroom=1.0
    )
    with pytest.raises(ProviderUnavailable) as exc_info:
        bind(WORKER_REF, status)
    assert exc_info.value.provider == "anthropic"
    assert "circuit breaker is open" in exc_info.value.reason


def test_bind_raises_when_quota_exactly_exhausted() -> None:
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_CLOSED, quota_headroom=0.0
    )
    with pytest.raises(ProviderUnavailable) as exc_info:
        bind(WORKER_REF, status)
    assert "quota exhausted" in exc_info.value.reason


def test_bind_raises_when_quota_negative() -> None:
    # Should never happen from a real ProviderHealth response (headroom
    # is documented [0.0, 1.0]), but bind() must not silently allow a
    # malformed/unexpected value through either.
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_CLOSED, quota_headroom=-0.1
    )
    with pytest.raises(ProviderUnavailable):
        bind(WORKER_REF, status)


def test_bind_prefers_breaker_open_reason_when_both_conditions_hold() -> None:
    # Breaker state is checked first: if the breaker is open, THAT'S the
    # actionable reason (quota exhaustion is beside the point if the
    # circuit is already tripped) — asserts the error message is
    # specific, not generic, even when multiple things are wrong at once.
    status = ProviderStatus(
        provider="anthropic", breaker_state=BREAKER_STATE_OPEN, quota_headroom=0.0
    )
    with pytest.raises(ProviderUnavailable) as exc_info:
        bind(WORKER_REF, status)
    assert "circuit breaker is open" in exc_info.value.reason
