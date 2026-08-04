"""scheduler.place — Phase-03 Step E: role -> model binding via live
provider health.

Model source is STATIC this phase (Dependencies.txt F10: the signed
registry replaces this in Phase-04) — bind() only ever has ONE candidate
worker_ref, read from static config elsewhere, not looked up from a
registry. Binding here means deciding whether THAT ONE static worker is
currently trustworthy enough to use, using live signals from
provider-gateway's ProviderHealth RPC (Step A7) — not picking among
alternatives (there are none this phase) and not implementing fallback
routing to a different model (Phase-03.txt's own TESTING REQUIREMENTS:
"fallback routing is added in Phase-06" — there is nothing to fall back
TO this phase regardless).

The reaction to health is the whole point of this module: a binder that
calls ProviderHealth and ignores the answer looks identical to a working
one as long as the provider stays healthy — the difference only shows
under degradation, which is why bind()'s own unit tests and this step's
live verification both focus on the unhealthy states, not the happy one.
"""

from dataclasses import dataclass

# BreakerState enum values, mirroring proto/provider/v1/provider.proto's
# numbering exactly (documented parity, verified by a test — same
# approach Step C used for RequestKind). Kept as plain ints, not imported
# from generated code, so bind()'s own decision logic has no grpc
# dependency; only the I/O wrapper below needs the real stub type.
BREAKER_STATE_CLOSED = 1
BREAKER_STATE_OPEN = 2
BREAKER_STATE_HALF_OPEN = 3


class ProviderUnavailable(Exception):
    """Raised when the static model's provider is not currently
    trustworthy to bind to — breaker OPEN, or fleet-wide quota fully
    exhausted. Mirrors provider-gateway's own FallbackSignal concept but
    one hop earlier: rather than dispatching a call provider-gateway
    would just deny anyway, the scheduler refuses to bind at all. This
    becomes a clean typed error to the client (Phase-03.txt's own
    resilience requirement) — not a fallback to a different model, since
    there is no different model this phase.
    """

    def __init__(self, provider: str, reason: str) -> None:
        self.provider = provider
        self.reason = reason
        super().__init__(f"provider {provider!r} unavailable: {reason}")


@dataclass(frozen=True)
class ProviderStatus:
    """The subset of a ProviderHealthResponse status entry bind() needs —
    decoupled from generated protobuf, same reasoning as
    planner.classifier.ClassifierInput and planner.templates.
    TemplateRequest."""

    provider: str
    breaker_state: int
    quota_headroom: float


def bind(worker_ref: str, status: ProviderStatus) -> str:
    """Pure decision: given the one static worker_ref and its provider's
    current health, either return worker_ref unchanged (bind succeeds)
    or raise ProviderUnavailable. No I/O — the ProviderHealth RPC call
    itself lives in bind_via_provider_health below, kept separate so
    this decision logic is unit-testable without a network call,
    mirroring provider-gateway's own quota.Decide (pure) / quota.Enforce
    (I/O) split in Go.
    """
    if status.breaker_state == BREAKER_STATE_OPEN:
        raise ProviderUnavailable(status.provider, "circuit breaker is open")
    if status.quota_headroom <= 0.0:
        raise ProviderUnavailable(status.provider, "fleet-wide quota exhausted")
    # BREAKER_STATE_HALF_OPEN is deliberately allowed through, not
    # refused: half-open exists specifically to let ONE trial call
    # through and test recovery. provider-gateway's own breaker.Check
    # already enforces "only one trial call at a time" when the actual
    # Invoke happens later (Step G) — if the scheduler also refused to
    # bind here, the breaker could never recover, since no trial call
    # would ever reach it.
    return worker_ref


def parse_provider(worker_ref: str) -> str:
    """worker_ref's "<provider>:<model>" convention (established
    Phase-02) — only the provider prefix, needed to know which
    ProviderHealth status applies."""
    provider, _, _ = worker_ref.partition(":")
    return provider


async def bind_via_provider_health(stub: object, worker_ref: str) -> str:
    """I/O wrapper: calls the real ProviderHealth RPC (Step A7) for
    worker_ref's provider and feeds the result into bind()'s pure
    decision. `stub` is a provider_pb2_grpc.ProviderServiceStub — typed
    as `object` here so this module doesn't force every caller (like
    bind()'s own unit tests) to import generated gRPC code; the real
    caller (Submit's handler, wired in a later step) has the real type.
    """
    # Deferred, local import (not at module top) so importing
    # scheduler.place itself — and unit-testing bind() — never requires
    # generated/ on sys.path at all; only actually CALLING this specific
    # I/O function does. Re-inserts the same relative path main.py sets
    # up at process start (redundant there, harmless) so this function
    # also works called from a standalone script that never ran main.py
    # (Step E's own live-verification script does exactly that).
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "generated"))
    from provider.v1 import provider_pb2

    provider = parse_provider(worker_ref)
    resp = await stub.ProviderHealth(provider_pb2.ProviderHealthRequest(provider=provider))  # type: ignore[attr-defined]
    if not resp.statuses:
        raise ProviderUnavailable(provider, "provider-gateway reported no status for this provider")
    s = resp.statuses[0]
    status = ProviderStatus(
        provider=s.provider, breaker_state=s.breaker_state, quota_headroom=s.quota_headroom
    )
    return bind(worker_ref, status)
