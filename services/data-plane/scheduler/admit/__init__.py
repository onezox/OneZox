"""scheduler.admit — Phase-03 Step E: fleet-wide admission control.

Mirrors edge-gateway's own Phase-01 admission gauge shape (Redis-backed
in-flight counter, accept/shed) — the established pattern in this
codebase for "is this service's own fleet at capacity," reused here
rather than inventing a third shape. Deliberately NOT the same concern
as provider-gateway's Phase-02 quota governor: quota protects a
PROVIDER's rate limits (provider:{name}:quota:{window}); admission
protects THIS service's own capacity (admission:data-plane:inflight) —
independent counters, independent purpose, same Redis-backed shape.

Split into a pure decision function (decide) and an I/O wrapper (admit/
release) that does the actual INCR/DECR — same split provider-gateway's
own quota.Decide/quota.Enforce used in Go, for the same reason: the
decision logic is unit-testable without a real Redis connection.
"""

from typing import Protocol

ADMISSION_KEY = "admission:data-plane:inflight"


class Shed(Exception):
    """Raised when the fleet is already at capacity. Mirrors
    edge-gateway's own accept/queue/shed vocabulary (Phase-01) — Phase-03
    implements accept-or-shed, not the queue state; nothing in
    Phase-03.txt calls for a request queue."""

    def __init__(self, inflight: int, limit: int) -> None:
        self.inflight = inflight
        self.limit = limit
        super().__init__(f"admission shed: {inflight} in-flight >= limit {limit}")


def decide(inflight: int, limit: int) -> None:
    """Pure decision: raises Shed if `inflight` (the gauge's value AFTER
    this request's own increment) is at or over `limit`. Returns
    normally (allow) otherwise. No I/O — a snapshot in, a decision out,
    same shape as provider-gateway's quota.Decide."""
    if inflight >= limit:
        raise Shed(inflight, limit)


class _IncrDecrClient(Protocol):
    async def incr(self, key: str) -> int: ...
    async def decr(self, key: str) -> int: ...


async def admit(redis_client: _IncrDecrClient, limit: int) -> int:
    """I/O wrapper: increments the fleet-wide in-flight gauge and applies
    decide() to the result. Returns the new in-flight count on success.
    On Shed, decrements back out — a rejected request must not
    permanently inflate the gauge. Caller MUST call release() when the
    request finishes (success or failure) if admit() succeeded — held
    for the request's full lifetime, same discipline Phase-01's
    AdmissionGuard used."""
    inflight = await redis_client.incr(ADMISSION_KEY)
    try:
        decide(inflight, limit)
    except Shed:
        await redis_client.decr(ADMISSION_KEY)
        raise
    return inflight


async def release(redis_client: _IncrDecrClient) -> None:
    await redis_client.decr(ADMISSION_KEY)
