"""request_log — Phase-03 Step F: minimal durable write, pulled forward
from Step H.

This is deliberately NOT the real metering integration: Step H owns the
actual token/cost math, usage_event population, and the honest
incomplete-usage handling design (a stream that ends without a
usage-bearing final delta must be recorded as incomplete, never
silently zero). request_log's own schema doesn't need any of that — it's
a simple per-request audit row (request_id, org_id, path taken, status,
latency) — which is why this one table's write path exists now: Step F's
statelessness proof needs something genuinely durable to test a pod kill
against, and this is the smallest real slice of Phase-03.txt's own
Deployment Step 1 that provides one without anticipating Step H's harder
problem.
"""

from typing import Any, Protocol


class _PgPool(Protocol):
    def acquire(self) -> Any: ...


async def write(
    pg_pool: _PgPool,
    request_id: str,
    org_id: str,
    path: str,
    status: str,
    latency_ms: int,
) -> None:
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO request_log (request_id, org_id, path, status, latency_ms) "
            "VALUES ($1, $2, $3, $4, $5)",
            request_id,
            org_id,
            path,
            status,
            latency_ms,
        )
