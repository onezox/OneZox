"""usage_event — Phase-03 Step H: the billing substrate write path (EC2).

0006_create_usage_event.sql already established the schema (pulled
forward in Step F, alongside request_log, to give that step's pod-kill
proof something genuinely durable to test against) and its own reasoning
for why tokens_in/tokens_out/orch_tokens/usd_cost are NULLABLE: NULL
means "not known," a different fact from zero, mirroring
proto/provider's own Delta.input_tokens/output_tokens presence
semantics (Step A). This module is what actually decides, at write
time, when those fields are populated versus left NULL — main.py's
Submit handler passes through exactly what aggregator.Usage reported,
never substituting an estimate or a zero for "not reported."

event_id (gen_random_uuid()) and ts (now()) both have column defaults;
this module never sets them itself.
"""

from typing import Any, Protocol


class _PgPool(Protocol):
    def acquire(self) -> Any: ...


async def write(
    pg_pool: _PgPool,
    org_id: str,
    request_id: str,
    tokens_in: int | None,
    tokens_out: int | None,
    orch_tokens: int | None,
    usd_cost: float | None,
    model_ref: str,
) -> None:
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO usage_event "
            "(org_id, request_id, tokens_in, tokens_out, orch_tokens, usd_cost, model_ref) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            org_id,
            request_id,
            tokens_in,
            tokens_out,
            orch_tokens,
            usd_cost,
            model_ref,
        )
