"""Unit tests for usage_event — Step H.

No real CockroachDB: write() is exercised against the same small fake
pool/connection double request_log's own tests use, proving the write
shape (right columns, right order, NULL passed through as Python None
rather than coerced to 0) without a live database. The live reconciliation
proof — that the number written here equals the fake's real reported
usage and the request span — is in Phase-03-Progress.txt, not here.
"""

import asyncio
from typing import Any

import usage_event


class FakeConn:
    def __init__(self, calls: list[tuple[str, tuple[Any, ...]]]) -> None:
        self._calls = calls

    async def execute(self, query: str, *args: Any) -> None:
        self._calls.append((query, args))

    async def __aenter__(self) -> "FakeConn":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class FakePool:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def acquire(self) -> FakeConn:
        return FakeConn(self.calls)


def test_write_inserts_real_reported_tokens_with_the_correct_columns() -> None:
    pool = FakePool()
    asyncio.run(
        usage_event.write(
            pool,
            org_id="org-1",
            request_id="req-1",
            tokens_in=42,
            tokens_out=17,
            orch_tokens=0,
            usd_cost=None,
            model_ref="fake:normal",
        )
    )

    assert len(pool.calls) == 1
    query, args = pool.calls[0]
    assert "INSERT INTO usage_event" in query
    assert args == ("org-1", "req-1", 42, 17, 0, None, "fake:normal")


def test_write_passes_none_through_as_null_not_zero() -> None:
    # The incomplete-usage case: tokens_in/tokens_out unknown after a
    # mid-stream failure. None must reach the query as-is — silently
    # substituting 0 here is exactly the billing-corrupting bug this step
    # exists to rule out.
    pool = FakePool()
    asyncio.run(
        usage_event.write(
            pool,
            org_id="org-1",
            request_id="req-2",
            tokens_in=None,
            tokens_out=None,
            orch_tokens=0,
            usd_cost=None,
            model_ref="fake:fail_mid_stream",
        )
    )

    _, args = pool.calls[0]
    assert args[2] is None
    assert args[3] is None
    assert args[2] != 0
    assert args[3] != 0
