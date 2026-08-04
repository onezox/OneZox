"""Unit tests for request_log — Step F.

No real CockroachDB: write() is exercised against a small fake pool/
connection double that records the exact SQL and parameters it received,
proving the write shape is correct (right columns, right order) without
a live database.
"""

import asyncio
from typing import Any

import request_log


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


def test_write_inserts_with_the_correct_columns_and_values() -> None:
    pool = FakePool()
    asyncio.run(
        request_log.write(
            pool,
            request_id="req-1",
            org_id="org-1",
            path="fast",
            status="ok",
            latency_ms=42,
        )
    )

    assert len(pool.calls) == 1
    query, args = pool.calls[0]
    assert "INSERT INTO request_log" in query
    assert args == ("req-1", "org-1", "fast", "ok", 42)
