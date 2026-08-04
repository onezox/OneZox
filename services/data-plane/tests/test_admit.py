"""Unit tests for scheduler.admit — Step E.

No real Redis: decide() is pure logic, and admit()/release() are tested
against a small in-memory fake client, mirroring provider-gateway's own
quota.FakeCounter pattern in Go. Async calls are driven via asyncio.run()
inside plain sync test functions rather than pulling in pytest-asyncio —
a new dev dependency shared across every Python service in this monorepo
(pyproject.toml's [dependency-groups]) isn't worth adding for one test
file's convenience.
"""

import asyncio

import pytest
from scheduler.admit import ADMISSION_KEY, Shed, admit, decide, release


class FakeRedis:
    """Minimal async incr/decr double — just enough to satisfy admit()'s
    _IncrDecrClient protocol, same "hermetic, no live Redis" rationale
    provider-gateway's FakeCounter used."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def decr(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) - 1
        return self.counts[key]


def test_decide_allows_under_limit() -> None:
    decide(inflight=5, limit=10)  # must not raise


def test_decide_sheds_at_limit() -> None:
    with pytest.raises(Shed):
        decide(inflight=10, limit=10)


def test_decide_sheds_over_limit() -> None:
    with pytest.raises(Shed):
        decide(inflight=11, limit=10)


def test_admit_increments_and_allows_under_limit() -> None:
    redis = FakeRedis()
    inflight = asyncio.run(admit(redis, limit=10))
    assert inflight == 1
    assert redis.counts[ADMISSION_KEY] == 1


def test_admit_sheds_and_decrements_back_when_at_limit() -> None:
    redis = FakeRedis()
    redis.counts[ADMISSION_KEY] = 9  # 9 already in flight, limit 10

    with pytest.raises(Shed):
        asyncio.run(admit(redis, limit=10))

    # A rejected request must not permanently inflate the gauge.
    assert redis.counts[ADMISSION_KEY] == 9


def test_release_decrements() -> None:
    redis = FakeRedis()
    redis.counts[ADMISSION_KEY] = 1
    asyncio.run(release(redis))
    assert redis.counts[ADMISSION_KEY] == 0
