"""Unit tests for working_memory — Step F.

No real Redis: tested against a small in-memory fake client that records
exactly what arguments each call received, so these tests can assert on
the ONE property that actually matters — TTL is set atomically in the
same call as the write, not a separate step that could be skipped or
race — not just that read-after-write happens to work.
"""

import asyncio

from working_memory import TTL_SECONDS, delete, key, read, write


class FakeRedis:
    """Records (value, ex) per key, exactly as the real client's set()
    call shape — lets tests assert the TTL argument was actually passed,
    not just that SOME write happened."""

    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int]] = {}
        self.deleted: list[str] = []

    async def set(self, name: str, value: str, ex: int) -> None:
        self.store[name] = (value, ex)

    async def get(self, name: str) -> str | None:
        entry = self.store.get(name)
        return entry[0] if entry else None

    async def delete(self, name: str) -> None:
        self.store.pop(name, None)
        self.deleted.append(name)


def test_key_naming() -> None:
    assert key("req-1") == "workingmem:req-1"


def test_write_sets_ttl_in_the_same_call_as_the_value() -> None:
    redis = FakeRedis()
    asyncio.run(write(redis, "req-1", {"transcript": ["hi"]}))

    stored_value, stored_ttl = redis.store[key("req-1")]
    assert stored_ttl == TTL_SECONDS
    assert stored_ttl > 0  # a TTL of 0 or None would mean "never expires"


def test_write_honors_an_explicit_ttl_override() -> None:
    redis = FakeRedis()
    asyncio.run(write(redis, "req-1", {}, ttl_seconds=5))
    _, stored_ttl = redis.store[key("req-1")]
    assert stored_ttl == 5


def test_read_returns_written_data() -> None:
    redis = FakeRedis()
    asyncio.run(write(redis, "req-1", {"role": "user", "content": "hi"}))
    result = asyncio.run(read(redis, "req-1"))
    assert result == {"role": "user", "content": "hi"}


def test_read_returns_none_for_a_request_id_never_written() -> None:
    redis = FakeRedis()
    assert asyncio.run(read(redis, "never-written")) is None


def test_read_returns_none_after_expiry_same_as_never_written() -> None:
    # Simulates TTL expiry: the fake client just doesn't have the key
    # anymore (a real Redis client would behave identically once the TTL
    # elapses) — callers must not be able to distinguish "expired" from
    # "never written", by design (see module docstring).
    redis = FakeRedis()
    asyncio.run(write(redis, "req-1", {"x": 1}, ttl_seconds=1))
    redis.store.pop(key("req-1"))  # simulate the TTL having elapsed
    assert asyncio.run(read(redis, "req-1")) is None


def test_delete_removes_the_key() -> None:
    redis = FakeRedis()
    asyncio.run(write(redis, "req-1", {"x": 1}))
    asyncio.run(read(redis, "req-1"))  # sanity: it's there
    asyncio.run(delete(redis, "req-1"))
    assert asyncio.run(read(redis, "req-1")) is None
