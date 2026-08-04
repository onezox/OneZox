"""working_memory — Phase-03 Step F: in-request-only transcript/DAG state.

The engine stays STATELESS (CLAUDE.md's own Phase-03 scope line): working
memory is keyed workingmem:{request_id}, TTL'd to the request's own
lifetime, and nothing here is ever durable. This module is the one place
that boundary is enforced in code — every write sets an expiry ATOMICALLY
(SET key value EX ttl, a single Redis command), not via a separate
EXPIRE call after the fact. A separate call would leave a real window
where the key exists with no TTL at all if the process died between the
two commands — which is exactly the "looks request-scoped until Redis
quietly fills with dead state" failure this module exists to rule out,
not just discourage.

TTL_SECONDS is an untuned placeholder bound on how long any single
request is expected to take (same "not tuned against real data" framing
already used for provider-gateway's quota/breaker thresholds and this
module's own admission/classifier siblings) — generous enough for a real
streamed completion, short enough that a key genuinely evaporates soon
after, TTL-enforced regardless of whether the owning pod is even still
alive to explicitly clean up.
"""

import json
from typing import Any, Protocol

KEY_PREFIX = "workingmem:"
TTL_SECONDS = 300  # untuned placeholder, see module docstring


def key(request_id: str) -> str:
    return f"{KEY_PREFIX}{request_id}"


class _RedisClient(Protocol):
    async def set(self, name: str, value: str, ex: int) -> Any: ...
    async def get(self, name: str) -> str | None: ...
    async def delete(self, name: str) -> Any: ...


async def write(
    redis_client: _RedisClient,
    request_id: str,
    data: dict[str, Any],
    ttl_seconds: int = TTL_SECONDS,
) -> None:
    """Writes `data` (JSON-serialized) under workingmem:{request_id},
    with the TTL set in the SAME command as the write — see module
    docstring for why that atomicity is the actual point, not a style
    preference."""
    await redis_client.set(key(request_id), json.dumps(data), ex=ttl_seconds)


async def read(redis_client: _RedisClient, request_id: str) -> dict[str, Any] | None:
    """Returns the stored data, or None if the key doesn't exist — either
    because nothing was ever written for this request, or because its
    TTL already expired. Callers must not distinguish those two cases;
    doing so would mean depending on timing, which is exactly what a
    stateless engine must not do."""
    raw = await redis_client.get(key(request_id))
    if raw is None:
        return None
    result: dict[str, Any] = json.loads(raw)
    return result


async def delete(redis_client: _RedisClient, request_id: str) -> None:
    """Explicit clean-path deletion at request end — the TTL is the
    safety net for the case this never runs (a killed pod, a crash), not
    a substitute for it. Both must independently guarantee the same
    outcome: no working memory outlives its request."""
    await redis_client.delete(key(request_id))
