"""aggregator — Phase-03 Step G: single-worker passthrough. Step H adds
usage capture (still passthrough — no new aggregation behavior).

Phase-03 only ever dispatches to ONE worker per request — provider.proto's
own InvokeRequest already assumes this (a single worker_ref, not a list).
This module RELAYS that one worker's stream; it does not aggregate,
merge, or arbitrate between multiple workers' outputs, does not track
provenance across contributions, and holds no blackboard state. That's
Phase-07's job (multi-agent). "Aggregator" here is Phase-03.txt's own
words — a passthrough synth, the smallest thing that could be called an
aggregator: one worker in, one stream out, nothing else.

Backpressure is inherent, not implemented: this is a pull-based async
generator that only asks provider-gateway for the next delta once its
own caller has consumed the previous yield — no manual buffering
anywhere, same discipline every other streaming relay in this codebase
already uses (provider-gateway's own Go relay, edge-gateway's SSE relay).

Step H: relay() now yields (SubmitResponse, Usage | None) pairs instead
of bare SubmitResponse. Usage is non-None exactly once, on the final
delta (provider.proto's own convention: usage fields are "set ONLY on
the final delta"), and its fields individually mirror that delta's own
presence semantics — a field is None when the provider didn't report it
(including the best-effort synthetic final delta provider-gateway sends
on an upstream error, which deliberately leaves both fields unset), never
coerced to 0. The caller (Submit's handler) is what decides what to do
with "usage incomplete" — this module's only job is to not lose or
fabricate the signal in translation.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

import grpc
from provider.v1 import provider_pb2


class _WorkerNodeLike(Protocol):
    """Structural shape relay() needs from a template DAG's WorkerNode —
    this module doesn't import planner.templates directly, so anything
    with these three READ-ONLY properties works (duck typing, matching
    this codebase's existing Protocol-based style, e.g. scheduler.admit's
    _IncrDecrClient). Declared as properties, not plain attributes:
    relay() only ever reads these, and the real WorkerNode is a frozen
    dataclass — a plain-attribute Protocol would wrongly demand a
    settable field."""

    @property
    def messages(self) -> tuple[Any, ...]: ...

    @property
    def max_tokens(self) -> int | None: ...

    @property
    def temperature(self) -> float | None: ...


@dataclass(frozen=True)
class Usage:
    """Mirrors provider.proto Delta.input_tokens/output_tokens field-for-
    field, including their presence semantics: None means "the provider
    didn't report this," not zero. Step H's metering write path is what
    turns this into usage_event's own NULL columns."""

    input_tokens: int | None
    output_tokens: int | None


class ProviderFallback(Exception):
    """Raised when provider-gateway's own breaker/quota governor signals
    a FallbackSignal mid-dispatch — provider-gateway's OWN real-time
    decision, made at actual dispatch time, distinct from
    scheduler.place's earlier ProviderHealth-based pre-check (which
    can't fully anticipate a state change between the check and the
    dispatch a moment later). Both become the same kind of clean typed
    error to the client — Phase-03.txt's own resilience requirement,
    fallback ROUTING to a different model is Phase-06."""

    def __init__(self, provider: str, reason: str) -> None:
        self.provider = provider
        self.reason = reason
        super().__init__(f"provider {provider!r} fallback: {reason}")


class ProviderStreamError(Exception):
    """Raised when the underlying gRPC call to provider-gateway itself
    fails (a genuine transport/upstream error, not a fallback signal) —
    wraps the original grpc.RpcError so the caller can produce a clean
    typed error without leaking a raw low-level gRPC error unexplained."""

    def __init__(self, cause: grpc.RpcError) -> None:
        self.cause = cause
        super().__init__(f"provider stream error: {cause}")


def _build_invoke_request(
    request_id: str, worker_ref: str, worker: _WorkerNodeLike
) -> provider_pb2.InvokeRequest:
    messages = [
        provider_pb2.ChatMessage(role=m.role, content=m.content) for m in worker.messages
    ]
    params = None
    if worker.max_tokens is not None or worker.temperature is not None:
        params = provider_pb2.InvokeParams(
            max_tokens=worker.max_tokens, temperature=worker.temperature
        )
    return provider_pb2.InvokeRequest(
        request_id=request_id, worker_ref=worker_ref, messages=messages, params=params
    )


def _usage_from_delta(delta: Any) -> Usage:
    return Usage(
        input_tokens=delta.input_tokens if delta.HasField("input_tokens") else None,
        output_tokens=delta.output_tokens if delta.HasField("output_tokens") else None,
    )


async def relay(
    provider_stub: Any, request_id: str, worker_ref: str, worker: _WorkerNodeLike
) -> AsyncIterator[tuple[Any, Usage | None]]:
    """Dispatches `worker` (a template DAG's one WorkerNode) to
    provider-gateway as worker_ref, and relays each delta as a
    (dataplane SubmitResponse, Usage | None) pair — Usage is set exactly
    once, on the final delta, else None. Raises ProviderFallback if
    provider-gateway signals one mid-dispatch, or ProviderStreamError if
    the gRPC call itself fails — the caller (Submit's handler) turns
    either into a clean typed error to the client, never lets a raw
    exception leak through unexplained.
    """
    from dataplane.v1 import dataplane_pb2

    invoke_req = _build_invoke_request(request_id, worker_ref, worker)
    try:
        async for resp in provider_stub.Invoke(invoke_req):
            if resp.HasField("fallback"):
                reason = provider_pb2.FallbackReason.Name(resp.fallback.reason)
                raise ProviderFallback(resp.fallback.provider, reason)
            delta = resp.delta
            submit_response = dataplane_pb2.SubmitResponse(
                request_id=delta.request_id,
                content=delta.content if delta.HasField("content") else None,
                finish_reason=delta.finish_reason if delta.HasField("finish_reason") else None,
                is_final=delta.is_final,
            )
            usage = _usage_from_delta(delta) if delta.is_final else None
            yield submit_response, usage
    except grpc.RpcError as e:
        raise ProviderStreamError(e) from e
