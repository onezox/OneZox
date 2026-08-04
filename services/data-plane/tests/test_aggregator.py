"""Unit tests for aggregator — Steps G and H.

_build_invoke_request is the one pure piece of Step G's own passthrough
(everything else is I/O against a real provider-gateway stream) — tested
directly against a small stand-in worker object satisfying
_WorkerNodeLike's shape. The actual point of Step G — that relay()
genuinely composes with a real provider-gateway and correctly turns a
FallbackSignal/stream error into a typed exception — is proven live
against the deployed cluster (Phase-03-Progress.txt), not here.

Step H's own tests below exercise relay()'s usage-capture "math" against
a fake provider_stub that yields hand-built provider_pb2.InvokeResponse
messages directly (no real gRPC, no cluster) — this proves the capture
logic itself: usage is read ONLY off the final delta, and mirrors that
delta's own field-level presence exactly (never coerced to zero), which
is what main.py's Submit handler relies on to write usage_event honestly.
42/17 below are provider-fake's own documented canned
cannedInputTokens/cannedOutputTokens (services/provider-fake/main.go) —
using the real values here, not arbitrary numbers, is what makes this a
regression guard against that constant drifting silently.

Async calls are driven via asyncio.run() inside plain sync test
functions, matching test_admit.py/test_place.py's own established
reasoning: not worth a new pytest-asyncio dev dependency for one file's
convenience.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from aggregator import Usage, _build_invoke_request, relay
from provider.v1 import provider_pb2


@dataclass(frozen=True)
class _FakeMessage:
    role: str
    content: str


@dataclass(frozen=True)
class _FakeWorker:
    messages: tuple[_FakeMessage, ...]
    max_tokens: int | None = None
    temperature: float | None = None


def test_build_invoke_request_carries_worker_ref_and_messages() -> None:
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),))
    req = _build_invoke_request("req-1", "fake:normal", worker)

    assert req.request_id == "req-1"
    assert req.worker_ref == "fake:normal"
    assert len(req.messages) == 1
    assert req.messages[0].role == "user"
    assert req.messages[0].content == "hi"


def test_build_invoke_request_omits_params_when_both_unset() -> None:
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),))
    req = _build_invoke_request("req-1", "fake:normal", worker)
    assert not req.HasField("params")


def test_build_invoke_request_includes_params_when_set() -> None:
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),), max_tokens=64, temperature=0.5)
    req = _build_invoke_request("req-1", "fake:normal", worker)
    assert req.HasField("params")
    assert req.params.max_tokens == 64
    assert req.params.temperature == 0.5


# --- Step H: usage capture "math" -------------------------------------------


def _delta_response(
    *,
    content: str | None = None,
    finish_reason: str | None = None,
    is_final: bool = False,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
) -> Any:
    delta = provider_pb2.Delta(request_id="req-1", is_final=is_final)
    if content is not None:
        delta.content = content
    if finish_reason is not None:
        delta.finish_reason = finish_reason
    if input_tokens is not None:
        delta.input_tokens = input_tokens
    if output_tokens is not None:
        delta.output_tokens = output_tokens
    return provider_pb2.InvokeResponse(delta=delta)


class _FakeProviderStub:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = responses

    def Invoke(self, req: Any) -> AsyncIterator[Any]:
        async def gen() -> AsyncIterator[Any]:
            for resp in self._responses:
                yield resp

        return gen()


async def _drain(stream: AsyncIterator[Any]) -> list[tuple[Any, Usage | None]]:
    return [item async for item in stream]


def test_relay_captures_real_usage_reported_on_final_delta() -> None:
    # 42/17 are provider-fake's real canned cannedInputTokens/
    # cannedOutputTokens (services/provider-fake/main.go) — the exact
    # numbers a live fake:normal call reports, not stand-ins.
    stub = _FakeProviderStub(
        [
            _delta_response(content="Hello "),
            _delta_response(content="world."),
            _delta_response(
                finish_reason="stop", is_final=True, input_tokens=42, output_tokens=17
            ),
        ]
    )
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),))
    results = asyncio.run(_drain(relay(stub, "req-1", "fake:normal", worker)))

    assert len(results) == 3
    assert results[0][1] is None
    assert results[1][1] is None
    final_response, final_usage = results[2]
    assert final_usage == Usage(input_tokens=42, output_tokens=17)
    assert final_response.finish_reason == "stop"
    assert final_response.is_final is True


def test_relay_reports_usage_as_incomplete_not_zero_on_error_delta() -> None:
    # Mirrors provider-gateway's own best-effort synthetic final delta
    # (main.go, on an UpstreamError): finish_reason="error", is_final=True,
    # usage fields deliberately left UNSET. The honest result is a Usage
    # with both fields None — never a Usage(0, 0), which would silently
    # claim the call cost nothing.
    stub = _FakeProviderStub(
        [
            _delta_response(content="partial "),
            _delta_response(finish_reason="error", is_final=True),
        ]
    )
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),))
    results = asyncio.run(_drain(relay(stub, "req-1", "fake:normal", worker)))

    assert results[0][1] is None
    final_response, final_usage = results[1]
    assert final_usage == Usage(input_tokens=None, output_tokens=None)
    assert final_usage != Usage(input_tokens=0, output_tokens=0)
    assert final_response.finish_reason == "error"


def test_relay_never_reads_usage_off_a_non_final_delta() -> None:
    # Even if a delta hypothetically carried token fields without being
    # final (never happens for a real provider, per provider.proto's own
    # "set ONLY on the final delta" convention) — relay() must not read
    # them early.
    stub = _FakeProviderStub(
        [
            _delta_response(content="x", input_tokens=99, output_tokens=99, is_final=False),
            _delta_response(finish_reason="stop", is_final=True, input_tokens=42, output_tokens=17),
        ]
    )
    worker = _FakeWorker(messages=(_FakeMessage("user", "hi"),))
    results = asyncio.run(_drain(relay(stub, "req-1", "fake:normal", worker)))

    assert results[0][1] is None
    assert results[1][1] == Usage(input_tokens=42, output_tokens=17)
