"""Unit test for aggregator — Step G.

_build_invoke_request is the one pure piece of this module (everything
else is I/O against a real provider-gateway stream) — tested directly
against a small stand-in worker object satisfying _WorkerNodeLike's
shape. The actual point of Step G — that relay() genuinely composes with
a real provider-gateway and correctly turns a FallbackSignal/stream error
into a typed exception — is proven live against the deployed cluster
(Phase-03-Progress.txt), not here.
"""

from dataclasses import dataclass

from aggregator import _build_invoke_request


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
