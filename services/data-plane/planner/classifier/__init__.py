"""planner.classifier — Phase-03 Step C: the complexity classifier.

Rules-based ONLY — no model call, ever. Deciding "is this simple enough
for the fast path" by asking an LLM would silently build Phase-06's job
(the LLM planner) inside the one path Phase-03.txt explicitly calls out
as having NO LLM planning cost. If a request doesn't fit the fast path's
one template, this module's only job is to say so cleanly and stop — not
force a degraded answer through the fast path, and not spend a model call
figuring out whether to.

This module takes ClassifierInput, not a generated SubmitRequest
protobuf, and imports nothing from grpc/generated code — zero network,
zero I/O, pure logic, so it can be unit tested (and reasoned about)
completely in isolation. Whatever eventually calls this (Submit's real
handler, wired in a later step) is responsible for extracting
ClassifierInput's two fields from a real request.

Scope note: with the current proto/dataplane wire contract, there is no
field expressing "this needs tool calls / multi-step reasoning" at all —
that expressiveness arrives whenever Phase-06 extends the contract for
its own planner. MAX_FAST_PATH_MESSAGES is therefore a deliberately
simple, untuned structural placeholder (same "not tuned against real
data" framing already used for provider-gateway's quota/breaker
thresholds in Phase-02) — a genuine, testable signal for "this has grown
past one simple exchange," not a claim to detect semantic complexity,
which this phase has no way to do without either an LLM (forbidden here)
or a richer contract (not this phase's job to build).
"""

from dataclasses import dataclass

# Mirrors proto/dataplane/v1/dataplane.proto's RequestKind enum values
# exactly (REQUEST_KIND_CHAT_COMPLETION = 1, REQUEST_KIND_RESPONSES = 2) —
# deliberately NOT imported from the generated dataplane_pb2 module so this
# package stays free of any grpc/generated-code dependency. Parity is
# asserted by tests/test_classifier.py, not just assumed.
KIND_CHAT_COMPLETION = 1
KIND_RESPONSES = 2

_FAST_PATH_KINDS = frozenset({KIND_CHAT_COMPLETION, KIND_RESPONSES})

# Untuned placeholder — see module docstring.
MAX_FAST_PATH_MESSAGES = 20


class NeedsDeliberatePath(Exception):
    """Raised when a request cannot be served by the fast path's one
    template DAG. Deliberately its own typed exception, not a generic
    ValueError or "unsupported" error — it marks the EXACT seam Phase-06's
    deliberate path (LLM planner, Workflow IR, cost gate) plugs into, so
    whatever catches it (Submit's real handler, a later step) can respond
    with an unambiguous "routes to the deliberate path, not yet available"
    signal instead of a vague failure a caller could mistake for a bug.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(
            f"requires the deliberate path (Phase-06 — LLM planner, Workflow IR, "
            f"cost gate — not yet available): {reason}"
        )


@dataclass(frozen=True)
class ClassifierInput:
    """The subset of a request classify() actually needs — kept separate
    from the full generated SubmitRequest so unit tests don't need to
    construct identity/streaming/model fields just to exercise routing
    logic, and so this package has no protobuf dependency at all."""

    kind: int
    message_count: int


def classify(req: ClassifierInput) -> None:
    """Raises NeedsDeliberatePath if `req` cannot be served by the fast
    path. Returns None (a routing decision, not data) if it can — the
    absence of an exception IS the "route to fast path" signal, mirroring
    how the rest of this codebase treats a clean return as success
    throughout (e.g. provider-gateway's breaker/quota Enforce functions)."""

    if req.kind not in _FAST_PATH_KINDS:
        raise NeedsDeliberatePath(
            f"request kind {req.kind} has no single-worker fast-path template"
        )

    if req.message_count > MAX_FAST_PATH_MESSAGES:
        raise NeedsDeliberatePath(
            f"{req.message_count} messages exceeds the fast path's bound "
            f"({MAX_FAST_PATH_MESSAGES}) for a single simple exchange"
        )
