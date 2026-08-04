"""planner.templates — Phase-03 Step D: the fast path's template DAG.

Hold the template-vs-plan distinction, because it's the whole point of
this module existing separately from a future planner: a TEMPLATE
produces a FIXED, predetermined execution shape — instantiate() always
returns exactly one WorkerNode, for every request that reaches it, no
matter what the request's content says. It does not read message
CONTENT to decide what shape the graph should be, does not branch on how
sophisticated a request "sounds," and imports nothing that could call a
model or a planner. That per-request reasoning about the graph's shape
IS planning — Phase-06's job, not this module's. The classifier (Step C)
is what already decided this request belongs on the fast path at all;
by the time anything calls instantiate(), that decision is over, and this
module does not repeat, second-guess, or refine it.

Single-worker only, on purpose — provider.proto's own InvokeRequest
already assumes one worker per call (Part G.1's "worker_ref" is a single
value, not a list). A general N-worker DAG executor "for flexibility"
would be building Phase-06/07's multi-agent scope now; Phase-03 only
ever produces the one shape below.

This module does not decide WHICH real model backs the worker — that is
the scheduler's job (Step E), using live provider health. instantiate()
only produces a logical role ("primary") and the request's own messages/
params, mechanically carried through.
"""

from dataclasses import dataclass

# The fast path has exactly one role. Not a registry of roles "for later"
# — Phase-03 produces this one shape; adding more roles is Phase-06/07
# scope (multi-agent), same reasoning the module docstring gives for
# staying single-worker.
PRIMARY_ROLE = "primary"


@dataclass(frozen=True)
class Message:
    role: str
    content: str


@dataclass(frozen=True)
class TemplateRequest:
    """The subset of a classified-simple request instantiate() needs —
    kept separate from the generated SubmitRequest protobuf, same reason
    planner.classifier.ClassifierInput is: zero grpc/generated-code
    dependency, and a caller assembles this from whatever request shape
    it actually has."""

    request_id: str
    messages: tuple[Message, ...]
    max_tokens: int | None = None
    temperature: float | None = None


@dataclass(frozen=True)
class WorkerNode:
    """One node in the fast-path DAG: what to send, not which real model
    backs it. role is a logical designation the scheduler (Step E) binds
    to an actual worker_ref via live provider health — never decided
    here."""

    role: str
    messages: tuple[Message, ...]
    max_tokens: int | None
    temperature: float | None


@dataclass(frozen=True)
class TemplateDAG:
    """The fast path's fixed execution shape: exactly one WorkerNode.
    Not a general graph type — there is no edge list, no multi-node
    traversal, because Phase-03 never produces more than one node. Adding
    that generality now would be building toward Phase-06/07's shape
    before there's a real second node to justify it."""

    request_id: str
    worker: WorkerNode


def instantiate(req: TemplateRequest) -> TemplateDAG:
    """Mechanically instantiates the ONE fast-path template from an
    already-classified-simple request. Every field on the resulting
    WorkerNode is a direct, unconditional copy from `req` — there is no
    branch anywhere in this function keyed on message content, count, or
    any other property of the request. That absence of branching is what
    makes this a template rather than a plan: the exact same shape comes
    out no matter what `req` contains, only the DATA inside that shape
    differs.
    """
    worker = WorkerNode(
        role=PRIMARY_ROLE,
        messages=req.messages,
        max_tokens=req.max_tokens,
        temperature=req.temperature,
    )
    return TemplateDAG(request_id=req.request_id, worker=worker)
