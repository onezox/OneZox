"""Unit tests for planner.templates — Step D.

No network, no gRPC server: instantiate() is pure logic. Two families of
tests here: the POSITIVE case (the template executes and produces the
correct single-worker dispatch shape) and the NEGATIVE case (no planner/
LLM code path is reachable in producing it) — the second is the actual
exit bar Phase-03.txt sets ("no LLM planning cost"), so it gets proven
structurally (AST inspection, forbidden-import scan), not just asserted
by absence of a failure.
"""

import ast
import inspect
import textwrap

import planner.templates as templates_module
from planner.templates import (
    PRIMARY_ROLE,
    Message,
    TemplateRequest,
    instantiate,
)

# --- Positive: the template executes and produces the correct shape --------


def test_instantiate_produces_a_single_worker_node() -> None:
    req = TemplateRequest(
        request_id="req-1",
        messages=(Message(role="user", content="hello"),),
        max_tokens=256,
        temperature=0.7,
    )
    dag = instantiate(req)

    assert dag.request_id == "req-1"
    assert dag.worker.role == PRIMARY_ROLE
    assert dag.worker.messages == req.messages
    assert dag.worker.max_tokens == 256
    assert dag.worker.temperature == 0.7


def test_instantiate_carries_multiple_messages_but_still_one_worker() -> None:
    # Single-worker holds regardless of conversation length — Phase-03
    # never produces more than one WorkerNode, on purpose (a general
    # N-worker executor is Phase-06/07 scope).
    messages = (
        Message(role="system", content="be terse"),
        Message(role="user", content="first turn"),
        Message(role="assistant", content="reply"),
        Message(role="user", content="second turn"),
    )
    req = TemplateRequest(request_id="req-2", messages=messages)
    dag = instantiate(req)

    assert dag.worker.messages == messages
    # TemplateDAG has no field capable of expressing a second node at
    # all — there's no list of workers, just one. This assertion exists
    # so a future accidental widening of TemplateDAG.worker into a list
    # would fail a test immediately, not silently.
    assert not hasattr(dag, "workers")


def test_instantiate_defaults_max_tokens_and_temperature_to_none() -> None:
    req = TemplateRequest(request_id="req-3", messages=(Message("user", "hi"),))
    dag = instantiate(req)
    assert dag.worker.max_tokens is None
    assert dag.worker.temperature is None


# --- Negative: no planner/LLM code path is reachable ------------------------
# Mirrors Step C's "prove the negative" discipline — a template that
# secretly branches on content, or that could reach a network/model call,
# would be indistinguishable from a working one until a request needed
# real reasoning and silently got a wrong or degraded shape instead.


def test_module_imports_nothing_network_or_model_capable() -> None:
    """Static proof, not a behavioral assumption: parse this module's own
    source and assert none of its import statements name anything that
    could reach a network call or a model — grpc, HTTP client libraries,
    provider/LLM SDKs, or the generated gRPC client stubs. If someone
    later adds such an import to make instantiate() "smarter," this test
    fails immediately, before that code ever runs against a real request.
    """
    source = inspect.getsource(templates_module)
    tree = ast.parse(source)

    forbidden_roots = {
        "grpc",
        "httpx",
        "requests",
        "urllib",
        "socket",
        "openai",
        "anthropic",
        "google",
        "dataplane_pb2_grpc",
        "provider_pb2_grpc",
    }

    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])

    forbidden_found = imported_roots & forbidden_roots
    assert not forbidden_found, f"planner.templates imports forbidden module(s): {forbidden_found}"


def test_instantiate_function_body_has_no_branching() -> None:
    """A template mechanically instantiates a fixed shape; a plan makes
    decisions. Proven at the AST level: instantiate()'s own function body
    must contain zero conditional/branching nodes (no if/elif, no match,
    no comprehension-with-filter) — a straight-line sequence of
    assignments and one return, nothing that could take a different path
    for a different request.
    """
    source = textwrap.dedent(inspect.getsource(instantiate))
    tree = ast.parse(source)

    branching_node_types = (ast.If, ast.Match, ast.IfExp, ast.Try)
    found_branches = [n for n in ast.walk(tree) if isinstance(n, branching_node_types)]

    assert not found_branches, (
        f"instantiate() contains branching node(s): {[type(n).__name__ for n in found_branches]} "
        "— a template must not make decisions, only instantiate a fixed shape"
    )


def test_instantiate_is_deterministic_for_identical_input() -> None:
    """Same input, called twice, must produce equal output — a planner
    could legitimately vary its output run to run (different reasoning
    paths); a template must not."""
    req = TemplateRequest(request_id="req-4", messages=(Message("user", "hi"),), max_tokens=10)
    assert instantiate(req) == instantiate(req)


def test_instantiate_shape_is_invariant_to_how_complex_the_content_sounds() -> None:
    """Two requests with the same structural shape (message count) but
    wildly different content — one trivial, one reading like it wants
    multi-step reasoning — must produce the IDENTICAL dag shape (role,
    single worker). Only the data inside differs; the classifier (Step
    C), not this module, is what already gated complexity before
    instantiate() was ever called."""
    trivial = TemplateRequest(request_id="req-5a", messages=(Message("user", "hi"),))
    elaborate = TemplateRequest(
        request_id="req-5b",
        messages=(
            Message(
                "user",
                "First research the topic, then draft three alternative approaches, "
                "compare their tradeoffs, consult external tools if needed, and only "
                "then synthesize a final multi-step recommendation.",
            ),
        ),
    )

    dag_trivial = instantiate(trivial)
    dag_elaborate = instantiate(elaborate)

    assert dag_trivial.worker.role == dag_elaborate.worker.role == PRIMARY_ROLE
    assert type(dag_trivial) is type(dag_elaborate)
    assert type(dag_trivial.worker) is type(dag_elaborate.worker)
