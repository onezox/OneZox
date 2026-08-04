"""Unit tests for planner.classifier — Step C.

No network, no gRPC server, no cluster access: classify() is pure logic,
and these tests exercise it directly. The one place this file reaches for
generated protobuf code is test_kind_constants_match_proto_enum, and only
to assert the classifier's own local constants stay in sync with
dataplane.proto's real RequestKind numbering — a regression guard, not a
dependency the classifier module itself has.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generated"))

from planner.classifier import (  # noqa: E402
    KIND_CHAT_COMPLETION,
    KIND_RESPONSES,
    MAX_FAST_PATH_MESSAGES,
    ClassifierInput,
    NeedsDeliberatePath,
    classify,
)


def test_kind_constants_match_proto_enum() -> None:
    """Guards against the classifier's deliberately-decoupled local
    constants silently drifting from the real proto numbering they're
    documented to mirror."""
    from dataplane.v1 import dataplane_pb2

    assert KIND_CHAT_COMPLETION == dataplane_pb2.REQUEST_KIND_CHAT_COMPLETION
    assert KIND_RESPONSES == dataplane_pb2.REQUEST_KIND_RESPONSES


# --- Fork direction 1: simple requests route to the fast path ---------------


def test_simple_chat_completion_routes_to_fast_path() -> None:
    req = ClassifierInput(kind=KIND_CHAT_COMPLETION, message_count=1)
    classify(req)  # must not raise


def test_simple_responses_kind_routes_to_fast_path() -> None:
    req = ClassifierInput(kind=KIND_RESPONSES, message_count=3)
    classify(req)  # must not raise


def test_message_count_exactly_at_the_bound_still_routes_to_fast_path() -> None:
    # Boundary check: the bound is inclusive (> triggers complex, not >=) —
    # a request AT the limit is still simple.
    req = ClassifierInput(kind=KIND_CHAT_COMPLETION, message_count=MAX_FAST_PATH_MESSAGES)
    classify(req)  # must not raise


# --- Fork direction 2: complex requests get the typed deliberate-path error --
# The fork that actually matters to prove: a classifier that silently routes
# everything to "simple" looks identical to a working one until a complex
# request gets a degraded fast-path answer. Every test below confirms
# classify() genuinely raises, not just that it CAN in principle.


def test_unsupported_kind_raises_needs_deliberate_path() -> None:
    # kind=3 is REQUEST_KIND_EMBEDDINGS — no single-worker chat/completion
    # template applies (also currently unreachable from edge-gateway,
    # which stays a 501 placeholder there per Phase-01, but data-plane
    # must not assume an upstream will always filter this correctly).
    req = ClassifierInput(kind=3, message_count=1)
    with pytest.raises(NeedsDeliberatePath) as exc_info:
        classify(req)
    assert "no single-worker fast-path template" in str(exc_info.value)


def test_unspecified_kind_raises_needs_deliberate_path() -> None:
    req = ClassifierInput(kind=0, message_count=1)  # REQUEST_KIND_UNSPECIFIED
    with pytest.raises(NeedsDeliberatePath):
        classify(req)


def test_message_count_over_the_bound_raises_needs_deliberate_path() -> None:
    req = ClassifierInput(kind=KIND_CHAT_COMPLETION, message_count=MAX_FAST_PATH_MESSAGES + 1)
    with pytest.raises(NeedsDeliberatePath) as exc_info:
        classify(req)
    assert "exceeds the fast path's bound" in str(exc_info.value)


def test_needs_deliberate_path_error_marks_the_phase06_seam() -> None:
    # The error must be specific enough to mark exactly where Phase-06's
    # deliberate path plugs in — not a generic "unsupported" message a
    # caller could mistake for a bug or a permanent limitation.
    req = ClassifierInput(kind=3, message_count=1)
    with pytest.raises(NeedsDeliberatePath) as exc_info:
        classify(req)
    message = str(exc_info.value)
    assert "deliberate path" in message
    assert "Phase-06" in message
    assert "not yet available" in message
    assert exc_info.value.reason  # the specific, structured reason is preserved
