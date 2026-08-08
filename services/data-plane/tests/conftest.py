"""Shared pytest setup for services/data-plane's test suite.

Puts generated/ on sys.path ONCE, here, so `from provider.v1 import
provider_pb2` (etc.) resolves for any test file run in isolation —
before this existed, that path was only inserted by test_classifier.py's
own top-level sys.path.insert, which happened to make test_place.py's
own proto-parity test pass too, but ONLY when the whole suite ran
together and test_classifier.py got collected first. Running
test_place.py alone (`pytest tests/test_place.py`) failed with
ModuleNotFoundError — a real test-isolation gap, not a hypothetical one,
caught while adding Step G's own aggregator test.

Also puts the data-plane package root itself on sys.path, for the same
reason one level up. Every test module imports its subject as a
TOP-LEVEL module (`import usage_event`, `from working_memory import
...`), which only resolves if services/data-plane is on sys.path. It was
never put there explicitly; it worked locally purely because
`python -m pytest` inserts the current working directory as sys.path[0].
The `pytest` CONSOLE SCRIPT does not do that, so CI -- which runs
`pytest . -q` -- failed to collect all 10 test modules with
ModuleNotFoundError, while the identical suite passed locally. Because
build-sign `needs:` that job, it was SKIPPED on every run since the CI
test gate was added, so no service was actually built or signed.

That is why this insert lives here rather than in CI's invocation:
making the suite depend on how the interpreter was launched is the
actual defect. Fixed here, `pytest`, `python -m pytest`, and a run from
any working directory all behave identically.
"""

import os
import sys

_HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(_HERE, "..", "generated"))
sys.path.insert(0, os.path.join(_HERE, ".."))
