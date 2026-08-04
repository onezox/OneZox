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
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generated"))
