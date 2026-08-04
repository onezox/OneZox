"""Unit tests for identity — Step I.

Pure logic, no I/O: validate() either raises or doesn't. Exercises both
forks, same discipline as classifier's own fork tests (Step C) — a
validator that silently accepts everything looks identical to a working
one until a request with no org_id actually arrives.
"""

import pytest
from identity import MissingIdentity, validate


def test_validate_accepts_a_real_org_id() -> None:
    validate("044122c4-6a3a-452a-ab1f-3f0dad5536e1")  # must not raise


def test_validate_rejects_empty_org_id() -> None:
    with pytest.raises(MissingIdentity):
        validate("")


def test_validate_does_not_require_user_id_or_project_id() -> None:
    # Not this module's job to check — user_id/project_id are legitimately
    # optional at the edge (normalize.rs's own unwrap_or_default()); this
    # test only documents that validate()'s signature has no opinion on
    # them at all (it only ever takes org_id).
    import inspect

    sig = inspect.signature(validate)
    assert list(sig.parameters) == ["org_id"]
