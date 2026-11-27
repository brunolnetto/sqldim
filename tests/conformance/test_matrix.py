"""The committed traceability matrix is the generated one, and expectations are live."""

from __future__ import annotations

from fnmatch import fnmatch

from tests.conformance import matrix
from tests.conformance.cases import all_cases
from tests.conformance.expectations import EXPECTATIONS
from tests.conformance.runners import BACKENDS


def test_committed_matrix_is_current() -> None:
    assert matrix.committed() == matrix.render(), (
        "regenerate: PYTHONPATH=. python -m tests.conformance.matrix --write"
    )


def test_every_expectation_matches_a_case_in_scope() -> None:
    names = {b.name: b for b in BACKENDS}
    cases = all_cases()
    for (pattern, backend_name), (_st, why) in EXPECTATIONS.items():
        assert backend_name in names, backend_name
        assert why, (pattern, backend_name)
        areas = names[backend_name].areas
        assert any(fnmatch(c.id, pattern) and c.area in areas for c in cases), (
            pattern,
            backend_name,
        )


def test_every_case_is_checked_by_the_oracle_and_listed() -> None:
    text = matrix.render()
    for c in all_cases():
        assert c.clause in text
