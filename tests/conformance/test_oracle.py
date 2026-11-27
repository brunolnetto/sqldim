"""The oracle reproduces every value the specification states.

A case with a stated ``expected`` checks the oracle against the spec's worked
example or table; a ``DERIVED`` case must at least be decided by the oracle.
These tests exercise the reference semantics alone — no ``sqldim`` code.
"""

from __future__ import annotations

import pytest

from tests.conformance.cases import DERIVED, Case, all_cases
from tests.conformance.reference import AREAS, oracle_result

CASES = all_cases()


@pytest.mark.parametrize("case", CASES, ids=str)
def test_oracle_decides_the_case(case: Case) -> None:
    result = oracle_result(case)
    if case.expected != DERIVED:
        assert result == case.expected, f"{case.clause}: {case.rule}"


def test_every_case_names_clause_rule_and_fixture() -> None:
    for c in CASES:
        assert c.area in AREAS, c.id
        assert c.clause and c.rule and c.fixture, c.id


def test_oracle_imports_nothing_from_sqldim() -> None:
    from pathlib import Path

    root = Path(__file__).parent / "oracle"
    for path in root.glob("*.py"):
        assert "sqldim" not in path.read_text(encoding="utf-8").replace(
            "``sqldim``", ""
        ), path.name


def test_negative_cases_are_present() -> None:
    """Rejections and false verdicts are covered, not only happy paths."""
    rejected = [c for c in CASES if c.expected == "REJECTED"]
    false = [c for c in CASES if c.expected is False]
    assert len(rejected) >= 20 and len(false) >= 20
