"""Eval scoring helpers — pass/fail assertions on NL interface ``EvalResult`` records.

All ``check_*`` functions return ``(passed: bool, detail: str)`` pairs.
The detail string explains why the check passed or failed, making reports
human-readable without re-running the case.
"""

from __future__ import annotations

from typing import Any


def check_has_result(result: dict[str, Any] | None) -> tuple[bool, str]:
    """Pass when result is non-None and has at least one row."""
    if result is None:
        return False, "result is None"
    count = result.get("count", 0)
    if count == 0:
        return False, f"result has 0 rows (columns: {result.get('columns', [])})"
    return True, f"{count} row(s) returned"


def _missing_columns(result: dict[str, Any], expected: list[str]) -> list[str]:
    """Return expected column names absent from *result* (case-insensitive)."""
    actual = {c.lower() for c in result.get("columns", [])}
    return [c for c in expected if c.lower() not in actual]


def check_columns_present(
    result: dict[str, Any] | None,
    expected: list[str],
) -> tuple[bool, str]:
    """Pass when all *expected* column names appear in the result (case-insensitive)."""
    if not expected:
        return True, "no column expectations"
    if result is None:
        return False, "result is None"
    missing = _missing_columns(result, expected)
    if missing:
        actual = {c.lower() for c in result.get("columns", [])}
        return False, f"missing columns: {missing} (actual: {sorted(actual)})"
    return True, f"all expected columns present: {expected}"


def check_table_referenced(
    sql: str | None,
    expected_table: str,
) -> tuple[bool, str]:
    """Pass when *expected_table* appears in the generated SQL (case-insensitive)."""
    if sql is None:
        return False, "no SQL generated"
    if expected_table.lower() in sql.lower():
        return True, f"table '{expected_table}' found in SQL"
    return False, f"table '{expected_table}' not found in SQL: {sql!r}"


def check_hop_budget(
    visited_nodes: list[str],
    max_hops: int,
) -> tuple[bool, str]:
    """Pass when the number of graph hops does not exceed *max_hops*."""
    n = len(visited_nodes)
    if n <= max_hops:
        return True, f"{n} hops (≤ {max_hops})"
    return False, f"{n} hops exceeds budget of {max_hops}"


def check_result_matches_expected(
    actual: dict[str, Any] | None,
    expected: dict[str, Any] | None,
) -> tuple[bool, str]:
    """Pass when *actual* result matches *expected* ground-truth result.

    Compares row count and sorted row data values (up to 100 rows).  Column
    *aliases* are intentionally ignored — the model may use a different alias
    for the metric column (e.g. ``count_accounts`` vs ``account_count``) while
    the data is semantically identical.  A note is appended to the detail
    string when aliases differ.  When *expected* is ``None`` the check is
    skipped.
    """
    if expected is None:
        return True, "no expected result (skipped)"

    exp_count = expected.get("count", 0)
    act_count = actual.get("count", 0) if actual is not None else 0

    if act_count != exp_count:
        return False, f"row count mismatch: expected {exp_count}, got {act_count}"

    if exp_count == 0:
        return True, "both queries return empty results"

    if actual is None:
        return False, "actual result is None; expected has data"

    # Informational alias note — not a failure condition
    exp_cols = sorted(c.lower() for c in expected.get("columns", []))
    act_cols = sorted(c.lower() for c in actual.get("columns", []))
    alias_note = f" (alias: {act_cols})" if act_cols != exp_cols else ""

    if exp_count <= 100:
        exp_rows = sorted(str(r) for r in expected.get("rows", []))
        act_rows = sorted(str(r) for r in actual.get("rows", []))
        if exp_rows != act_rows:
            return False, f"data mismatch: {act_count} row(s) differ{alias_note}"

    return True, f"matches expected: {act_count} row(s){alias_note}"


def _earned_sum(checks: tuple, w: list[float]) -> float:  # type: ignore[type-arg]
    """Return the sum of weights for passing checks."""
    return sum(wi for (passed, _), wi in zip(checks, w) if passed)


def score_case(
    *checks: tuple[bool, str],
    weights: list[float] | None = None,
) -> float:
    """Return a weighted [0.0, 1.0] score from a sequence of (passed, detail) pairs.

    Equal weights are used when *weights* is ``None``.
    """
    if not checks:
        return 1.0
    w = weights if weights is not None else [1.0] * len(checks)
    total = sum(w)
    if total == 0.0:
        return 1.0
    return round(_earned_sum(checks, w) / total, 4)


__all__ = [
    "check_has_result",
    "check_columns_present",
    "check_table_referenced",
    "check_hop_budget",
    "check_result_matches_expected",
    "score_case",
]
