"""Construction-time rules of the query language (§3.2, §3.5, §4.1, §5.2).

Each function returns :data:`REJECTED` or ``"ACCEPTED"`` for a construct
described as data.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tests.conformance.oracle import REJECTED

ACCEPTED = "ACCEPTED"

# §4.1 band table: which Ref kinds and predicate forms each filter admits.
_BANDS = {
    "WHERE": {"PropRef", "PathPred", "SignaturePred"},
    "HAVING": {"AggRef"},
    "QUALIFY": {"WinRef"},
}


def band(case_input: Mapping[str, Any]) -> str:
    """§4.1 / §5.2: a filter admits only the Refs of its own band."""
    allowed = _BANDS[case_input["band"]]
    return ACCEPTED if set(case_input["uses"]) <= allowed else REJECTED


def repeat(case_input: Mapping[str, Any]) -> str:
    """R2–R4 of §3.2 (R1 needs the schema and is checked where the hop is typed)."""
    lo, hi, mode = (
        case_input["min"],
        case_input["max"],
        case_input.get("mode", "SIMPLE"),
    )
    if not (isinstance(lo, int) and lo >= 0):
        return REJECTED  # R2
    if hi is not None and not (hi >= 1 and lo <= hi):
        return REJECTED  # R2
    if case_input.get("nested"):
        return REJECTED  # R3
    if mode == "WALK" and hi is None:
        return REJECTED  # R4
    return ACCEPTED


def null_literal(case_input: Mapping[str, Any]) -> str:
    """§4.1: NULL is not a literal; ``Compare(e, op, NULL)`` is not expressible.

    ``IsNull(e)`` / ``IsNotNull(e)`` are the forms that test for NULL.
    """
    if case_input["form"] in ("isnull", "isnotnull"):
        return ACCEPTED
    return REJECTED if case_input["value"] is None else ACCEPTED


def alias_scope(case_input: Mapping[str, Any]) -> str:
    """§3.2 / §3.5: an alias in C ∩ L is rejected at construction."""
    clash = set(case_input["outer"]) & set(case_input["path"])
    return REJECTED if clash else ACCEPTED
