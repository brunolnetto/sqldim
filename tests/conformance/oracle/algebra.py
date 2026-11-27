"""The question algebra over sets of tuples (§8.13).

``(Q_algebra, UNION, INTERSECT, ∅, Q_top)`` is the Boolean algebra of subsets
of the result domain: every element denotes a set, so a leaf denotes the set
of rows of its table that satisfy its filter, and UNION, INTERSECT and EXCEPT
are the set operations.  ``JOIN`` correlates two answers on their first
column (``l.id = r.id``) and returns the pairs, concatenated.  Rule 11
(§6.2, §8.14) replaces a composition only by an equivalent one, so the
minimised composition denotes the same set: ``minimise`` does not change the
answer computed here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tests.conformance.oracle import Undefined

_COLS = {"id": 0, "x": 1}


def _leaf(tables: Mapping[str, list], spec: tuple) -> set[tuple]:
    table, pred = spec
    rows = set(map(tuple, tables[table]))
    if pred is None:
        return rows
    col, op, c = pred
    if op != "=":
        raise Undefined(f"leaf filter {op!r}")
    return {r for r in rows if r[_COLS[col]] == c}


def _compose(op: str, a: set[tuple], b: set[tuple]) -> set[tuple]:
    if op == "UNION":
        return a | b
    if op == "INTERSECT":
        return a & b
    if op == "EXCEPT":
        return a - b
    if op == "JOIN":
        return {x + y for x in a for y in b if x[0] == y[0]}
    raise Undefined(f"composition {op!r}")


def evaluate(case_input: Mapping[str, Any]) -> list[tuple]:
    """The sorted set of tuples the final composition denotes."""
    env = {n: _leaf(case_input["tables"], s) for n, s in case_input["leaves"].items()}
    for name, left, op, right in case_input["compose"]:
        env[name] = _compose(op, env[left], env[right])
    return sorted(env[case_input["final"]])
