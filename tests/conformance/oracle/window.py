"""Band B3: window functions and QUALIFY (§4.1 band table, §5.2).

A window value is computed per partition over the rows of the context; the
QUALIFY predicate keeps a row only when it is TRUE (Kleene logic, §4.1 NULL
rules: UNKNOWN does not pass a filter).  Only the window functions the cases
use are defined: ``ROW_NUMBER`` over an ordering without ties or NULLs in its
keys, and ``SUM`` over a whole partition.  The specification does not fix the
place of NULL in an ORDER BY, so no case orders by a nullable key.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from tests.conformance.oracle import Undefined
from tests.conformance.oracle.kleene import and_, compare, is_true


def _partitions(rows: Sequence[Mapping[str, Any]], key: str) -> dict[Any, list]:
    out: dict[Any, list] = {}
    for r in rows:
        if r[key] is None:
            raise Undefined("a NULL partition key is not decided by the oracle")
        out.setdefault(r[key], []).append(r)
    return out


def _row_number(rows, partition: str, order: Sequence[tuple[str, str]]) -> dict:
    rn: dict[Any, int] = {}
    for part in _partitions(rows, partition).values():
        for col, _dir in order:
            if any(r[col] is None for r in part):
                raise Undefined("NULL in an ORDER BY key is not decided")
        ranked = list(part)
        for col, direction in reversed(order):  # stable sort, last key first
            ranked.sort(key=lambda r: r[col], reverse=direction == "DESC")
        for i, r in enumerate(ranked, start=1):
            rn[r["id"]] = i
    return rn


def _sum(rows, partition: str, arg: str) -> dict:
    out: dict[Any, Any] = {}
    for part in _partitions(rows, partition).values():
        present = [r[arg] for r in part if r[arg] is not None]
        total = sum(present) if present else None  # SQL SUM ignores NULLs
        for r in part:
            out[r["id"]] = total
    return out


def window_value(i: Mapping[str, Any]) -> dict:
    fn = i["window"]
    if fn[0] == "ROW_NUMBER":
        return _row_number(i["rows"], fn[1], fn[2])
    if fn[0] == "SUM":
        return _sum(i["rows"], fn[1], fn[2])
    raise Undefined(f"window function {fn[0]!r}")


def qualify(i: Mapping[str, Any]) -> list:
    """Ids of the rows whose QUALIFY predicate is TRUE, sorted."""
    w = window_value(i)
    kept = []
    for r in i["rows"]:
        env = {**r, "w": w[r["id"]]}
        verdict = and_(compare(env[col], op, lit) for col, op, lit in i["qualify"])
        if is_true(verdict):
            kept.append(r["id"])
    return sorted(kept)
