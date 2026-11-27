"""Aggregation: additivity, temporal windows, fan-out and drill-across.

* AD1–AD4 and the grain defaults of ``Additivity`` (§2.4);
* ``TemporalAgg`` with ``LATEST`` / ``EARLIEST`` (§4.2);
* ``over = ENTITIES | PATHS`` (§4.2 *What an aggregate counts*);
* drill-across of a constellation, DA1–DA4 (§5.2.2).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from tests.conformance.oracle import REJECTED, Undefined

VALID, VALID_WITH_WARNING = "VALID", "VALID_WITH_WARNING"


def _kind(case_input: Mapping[str, Any]) -> tuple[str, frozenset[str]]:
    """The declared kind, or the default from the grain (§2.4)."""
    kind = case_input.get("kind")
    if kind is None:
        grain = case_input["grain"]
        if grain in ("EVENT", "ACCUMULATING"):
            return "ADDITIVE", frozenset()
        if grain == "PERIOD":
            return "SEMI_ADDITIVE", frozenset({"TIME"})
        raise Undefined(f"unknown grain {grain!r}")
    if isinstance(kind, (tuple, list)):
        return kind[0], frozenset(kind[1])
    return kind, frozenset()


def _confined(axis: str, case_input: Mapping[str, Any]) -> bool:
    """AD2: is each tuple set summed confined to one value of *axis*?"""
    if axis in case_input.get("group_by_axes", ()):
        return True
    if axis in case_input.get("where_pins", ()):
        return True
    return axis == "TIME" and case_input.get("window") in ("LATEST", "EARLIEST")


def additivity(case_input: Mapping[str, Any]) -> str:
    """Decision of AD1–AD4 for one aggregate over one measure."""
    kind, over = _kind(case_input)
    fn = case_input["fn"]
    if fn in ("COUNT", "MIN", "MAX"):
        return VALID  # AD4
    if fn == "AVG":
        return VALID_WITH_WARNING if kind == "NON_ADDITIVE" else VALID  # AD3, AD4
    if fn != "SUM":
        raise Undefined(f"AD1–AD4 do not decide {fn}")
    if kind == "ADDITIVE":
        return VALID  # AD1
    if kind == "NON_ADDITIVE":
        return REJECTED  # AD3
    if kind == "SEMI_ADDITIVE":
        return VALID if all(_confined(a, case_input) for a in over) else REJECTED
    raise Undefined(f"unknown additivity kind {kind!r}")


def temporal_agg(case_input: Mapping[str, Any]) -> list[tuple[Any, float]]:
    """``TemporalAgg(SUM, arg, ts, LATEST|EARLIEST)`` per group (§4.2).

    Only the tuples of the last (first) period of each group contribute.
    """
    rows, key, ts, arg = (case_input[k] for k in ("rows", "group", "ts", "arg"))
    window = case_input["window"]
    pick = max if window == "LATEST" else min
    groups: dict[Any, list[Mapping[str, Any]]] = {}
    for r in rows:
        groups.setdefault(r[key], []).append(r)
    out = []
    for g, rs in sorted(groups.items()):
        period = pick(r[ts] for r in rs)
        out.append((g, sum(r[arg] for r in rs if r[ts] == period)))
    return out


def fan_out(case_input: Mapping[str, Any]) -> list[tuple[Any, float]]:
    """An aggregate over a PathJoin context under ``over`` (§4.2).

    ENTITIES: each distinct instance of the node alias the argument references
    contributes once per group; PATHS: every tuple contributes.
    """
    tuples, props = case_input["tuples"], case_input["props"]
    fn, alias, prop = case_input["fn"], case_input["alias"], case_input.get("prop")
    group, over = case_input["group"], case_input.get("over", "ENTITIES")
    groups: dict[Any, list[Mapping[str, str]]] = {}
    for t in tuples:
        groups.setdefault(t[group], []).append(t)
    out = []
    for g, ts in sorted(groups.items()):
        bound = [t[alias] for t in ts]
        if over == "ENTITIES":
            bound = sorted(set(bound))
        if fn == "COUNT":
            out.append((g, len(bound)))
        elif fn == "SUM":
            out.append((g, sum(props[n][prop] for n in bound)))
        else:
            raise Undefined(fn)
    return out


# ---------------------------------------------------------------------------
# Drill-across (§5.2.2)
# ---------------------------------------------------------------------------


def _branch_agg(fn: str, rows: list[Mapping[str, Any]], prop: str | None) -> Any:
    if fn == "COUNT":
        return len(rows)
    vals = [r[prop] for r in rows if r.get(prop) is not None]  # type: ignore[index]
    if fn == "SUM":
        return sum(vals) if vals else None
    raise Undefined(fn)


def _div(a: Any, b: Any) -> Any:
    if a is None or b is None or b == 0:
        return None  # §4.1: NULL operand or division by zero is NULL
    return a / b


def drill_across(case_input: Mapping[str, Any]) -> list[tuple]:
    """Steps 1–3 of §5.2.2 with DA4 for an absent branch."""
    branches: Mapping[str, Sequence[Mapping[str, Any]]] = case_input["branches"]
    key = case_input["key"]
    per_branch: dict[str, dict[Any, list]] = {}
    for name, rows in branches.items():  # step 1: per branch, group by the key
        grouped: dict[Any, list] = {}
        for r in rows:
            grouped.setdefault(r[key], []).append(r)
        per_branch[name] = grouped
    keys = sorted({k for g in per_branch.values() for k in g})  # step 2: FULL OUTER
    out = []
    for k in keys:
        values: dict[str, Any] = {}
        for name, fn, branch, prop in case_input["aggregates"]:
            if fn == "DIV":  # step 3: cross-branch expression on aligned rows
                values[name] = _div(values[branch], values[prop])
                continue
            rows = per_branch[branch].get(k)
            if rows is None:  # DA4
                values[name] = 0 if fn == "COUNT" else None
            else:
                values[name] = _branch_agg(fn, rows, prop)
        out.append((k,) + tuple(values[a[0]] for a in case_input["aggregates"]))
    return out


def constellation_check(case_input: Mapping[str, Any]) -> str:
    """DA1–DA3 well-formedness of a constellation query, at construction."""
    for branches in case_input.get("aggregate_branches", ()):
        if len(set(branches)) != 1:
            return REJECTED  # DA1
    conformed = set(case_input["conformed_for_all"])
    for dim in case_input.get("group_by_dims", ()):
        if dim not in conformed:
            return REJECTED  # DA2
    for branches in case_input.get("where_branches", ()):
        if len(set(branches)) > 1:
            return REJECTED  # DA3
    return VALID
