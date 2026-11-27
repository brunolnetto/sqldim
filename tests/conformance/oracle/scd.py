"""Versions, validity and SCD resolution at an instant (§2.5, §2.6, ADR 0003).

Instants are ISO strings (a date or a timestamp) read as ``datetime``.  Both
windows are half-open: ``effective_from ≤ t < effective_to`` and
``valid_from ≤ t < valid_to``; a NULL bound is open on that side.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from tests.conformance.oracle import REJECTED, Undefined


def instant(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def version(rows: Sequence[Mapping[str, Any]], key: str, t: str) -> Mapping | None:
    """``version(T, key, t)`` of §2.6: at most one row by M2."""
    at = instant(t)
    found = [
        r
        for r in rows
        if r["entity_key"] == key
        and instant(r["effective_from"]) <= at  # type: ignore[operator]
        and (r["effective_to"] is None or at < instant(r["effective_to"]))  # type: ignore[operator]
    ]
    if len(found) > 1:
        raise Undefined("M2 violated: overlapping versions")
    return found[0] if found else None


def valid(edge: Mapping[str, Any], t: str) -> bool:
    """``valid(e, t)`` of §2.6."""
    at = instant(t)
    lo, hi = instant(edge.get("valid_from")), instant(edge.get("valid_to"))
    return (lo is None or lo <= at) and (hi is None or at < hi)  # type: ignore[operator]


def _read(dims, fact, mode: str, tau: str, ref: str) -> Mapping | None:
    """The dimension version a fact → dimension hop reads (§2.5 table)."""
    if ref == "VERSION_KEY":
        stored = next(r for r in dims if r["version_key"] == fact["dim_ref"])
        if mode == "AS_WAS":
            return stored
        return version(dims, stored["entity_key"], tau)
    if ref == "ENTITY_KEY":
        at = fact["event_time"] if mode == "AS_WAS" else tau
        return version(dims, fact["dim_ref"], at)
    raise Undefined(f"unknown reference kind {ref!r}")


def measure_by(case_input: Mapping[str, Any]) -> Any:
    """``SUM(measure)`` grouped by a dimension attribute read through the hop.

    ``mode`` defaults to AS_WAS (§2.5 *Default*).  A missing version follows
    ``node_resolution``: STRICT rejects, LAX drops the tuple (§2.5, §2.6).
    """
    dims, facts = case_input["dim_rows"], case_input["facts"]
    mode = case_input.get("mode", "AS_WAS")
    tau, ref = case_input["tau"], case_input.get("reference", "ENTITY_KEY")
    attr, measure = case_input["attr"], case_input["measure"]
    out: dict[Any, float] = {}
    for fact in facts:
        row = _read(dims, fact, mode, tau, ref)
        if row is None:
            res = case_input.get("node_resolution")
            if res == "STRICT":
                return REJECTED
            if res != "LAX":
                raise Undefined("node_resolution has no default (§5.1)")
            continue
        out[row[attr]] = out.get(row[attr], 0) + fact[measure]
    return sorted(out.items())


def valid_neighbours(case_input: Mapping[str, Any]) -> list[str]:
    """Reverse hop over a bridge with a validity window, at one instant."""
    target, tau = case_input["target"], case_input["tau"]
    return sorted(
        e["source_key"]
        for e in case_input["edges"]
        if e["target_key"] == target and valid(e, tau)
    )
