"""``TemporalOrdering.to_sql()`` evaluated on DuckDB (§3.2, §8.8).

Every pair of the fixture grid is a row of one table; the relation's SQL is a
column of one query, and a pair is related iff that column ``IS TRUE`` (a
filter keeps a tuple only when its predicate is TRUE, §4.1).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import duckdb

from sqldim.core.query.dgm import temporal
from tests.conformance.cases import Case
from tests.conformance.oracle.allen import domain_of
from tests.conformance.reference import interval_pairs, point_pairs
from tests.conformance.runners import Backend, NotExpressible
from tests.conformance.runners.duckdb_sql import insert_rows


def _cls(name: str) -> Any:
    cls = getattr(temporal, name, None)
    if cls is None:
        raise NotExpressible(f"sqldim has no TemporalOrdering {name}")
    return cls


def _truth(sql: str, rows: list[tuple], cols: tuple[str, ...]) -> list[bool]:
    con = duckdb.connect()
    con.execute(
        f"CREATE TABLE g (k INTEGER, {', '.join(c + ' INTEGER' for c in cols)})"
    )
    insert_rows(con, "g", [[n, *r] for n, r in enumerate(rows)])
    out = con.execute(f"SELECT k, ({sql}) IS TRUE FROM g ORDER BY k").fetchall()
    return [bool(v) for _k, v in out]


def _allen(case: Case) -> Any:
    rel = _cls(case.input["relation"])(("ls", "le"), ("rs", "re"))
    pairs = [p for p in interval_pairs() if domain_of(*p) == case.input["domain"]]
    held = _truth(
        rel.to_sql(), [(*lv, *rv) for lv, rv in pairs], ("ls", "le", "rs", "re")
    )
    return [p for p, h in zip(pairs, held) if h]


def _point(case: Case) -> Any:
    cls, tol = _cls(case.input["relation"]), case.input.get("tolerance")
    rel = cls("a", "b") if tol is None else cls("a", "b", tolerance=tol)
    pairs = point_pairs()
    held = _truth(rel.to_sql(), pairs, ("a", "b"))
    return [p for p, h in zip(pairs, held) if h]


def _typing(case: Case) -> Any:
    i = case.input
    if "types" in i:
        raise NotExpressible("TemporalOrdering arguments are untyped SQL text")
    args = [("a.window_start", "a.window_end") if a == "interval" else "s.occurred_on"
            for a in i["args"]]  # fmt: skip
    kwargs = {"tolerance": i["tolerance"]} if "tolerance" in i else {}
    try:
        _cls(i["relation"])(*args, **kwargs)
    except (TypeError, ValueError):
        return "REJECTED"
    return "ACCEPTED"


def _theorem(case: Case) -> Any:
    from tests.conformance.oracle.allen import ALLEN

    rels = [_cls(name)(("ls", "le"), ("rs", "re")) for name in ALLEN]
    pairs = [p for p in interval_pairs() if domain_of(*p) in case.input["domains"]]
    rows = [(*lv, *rv) for lv, rv in pairs]
    cols = ("ls", "le", "rs", "re")
    held = [_truth(r.to_sql(), rows, cols) for r in rels]
    want = 0 if case.input.get("none") else 1
    return all(sum(h[k] for h in held) == want for k in range(len(pairs)))


_RUN: dict[str, Callable[[Case], Any]] = {
    "allen": _allen,
    "point": _point,
    "ordering_typing": _typing,
    "allen_theorem": _theorem,
}

BACKEND = Backend(
    "duckdb-ordering",
    "TemporalOrdering.to_sql() evaluated on DuckDB",
    frozenset(_RUN),
    lambda case: _RUN[case.area](case),
)
