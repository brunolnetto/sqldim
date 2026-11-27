"""``QuestionAlgebra`` on DuckDB, minimised with Rule 11 when the case asks.

The tables are loaded as ``(id INTEGER, x VARCHAR)``; each leaf is a
``DGMQuery`` anchored on its table.  The result is every row the emitted SQL
returns, duplicates kept, so a bag-valued answer differs from the oracle's set.
"""

from __future__ import annotations

from typing import Any

import duckdb

from sqldim.core.query.dgm import DGMQuery, PropRef, ScalarPred
from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
from tests.conformance.cases import Case
from tests.conformance.runners import Backend


def _algebra(i: dict) -> QuestionAlgebra:
    alg = QuestionAlgebra()
    for name, (table, pred) in i["leaves"].items():
        q = DGMQuery().anchor(table, "t")
        if pred is not None:
            q = q.where(ScalarPred(PropRef("t", pred[0]), pred[1], pred[2]))
        alg.add(name, q)
    for name, left, op, right in i["compose"]:
        on = "l.id = r.id" if op == "JOIN" else None
        alg.compose(left, ComposeOp[op], right, name=name, on=on)
    return alg


def _run(case: Case) -> Any:
    i = dict(case.input)
    alg = _algebra(i)
    if i["minimise"]:
        alg = alg.minimize(DGMPredicateBDD(BDDManager()))
    con = duckdb.connect()
    try:
        for table, rows in i["tables"].items():
            con.execute(f"CREATE TABLE {table} (id INTEGER, x VARCHAR)")
            con.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows)
        return sorted(con.execute(alg.to_sql(i["final"])).fetchall())
    finally:
        con.close()


BACKEND = Backend(
    "duckdb-question-algebra",
    "QuestionAlgebra.to_sql() on DuckDB, after QuestionAlgebra.minimize() "
    "(Rule 11) when the case asks for it",
    frozenset({"algebra"}),
    _run,
)
