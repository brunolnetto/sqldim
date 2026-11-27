"""``DGMQuery`` executed on DuckDB, through two code paths.

* ``duckdb-dgmquery`` — ``DGMQuery.to_sql()`` (the ``execute()`` path);
* ``duckdb-plan-ir`` — ``DGMPlanner.plan_query()``: lowering to the plan IR
  of #148 and SQL emitted from it.

The fixture is loaded into an in-memory DuckDB database following the
relational mapping of §2.6: one NodeTable per node type, and a verb or bridge
edge type as a ``ForeignKey(holder = TARGET)`` column ``<label>_src``.  The
query is built with the public ``sqldim`` API only.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, ContextManager

import duckdb

from sqldim.core.query.dgm import (
    AND,
    ALL,
    EVENTUALLY,
    GLOBALLY,
    K_SHORTEST,
    MIN_WEIGHT,
    NEXT,
    NOT,
    ONCE,
    PREVIOUSLY,
    SHORTEST,
    UNBOUNDED,
    AggRef,
    ArithExpr,
    BridgeHop,
    DGMPlanner,
    DGMQuery,
    PathPred,
    PropRef,
    QueryTarget,
    Quantifier,
    RawPred,
    Repeat,
    RepeatMode,
    ScalarPred,
    SinceMode,
    UntilMode,
    VerbHop,
    WinRef,
)
from sqldim.exceptions import SemanticError
from tests.conformance.cases import Case
from tests.conformance.cases.fixtures import GRAPHS, INPUTS
from tests.conformance.runners import REJECTED, Backend, NotExpressible

Executor = Callable[[DGMQuery], str]
Session = Callable[[], ContextManager[duckdb.DuckDBPyConnection]]


@contextmanager
def memory_session() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()


@dataclass(frozen=True)
class Engine:
    """How SQL is produced (*sql*) and where it runs (*session*)."""

    sql: Executor
    session: Session = memory_session


def _legacy_sql(q: DGMQuery) -> str:
    return q.to_sql()


def _ir_sql(q: DGMQuery) -> str:
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]
    return planner.plan_query(q).query_text


# ---------------------------------------------------------------------------
# Loading a fixture
# ---------------------------------------------------------------------------


def _sql_type(values: Sequence[object]) -> str:
    present = [v for v in values if v is not None]
    if not present:
        return "DOUBLE"
    if all(isinstance(v, bool) for v in present):
        return "BOOLEAN"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in present):
        return "BIGINT"
    if all(isinstance(v, (int, float)) for v in present):
        return "DOUBLE"
    return "VARCHAR"


def create(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: Sequence[Mapping],
    types: Mapping[str, str] | None = None,
) -> None:
    """A table for *rows*; *types* overrides the inferred type of a column."""
    cols: list[str] = []
    for r in rows:
        cols += [k for k in r if k not in cols]
    types = {c: (types or {}).get(c) or _sql_type([r.get(c) for r in rows])
             for c in cols}  # fmt: skip
    con.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {types[c]}' for c in cols)})")
    insert_rows(con, table, [[r.get(c) for c in cols] for r in rows])


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: Sequence) -> None:
    """One parameterised INSERT (``executemany`` is far slower here)."""
    if not rows:
        return
    one = "(" + ", ".join("?" for _ in rows[0]) + ")"
    params = [v for r in rows for v in r]
    con.execute(f"INSERT INTO {table} VALUES {', '.join(one for _ in rows)}", params)


def _type_of(props: Mapping[str, Any]) -> str:
    return str(props.get("type", "Node"))


def table_of(node_type: str) -> str:
    return f"t_{node_type.lower()}"


def load_graph(con: duckdb.DuckDBPyConnection, graph: Mapping[str, Any]) -> None:
    """NodeTables with a ForeignKey column per incoming edge label (§2.6 M3)."""
    rows: dict[str, list[dict[str, Any]]] = {}
    by_id: dict[str, dict[str, Any]] = {}
    for nid, props in graph["nodes"].items():
        row = {"id": nid, **{k: v for k, v in props.items() if k != "type"}}
        rows.setdefault(_type_of(props), []).append(row)
        by_id[nid] = row
    for src, dst, label in graph["edges"]:
        col = f"{label}_src"
        if by_id[dst].get(col) is not None:
            raise AssertionError(f"{dst} has two incoming {label!r} edges")
        by_id[dst][col] = src
    for type_, rs in rows.items():
        create(con, table_of(type_), rs)


def _run(eng: Engine, q: DGMQuery, setup: Callable[[Any], None]) -> Any:
    """SQL of *q*, executed; a construction or lowering error is REJECTED."""
    try:
        sql = eng.sql(q)
    except SemanticError:
        return REJECTED
    with eng.session() as con:
        setup(con)
        return con.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# PathPred (ctl, pathpred)
# ---------------------------------------------------------------------------

_MODES: dict[str, object] = {
    "EVENTUALLY": EVENTUALLY,
    "GLOBALLY": GLOBALLY,
    "NEXT": NEXT,
    "ONCE": ONCE,
    "PREVIOUSLY": PREVIOUSLY,
}


def _pred(p: Sequence, alias: str, outer: str) -> Any:
    """Predicate data → a sqldim predicate over the path alias."""
    tag = p[0]
    if tag == "prop":
        return ScalarPred(PropRef(f"{alias}.{p[1]}"), p[2], p[3])
    if tag == "outer":
        # fs.k op c.k2 as (fs.k - c.k2) op 0: same truth value, NULLs included;
        # ScalarPred takes a literal on its right-hand side.
        diff = ArithExpr(PropRef(f"{alias}.{p[1]}"), "-", PropRef(f"{outer}.{p[3]}"))
        return ScalarPred(diff, p[2], 0)
    if tag == "not":
        return NOT(_pred(p[1], alias, outer))
    if tag == "and":
        return AND(*(_pred(q, alias, outer) for q in p[1:]))
    if tag == "true":
        return RawPred("TRUE")
    raise NotExpressible(f"predicate form {tag!r}")


def _mode(mode: Any, alias: str, outer: str) -> object:
    if mode is None:
        return None
    if isinstance(mode, (tuple, list)):
        cls = {"UNTIL": UntilMode, "SINCE": SinceMode}[mode[0]]
        return cls(_pred(mode[1], alias, outer))
    return _MODES[mode]


def _strategy(spec: Sequence) -> object:
    kind = spec[0]
    if kind == "K_SHORTEST":
        return K_SHORTEST(spec[1])
    return {"ALL": ALL, "SHORTEST": SHORTEST, "MIN_WEIGHT": MIN_WEIGHT}[kind]()


def _target_type(graph: Mapping[str, Any], label: str) -> str:
    dst = next(d for _s, d, lab in graph["edges"] if lab == label)
    return _type_of(graph["nodes"][dst])


def _path(graph: Mapping[str, Any], pattern: Sequence, outer: str, alias: str) -> Any:
    if len(pattern) != 1:
        raise NotExpressible("only one-element patterns are mapped")
    elem = pattern[0]
    if elem[2] != "fwd":
        raise NotExpressible("reverse hops are not mapped")
    table = table_of(_target_type(graph, elem[1]))
    on = f"{alias}.{elem[1]}_src = {outer}.id"
    if elem[0] == "hop":
        return VerbHop(outer, elem[1], alias, table=table, on=on)
    _, label, _d, lo, hi, mode = elem
    hop = BridgeHop(outer, label, alias, table=table, on=on)
    return Repeat(hop, lo, UNBOUNDED if hi is None else hi, mode=RepeatMode[mode])


def _path_pred_query(case: Case) -> tuple[DGMQuery, str]:
    i, graph = case.input, GRAPHS[case.fixture]
    if i.get("on_empty", "REQUIRE_WITNESS") != "REQUIRE_WITNESS":
        raise NotExpressible("PathPred has no on_empty field")
    outer = "c"
    alias = (i.get("aliases") or {}).get("path", ["fs"])[0]
    try:
        pp = PathPred(
            outer,
            _path(graph, i["pattern"], outer, alias),
            _pred(i["phi"], alias, outer),
            quantifier=Quantifier[i["quantifier"]],
            strategy=_strategy(i.get("strategy", ("ALL",))),
            temporal_mode=_mode(i.get("mode"), alias, outer),
            promote=i.get("promote") or None,
        )
        where = NOT(pp) if i.get("negate") else pp
        start_type = _type_of(graph["nodes"][i["start"]])
        q = DGMQuery().anchor(table_of(start_type), outer).where(where)
    except SemanticError:
        q = None  # type: ignore[assignment]
    return q, i["start"]


def _path_pred(eng: Engine) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        q, start = _path_pred_query(case)
        if q is None:
            return REJECTED
        rows = _run(eng, q, lambda con: load_graph(con, GRAPHS[case.fixture]))
        if rows == REJECTED:
            return REJECTED
        return start in {r[0] for r in rows}

    return run


# ---------------------------------------------------------------------------
# Repeat as a PathJoin (repeat_join)
# ---------------------------------------------------------------------------


def _repeat_join(eng: Engine) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        graph, i = GRAPHS[case.fixture], case.input
        try:
            rep = _path(graph, i["pattern"], "x", "y")
            q = (
                DGMQuery()
                .anchor(table_of("X"), "x")
                .path_join(rep)
                .where(ScalarPred(PropRef("x.id"), "=", i["start"]))
                .group_by("y.id")
                .agg(n="COUNT(*)")
            )
        except SemanticError:
            return REJECTED
        rows = _run(eng, q, lambda con: load_graph(con, graph))
        if rows == REJECTED:
            return REJECTED
        return sorted(y for y, n in rows if y is not None for _ in range(n))

    return run


# ---------------------------------------------------------------------------
# Construction rules (construction)
# ---------------------------------------------------------------------------

_HOP = BridgeHop("x", "next", "y", table="t_x", on="y.next_src = x.id")


def _repeat_ctor(i: Mapping[str, Any]) -> Any:
    hi = UNBOUNDED if i["max"] is None else i["max"]
    mode = RepeatMode[i.get("mode", "SIMPLE")]
    body: Any = Repeat(_HOP, 1, 2) if i.get("nested") else _HOP
    return Repeat(body, i["min"], hi, mode=mode)


def _band_pred(use: str) -> Any:
    if use == "PathPred":
        hop = VerbHop("c", "placed", "fs", table="t_sale", on="fs.placed_src = c.id")
        return PathPred("c", hop, ScalarPred(PropRef("fs.revenue"), ">", 1))
    ref = {"AggRef": AggRef("total"), "WinRef": WinRef("rn"),
           "PropRef": PropRef("c.id")}[use]  # fmt: skip
    return ScalarPred(ref, ">", 1)


def _band_query(i: Mapping[str, Any]) -> DGMQuery:
    preds = [_band_pred(u) for u in i["uses"]]
    pred = preds[0] if len(preds) == 1 else AND(*preds)
    q = DGMQuery().anchor("t_customer", "c")
    if i["band"] == "WHERE":
        return q.where(pred)
    if i["band"] == "HAVING":
        return q.group_by("c.id").agg(total="COUNT(*)").having(pred)
    return q.window(rn="ROW_NUMBER() OVER (ORDER BY c.id)").qualify(pred)


def _null_query(i: Mapping[str, Any]) -> DGMQuery:
    op = "IS" if i["form"] == "isnull" else "="
    return (
        DGMQuery()
        .anchor("t_customer", "c")
        .where(ScalarPred(PropRef("c.id"), op, i["value"]))
    )


def _construction(eng: Engine) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        i = case.input
        try:
            if i["kind"] == "repeat":
                _repeat_ctor(i)
                return "ACCEPTED"
            q = _band_query(i) if i["kind"] == "band" else _null_query(i)
            eng.sql(q)
        except SemanticError:
            return REJECTED
        return "ACCEPTED"

    return run


# ---------------------------------------------------------------------------
# Aggregation over a PathJoin context (fanout, drill_across)
# ---------------------------------------------------------------------------


def _fanout(eng: Engine) -> Callable[[Case], Any]:
    def setup(con: Any, fixture: Mapping[str, Any]) -> None:
        create(con, "sale", [{"id": s, **p} for s, p in fixture["props"].items()])
        create(con, "link", [{"sale_id": t["s"], "campaign": t["p"],
                              "store_id": t["st"]} for t in fixture["tuples"]])  # fmt: skip
        create(
            con, "store", [{"id": st} for st in {t["st"] for t in fixture["tuples"]}]
        )

    def run(case: Case) -> Any:
        fixture, i = INPUTS[case.fixture], case.input
        agg = f"SUM(s.{i['prop']})" if i["fn"] == "SUM" else "COUNT(s.id)"
        # PATHS is asked for explicitly; ENTITIES is left to the default (§4.2)
        over = {"over": "PATHS"} if i["over"] == "PATHS" else {}
        q = (
            DGMQuery()
            .anchor("sale", "s")
            .path_join(VerbHop("s", "via", "p", table="link", on="p.sale_id = s.id"))
            .path_join(
                BridgeHop("p", "at", "st", table="store", on="st.id = p.store_id")
            )
            .group_by("st.id")
            .agg(v=agg, **over)
        )
        rows = _run(eng, q, lambda con: setup(con, fixture))
        return rows if rows == REJECTED else sorted((k, int(v)) for k, v in rows)

    return run


def _drill(eng: Engine) -> Callable[[Case], Any]:
    def setup(con: Any, fixture: Mapping[str, Any]) -> None:
        keys = sorted(
            {r["customer"] for rs in fixture["branches"].values() for r in rs}
        )
        create(con, "customer", [{"id": k} for k in keys])
        for name, rs in fixture["branches"].items():
            create(con, name, [{**r, "customer_id": r["customer"]} for r in rs])

    def run(case: Case) -> Any:
        fixture = INPUTS[case.fixture]
        q = DGMQuery().anchor("customer", "c")
        aliases = {}
        for n, (name, _rows) in enumerate(fixture["branches"].items()):
            alias = f"f{n}"
            aliases[name] = alias
            q.path_join(VerbHop("c", name, alias, table=name,
                                on=f"{alias}.customer_id = c.id"))  # fmt: skip
        exprs: dict[str, str] = {}
        for name, fn, branch, prop in case.input["aggregates"]:
            if fn == "DIV":
                exprs[name] = f"{exprs[branch]} / {exprs[prop]}"
            elif fn == "COUNT":
                exprs[name] = f"COUNT({aliases[branch]}.id)"
            else:
                exprs[name] = f"{fn}({aliases[branch]}.{prop})"
        q.group_by("c.id").agg(**exprs)
        rows = _run(eng, q, lambda con: setup(con, fixture))
        return rows if rows == REJECTED else sorted(tuple(r) for r in rows)

    return run


# ---------------------------------------------------------------------------
# SCD resolution and validity (scd_measure, edge_validity)
# ---------------------------------------------------------------------------

TS = "TIMESTAMP"


def _scd(eng: Engine) -> Callable[[Case], Any]:
    """τ is stated with ``as_of``; the SCD mode is passed only when the case
    states one, so a case without it reads the implementation's default.
    ``event_time(Sale) = occurred_on`` is the mapping of the fixture (§2.5)."""

    def setup(con: Any, i: Mapping[str, Any]) -> None:
        dims = [
            {"id": r["entity_key"], "version_key": r["version_key"],
             i["attr"]: r[i["attr"]], "valid_from": r["effective_from"],
             "valid_to": r["effective_to"], "is_current": r["effective_to"] is None}
            for r in i["dim_rows"]
        ]  # fmt: skip
        create(con, "dim_customer", dims, {"valid_from": TS, "valid_to": TS})
        create(con, "fact_sale", [{"id": f["id"], "customer_key": f["dim_ref"],
                                   "occurred_on": f["event_time"],
                                   "revenue": f["revenue"]} for f in i["facts"]],
               {"occurred_on": TS})  # fmt: skip

    def run(case: Case) -> Any:
        i = case.input
        ref: dict[str, Any] = {"event_time": "occurred_on"}
        if i.get("reference") == "VERSION_KEY":
            ref = {"version_key": "version_key"}
        if "mode" in i:
            ref["mode"] = i["mode"]
        q = DGMQuery().anchor("fact_sale", "f")
        if i.get("node_resolution"):
            q.temporal_context(i["tau"], node_resolution=i["node_resolution"])
        q.join_dim("dim_customer", "customer_key", "id", **ref).as_of(i["tau"])
        q.group_by(f"d_dim_customer.{i['attr']}").agg(total="SUM(f.revenue)")
        rows = _run(eng, q, lambda con: setup(con, i))
        return (
            rows
            if rows == REJECTED
            else sorted(((k, int(v)) for k, v in rows), key=lambda r: (r[0] is None, r))
        )

    return run


def _validity(eng: Engine) -> Callable[[Case], Any]:
    def setup(con: Any, i: Mapping[str, Any]) -> None:
        people = {e["source_key"] for e in i["edges"]} | {
            e["target_key"] for e in i["edges"]
        }
        create(con, "employee", [{"id": p} for p in sorted(people)])
        create(con, "bridge_reports_to", list(i["edges"]),
               {"valid_from": TS, "valid_to": TS})  # fmt: skip

    def run(case: Case) -> Any:
        i = case.input
        hop = BridgeHop("m", "reports_to", "b", table="bridge_reports_to",
                        on="b.target_key = m.id")  # fmt: skip
        q = DGMQuery().anchor("employee", "m").path_join(hop)
        q.where(ScalarPred(PropRef("m.id"), "=", i["target"]))
        if i.get("tau_source") == "context":
            q.temporal_context(i["tau"], node_resolution="LAX", edge_resolution="LAX")
        else:
            q.as_of(i["tau"])
        q.group_by("b.source_key").agg(n="COUNT(*)")
        rows = _run(eng, q, lambda con: setup(con, i))
        return (
            rows if rows == REJECTED else sorted(k for k, _n in rows if k is not None)
        )

    return run


# ---------------------------------------------------------------------------
# Band B3: window and QUALIFY (window)
# ---------------------------------------------------------------------------

WINDOW_TYPES = {"id": "VARCHAR", "customer": "VARCHAR", "revenue": "BIGINT",
                "discount": "DOUBLE"}  # fmt: skip


def _window_sql(fn: Sequence) -> str:
    if fn[0] == "ROW_NUMBER":
        order = ", ".join(f"s.{col} {d}" for col, d in fn[2])
        return f"ROW_NUMBER() OVER (PARTITION BY s.{fn[1]} ORDER BY {order})"
    return f"SUM(s.{fn[2]}) OVER (PARTITION BY s.{fn[1]})"


def _window(eng: Engine) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        i = case.input
        preds = [
            ScalarPred(WinRef("w") if col == "w" else PropRef(f"s.{col}"), op, lit)
            for col, op, lit in i["qualify"]
        ]
        q = (
            DGMQuery()
            .anchor("sale", "s")
            .window(w=_window_sql(i["window"]))
            .qualify(preds[0] if len(preds) == 1 else AND(*preds))
        )
        rows = _run(eng, q, lambda con: create(con, "sale", i["rows"], WINDOW_TYPES))
        return rows if rows == REJECTED else sorted(r[0] for r in rows)

    return run


#: Areas whose cases execute a query; ``construction`` only builds and emits.
EXECUTED_AREAS = frozenset(
    {"ctl", "pathpred", "repeat_join", "fanout", "drill_across", "scd_measure",
     "edge_validity", "window"}
)  # fmt: skip


def backend(
    name: str,
    description: str,
    eng: Engine,
    *,
    areas: frozenset[str] | None = None,
    requires: Callable[[], str | None] | None = None,
) -> Backend:
    runners: dict[str, Callable[[Case], Any]] = {
        "ctl": _path_pred(eng),
        "pathpred": _path_pred(eng),
        "repeat_join": _repeat_join(eng),
        "construction": _construction(eng),
        "fanout": _fanout(eng),
        "drill_across": _drill(eng),
        "scd_measure": _scd(eng),
        "edge_validity": _validity(eng),
        "window": _window(eng),
    }
    scope = frozenset(runners) if areas is None else areas
    return Backend(name, description, scope,
                   lambda case: runners[case.area](case),
                   requires=requires)  # fmt: skip


LEGACY = backend(
    "duckdb-dgmquery", "DGMQuery.to_sql() executed on DuckDB", Engine(_legacy_sql)
)
PLAN_IR = backend(
    "duckdb-plan-ir",
    "DGMPlanner.plan_query() (plan IR, #148) executed on DuckDB",
    Engine(_ir_sql),
)
