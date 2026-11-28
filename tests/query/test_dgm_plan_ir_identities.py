"""Node identities from a declared model in the plan IR (#183).

``DGMPlanner(model=…)`` reads the entity keys of the model's NodeTables
(§2.6).  With them the IR emits SIMPLE and TRAIL repetition and other Repeat
bounds (§3.2), the maximal traces of a temporal mode over a Repeat (§4.1,
ADR 0001) and the order ≺ of SHORTEST, K_SHORTEST and MIN_WEIGHT (§3.3).
Without them those constructs are refused, and every other plan keeps its
SQL and its ``dag_hash``.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm import (
    EVENTUALLY,
    GLOBALLY,
    K_SHORTEST,
    MIN_WEIGHT,
    NEXT,
    ONCE,
    SHORTEST,
    UNBOUNDED,
    BridgeHop,
    Compose as PathCompose,
    DGMPlanner,
    DGMQuery,
    PathPred,
    PropRef,
    QueryTarget,
    Quantifier,
    Repeat,
    RepeatMode,
    ScalarPred,
    UntilMode,
    VerbHop,
)
from sqldim.core.query.dgm.model import (
    DGMModel,
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    NodeKind,
    NodeTable,
    NodeType,
    Reference,
)
from sqldim.core.query.dgm.planner import UnsupportedConstruct
from sqldim.core.query.dgm.planner import _ir_emit
from sqldim.core.query.dgm.planner._ir_identity import dag_hash
from sqldim.core.query.dgm.runtime._spec import InvalidSpec, encode_spec

EMPLOYEES = [  # a chain 1 ← 2 ← 3 ← 4 ← 5 and a cycle 6 ⇄ 7
    (1, None, "CEO"),
    (2, 1, "VP"),
    (3, 2, "Dir"),
    (4, 3, "Mgr"),
    (5, 4, "Eng"),
    (6, 7, "A"),
    (7, 6, "B"),
]


def _fk(label: str, col: str) -> ForeignKey:
    return ForeignKey(Holder.SOURCE, (col,), Reference.ENTITY_KEY)


def model(*, weighted: bool = False, version_key: str | None = None) -> DGMModel:
    return DGMModel(
        (
            NodeType(
                "Employee",
                NodeKind.DIM,
                mapping=NodeTable("employee", ("id",), version_key=version_key),
            ),
            NodeType("Customer", NodeKind.DIM, mapping=NodeTable("customer", ("id",))),
            NodeType("Sale", NodeKind.FACT, mapping=NodeTable("sale", ("id",))),
        ),
        (
            EdgeType(
                "Employee",
                "Employee",
                "reports_to",
                EdgeKind.BRIDGE,
                weighted=weighted,
                mapping=_fk("reports_to", "manager_id"),
            ),
            EdgeType("Customer", "Sale", "placed", EdgeKind.VERB),
        ),
    )


def planner(m: DGMModel | None = None) -> DGMPlanner:
    return DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB, model=m)  # type: ignore[arg-type]


def db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE employee (id INTEGER, manager_id INTEGER, title VARCHAR)")
    con.executemany("INSERT INTO employee VALUES (?, ?, ?)", EMPLOYEES)
    con.execute("CREATE TABLE customer (id VARCHAR)")
    con.execute("INSERT INTO customer VALUES ('c1'), ('c2')")
    con.execute("CREATE TABLE sale (id VARCHAR, customer_id VARCHAR, revenue INTEGER)")
    con.execute(
        "INSERT INTO sale VALUES ('s2', 'c1', 600), ('s1', 'c1', 300), "
        "('s3', 'c2', 700)"
    )
    return con


def reports_to(frm: str = "e", to: str = "m") -> BridgeHop:
    return BridgeHop(
        frm, "reports_to", to, table="employee", on=f"{to}.id = {frm}.manager_id"
    )


def managers(r: Repeat) -> DGMQuery:
    return (
        DGMQuery()
        .anchor("employee", "e")
        .path_join(r)
        .group_by("e.id")
        .agg(n="COUNT(m.id)", over="PATHS")
    )


def run(q: DGMQuery, m: DGMModel | None = None) -> list[Any]:
    return sorted(
        db().execute(planner(m or model()).plan_query(q).query_text).fetchall()
    )


# ---------------------------------------------------------------------------
# Repeat as a PathJoin (§3.2)
# ---------------------------------------------------------------------------


class TestRecursiveRepeat:
    def test_simple_stops_before_a_revisit(self) -> None:
        rows = run(managers(Repeat(reports_to(), 1, UNBOUNDED)))
        assert dict(rows) == {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 1, 7: 1}

    def test_trail_may_revisit_a_node_not_an_edge(self) -> None:
        r = Repeat(reports_to(), 1, UNBOUNDED, mode=RepeatMode.TRAIL)
        assert dict(run(managers(r))) == {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 2, 7: 2}

    def test_min_zero_includes_the_start_node(self) -> None:
        r = Repeat(reports_to(), 0, 2, mode=RepeatMode.WALK)
        assert dict(run(managers(r))) == {1: 1, 2: 2, 3: 3, 4: 3, 5: 3, 6: 3, 7: 3}

    def test_simple_with_a_bound(self) -> None:
        r = Repeat(reports_to(), 2, 3)
        assert dict(run(managers(r))) == {1: 0, 2: 0, 3: 1, 4: 2, 5: 2, 6: 0, 7: 0}

    def test_refused_without_identities(self) -> None:
        with pytest.raises(UnsupportedConstruct, match=r"DGMPlanner\(model="):
            planner().plan_query(managers(Repeat(reports_to(), 1, UNBOUNDED)))

    def test_a_versioned_node_table_has_no_identity(self) -> None:
        q = managers(Repeat(reports_to(), 1, UNBOUNDED))
        with pytest.raises(UnsupportedConstruct, match="version_key"):
            planner(model(version_key="vk")).plan_query(q)

    def test_depth_alias_is_refused(self) -> None:
        q = managers(Repeat(reports_to(), 1, UNBOUNDED, depth_alias="lvl"))
        with pytest.raises(UnsupportedConstruct, match="depth_alias"):
            planner(model()).plan_query(q)

    def test_truncation_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_ir_emit, "MAX_REPEAT_DEPTH", 2)
        sql = planner(model()).plan_query(managers(Repeat(reports_to(), 1, UNBOUNDED)))
        with pytest.raises(duckdb.Error, match="MAX_REPEAT_DEPTH"):
            db().execute(sql.query_text).fetchall()

    def test_composite_entity_keys(self) -> None:
        m = DGMModel(
            (NodeType("Emp", NodeKind.DIM, mapping=NodeTable("emp2", ("org", "id"))),),
            (EdgeType("Emp", "Emp", "up", EdgeKind.BRIDGE),),
        )
        hop = BridgeHop(
            "e", "up", "m", table="emp2", on="m.org = e.morg AND m.id = e.mid"
        )
        q = (
            DGMQuery()
            .anchor("emp2", "e")
            .path_join(Repeat(hop, 1, UNBOUNDED))
            .group_by("e.org", "e.id")
            .agg(n="COUNT(m.id)", over="PATHS")
        )
        con = duckdb.connect()
        con.execute("CREATE TABLE emp2 (org VARCHAR, id INT, morg VARCHAR, mid INT)")
        con.execute(  # a ⇄ b in org x; id 1 is shared by two orgs
            "INSERT INTO emp2 VALUES ('x', 1, 'x', 2), ('x', 2, 'x', 1), "
            "('y', 1, 'x', 1)"
        )
        rows = con.execute(planner(m).plan_query(q).query_text).fetchall()
        assert sorted(rows) == [("x", 1, 1), ("x", 2, 1), ("y", 1, 2)]


class TestIdentityIsOnlyWhereNeeded:
    def test_an_unrolled_walk_does_not_depend_on_the_model(self) -> None:
        q = managers(Repeat(reports_to(), 1, 3, mode=RepeatMode.WALK))
        with_model, without = planner(model()).plan_query(q), planner().plan_query(q)
        assert with_model.query_text == without.query_text
        assert dag_hash(with_model.ir) == dag_hash(without.ir)  # type: ignore[arg-type]

    def test_keys_are_part_of_the_identity(self) -> None:
        def h(m: DGMModel) -> str:
            q = managers(Repeat(reports_to(), 1, UNBOUNDED))
            return dag_hash(planner(m).plan_query(q).ir)  # type: ignore[arg-type]

        assert h(model()) == h(model())
        other = DGMModel(
            (NodeType("E", NodeKind.DIM, mapping=NodeTable("employee", ("title",))),),
        )
        assert h(other) != h(model())

    def test_reserved_alias_prefix(self) -> None:
        q = DGMQuery().anchor("employee", "__dgm_w")
        with pytest.raises(UnsupportedConstruct, match="reserves"):
            planner().plan_query(q)


# ---------------------------------------------------------------------------
# Temporal modes over a Repeat (§4.1, ADR 0001)
# ---------------------------------------------------------------------------


def _title(op: str, value: str) -> ScalarPred:
    return ScalarPred(PropRef("m", "title"), op, value)


def _where(pp: PathPred) -> DGMQuery:
    return DGMQuery().anchor("employee", "e").where(pp)


def _ids(q: DGMQuery, m: DGMModel | None = None) -> list[int]:
    return [r[0] for r in run(q, m)]


class TestTraces:
    def test_eventually(self) -> None:
        pp = PathPred(
            "e", Repeat(reports_to(), 1, UNBOUNDED), _title("=", "VP"),
            temporal_mode=EVENTUALLY,
        )  # fmt: skip
        assert _ids(_where(pp)) == [3, 4, 5]

    def test_globally_ends_at_a_terminal_or_a_blocked_revisit(self) -> None:
        pp = PathPred(
            "e",
            Repeat(reports_to(), 0, UNBOUNDED),
            ScalarPred(PropRef("m", "id"), ">", 2),
            temporal_mode=GLOBALLY,
        )
        # 6 → 7 and back to 6 is blocked (SIMPLE): the trace (6 7) is maximal
        assert _ids(_where(pp)) == [6, 7]

    def test_until_is_strong(self) -> None:
        pp = PathPred(
            "e",
            Repeat(reports_to(), 0, UNBOUNDED),
            _title("<>", "VP"),
            temporal_mode=UntilMode(_title("=", "VP")),
        )
        assert _ids(_where(pp)) == [2, 3, 4, 5]

    def test_forall_needs_a_witness(self) -> None:
        pp = PathPred(
            "e",
            Repeat(reports_to(), 1, UNBOUNDED),
            ScalarPred(PropRef("m", "id"), "<", 4),
            quantifier=Quantifier.FORALL,
            temporal_mode=GLOBALLY,
        )
        # employee 1 has no manager: T(s) is empty and REQUIRE_WITNESS holds
        assert _ids(_where(pp)) == [2, 3, 4]

    def test_refused_without_identities_and_by_to_sql(self) -> None:
        pp = PathPred(
            "e", Repeat(reports_to(), 1, UNBOUNDED), _title("=", "VP"),
            temporal_mode=EVENTUALLY,
        )  # fmt: skip
        with pytest.raises(UnsupportedConstruct, match="DGMPlanner"):
            planner().plan_query(_where(pp))
        with pytest.raises(UnsupportedConstruct, match="to_sql"):
            _where(pp).to_sql()

    def test_mode_over_a_composed_pattern_is_refused(self) -> None:
        path = PathCompose(
            VerbHop("e", "is", "c", table="customer", on="c.id = e.title"),
            Repeat(reports_to("c", "m"), 1, 2),
        )
        pp = PathPred("e", path, _title("=", "VP"), temporal_mode=EVENTUALLY)
        with pytest.raises(UnsupportedConstruct, match="one Repeat"):
            planner(model()).plan_query(_where(pp))


class TestNext:
    def test_next_over_a_repeat_is_its_first_hop(self) -> None:
        pp = PathPred(
            "e", Repeat(reports_to(), 0, UNBOUNDED), _title("=", "VP"),
            temporal_mode=NEXT,
        )  # fmt: skip
        hop = PathPred("e", reports_to(), _title("=", "VP"))
        assert _where(pp).to_sql() == _where(hop).to_sql()
        assert _ids(_where(pp), model()) == [3]

    def test_next_over_two_hops_reads_the_first(self) -> None:
        path = PathCompose(reports_to("e", "m"), reports_to("m", "g"))
        pp = PathPred("e", path, _title("=", "VP"), temporal_mode=NEXT)
        assert _ids(_where(pp), model()) == [3]

    def test_next_reading_a_later_alias_is_refused(self) -> None:
        path = PathCompose(reports_to("e", "m"), reports_to("m", "g"))
        sub = ScalarPred(PropRef("g", "title"), "=", "VP")
        pp = PathPred("e", path, sub, temporal_mode=NEXT)
        with pytest.raises(UnsupportedConstruct, match="Succ"):
            _where(pp).to_sql()

    def test_next_after_an_optional_repeat_is_refused(self) -> None:
        path = PathCompose(Repeat(reports_to(), 0, 2), reports_to("m", "g"))
        pp = PathPred("e", path, _title("=", "VP"), temporal_mode=NEXT)
        with pytest.raises(UnsupportedConstruct, match="min = 0"):
            planner(model()).plan_query(_where(pp))


class TestPastModes:
    def test_refused_with_the_reason(self) -> None:
        pp = PathPred(
            "e", Repeat(reports_to(), 1, 2), _title("=", "VP"), temporal_mode=ONCE
        )
        with pytest.raises(UnsupportedConstruct, match="truth condition"):
            planner(model()).plan_query(_where(pp))


# ---------------------------------------------------------------------------
# Strategies (§3.3)
# ---------------------------------------------------------------------------


def _placed(strategy: object, quantifier: Quantifier | None = None) -> DGMQuery:
    hop = VerbHop("c", "placed", "s", table="sale", on="s.customer_id = c.id")
    pp = PathPred(
        "c",
        hop,
        ScalarPred(PropRef("s", "revenue"), ">", 500),
        quantifier=quantifier,
        strategy=strategy,  # type: ignore[arg-type]
    )
    return DGMQuery().anchor("customer", "c").where(pp)


class TestStrategies:
    def test_shortest_is_the_least_key(self) -> None:
        # c1's ≺-least sale is s1 (300): it alone is selected
        assert run(_placed(SHORTEST())) == [("c2",)]

    def test_k_shortest(self) -> None:
        assert run(_placed(K_SHORTEST(2))) == [("c1",), ("c2",)]
        forall = _placed(K_SHORTEST(2), Quantifier.FORALL)
        assert run(forall) == [("c2",)]

    def test_min_weight_over_unit_weights_is_shortest(self) -> None:
        assert run(_placed(MIN_WEIGHT())) == run(_placed(SHORTEST()))

    def test_min_weight_needs_declared_weights(self) -> None:
        hop = reports_to()
        pp = PathPred("e", hop, _title("=", "VP"), strategy=MIN_WEIGHT())
        with pytest.raises(UnsupportedConstruct, match="weighted bridge"):
            planner(model(weighted=True)).plan_query(_where(pp))
        bare = DGMModel(
            (NodeType("E", NodeKind.DIM, mapping=NodeTable("employee", ("id",))),)
        )
        with pytest.raises(UnsupportedConstruct, match="no declared edge type"):
            planner(bare).plan_query(_where(pp))

    def test_refused_without_identities(self) -> None:
        with pytest.raises(UnsupportedConstruct, match="entity keys"):
            planner().plan_query(_placed(SHORTEST()))
        with pytest.raises(UnsupportedConstruct, match="to_sql"):
            _placed(SHORTEST()).to_sql()

    def test_k_must_be_positive(self) -> None:
        with pytest.raises(UnsupportedConstruct, match="k ≥ 1"):
            planner(model()).plan_query(_placed(K_SHORTEST(0)))

    def test_strategy_with_a_mode_is_refused(self) -> None:
        hop = VerbHop("c", "placed", "s", table="sale", on="s.customer_id = c.id")
        pp = PathPred(
            "c", hop, ScalarPred(PropRef("s", "revenue"), ">", 500),
            strategy=SHORTEST(), temporal_mode=EVENTUALLY,
        )  # fmt: skip
        with pytest.raises(UnsupportedConstruct, match="does not state"):
            planner(model()).plan_query(DGMQuery().anchor("customer", "c").where(pp))

    def test_identity_and_spec(self) -> None:
        a = planner(model()).plan_query(_placed(SHORTEST())).ir
        b = planner(model()).plan_query(_placed(K_SHORTEST(1))).ir
        assert dag_hash(a) != dag_hash(b)  # type: ignore[arg-type]
        with pytest.raises(InvalidSpec):
            encode_spec(a)  # type: ignore[arg-type]
