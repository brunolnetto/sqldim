"""Physical plan IR, canonical identity and deps(plan) (DGM §6.2, §8.14; #148)."""

from __future__ import annotations

import dataclasses
import enum
import json
from datetime import timedelta
from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm import (
    AND,
    NOT,
    OR,
    DGMQuery,
    PathPred,
    PropRef,
    AggRef,
    WinRef,
    RawPred,
    ScalarPred,
    VerbHop,
    BridgeHop,
    Compose as PathCompose,
)
from sqldim.core.query.dgm.algebra import ComposeOp, ComposedQuery, QuestionAlgebra
from sqldim.core.query.dgm.preds import SHORTEST, Quantifier
from sqldim.core.query.dgm.temporal import EVENTUALLY
from sqldim.core.query.dgm.planner import (
    CANON_VERSION,
    CanonicalEncodingError,
    DGMPlanner,
    Dependency,
    ExportPlan,
    PlanIdentityUnavailable,
    QueryTarget,
    StaticCatalog,
    UnknownRelation,
    UnsafeFragment,
    UnsupportedConstruct,
    dag_hash,
    deps,
    emit_sql,
    predicate_hash,
    question_key,
    relations_read,
)
from sqldim.core.query.dgm.planner import _ir_identity
from sqldim.core.query.dgm.planner._canon import canonical_encode, canonical_hash
from sqldim.core.query.dgm.planner._ir import (
    Compare,
    Conj,
    Disj,
    Fragment,
    Literal,
    Neg,
    ReadRole,
    check_fragment,
)
from sqldim.core.query.dgm.planner._ir_build import lower_pred


def planner(target: QueryTarget = QueryTarget.SQL_DUCKDB) -> DGMPlanner:
    return DGMPlanner(None, None, None, None, target)  # type: ignore[arg-type]


def sale_hop() -> VerbHop:
    return VerbHop("c", "placed", "s", table="fact_sale", on="c.id = s.customer_id")


def return_pred(threshold: int = 1) -> PathPred:
    return PathPred(
        "c",
        VerbHop("c", "returned", "r", table="fact_return", on="r.customer_id = c.id"),
        ScalarPred(PropRef("r", "amount"), ">", threshold),
    )


# ---------------------------------------------------------------------------
# Fixture queries — each is one shape the planner lowers
# ---------------------------------------------------------------------------


def q_anchor() -> DGMQuery:
    return DGMQuery().anchor("dim_customer", "c")


def q_path_join_where() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("dim_customer", "c")
        .path_join(sale_hop())
        .where(ScalarPred(PropRef("c", "region"), "=", "EU"))
    )


def q_band2() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("dim_customer", "c")
        .path_join(sale_hop())
        .group_by("c.region")
        .agg(total="SUM(s.revenue)", n="COUNT(*)")
        .having(ScalarPred(AggRef("total"), ">", 10))
    )


def q_band3() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("fact_sale", "s")
        .window(
            rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)"
        )
        .qualify(ScalarPred(WinRef("rn"), "<=", 2))
    )


def q_exists() -> DGMQuery:
    return DGMQuery().anchor("dim_customer", "c").where(return_pred())


def q_exists_two_hops() -> DGMQuery:
    path = PathCompose(
        VerbHop("c", "placed", "s2", table="fact_sale", on="s2.customer_id = c.id"),
        BridgeHop("s2", "tagged", "t", table="bridge_tag", on="t.sale_id = s2.id"),
    )
    return (
        DGMQuery()
        .anchor("dim_customer", "c")
        .where(PathPred("c", path, ScalarPred(PropRef("t", "tag"), "=", "promo")))
    )


def q_scd_current() -> DGMQuery:
    return DGMQuery("fact_sale").join_dim("dim_customer", "customer_id", mode="AS_IS")


def q_scd_as_of() -> DGMQuery:
    return (
        DGMQuery("fact_sale")
        .join_dim("dim_customer", "customer_id", mode="AS_IS")
        .as_of("2024-06-30")
        .where("f.revenue > 0")
    )


def q_path_join_as_of() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("fact_sale", "s")
        .path_join(
            VerbHop("s", "by", "c", table="dim_customer", on="s.customer_id = c.id")
        )
        .as_of("2024-01-01")
    )


def q_bool_tree() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("dim_customer", "c")
        .where(
            OR(
                ScalarPred(PropRef("c", "region"), "=", "EU"),
                NOT(ScalarPred(PropRef("c", "tier"), "IS", None)),
            )
        )
        .where(RawPred("c.id > 0"))
    )


def q_derived() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("v_big_customer", "b")
        .where(ScalarPred(PropRef("b", "id"), ">", 0))
    )


SINGLE_FIXTURES = {
    "anchor": q_anchor,
    "path_join_where": q_path_join_where,
    "band2": q_band2,
    "band3": q_band3,
    "exists": q_exists,
    "exists_two_hops": q_exists_two_hops,
    "scd_current": q_scd_current,
    "scd_as_of": q_scd_as_of,
    "path_join_as_of": q_path_join_as_of,
    "bool_tree": q_bool_tree,
    "derived": q_derived,
}


def alg_union() -> tuple[QuestionAlgebra, str]:
    alg = QuestionAlgebra()
    alg.add("eu", q_bool_tree())
    alg.add("returners", q_exists())
    alg.add("unused", DGMQuery().anchor("fact_unrelated", "u"))
    alg.compose("eu", ComposeOp.UNION, "returners", name="either")
    return alg, "either"


def alg_join() -> tuple[QuestionAlgebra, str]:
    alg = QuestionAlgebra()
    alg.add("a", q_anchor())
    alg.add("b", q_scd_current())
    alg.compose("a", ComposeOp.JOIN, "b", name="j", on="l.id = r.customer_id")
    return alg, "j"


def alg_nested() -> tuple[QuestionAlgebra, str]:
    alg = QuestionAlgebra()
    alg.add("a", q_anchor())
    alg.add("b", q_exists())
    alg.add("d", q_derived())
    alg.compose("a", ComposeOp.INTERSECT, "b", name="ab")
    alg.compose("ab", ComposeOp.EXCEPT, "d", name="final")
    return alg, "final"


ALG_FIXTURES = {"union": alg_union, "join": alg_join, "nested": alg_nested}


# ---------------------------------------------------------------------------
# A catalog and a DuckDB instance for the fixtures
# ---------------------------------------------------------------------------

TABLES = {
    "dim_customer": "id INTEGER, region VARCHAR, tier VARCHAR, is_current BOOLEAN, "
    "valid_from DATE, valid_to DATE",
    "fact_sale": "id INTEGER, customer_id INTEGER, revenue DOUBLE",
    "fact_return": "customer_id INTEGER, amount DOUBLE",
    "bridge_tag": "sale_id INTEGER, tag VARCHAR",
    "fact_unrelated": "id INTEGER, customer_id INTEGER, region VARCHAR, "
    "tier VARCHAR, is_current BOOLEAN, valid_from DATE, valid_to DATE",
}
VIEWS = {
    "v_big_customer": (
        "SELECT c.* FROM dim_customer c JOIN fact_sale s ON s.customer_id = c.id",
        frozenset({"dim_customer", "fact_sale"}),
    )
}
CATALOG = StaticCatalog(
    base=frozenset(TABLES),
    derived={v: bases for v, (_, bases) in VIEWS.items()},
)


def database_with(relations: set[str]) -> duckdb.DuckDBPyConnection:
    """A DuckDB holding only *relations* (views get the bases they need)."""
    con = duckdb.connect()
    for t in sorted(relations & set(TABLES)):
        con.execute(f"CREATE TABLE {t} ({TABLES[t]})")
    for v in sorted(relations & set(VIEWS)):
        con.execute(f"CREATE VIEW {v} AS {VIEWS[v][0]}")
    return con


def referenced_tables(sql: str) -> set[str]:
    """Every BASE_TABLE reference DuckDB's parser finds in *sql*."""
    tree = json.loads(
        duckdb.sql("SELECT json_serialize_sql(?)", params=[sql]).fetchone()[0]  # type: ignore[index]
    )
    assert tree["error"] is False, tree
    found: set[str] = set()

    def walk(n: Any) -> None:
        if isinstance(n, dict):
            assert n.get("type") != "TABLE_FUNCTION", n
            if n.get("type") == "BASE_TABLE":
                found.add(n["table_name"])
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    walk(tree)
    return found


def all_plans() -> list[tuple[str, ExportPlan]]:
    out = [(k, planner().plan_query(f())) for k, f in SINGLE_FIXTURES.items()]
    for k, f in ALG_FIXTURES.items():
        alg, final = f()
        out.append((f"alg:{k}", planner().plan_query(alg, final=final)))
    return out


# ---------------------------------------------------------------------------
# Criterion 5 — the emitted SQL reads no relation outside deps(plan)
# ---------------------------------------------------------------------------


class TestEmittedSqlReadsOnlyDeps:
    @pytest.mark.parametrize("name,plan", all_plans(), ids=lambda x: str(x)[:20])
    def test_parsed_relations_are_declared(self, name: str, plan: ExportPlan) -> None:
        assert plan.ir is not None
        cte_names = {c.name for c in plan.ir.ctes}
        parsed = referenced_tables(plan.query_text) - cte_names
        assert parsed == set(relations_read(plan.ir))

    @pytest.mark.parametrize("name,plan", all_plans(), ids=lambda x: str(x)[:20])
    def test_runs_against_a_database_holding_only_deps(
        self, name: str, plan: ExportPlan
    ) -> None:
        dep_names = {d.relation for d in plan.deps(CATALOG)}
        assert all(d.partitions is None for d in plan.deps(CATALOG))
        assert plan.ir is not None
        views = set(relations_read(plan.ir)) & set(VIEWS)
        con = database_with(dep_names | views)
        con.execute(
            plan.query_text
        ).fetchall()  # a read outside deps → CatalogException

    @pytest.mark.parametrize("name", sorted(SINGLE_FIXTURES))
    def test_each_dependency_is_needed(self, name: str) -> None:
        plan = planner().plan_query(SINGLE_FIXTURES[name]())
        dep_names = {d.relation for d in plan.deps(CATALOG)}
        assert plan.ir is not None
        views = set(relations_read(plan.ir)) & set(VIEWS)
        for missing in dep_names:
            with pytest.raises(duckdb.Error):
                con = database_with((dep_names - {missing}) | views)
                con.execute(plan.query_text).fetchall()

    def test_unreferenced_cte_is_not_emitted_or_depended_on(self) -> None:
        alg, final = alg_union()
        plan = planner().plan_query(alg, final=final)
        assert "fact_unrelated" not in plan.query_text
        assert Dependency("fact_unrelated") not in plan.deps(CATALOG)


# ---------------------------------------------------------------------------
# Criterion 1 — typed IR, SQL emitted from it
# ---------------------------------------------------------------------------


class TestEmission:
    @pytest.mark.parametrize("name", sorted(SINGLE_FIXTURES))
    def test_single_query_sql_equals_legacy_rendering(self, name: str) -> None:
        q = SINGLE_FIXTURES[name]()
        assert planner().plan_query(q).query_text == q.to_sql()

    def test_query_text_is_emitted_from_the_ir(self) -> None:
        plan = planner().plan_query(q_path_join_where())
        assert plan.ir is not None
        assert plan.query_text == emit_sql(plan.ir)
        root = plan.ir.root
        assert not isinstance(root, type(None))
        moved = dataclasses.replace(
            plan.ir,
            root=dataclasses.replace(
                root,  # type: ignore[type-var]
                anchor=dataclasses.replace(root.anchor, relation="dim_other"),  # type: ignore[union-attr]
            ),
        )
        assert "FROM dim_other c" in emit_sql(moved)

    def test_text_that_disagrees_with_the_ir_is_rejected(self) -> None:
        plan = planner().plan_query(q_anchor())
        with pytest.raises(ValueError, match="does not match"):
            ExportPlan(QueryTarget.SQL_DUCKDB, "SELECT 1", ir=plan.ir)

    def test_lowering_is_deterministic(self) -> None:
        a = planner().plan_query(q_band2())
        b = planner().plan_query(q_band2())
        assert a.ir == b.ir and a.query_text == b.query_text

    def test_ir_records_read_roles(self) -> None:
        plan = planner().plan_query(q_exists_two_hops())
        assert plan.ir is not None
        roles = {(s.relation, s.role) for s in plan.ir.root.scans()}  # type: ignore[union-attr]
        assert roles == {
            ("dim_customer", ReadRole.ANCHOR),
            ("fact_sale", ReadRole.EXISTS_PATH),
            ("bridge_tag", ReadRole.EXISTS_PATH),
        }
        scd = planner().plan_query(q_scd_as_of()).ir
        assert scd is not None
        assert ("dim_customer", ReadRole.VERSIONED_JOIN) in {
            (s.relation, s.role)
            for s in scd.root.scans()  # type: ignore[union-attr]
        }

    def test_string_literals_are_escaped(self) -> None:
        q = DGMQuery().anchor("dim_customer", "c")
        q.where(ScalarPred(PropRef("c", "region"), "=", "x' OR 1=1 OR '"))
        sql = planner().plan_query(q).query_text
        assert "'x'' OR 1=1 OR '''" in sql
        assert referenced_tables(sql) == {"dim_customer"}

    def test_forall_is_emitted_as_not_exists(self) -> None:
        pp = return_pred()
        pp.quantifier = Quantifier.FORALL
        sql = planner().plan_query(DGMQuery().anchor("dim_customer", "c").where(pp))
        text = sql.query_text
        # NotTrue (§4.1) and the REQUIRE_WITNESS guard (ADR 0001)
        assert (
            "NOT EXISTS (SELECT 1 FROM fact_return r WHERE (r.amount > 1) IS NOT TRUE"
            in text
        )
        assert "AND EXISTS (SELECT 1 FROM fact_return r WHERE " in text

    def test_algebra_emits_topological_with(self) -> None:
        alg, final = alg_nested()
        plan = planner().plan_query(alg, final=final)
        assert plan.ir is not None
        assert [c.name for c in plan.ir.ctes] == ["a", "b", "ab", "d", "final"]
        assert plan.query_text.endswith("SELECT * FROM final")
        assert plan.ir.ctes[-1].node is plan.ir.root

    def test_identical_leaves_share_one_cte(self) -> None:
        alg = QuestionAlgebra()
        alg.add("x", q_anchor())
        alg.add("y", q_anchor())
        alg.compose("x", ComposeOp.JOIN, "y", name="j", on="l.id = r.id")
        plan = planner().plan_query(alg, final="j")
        assert plan.ir is not None
        assert [c.name for c in plan.ir.ctes] == ["x", "j"]
        assert "FROM x l\nJOIN x r ON l.id = r.id" in plan.query_text

    def test_write_plan_kept(self) -> None:
        from sqldim.core.query.dgm.planner import SinkTarget

        p = DGMPlanner(
            None, None, None, None, QueryTarget.SQL_DUCKDB, SinkTarget.PARQUET
        )  # type: ignore[arg-type]
        plan = p.plan_query(q_anchor(), table_name="out")
        assert plan.write_plan is not None
        assert plan.write_plan.describe() == "COPY TO (FORMAT PARQUET)"
        assert plan.sink_target is SinkTarget.PARQUET


class TestUnsupportedConstructsFailClosed:
    def _plan(self, q: object, **kw: Any) -> ExportPlan:
        return planner().plan_query(q, **kw)  # type: ignore[arg-type]

    def test_temporal_mode_over_one_position_is_lowered(self) -> None:
        """EF over one evaluation position is EXISTS (§4.1, ADR 0001)."""
        pp = return_pred()
        pp.temporal_mode = EVENTUALLY
        q = DGMQuery().anchor("dim_customer", "c").where(pp)
        plain = DGMQuery().anchor("dim_customer", "c").where(return_pred())
        assert self._plan(q).query_text == self._plan(plain).query_text == q.to_sql()

    def test_temporal_mode_without_one_position(self) -> None:
        pp = return_pred()
        pp.sub_filter = ScalarPred(PropRef("c", "tier"), "=", "gold")
        pp.temporal_mode = EVENTUALLY
        with pytest.raises(UnsupportedConstruct, match="evaluation position"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(pp))

    def test_restricting_strategy(self) -> None:
        pp = return_pred()
        pp.strategy = SHORTEST()
        with pytest.raises(UnsupportedConstruct, match="SHORTEST"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(pp))

    def test_promote(self) -> None:
        pp = return_pred()
        pp.promote = True
        with pytest.raises(UnsupportedConstruct, match="promote"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(pp))

    def test_hop_without_table(self) -> None:
        pp = PathPred(
            "c", VerbHop("c", "x", "r"), ScalarPred(PropRef("r", "a"), "=", 1)
        )
        with pytest.raises(UnsupportedConstruct, match="explicit table"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(pp))

    def test_path_alias_shadowing(self) -> None:
        pp = PathPred(
            "c",
            VerbHop("c", "x", "c", table="fact_sale", on="c.id = c.customer_id"),
            ScalarPred(PropRef("c", "a"), "=", 1),
        )
        from sqldim.exceptions import SemanticError

        # rejected by the §3.5 rule (C ∩ L ≠ ∅), before lowering (#173)
        with pytest.raises(SemanticError, match="C ∩ L"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(pp))

    def test_signature_pred(self) -> None:
        from sqldim.core.query.dgm.preds import SequenceMatch, SignaturePred

        sp = SignaturePred("p", ["a"], SequenceMatch.EXACT)
        with pytest.raises(UnsupportedConstruct, match="SignaturePred"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(sp))

    @pytest.mark.parametrize(
        "raw",
        [
            "c.id IN (SELECT id FROM secret)",
            "c.id = 1 -- comment",
            "c.id = 1; DROP TABLE x",
            "c.id = 1 /* x */",
            "secret.id = c.id",
            'c."id" = 1',
            "read_parquet('x.parquet') IS NOT NULL",
        ],
    )
    def test_raw_fragment_that_could_read_a_relation(self, raw: str) -> None:
        with pytest.raises(UnsafeFragment):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(raw))

    def test_on_clause_with_unknown_alias(self) -> None:
        hop = VerbHop("c", "p", "s", table="fact_sale", on="z.id = s.customer_id")
        with pytest.raises(UnsafeFragment, match="'z'"):
            self._plan(DGMQuery().anchor("dim_customer", "c").path_join(hop))

    def test_table_function_as_relation(self) -> None:
        with pytest.raises(UnsupportedConstruct, match="not a table name"):
            self._plan(DGMQuery().anchor("read_parquet('x')", "c"))

    def test_duplicate_alias(self) -> None:
        hop = VerbHop("c", "p", "c", table="fact_sale", on="c.id = c.customer_id")
        with pytest.raises(UnsupportedConstruct, match="bound twice"):
            self._plan(DGMQuery().anchor("dim_customer", "c").path_join(hop))

    def test_unsupported_operator_and_value(self) -> None:
        q = DGMQuery().anchor("dim_customer", "c")
        q.where(ScalarPred(PropRef("c", "id"), "IN", (1, 2)))
        with pytest.raises(UnsupportedConstruct, match="operator"):
            self._plan(q)
        q2 = DGMQuery().anchor("dim_customer", "c")
        q2.where(ScalarPred(PropRef("c", "id"), "=", object()))
        with pytest.raises(UnsupportedConstruct, match="literal"):
            self._plan(q2)

    def test_empty_conjunction(self) -> None:
        with pytest.raises(UnsupportedConstruct, match="empty AND"):
            self._plan(DGMQuery().anchor("dim_customer", "c").where(AND()))

    def test_temporal_context(self) -> None:
        q = DGMQuery().anchor("dim_customer", "c").temporal_context("2024-01-01")
        with pytest.raises(UnsupportedConstruct, match="TemporalContext"):
            self._plan(q)

    def test_missing_anchor_keeps_legacy_error(self) -> None:
        from sqldim.exceptions import SemanticError

        with pytest.raises(SemanticError, match="anchor"):
            self._plan(DGMQuery())

    def test_raw_sql_cte(self) -> None:
        alg = QuestionAlgebra()
        alg._ctes["raw"] = ComposedQuery(name="raw", sql="SELECT * FROM secret")
        with pytest.raises(UnsupportedConstruct, match="raw SQL"):
            self._plan(alg, final="raw")

    def test_tampered_composition(self) -> None:
        alg, final = alg_union()
        alg._ctes[final]._sql = "SELECT * FROM secret"
        with pytest.raises(UnsupportedConstruct, match="compose"):
            self._plan(alg, final=final)

    def test_reserved_word_as_cte_name(self) -> None:
        alg = QuestionAlgebra()
        alg.add("a", q_anchor())
        alg.add("b", q_exists())
        alg.compose("a", ComposeOp.UNION, "b", name="both")
        with pytest.raises(UnsupportedConstruct, match="reserved"):
            self._plan(alg, final="both")

    def test_unknown_final(self) -> None:
        alg, _ = alg_union()
        with pytest.raises(UnsupportedConstruct, match="not registered"):
            self._plan(alg, final="nope")

    def test_final_argument_mismatch(self) -> None:
        alg, _ = alg_union()
        with pytest.raises(UnsupportedConstruct, match="needs final"):
            self._plan(alg)
        with pytest.raises(UnsupportedConstruct, match="only to a QuestionAlgebra"):
            self._plan(q_anchor(), final="x")

    @pytest.mark.parametrize(
        "target", [QueryTarget.CYPHER, QueryTarget.SPARQL, QueryTarget.SQL_POSTGRESQL]
    )
    def test_non_duckdb_target(self, target: QueryTarget) -> None:
        with pytest.raises(UnsupportedConstruct, match="no emitter"):
            planner(target).plan_query(q_anchor())


# ---------------------------------------------------------------------------
# Criterion 3 — canonical serialisation, dag_hash, question_key
# ---------------------------------------------------------------------------


class _E(enum.Enum):
    A = "a"


class TestCanonicalEncoding:
    def test_type_tags_separate_equal_looking_values(self) -> None:
        values = [1, "1", True, 1.0, b"1", None, (1,), ("1",)]
        encodings = {canonical_encode(v) for v in values}
        assert len(encodings) == len(values)

    def test_length_prefix_separates_concatenations(self) -> None:
        assert canonical_encode(("ab", "c")) != canonical_encode(("a", "bc"))
        assert canonical_encode((("a",), "b")) != canonical_encode(("a", ("b",)))

    def test_hash_is_untruncated_sha256(self) -> None:
        h = canonical_hash("x")
        assert len(h) == 64 and int(h, 16) >= 0

    @pytest.mark.parametrize("bad", [{"a": 1}, {1}, _E.A, object(), float("nan")])
    def test_values_without_encoding_are_rejected(self, bad: object) -> None:
        with pytest.raises(CanonicalEncodingError):
            canonical_encode(bad)


def key(plan: ExportPlan, schema: str = "schema-1", **params: object) -> str:
    return plan.question_key(schema_version=schema, params=params)


class TestQuestionKey:
    def test_same_question_built_twice_has_the_same_key(self) -> None:
        assert key(planner().plan_query(q_band2())) == key(
            planner().plan_query(q_band2())
        )

    def test_cte_names_are_not_part_of_the_key(self) -> None:
        a = QuestionAlgebra()
        a.add("x", q_anchor())
        a.add("y", q_exists())
        a.compose("x", ComposeOp.UNION, "y", name="u")
        b = QuestionAlgebra()
        b.add("p", q_anchor())
        b.add("q", q_exists())
        b.compose("p", ComposeOp.UNION, "q", name="w")
        pa, pb = planner().plan_query(a, final="u"), planner().plan_query(b, final="w")
        assert pa.query_text != pb.query_text
        assert key(pa) == key(pb)

    @pytest.mark.parametrize("op", [ComposeOp.UNION, ComposeOp.INTERSECT])
    def test_commutative_operators_sort_children(self, op: ComposeOp) -> None:
        assert self._swap_keys(op)[0] == self._swap_keys(op)[1]

    @pytest.mark.parametrize("op", [ComposeOp.EXCEPT, ComposeOp.WITH])
    def test_ordered_operators_keep_child_order(self, op: ComposeOp) -> None:
        k1, k2 = self._swap_keys(op)
        assert k1 != k2

    def _swap_keys(self, op: ComposeOp) -> tuple[str, str]:
        keys = []
        for left, right in (("a", "b"), ("b", "a")):
            alg = QuestionAlgebra()
            alg.add("a", q_anchor())
            alg.add("b", q_exists())
            alg.compose(left, op, right, name="f")
            keys.append(key(planner().plan_query(alg, final="f")))
        return keys[0], keys[1]

    def test_algebra_with_a_leaf_final_matches_the_query(self) -> None:
        alg = QuestionAlgebra()
        alg.add("single", q_band2())
        assert key(planner().plan_query(alg, final="single")) == key(
            planner().plan_query(q_band2())
        )

    def test_key_inputs_each_change_the_key(self, monkeypatch: Any) -> None:
        plan = planner().plan_query(q_exists())
        base = key(plan)
        assert key(plan, schema="schema-2") != base
        assert key(plan, limit=10) != base
        assert key(plan, limit=10) != key(plan, limit=11)
        assert (
            key(
                planner().plan_query(
                    DGMQuery().anchor("dim_customer", "c").where(return_pred(2))
                )
            )
            != base
        )
        assert (
            key(
                planner().plan_query(
                    DGMQuery()
                    .anchor("dim_customer", "k")
                    .where(
                        PathPred(
                            "k",
                            VerbHop(
                                "k",
                                "returned",
                                "r",
                                table="fact_return",
                                on="r.customer_id = k.id",
                            ),
                            ScalarPred(PropRef("r", "amount"), ">", 1),
                        )
                    )
                )
            )
            != base
        )
        monkeypatch.setattr(_ir_identity, "CANON_VERSION", CANON_VERSION + "-next")
        assert key(plan) != base

    def test_params_are_sorted_by_name(self) -> None:
        plan = planner().plan_query(q_anchor())
        assert plan.question_key(schema_version="s", params={"a": 1, "b": 2}) == (
            plan.question_key(schema_version="s", params={"b": 2, "a": 1})
        )

    def test_invalid_key_inputs(self) -> None:
        plan = planner().plan_query(q_anchor())
        with pytest.raises(ValueError, match="schema_version"):
            plan.question_key(schema_version="")
        with pytest.raises(TypeError, match="parameter names"):
            question_key(plan.ir, schema_version="s", params={1: 2})  # type: ignore[arg-type, dict-item]
        with pytest.raises(CanonicalEncodingError):
            plan.question_key(schema_version="s", params={"p": {1, 2}})

    def test_plan_without_ir_has_no_identity(self) -> None:
        plan = planner().build_plan("SELECT * FROM t")
        with pytest.raises(PlanIdentityUnavailable, match="no IR"):
            key(plan)
        with pytest.raises(PlanIdentityUnavailable):
            plan.deps(CATALOG)

    def test_steps_added_outside_the_ir_have_no_identity(self) -> None:
        from sqldim.core.query.dgm.planner import PreComputation

        plan = planner().plan_query(q_anchor())
        plan.pre_compute.append(PreComputation("pc", "SELECT 1"))
        with pytest.raises(PlanIdentityUnavailable, match="pre_compute"):
            key(plan)
        with pytest.raises(PlanIdentityUnavailable):
            plan.deps(CATALOG)


def _c(col: str, v: object) -> Compare:
    return Compare(Fragment(f"c.{col}"), "=", Literal(v))


class TestPredicateHash:
    def test_commutativity_and_absorption(self) -> None:
        a, b = _c("x", 1), _c("y", 2)
        assert predicate_hash(Conj((a, b))) == predicate_hash(Conj((b, a)))
        assert predicate_hash(Disj((a, Conj((a, b))))) == predicate_hash(a)
        assert predicate_hash(Neg(Neg(a))) == predicate_hash(a)

    def test_distinct_filters_differ(self) -> None:
        a, b = _c("x", 1), _c("y", 2)
        hashes = {
            predicate_hash(p)
            for p in (
                a,
                b,
                Conj((a, b)),
                Disj((a, b)),
                Neg(a),
                _c("x", "1"),
                _c("x", True),
            )
        }
        assert len(hashes) == 7

    def test_equal_filters_built_from_distinct_objects(self) -> None:
        scope = frozenset({"c"})
        p1 = lower_pred(
            AND(ScalarPred(PropRef("c", "x"), "=", 1), RawPred("c.y > 2")), scope
        )
        p2 = lower_pred(
            AND(RawPred("c.y > 2"), ScalarPred(PropRef("c", "x"), "=", 1)), scope
        )
        assert predicate_hash(p1) == predicate_hash(p2)

    def test_where_and_having_are_hashed_in_their_band(self) -> None:
        plan = planner().plan_query(q_band2())
        assert plan.ir is not None
        assert dag_hash(plan.ir) == dag_hash(plan.ir.root)


# ---------------------------------------------------------------------------
# Criterion 2 — deps(plan)
# ---------------------------------------------------------------------------


class TestDeps:
    def test_exists_hops_and_versioned_joins_are_dependencies(self) -> None:
        assert planner().plan_query(q_exists_two_hops()).deps(CATALOG) == (
            Dependency("bridge_tag"),
            Dependency("dim_customer"),
            Dependency("fact_sale"),
        )
        assert planner().plan_query(q_scd_as_of()).deps(CATALOG) == (
            Dependency("dim_customer"),
            Dependency("fact_sale"),
        )

    def test_derived_views_are_replaced_by_their_bases(self) -> None:
        plan = planner().plan_query(q_derived())
        assert plan.ir is not None
        assert relations_read(plan.ir) == {"v_big_customer"}
        assert plan.deps(CATALOG) == (
            Dependency("dim_customer"),
            Dependency("fact_sale"),
        )

    def test_undeclared_relation_is_an_error(self) -> None:
        plan = planner().plan_query(DGMQuery().anchor("mystery", "m"))
        assert plan.ir is not None
        with pytest.raises(UnknownRelation, match="mystery"):
            deps(plan.ir, CATALOG)

    @pytest.mark.parametrize(
        "base,derived,msg",
        [
            ({"a"}, {"a": frozenset({"a"})}, "both"),
            ({"a"}, {"v": frozenset()}, "no base"),
            ({"a"}, {"v": frozenset({"b"})}, "undeclared"),
        ],
    )
    def test_catalog_declarations_are_checked(
        self, base: set[str], derived: dict[str, frozenset[str]], msg: str
    ) -> None:
        with pytest.raises(ValueError, match=msg):
            StaticCatalog(frozenset(base), derived)

    def test_fragment_scope_check_is_by_alias(self) -> None:
        assert check_fragment("SUM(s.revenue) / NULLIF(COUNT(*), 0)", frozenset({"s"}))
        with pytest.raises(UnsafeFragment):
            check_fragment("SUM(t.revenue)", frozenset({"s"}))
        with pytest.raises(UnsafeFragment):
            check_fragment("   ", frozenset({"s"}))


# ---------------------------------------------------------------------------
# Criterion 4 — PreComputation time estimate and approximate variant (Step 1b)
# ---------------------------------------------------------------------------

from sqldim.core.query.dgm.planner import (  # noqa: E402
    ApproximateVariant,
    ClarifyDecision,
    CostEstimate,
    DeclaredPrecomputeProvider,
    ExecutionBudget,
    ExecutionGate,
    PreComputation,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    Substitution,
)


def _cost(n: int) -> CostEstimate:
    return CostEstimate(cpu_ops=n, io_ops=0)


def _budget(*subs: Substitution) -> ExecutionBudget:
    return ExecutionBudget(
        max_estimated_cost=_cost(10_000),
        max_result_rows=1_000,
        max_wall_time=timedelta(seconds=30),
        max_precompute_time=timedelta(seconds=10),
        streaming_threshold=_cost(100),
        async_threshold=_cost(1_000),
        substitutions=frozenset(subs),
    )


def _variant(seconds: int = 1, cost: int = 5) -> ApproximateVariant:
    return ApproximateVariant(
        name="entropy_approx",
        query="SELECT 1",
        estimated_time=timedelta(seconds=seconds),
        cost_estimate=_cost(cost),
        accuracy="reservoir sample of path instances (k=1000)",
    )


def _pc_plan(
    *, seconds: int | None = 60, pc_cost: int | None = 40, variant: Any = "default"
) -> ExportPlan:
    v = _variant() if variant == "default" else variant
    pc = PreComputation(
        name="entropy",
        query="SELECT 2",
        estimated_time=None if seconds is None else timedelta(seconds=seconds),
        cost_estimate=None if pc_cost is None else _cost(pc_cost),
        approximation=v,
    )
    return ExportPlan(
        QueryTarget.SQL_DUCKDB,
        "SELECT 3",
        pre_compute=[pc],
        cost_estimate=_cost(50),
        estimated_rows=10,
    )


def _gate() -> ExecutionGate:
    return ExecutionGate(precompute=DeclaredPrecomputeProvider())


class TestPreComputationDeclarations:
    def test_defaults_keep_existing_constructors(self) -> None:
        pc = PreComputation(name="x", query="q")
        assert pc.estimated_time is None and pc.approximation is None

    def test_negative_time_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            PreComputation(name="x", query="q", estimated_time=timedelta(seconds=-1))

    def test_variant_requires_time_and_accuracy(self) -> None:
        with pytest.raises(ValueError, match="accuracy"):
            dataclasses.replace(_variant(), accuracy=" ")
        with pytest.raises(ValueError, match="estimated_time"):
            dataclasses.replace(_variant(), estimated_time=None)  # type: ignore[arg-type]

    def test_provider_reports_only_declared_times(self) -> None:
        p = DeclaredPrecomputeProvider()
        assert p.estimated_time(PreComputation("x", "q")) is None
        t = timedelta(seconds=3)
        assert p.estimated_time(PreComputation("x", "q", estimated_time=t)) == t


class TestStep1bWithDeclaredProvider:
    def test_slow_precomputation_offers_its_approximation(self) -> None:  # T19
        d = _gate().gate(_pc_plan(), _budget())
        assert isinstance(d, ClarifyDecision)
        (opt,) = d.options
        assert opt.substitution is Substitution.APPROX_ALGORITHM
        assert opt.cost == CostEstimate(cpu_ops=15, io_ops=0, note="APPROX_ALGORITHM")
        assert opt.warning == "entropy: reservoir sample of path instances (k=1000)"
        assert [pc.name for pc in opt.plan.pre_compute] == ["entropy_approx"]

    def test_allowed_approximation_is_a_rewrite(self) -> None:  # T19
        d = _gate().gate(_pc_plan(), _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RewriteDecision)
        assert isinstance(d.then, ProceedDecision)
        assert d.plan.pre_compute[0].estimated_time == timedelta(seconds=1)
        with pytest.raises(PlanIdentityUnavailable):
            d.plan.question_key(schema_version="s")

    def test_no_declared_variant_rejects(self) -> None:  # T20
        d = _gate().gate(_pc_plan(variant=None), _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RejectDecision)

    def test_undeclared_time_rejects(self) -> None:
        d = _gate().gate(_pc_plan(seconds=None), _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RejectDecision) and "time estimate" in d.reason

    def test_fast_precomputation_proceeds(self) -> None:
        assert isinstance(_gate().gate(_pc_plan(seconds=2), _budget()), ProceedDecision)

    def test_undeclared_step_cost_rejects(self) -> None:
        d = _gate().gate(_pc_plan(pc_cost=None), _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RejectDecision)

    def test_variant_over_the_time_budget_rejects(self) -> None:
        d = _gate().gate(
            _pc_plan(variant=_variant(seconds=11)),
            _budget(Substitution.APPROX_ALGORITHM),
        )
        assert isinstance(d, RejectDecision)

    def test_variant_over_the_cost_budget_rejects(self) -> None:
        d = _gate().gate(
            _pc_plan(variant=_variant(cost=20_000)),
            _budget(Substitution.APPROX_ALGORITHM),
        )
        assert isinstance(d, RejectDecision)

    def test_inconsistent_costs_are_an_error(self) -> None:
        with pytest.raises(ValueError, match="cannot be priced"):
            _gate().gate(_pc_plan(pc_cost=500), _budget(Substitution.APPROX_ALGORITHM))

    def test_slow_steps_must_belong_to_the_plan(self) -> None:
        plan = _pc_plan()
        stranger = dataclasses.replace(plan.pre_compute[0])
        with pytest.raises(ValueError, match="steps of the plan"):
            DeclaredPrecomputeProvider().approximate(plan, [stranger])
        assert DeclaredPrecomputeProvider().approximate(plan, []) is None
