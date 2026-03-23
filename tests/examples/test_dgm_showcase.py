"""
Comprehensive tests for sqldim/application/examples/features/dgm/showcase.py

Coverage targets
----------------
Every demo function in the DGM showcase is exercised individually so that
errors surface with a focused failure message.  The suite also validates
result cardinalities and key properties against the known static fixture
(3 customers, 3 products, 2 segments, 6 sales, 1 bridge table with 2 rows).
"""
from __future__ import annotations

import duckdb
import pytest

from sqldim.application.datasets.domains.dgm.sources.dgm import DGMShowcaseSource
from sqldim.application.examples.features.dgm.showcase import (
    demo_annotation_sigma,
    demo_b1_b2_having,
    demo_b1_b3_qualify,
    demo_b1_filter,
    demo_b1_not,
    demo_b1_path_pred,
    demo_bdd_predicate,
    demo_bridge_path,
    demo_correlate,
    demo_cse,
    demo_edge_kind_classification,
    demo_full_pipeline,
    demo_planner,
    demo_question_algebra,
    run_all,
)


@pytest.fixture(scope="module")
def con():
    """Shared in-memory DuckDB connection populated with the DGM fixture."""
    conn = duckdb.connect()
    DGMShowcaseSource().setup(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

class TestEdgeKindClassification:
    def test_runs_without_error(self, capsys):
        demo_edge_kind_classification()

    def test_prints_edge_kinds(self, capsys):
        demo_edge_kind_classification()
        out = capsys.readouterr().out
        assert "kind=" in out
        # Two edge kinds exist: verb and bridge
        assert "verb" in out.lower() or "bridge" in out.lower()


# ---------------------------------------------------------------------------
# B1 — Context filter
# ---------------------------------------------------------------------------

class TestB1Filter:
    def test_runs_without_error(self, con, capsys):
        demo_b1_filter(con)

    def test_retail_customer_count(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, VerbHop
        hop_c = VerbHop(
            "s", "placed_by", "c",
            table="dgm_showcase_customer", on="c.id = s.customer_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop_c)
            .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
            .execute(con)
        )
        # Alice (3 sales) + Carol (2 sales) = 5 retail sales
        assert len(rows) == 5

    def test_wholesale_customer_count(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, VerbHop
        hop_c = VerbHop(
            "s", "placed_by", "c",
            table="dgm_showcase_customer", on="c.id = s.customer_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop_c)
            .where(ScalarPred(PropRef("c", "segment"), "=", "wholesale"))
            .execute(con)
        )
        # Bob (1 sale)
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# B1 — PathPred EXISTS
# ---------------------------------------------------------------------------

class TestB1PathPred:
    def test_runs_without_error(self, con, capsys):
        demo_b1_path_pred(con)

    def test_electronics_sales_count(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, VerbHop, PathPred
        hop = VerbHop(
            "s", "includes", "d",
            table="dgm_showcase_product", on="d.id = s.customer_id",
        )
        pp = PathPred(
            anchor="s",
            path=hop,
            sub_filter=ScalarPred(PropRef("d", "category"), "=", "electronics"),
        )
        # Independent verification: product_id=1 is electronics; sales 1,4,5 reference it
        rows = con.execute(
            "SELECT * FROM dgm_showcase_sale WHERE product_id = 1"
        ).fetchall()
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# B1 — NOT predicate
# ---------------------------------------------------------------------------

class TestB1Not:
    def test_runs_without_error(self, con, capsys):
        demo_b1_not(con)

    def test_exclude_clearance_result_count(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, VerbHop, NOT
        hop_d = VerbHop(
            "s", "includes", "d",
            table="dgm_showcase_product", on="d.id = s.product_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop_d)
            .where(NOT(ScalarPred(PropRef("d", "category"), "=", "clearance")))
            .execute(con)
        )
        # sale 2 uses product_id=2 (clearance) → excluded; 5 rows remain
        assert len(rows) == 5


# ---------------------------------------------------------------------------
# B1 ∘ B2 — Aggregation + HAVING
# ---------------------------------------------------------------------------

class TestB1B2Having:
    def test_runs_without_error(self, con, capsys):
        demo_b1_b2_having(con)

    def test_having_revenue_gt_4000(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, AggRef, VerbHop
        hop = VerbHop(
            "s", "placed_by", "c",
            table="dgm_showcase_customer", on="c.id = s.customer_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop)
            .group_by("c.id", "c.email")
            .agg(total_rev="SUM(s.revenue)", sale_cnt="COUNT(*)")
            .having(ScalarPred(AggRef("total_rev"), ">", 4000))
            .execute(con)
        )
        # Alice has 1500+200+3500=5200 > 4000; Bob has exactly 4000 (not >)
        assert len(rows) == 1
        assert rows[0][1] == "alice@x"

    def test_having_revenue_gte_4000_includes_bob(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, AggRef, VerbHop
        hop = VerbHop(
            "s", "placed_by", "c",
            table="dgm_showcase_customer", on="c.id = s.customer_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop)
            .group_by("c.id", "c.email")
            .agg(total_rev="SUM(s.revenue)")
            .having(ScalarPred(AggRef("total_rev"), ">=", 4000))
            .execute(con)
        )
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# B1 ∘ B3 — Window ranking (QUALIFY)
# ---------------------------------------------------------------------------

class TestB1B3Qualify:
    def test_runs_without_error(self, con, capsys):
        demo_b1_b3_qualify(con)

    def test_one_top_sale_per_customer(self, con):
        from sqldim import DGMQuery, ScalarPred, WinRef
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .window(
                rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)"
            )
            .qualify(ScalarPred(WinRef("rn"), "=", 1))
            .execute(con)
        )
        # One row per customer → 3 customers = 3 rows
        assert len(rows) == 3

    def test_top_revenue_per_customer_values(self, con):
        rows = con.execute("""
            SELECT customer_id, MAX(revenue) as top_rev
            FROM dgm_showcase_sale
            GROUP BY customer_id
            ORDER BY customer_id
        """).fetchall()
        # Alice(id=1): max is 3500, Bob(id=2): 4000, Carol(id=3): 2000
        assert rows[0] == (1, 3500.0)
        assert rows[1] == (2, 4000.0)
        assert rows[2] == (3, 2000.0)


# ---------------------------------------------------------------------------
# B1 ∘ B2 ∘ B3 — Full pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_runs_without_error(self, con, capsys):
        demo_full_pipeline(con)

    def test_alice_wins_retail_rank(self, con):
        from sqldim import DGMQuery, ScalarPred, PropRef, AggRef, WinRef, VerbHop
        hop = VerbHop(
            "s", "placed_by", "c",
            table="dgm_showcase_customer", on="c.id = s.customer_id",
        )
        rows = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .path_join(hop)
            .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
            .group_by("c.id", "c.email")
            .agg(total_rev="SUM(s.revenue)")
            .having(ScalarPred(AggRef("total_rev"), ">", 1000))
            .window(rnk="RANK() OVER (ORDER BY SUM(s.revenue) DESC)")
            .qualify(ScalarPred(WinRef("rnk"), "=", 1))
            .execute(con)
        )
        assert len(rows) == 1
        assert rows[0][1] == "alice@x"


# ---------------------------------------------------------------------------
# Bridge path traversal
# ---------------------------------------------------------------------------

class TestBridgePath:
    def test_runs_without_error(self, con, capsys):
        demo_bridge_path(con)

    def test_bridge_data_integrity(self, con):
        # Verify the bridge table has correct rows
        rows = con.execute(
            "SELECT product_id, segment_id FROM dgm_showcase_prod_seg ORDER BY id"
        ).fetchall()
        assert rows == [(1, 1), (3, 2)]


# ---------------------------------------------------------------------------
# BDD predicate compilation
# ---------------------------------------------------------------------------

class TestBDDPredicate:
    def test_runs_without_error(self, capsys):
        demo_bdd_predicate()

    def test_bdd_and_is_satisfiable(self, capsys):
        from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
        from sqldim import ScalarPred, PropRef, AND
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p1 = ScalarPred(PropRef("s", "revenue"), ">", 1000)
        p2 = ScalarPred(PropRef("s", "sale_year"), "=", 2024)
        uid = bdd.compile(AND(p1, p2))
        assert bdd.is_satisfiable(uid)

    def test_bdd_implication_one_way(self, capsys):
        from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
        from sqldim import ScalarPred, PropRef, AND
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p1 = ScalarPred(PropRef("s", "revenue"), ">", 1000)
        p2 = ScalarPred(PropRef("s", "sale_year"), "=", 2024)
        uid_and = bdd.compile(AND(p1, p2))
        uid_p1 = bdd.compile(p1)
        assert bdd.implies(uid_and, uid_p1)     # (p1 AND p2) ⊨ p1
        assert not bdd.implies(uid_p1, uid_and) # p1 ⊭ (p1 AND p2)

    def test_bdd_to_sql_is_non_empty(self, capsys):
        from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
        from sqldim import ScalarPred, PropRef, AND
        mgr = BDDManager()
        bdd = DGMPredicateBDD(mgr)
        p1 = ScalarPred(PropRef("s", "revenue"), ">", 1000)
        p2 = ScalarPred(PropRef("s", "sale_year"), "=", 2024)
        uid = bdd.compile(AND(p1, p2))
        sql = bdd.to_sql(uid)
        # BDD renders a canonical SQL fragment (uses abstract var names)
        assert isinstance(sql, str) and len(sql) > 0


# ---------------------------------------------------------------------------
# Schema annotation layer (Σ) + recommender
# ---------------------------------------------------------------------------

class TestAnnotationSigma:
    def test_runs_without_error(self, capsys):
        demo_annotation_sigma()

    def test_sigma_annotations_loaded(self):
        from sqldim.core.query.dgm.annotations import (
            AnnotationSigma, Grain, GrainKind, SCDType, SCDKind,
            Conformed, BridgeSemantics, BridgeSemanticsKind,
        )
        sigma = AnnotationSigma(
            annotations=[
                Grain(fact="dgm_showcase_sale", grain=GrainKind.PERIOD),
                SCDType(dim="dgm_showcase_customer", scd=SCDKind.SCD2),
                Conformed(dim="dgm_showcase_customer", fact_types=frozenset({"Sale"})),
                BridgeSemantics(bridge="dgm_showcase_prod_seg", sem=BridgeSemanticsKind.STRUCTURAL),
            ]
        )
        assert len(sigma) == 4

    def test_sigma_scd_lookup(self):
        from sqldim.core.query.dgm.annotations import (
            AnnotationSigma, SCDType, SCDKind,
        )
        from sqldim.core.query.dgm.annotations import SCDKind
        sigma = AnnotationSigma(
            annotations=[SCDType(dim="dgm_showcase_customer", scd=SCDKind.SCD2)]
        )
        assert sigma.scd_of("dgm_showcase_customer") == SCDKind.SCD2

    def test_sigma_conformed_check(self):
        from sqldim.core.query.dgm.annotations import AnnotationSigma, Conformed
        sigma = AnnotationSigma(
            annotations=[Conformed(dim="dgm_showcase_customer", fact_types=frozenset({"Sale"}))]
        )
        assert sigma.is_conformed("dgm_showcase_customer", "Sale")
        assert not sigma.is_conformed("dgm_showcase_customer", "Unknown")

    def test_recommender_produces_suggestions(self):
        from sqldim.core.query.dgm.annotations import (
            AnnotationSigma, Grain, GrainKind, SCDType, SCDKind,
            Conformed, BridgeSemantics, BridgeSemanticsKind,
        )
        from sqldim.core.query.dgm.recommender import DGMRecommender
        sigma = AnnotationSigma(
            annotations=[
                Grain(fact="dgm_showcase_sale", grain=GrainKind.PERIOD),
                SCDType(dim="dgm_showcase_customer", scd=SCDKind.SCD2),
                Conformed(dim="dgm_showcase_customer", fact_types=frozenset({"Sale"})),
                BridgeSemantics(bridge="dgm_showcase_prod_seg", sem=BridgeSemanticsKind.STRUCTURAL),
            ]
        )
        rec = DGMRecommender(sigma)
        suggestions = rec.run_annotation_rules()
        assert isinstance(suggestions, list)

    def test_recommender_routing(self):
        from sqldim.core.query.dgm.annotations import AnnotationSigma
        from sqldim.core.query.dgm.recommender import DGMRecommender
        sigma = AnnotationSigma(annotations=[])
        rec = DGMRecommender(sigma)
        high = rec.route(entropy=0.9)
        low = rec.route(entropy=0.1)
        assert high != low


# ---------------------------------------------------------------------------
# DGM planner + exporters
# ---------------------------------------------------------------------------

class TestPlanner:
    def test_runs_without_error(self, capsys):
        demo_planner()

    def test_plan_targets(self):
        from sqldim.core.query.dgm.annotations import AnnotationSigma
        from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
        from sqldim.core.query.dgm.graph import GraphStatistics
        from sqldim import DGMQuery, ScalarPred, PropRef
        sigma = AnnotationSigma(annotations=[])
        stats = GraphStatistics(node_count=3, edge_count=6)
        planner = DGMPlanner(
            cost_model=None,
            statistics=stats,
            annotations=sigma,
            rules=None,
            query_target=QueryTarget.SQL_DUCKDB,
            sink_target=SinkTarget.DUCKDB,
        )
        sql = (
            DGMQuery()
            .anchor("dgm_showcase_sale", "s")
            .where(ScalarPred(PropRef("s", "sale_year"), "=", 2024))
            .to_sql()
        )
        plan = planner.build_plan(sql)
        assert plan.query_target == QueryTarget.SQL_DUCKDB

    def test_json_exporter_produces_output(self):
        from sqldim.core.query.dgm.annotations import AnnotationSigma
        from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
        from sqldim.core.query.dgm.graph import GraphStatistics
        from sqldim.core.query.dgm.exporters import DGMJSONExporter
        from sqldim import DGMQuery, ScalarPred, PropRef
        planner = DGMPlanner(
            cost_model=None,
            statistics=GraphStatistics(node_count=2, edge_count=4),
            annotations=AnnotationSigma(annotations=[]),
            rules=None,
            query_target=QueryTarget.SQL_DUCKDB,
            sink_target=SinkTarget.DUCKDB,
        )
        sql = DGMQuery().anchor("dgm_showcase_sale", "s").to_sql()
        plan = planner.build_plan(sql)
        out = DGMJSONExporter().export(plan)
        assert len(out) > 0

    def test_yaml_exporter_produces_output(self):
        from sqldim.core.query.dgm.annotations import AnnotationSigma
        from sqldim.core.query.dgm.planner import DGMPlanner, QueryTarget, SinkTarget
        from sqldim.core.query.dgm.graph import GraphStatistics
        from sqldim.core.query.dgm.exporters import DGMYAMLExporter
        from sqldim import DGMQuery
        planner = DGMPlanner(
            cost_model=None,
            statistics=GraphStatistics(node_count=2, edge_count=4),
            annotations=AnnotationSigma(annotations=[]),
            rules=None,
            query_target=QueryTarget.SQL_DUCKDB,
            sink_target=SinkTarget.DUCKDB,
        )
        plan = planner.build_plan(DGMQuery().anchor("dgm_showcase_sale", "s").to_sql())
        out = DGMYAMLExporter().export(plan)
        assert len(out) > 0


# ---------------------------------------------------------------------------
# §8.13 QuestionAlgebra — Example 7
# ---------------------------------------------------------------------------

class TestDemoQuestionAlgebra:
    def test_runs_without_error(self, capsys):
        demo_question_algebra()

    def test_registers_composed_ctes(self, capsys):
        demo_question_algebra()
        out = capsys.readouterr().out
        assert "q_all_segments" in out
        assert "q_cross" in out

    def test_union_sql_emitted(self, capsys):
        demo_question_algebra()
        out = capsys.readouterr().out
        assert "UNION ALL" in out

    def test_join_sql_emitted(self, capsys):
        demo_question_algebra()
        out = capsys.readouterr().out
        assert "JOIN" in out

    def test_with_clause_emitted(self, capsys):
        demo_question_algebra()
        out = capsys.readouterr().out
        assert "WITH" in out

    def test_total_cte_count_printed(self, capsys):
        demo_question_algebra()
        out = capsys.readouterr().out
        assert "6" in out  # 3 leaf + 3 composed

    def test_to_sql_programmatic(self):
        """Verify to_sql() output directly, independent of print output."""
        from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
        from sqldim import DGMQuery, ScalarPred, PropRef
        qa = QuestionAlgebra()
        qa.add("q1", DGMQuery().anchor("t", "x").where(
            ScalarPred(PropRef("x", "a"), "=", 1)))
        qa.add("q2", DGMQuery().anchor("t", "x").where(
            ScalarPred(PropRef("x", "a"), "=", 2)))
        qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        sql = qa.to_sql("q_union")
        assert sql.startswith("WITH")
        assert "UNION ALL" in sql
        assert "SELECT * FROM q_union" in sql


# ---------------------------------------------------------------------------
# §6.2 Rule 11 CSE — Example 8
# ---------------------------------------------------------------------------

class TestDemoCSE:
    def test_runs_without_error(self, capsys):
        demo_cse()

    def test_detects_shared_predicate(self, capsys):
        demo_cse()
        out = capsys.readouterr().out
        assert "Shared predicate groups detected: 1" in out

    def test_cse_cte_name_in_output(self, capsys):
        demo_cse()
        out = capsys.readouterr().out
        assert "__cse_" in out

    def test_original_not_mutated(self, capsys):
        demo_cse()
        out = capsys.readouterr().out
        assert "Original algebra not mutated" in out

    def test_cse_precedes_sharing_ctes(self, capsys):
        demo_cse()
        out = capsys.readouterr().out
        # Optimised order line: __cse_... appears before us_sales
        opt_line = next(l for l in out.splitlines() if "Optimised CTE order" in l)
        assert opt_line.index("__cse_") < opt_line.index("us_sales")

    def test_programmatic_cse_detection(self):
        """Verify find_shared_predicates + apply_cse programmatically."""
        from sqldim.core.query.dgm.algebra import QuestionAlgebra
        from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
        from sqldim.core.query.dgm._cse import find_shared_predicates, apply_cse
        from sqldim import DGMQuery, ScalarPred, PropRef

        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        qa = QuestionAlgebra()
        qa.add("a", DGMQuery().anchor("t", "f").where(pred))
        qa.add("b", DGMQuery().anchor("t", "f").where(pred))
        bdd = DGMPredicateBDD(BDDManager())
        shared = find_shared_predicates(qa, bdd)
        assert len(shared) == 1
        new_qa = apply_cse(qa, bdd)
        cse_names = [n for n in new_qa.names if n.startswith("__cse_")]
        assert len(cse_names) == 1
        assert new_qa.names.index(cse_names[0]) < new_qa.names.index("a")


# ---------------------------------------------------------------------------
# §7.2 CORRELATE — Example 9
# ---------------------------------------------------------------------------

class TestDemoCorrelate:
    def test_runs_without_error(self, capsys):
        demo_correlate()

    def test_three_suggestions_for_three_shared_anchor_ctes(self, capsys):
        demo_correlate()
        out = capsys.readouterr().out
        assert "CORRELATE suggestions (3 total)" in out

    def test_suggestions_band_algebra(self, capsys):
        demo_correlate()
        out = capsys.readouterr().out
        assert "[algebra]" in out

    def test_o_events_not_suggested(self, capsys):
        demo_correlate()
        out = capsys.readouterr().out
        # Suggestion lines start with [algebra]; clickevts / o_events anchor
        # should not appear on any suggestion line.
        suggestion_lines = [l for l in out.splitlines() if "[algebra]" in l]
        assert all("o_events" not in line for line in suggestion_lines)

    def test_all_three_pairs_present(self, capsys):
        demo_correlate()
        out = capsys.readouterr().out
        assert "us_sales" in out and "eu_sales" in out and "apac_sales" in out

    def test_programmatic_suggest_correlations(self):
        """Verify suggest_correlations() returns a list of Suggestion objects."""
        from sqldim.core.query.dgm.algebra import QuestionAlgebra
        from sqldim.core.query.dgm.annotations import AnnotationSigma
        from sqldim.core.query.dgm.recommender import DGMRecommender, SuggestionKind
        from sqldim import DGMQuery

        qa = QuestionAlgebra()
        qa.add("p", DGMQuery().anchor("fact_t", "f"))
        qa.add("q", DGMQuery().anchor("fact_t", "f"))
        rec = DGMRecommender(AnnotationSigma([]))
        suggestions = rec.suggest_correlations(qa)
        assert len(suggestions) == 1
        assert suggestions[0].kind is SuggestionKind.CORRELATE
        assert suggestions[0].band == "algebra"
        assert "fact_t" in suggestions[0].text


# ---------------------------------------------------------------------------
# run_all smoke test
# ---------------------------------------------------------------------------

class TestRunAll:
    def test_run_all_completes(self, capsys):
        run_all()
        out = capsys.readouterr().out
        assert "DGM showcase complete" in out
