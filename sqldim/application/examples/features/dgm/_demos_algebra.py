"""DGM showcase — algebra demos (QuestionAlgebra, CSE, CORRELATE)."""

from __future__ import annotations

from sqldim import DGMQuery, PropRef, ScalarPred
from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
from sqldim.core.query.dgm._cse import find_shared_predicates, apply_cse
from sqldim.application.examples.features.dgm._demos_dql import _section


def demo_question_algebra() -> None:
    """Example 7 — §8.13 QuestionAlgebra: compose multiple DGMQueries into a
    single WITH … SELECT statement."""
    _section("Example 7 — §8.13 QuestionAlgebra: CTE composition")

    q_retail = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "segment"), "=", "retail"))
    )
    q_wholesale = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "segment"), "=", "wholesale"))
    )
    q_highval = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "revenue"), ">", 500))
    )

    qa = (
        QuestionAlgebra()
        .add("q_retail", q_retail)
        .add("q_wholesale", q_wholesale)
        .add("q_highval", q_highval)
    )

    qa.compose("q_retail", ComposeOp.UNION, "q_wholesale", name="q_all_segments")
    print(f"  Registered CTEs: {qa.names}")

    qa.compose(
        "q_wholesale", ComposeOp.INTERSECT, "q_highval", name="q_wholesale_highval"
    )
    qa.compose(
        "q_retail", ComposeOp.JOIN, "q_highval", name="q_cross", on="l.id = r.id"
    )

    sql_union = qa.to_sql("q_all_segments")
    print(f"\n  UNION query ({sql_union.count(chr(10)) + 1} lines):\n")
    for line in sql_union.splitlines():
        print(f"    {line}")

    sql_cross = qa.to_sql("q_cross")
    print(f"\n  JOIN query ({sql_cross.count(chr(10)) + 1} lines):\n")
    for line in sql_cross.splitlines():
        print(f"    {line}")

    print(f"\n  Total named CTEs in algebra: {len(qa)}")
    print("  Example 7 complete.")


def _print_cse_ctesql(optimised_qa: QuestionAlgebra) -> None:
    cse_ctes = [n for n in optimised_qa.names if n.startswith("__cse_")]
    print(f"  Injected CSE CTEs  : {cse_ctes}")
    for cse_name in cse_ctes:
        print(f"\n  {cse_name} SQL:")
        for line in optimised_qa[cse_name].to_cte_sql().splitlines():
            print(f"    {line}")


def _print_cse_results(
    qa: QuestionAlgebra,
    optimised_qa: QuestionAlgebra,
    shared: dict,
) -> None:
    print(f"  Shared predicate groups detected: {len(shared)}")
    for bdd_id, names in shared.items():
        print(f"    BDD node {bdd_id}: {names}")
    print(f"\n  Original CTE order : {qa.names}")
    print(f"  Optimised CTE order: {optimised_qa.names}")
    _print_cse_ctesql(optimised_qa)


def demo_cse() -> None:
    """Example 8 — §6.2 Rule 11: Cross-CTE Common Sub-expression Elimination."""
    _section("Example 8 — §6.2 Rule 11: Cross-CTE CSE")

    shared_pred = ScalarPred(PropRef("f", "region"), "=", "US")
    q1 = DGMQuery().anchor("o_fact", "f").where(shared_pred)
    q2 = DGMQuery().anchor("o_fact", "f").where(shared_pred)
    q3 = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "revenue"), ">", 200))
    )

    qa = QuestionAlgebra()
    qa.add("us_sales", q1)
    qa.add("us_sales_copy", q2)
    qa.add("highval_sales", q3)

    bdd = DGMPredicateBDD(BDDManager())
    shared = find_shared_predicates(qa, bdd)
    optimised_qa = apply_cse(qa, bdd)

    _print_cse_results(qa, optimised_qa, shared)

    assert qa.names == ["us_sales", "us_sales_copy", "highval_sales"]
    print("\n  Original algebra not mutated. Example 8 complete.")


def demo_correlate() -> None:
    """Example 9 — §7.2 CORRELATE: cross-question JOIN recommendations."""
    from sqldim.core.query.dgm.annotations import AnnotationSigma
    from sqldim.core.query.dgm.recommender import DGMRecommender

    _section("Example 9 — §7.2 CORRELATE recommendations")

    q_us = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "region"), "=", "US"))
    )
    q_eu = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "region"), "=", "EU"))
    )
    q_apac = (
        DGMQuery()
        .anchor("o_fact", "f")
        .where(ScalarPred(PropRef("f", "region"), "=", "APAC"))
    )
    q_events = (
        DGMQuery()
        .anchor("o_events", "e")
        .where(ScalarPred(PropRef("e", "type"), "=", "click"))
    )

    qa = (
        QuestionAlgebra()
        .add("us_sales", q_us)
        .add("eu_sales", q_eu)
        .add("apac_sales", q_apac)
        .add("clickevts", q_events)
    )

    rec = DGMRecommender(AnnotationSigma([]))
    suggestions = rec.suggest_correlations(qa)

    print(f"  CTEs in algebra: {qa.names}")
    print(f"  CORRELATE suggestions ({len(suggestions)} total):\n")
    for s in suggestions:
        print(f"    [{s.band}] priority={s.priority}  {s.text}")

    assert len(suggestions) == 3, f"Expected 3 suggestions, got {len(suggestions)}"
    print("\n  All 3 o_fact pairs surfaced; o_events not correlated with o_fact.")
    print("  Example 9 complete.")
