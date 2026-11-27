"""Drill-across of a constellation (§5.2.2, DA1–DA4), issue #173.

The conformance suite checks the §5.2.2 example (``DA-01``) and DA1–DA3
(``DA-WF-*``); these tests pin the API and the rendering details.
"""

from __future__ import annotations

import duckdb
import pytest

from sqldim.core.query.dgm import DGMPlanner, DGMQuery, QueryTarget, VerbHop
from sqldim.core.query.dgm._drill_across import null_on_zero
from sqldim.core.query.dgm.planner import UnsupportedConstruct
from sqldim.core.query.dgm.refs import AggRef
from sqldim.core.query.dgm.preds import ScalarPred
from sqldim.exceptions import SemanticError


def _db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE customer (id VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO customer VALUES ('C1','N'), ('C2','N'), ('C3','S')")
    con.execute("CREATE TABLE sale (id VARCHAR, customer_id VARCHAR, revenue INT)")
    con.execute(
        "INSERT INTO sale VALUES ('S1','C1',100), ('S2','C1',200), "
        "('S3','C2',50), ('S4','C2',70)"
    )
    con.execute("CREATE TABLE ret (id VARCHAR, customer_id VARCHAR, amount INT)")
    con.execute(
        "INSERT INTO ret VALUES ('R1','C1',10), ('R2','C1',20), ('R3','C1',30), "
        "('R4','C3',5)"
    )
    return con


def _q() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("customer", "c")
        .path_join(VerbHop("c", "placed", "s", table="sale", on="s.customer_id = c.id"))
        .path_join(
            VerbHop("c", "returned", "r", table="ret", on="r.customer_id = c.id")
        )
        .drill_across(["s"], ["r"])
    )


def test_section_5_2_2_example():
    q = (
        _q()
        .group_by("c.id")
        .agg(
            n_sales="COUNT(s.id)",
            revenue="SUM(s.revenue)",
            n_returns="COUNT(r.id)",
            rate="COUNT(r.id) / COUNT(s.id)",
        )
    )
    assert sorted(q.execute(_db())) == [
        ("C1", 2, 300, 3, 1.5),
        ("C2", 2, 120, 0, 0.0),
        ("C3", 0, None, 1, None),
    ]


def test_without_group_by_each_branch_yields_one_row():
    q = _q().agg(revenue="SUM(s.revenue)", returned="SUM(r.amount)")
    assert q.execute(_db()) == [(420, 65)]


def test_having_applies_to_the_aligned_rows():
    q = (
        _q()
        .group_by("c.region")
        .agg(n_sales="COUNT(s.id)", n_returns="COUNT(r.id)")
        .having(ScalarPred(AggRef("n_sales"), ">", 0))
    )
    assert q.execute(_db()) == [("N", 4, 3)]


def test_where_conjunct_applies_to_its_branch_only():
    from sqldim.core.query.dgm import AND, PropRef

    q = (
        _q()
        .where(
            AND(
                ScalarPred(PropRef("s", "revenue"), ">", 60),
                ScalarPred(PropRef("c", "region"), "=", "N"),
            )
        )
        .group_by("c.id")
        .agg(n_sales="COUNT(s.id)", n_returns="COUNT(r.id)")
    )
    assert sorted(q.execute(_db())) == [("C1", 2, 3), ("C2", 1, 0)]


@pytest.mark.parametrize(
    ("build", "rule"),
    [
        (lambda q: q.agg(x="SUM(s.revenue + r.amount)"), "DA1"),
        (lambda q: q.agg(x="COUNT(*)"), "DA1"),
        (lambda q: q.group_by("s.id").agg(x="COUNT(r.id)"), "DA2"),
        (
            lambda q: q.where("s.revenue > r.amount").agg(x="COUNT(r.id)"),
            "DA3",
        ),
    ],
)
def test_ill_formed_constellations_are_rejected(build, rule):
    with pytest.raises(SemanticError, match=rule):
        build(_q()).to_sql()


@pytest.mark.parametrize(
    "branches",
    [(["s"],), (["s"], []), (["s"], ["s", "r"])],
)
def test_branches_are_two_or_more_disjoint_sets(branches):
    with pytest.raises(SemanticError, match="drill_across"):
        DGMQuery().anchor("customer", "c").drill_across(*branches)


def test_unknown_alias_is_rejected():
    q = DGMQuery().anchor("customer", "c").drill_across(["s"], ["r"])
    with pytest.raises(SemanticError, match="not aliases of the context"):
        q.agg(x="COUNT(s.id)").to_sql()


def test_anchor_inside_a_branch_is_refused():
    q = (
        DGMQuery()
        .anchor("sale", "s")
        .path_join(VerbHop("s", "by", "c", table="customer", on="s.customer_id = c.id"))
        .path_join(
            VerbHop("c", "returned", "r", table="ret", on="r.customer_id = c.id")
        )
        .drill_across(["s"], ["r"])
        .agg(x="COUNT(s.id)")
    )
    with pytest.raises(UnsupportedConstruct, match="anchor"):
        q.to_sql()


def test_plan_ir_refuses_a_constellation_after_the_da_checks():
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]
    with pytest.raises(UnsupportedConstruct, match="drill-across"):
        planner.plan_query(_q().group_by("c.id").agg(n="COUNT(s.id)"))
    bad = _q().agg(x="SUM(s.revenue + r.amount)")
    with pytest.raises(SemanticError, match="DA1"):
        planner.plan_query(bad)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("a / b", "a / NULLIF(b, 0)"),
        ("x/COALESCE(b1.a0, 0)", "x/ NULLIF(COALESCE(b1.a0, 0), 0)"),
        ("(a + 1) / (b - 1) * 2", "(a + 1) / NULLIF((b - 1), 0) * 2"),
        ("a / b / c", "a / NULLIF(b, 0) / NULLIF(c, 0)"),
        ("a + b", "a + b"),
    ],
)
def test_null_on_zero(sql, expected):
    assert null_on_zero(sql) == expected
