"""DGMQuery construction rules, fail-closed rendering, ``over`` and SCD resolution.

Issue #173: the conformance suite (``tests/conformance``) checks the values;
these tests pin the API each fix exposes.
"""

from __future__ import annotations

import duckdb
import pytest

from sqldim.core.query.dgm import (
    ALL,
    K_SHORTEST,
    MIN_WEIGHT,
    SHORTEST,
    BridgeHop,
    DGMQuery,
    PathPred,
    PropRef,
    Quantifier,
    ScalarPred,
    SinceMode,
    VerbHop,
)
from sqldim.core.query.dgm._entities import entity_aggregate
from sqldim.core.query.dgm.planner import UnsupportedConstruct
from sqldim.exceptions import SemanticError

HOP = VerbHop("c", "placed", "s", table="sale", on="s.customer_id = c.id")
PHI = ScalarPred(PropRef("s", "revenue"), ">", 1)


# ---------------------------------------------------------------------------
# §4.1 NULL is not a literal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", ["=", "!=", "<>", "<", ">="])
def test_comparison_with_null_rejected(op):
    with pytest.raises(SemanticError, match="NULL is not a literal"):
        ScalarPred(PropRef("c", "id"), op, None)


@pytest.mark.parametrize("op", ["IS", "IS NOT"])
def test_null_test_accepted(op):
    assert ScalarPred(PropRef("c", "id"), op, None).to_sql() == f"c.id {op} NULL"


# ---------------------------------------------------------------------------
# §3.5 / §4.1 construction rules of PathPred
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", [None, ALL(), K_SHORTEST(2)])
def test_promote_rejected_unless_single_instance_strategy(strategy):
    with pytest.raises(SemanticError, match="promote"):
        PathPred("c", HOP, PHI, strategy=strategy, promote=True)


@pytest.mark.parametrize("strategy", [SHORTEST(), MIN_WEIGHT()])
def test_promote_accepted_for_shortest_and_min_weight(strategy):
    assert PathPred("c", HOP, PHI, strategy=strategy, promote=True).promote


def test_forall_with_since_rejected():
    bridge = BridgeHop("c", "next", "s", table="sale", on="s.prev_id = c.id")
    with pytest.raises(SemanticError, match="FORALL with ONCE or SINCE"):
        PathPred(
            "c",
            bridge,
            PHI,
            quantifier=Quantifier.FORALL,
            temporal_mode=SinceMode(PHI),
        )


def test_alias_clash_with_context_rejected():
    clash = VerbHop("c", "placed", "c", table="sale", on="c.customer_id = c.id")
    q = DGMQuery().anchor("customer", "c").where(PathPred("c", clash, PHI))
    with pytest.raises(SemanticError, match="C ∩ L"):
        q.to_sql()


# ---------------------------------------------------------------------------
# to_sql() fails closed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", [SHORTEST(), K_SHORTEST(2), MIN_WEIGHT()])
def test_strategy_other_than_all_refused(strategy):
    q = (
        DGMQuery()
        .anchor("customer", "c")
        .where(PathPred("c", HOP, PHI, strategy=strategy))
    )
    with pytest.raises(UnsupportedConstruct, match="strategy"):
        q.to_sql()


def test_temporal_context_refused():
    q = DGMQuery().anchor("customer", "c").temporal_context("2024-01-01")
    with pytest.raises(UnsupportedConstruct, match="TemporalContext"):
        q.to_sql()


# ---------------------------------------------------------------------------
# §4.2 over = ENTITIES (default) | PATHS
# ---------------------------------------------------------------------------


def _fan_out_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE sale (id VARCHAR, revenue INTEGER)")
    con.execute("INSERT INTO sale VALUES ('S1', 100), ('S2', NULL)")
    con.execute("CREATE TABLE link (sale_id VARCHAR, campaign VARCHAR, store VARCHAR)")
    con.execute(
        "INSERT INTO link VALUES ('S1', 'A', 'T'), ('S1', 'B', 'T'), ('S2', 'A', 'T')"
    )
    return con


def _fan_out_query() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("sale", "s")
        .path_join(VerbHop("s", "via", "p", table="link", on="p.sale_id = s.id"))
        .group_by("p.store")
    )


def test_entities_is_the_default():
    q = _fan_out_query().agg(
        total="SUM(s.revenue)", n="COUNT(s.id)", a="AVG(s.revenue)"
    )
    assert q.execute(_fan_out_db()) == [("T", 100, 2, 100.0)]


def test_paths_must_be_asked_for():
    q = _fan_out_query().agg(total="SUM(s.revenue)", n="COUNT(s.id)", over="PATHS")
    assert q.execute(_fan_out_db()) == [("T", 200, 3)]


def test_plan_ir_renders_the_same_aggregates():
    from sqldim.core.query.dgm import DGMPlanner, QueryTarget

    q = _fan_out_query().agg(total="SUM(s.revenue)")
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]
    assert planner.plan_query(q).query_text == q.to_sql()


def test_unknown_over_rejected():
    with pytest.raises(SemanticError, match="ENTITIES or PATHS"):
        DGMQuery().anchor("sale", "s").agg(over="TUPLES", n="COUNT(s.id)")


@pytest.mark.parametrize(
    "expr",
    ["COUNT(*)", "MIN(s.revenue)", "MAX(s.revenue)", "COUNT(DISTINCT s.id)", "SUM(1)"],
)
def test_aggregates_equal_under_both_units_are_kept(expr):
    assert entity_aggregate(expr, ["s", "p"]) == expr


def test_filter_has_no_entities_rendering():
    with pytest.raises(UnsupportedConstruct, match="FILTER or OVER"):
        entity_aggregate("SUM(s.revenue) FILTER (WHERE s.revenue > 0)", ["s"])


def test_no_path_join_leaves_sql_unchanged():
    q = DGMQuery().anchor("sale", "s").group_by("s.id").agg(total="SUM(s.revenue)")
    assert "SUM(s.revenue) AS total" in q.to_sql()


# ---------------------------------------------------------------------------
# §2.5 SCD resolution of join_dim
# ---------------------------------------------------------------------------


def _scd_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dim_customer (vk INTEGER, id VARCHAR, region VARCHAR, "
        "valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)"
    )
    con.execute(
        "INSERT INTO dim_customer VALUES "
        "(101, 'C42', 'South', '2023-01-01', '2024-03-01', FALSE), "
        "(102, 'C42', 'North', '2024-03-01', NULL, TRUE)"
    )
    con.execute(
        "CREATE TABLE fact_sale (id VARCHAR, ck VARCHAR, vk INTEGER, "
        "occurred_on TIMESTAMP, revenue INTEGER)"
    )
    con.execute(
        "INSERT INTO fact_sale VALUES ('S1', 'C42', 101, '2023-06-10', 600), "
        "('S2', 'C42', 102, '2024-05-02', 400)"
    )
    return con


def _by_region(**join) -> list:
    q = (
        DGMQuery("fact_sale")
        .join_dim("dim_customer", join.pop("fk", "ck"), "id", **join)
        .by("d_dim_customer.region")
        .sum("f.revenue")
        .as_of("2024-06-30")
    )
    return sorted(q.execute(_scd_db()))


def test_as_was_entity_key_reads_the_version_at_event_time():
    assert _by_region(event_time="occurred_on") == [("North", 400), ("South", 600)]


def test_as_is_entity_key_reads_the_version_at_tau():
    assert _by_region(mode="AS_IS") == [("North", 1000)]


def test_as_was_version_key_reads_the_stored_version():
    assert _by_region(fk="vk", version_key="vk") == [("North", 400), ("South", 600)]


def test_as_is_version_key_maps_to_the_entity_at_tau():
    assert _by_region(fk="vk", version_key="vk", mode="AS_IS") == [("North", 1000)]


def test_as_was_over_entity_key_needs_event_time():
    with pytest.raises(SemanticError, match="event_time"):
        DGMQuery("fact_sale").join_dim("dim_customer", "ck")


def test_unknown_mode_rejected():
    with pytest.raises(SemanticError, match="AS_WAS or AS_IS"):
        DGMQuery("fact_sale").join_dim("dim_customer", "ck", mode="LATEST")
