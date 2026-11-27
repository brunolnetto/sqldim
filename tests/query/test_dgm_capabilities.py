"""Declared capabilities per target and sink, and the fail-closed gate (#138)."""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm import (
    CypherExporter,
    DGMPlanner,
    DGMQuery,
    ExportPlan,
    SPARQLExporter,
)
from sqldim.core.query.dgm.planner import (
    QUERY_PROFILES,
    SINK_PROFILES,
    Capability,
    QueryTarget,
    SinkTarget,
    Support,
    UnsupportedCapability,
    query_profile,
    sink_profile,
)
from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct


def _plan(target: QueryTarget) -> ExportPlan:
    planner = DGMPlanner(None, None, None, None, target)  # type: ignore[arg-type]
    return planner.plan_query(DGMQuery().anchor("events", "e"))


def test_every_target_and_sink_is_declared() -> None:
    assert set(QUERY_PROFILES) == set(QueryTarget)
    assert set(SINK_PROFILES) == set(SinkTarget)


@pytest.mark.parametrize(
    "target", [t for t, p in QUERY_PROFILES.items() if p.emitter is None], ids=str
)
def test_a_target_without_emitter_is_refused(target: QueryTarget) -> None:
    with pytest.raises(UnsupportedCapability, match="no emitter"):
        _plan(target)


@pytest.mark.parametrize(
    "target", [t for t, p in QUERY_PROFILES.items() if p.emitter], ids=str
)
def test_a_target_with_emitter_plans(target: QueryTarget) -> None:
    assert _plan(target).query_target is target


def test_unsupported_capability_is_an_unsupported_construct() -> None:
    assert issubclass(UnsupportedCapability, UnsupportedConstruct)


def test_require_raises_only_for_unsupported() -> None:
    delta = sink_profile(SinkTarget.DELTA)
    with pytest.raises(UnsupportedCapability, match="merge"):
        delta.require(Capability.MERGE)
    assert delta.require(Capability.APPEND).support is Support.PARTIAL
    duck = query_profile(QueryTarget.SQL_DUCKDB)
    assert duck.require(Capability.WINDOW).support is Support.SUPPORTED


def test_an_undeclared_capability_is_unsupported() -> None:
    with pytest.raises(UnsupportedCapability, match="not declared"):
        query_profile(QueryTarget.SQL_DUCKDB).require(Capability.MERGE)


def test_unknown_target_is_refused() -> None:
    with pytest.raises(UnsupportedCapability):
        query_profile("SQL_ORACLE")  # type: ignore[arg-type]
    with pytest.raises(UnsupportedCapability):
        sink_profile("HUDI")  # type: ignore[arg-type]


@pytest.mark.parametrize("exporter", [CypherExporter(), SPARQLExporter()])
def test_exporters_do_not_relabel_another_targets_text(exporter: object) -> None:
    plan = _plan(QueryTarget.SQL_DUCKDB)
    with pytest.raises(UnsupportedCapability, match="SQL_DUCKDB"):
        exporter.export(plan)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Window functions per execution profile (#178)
# ---------------------------------------------------------------------------


def _window_query(expr: str) -> DGMQuery:
    return DGMQuery().anchor("sale", "s").window(w=expr)


@pytest.mark.parametrize(
    ("expr", "cap"),
    [
        ("ROW_NUMBER() OVER (ORDER BY s.id)", Capability.WINDOW),
        ("dense_rank() OVER (ORDER BY s.id)", Capability.WINDOW),
        ("SUM(s.revenue) OVER (PARTITION BY s.customer)", Capability.WINDOW_AGGREGATE),
        ("LAG(s.revenue) OVER (ORDER BY s.id)", Capability.WINDOW_AGGREGATE),
        ("s.revenue", Capability.WINDOW_AGGREGATE),  # not recognised: fail-closed
    ],
)
def test_window_capability(expr: str, cap: Capability) -> None:
    from sqldim.core.query.dgm.planner._capabilities import window_capability

    assert window_capability(expr) is cap


def test_pg_dd_refuses_an_aggregate_window_before_execution() -> None:
    from sqldim.core.query.dgm.planner import POSTGRESQL_VIA_DUCKDB

    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB,  # type: ignore[arg-type]
                         execution_profile=POSTGRESQL_VIA_DUCKDB)  # fmt: skip
    q = _window_query("SUM(s.revenue) OVER (PARTITION BY s.customer)")
    with pytest.raises(UnsupportedCapability, match="window_aggregate"):
        planner.plan_query(q)
    ok = _window_query("ROW_NUMBER() OVER (PARTITION BY s.customer ORDER BY s.id)")
    assert "ROW_NUMBER" in planner.plan_query(ok).query_text


def test_duckdb_plans_an_aggregate_window() -> None:
    planner = DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]
    q = _window_query("SUM(s.revenue) OVER (PARTITION BY s.customer)")
    assert "SUM(s.revenue)" in planner.plan_query(q).query_text
