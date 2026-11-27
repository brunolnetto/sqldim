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
