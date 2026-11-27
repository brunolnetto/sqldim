"""Write planning per sink: planner Rules 8 and 10 (§6.2).

``dgm-write-plan`` asks ``DGMPlanner.apply_rule_8`` and ``apply_rule_10`` the
questions of :mod:`tests.conformance.cases.export`.  Rule 8 returns a typed
:class:`SinkWritePlan` (#178) whose ``target``, ``partition_by_time_unit``
and ``append_only`` are read; Rule 10 returns a typed :class:`ArtifactPlan`
whose ``write.operation`` is read.  A planning error is :data:`REJECTED`.

An ICEBERG plan needs an attached catalog (§6.2).  The questions about its
target form and partitioning are asked of a planner with one,
:data:`CATALOG`; ``iceberg_catalog`` asks with the case's ``catalog`` and
``merge`` with the case's ``engine`` and ``merge_on_read``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from sqldim.core.query.dgm import DGMPlanner, DGMQuery
from sqldim.core.query.dgm.annotations import PipelineArtifact, WriteModeKind
from sqldim.core.query.dgm.planner import (
    IcebergCatalog,
    QueryTarget,
    SinkTarget,
    SinkWritePlan,
)
from sqldim.core.query.dgm.planner._artifact_plan import ArtifactSnapshot, ArtifactState
from sqldim.core.query.dgm.planner._rule10 import ArtifactTarget
from sqldim.exceptions import SemanticError
from tests.conformance.cases import Case
from tests.conformance.runners import REJECTED, Backend

W0, W1, NOW = datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 2, 1)
_SCAN = re.compile(r"\b\w+_scan\b", re.I)
#: The attached catalog of the ICEBERG questions that are not about it.
CATALOG = IcebergCatalog("iceberg_catalog", "2.0", merge_on_read=True)


def _planner(
    sink: SinkTarget | None = None, catalog: IcebergCatalog | None = None
) -> DGMPlanner:
    return DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB, sink,  # type: ignore[arg-type]
                      iceberg_catalog=catalog)  # fmt: skip


def _rule8(
    sink: str,
    *,
    temporal_agg: bool = False,
    q_delta: bool = False,
    catalog: IcebergCatalog | None = CATALOG,
) -> SinkWritePlan:
    target = SinkTarget[sink]
    planner = _planner(catalog=catalog if target is SinkTarget.ICEBERG else None)
    return planner.apply_rule_8(target, temporal_agg, q_delta, None)


def _target(i: Mapping[str, Any]) -> str:
    plan = _rule8(i["sink"])
    named = (plan.target, plan.statement.value)
    return "SCAN" if any(_SCAN.search(t) for t in named) else "WRITABLE"


def _partition(i: Mapping[str, Any]) -> bool:
    return _rule8(i["sink"], temporal_agg=i["temporal_agg"]).partition_by_time_unit


def _append(i: Mapping[str, Any]) -> bool:
    return _rule8(i["sink"], q_delta=i["q_delta"]).append_only


def _catalog_of(i: Mapping[str, Any]) -> IcebergCatalog | None:
    name = i.get("catalog")
    return None if name is None else IcebergCatalog(name, CATALOG.engine_version)


def _iceberg_catalog(i: Mapping[str, Any]) -> str:
    try:
        _rule8(i["sink"], catalog=_catalog_of(i))
    except SemanticError:
        return REJECTED
    return "ACCEPTED"


def _merge(i: Mapping[str, Any]) -> str:
    """REFRESH from Stale: Rule 10 plans a MERGE (§2.4 write_mode)."""
    artifact = PipelineArtifact(
        "Window", "daily", 3600, 90, WriteModeKind.REFRESH, transitions="T"
    )
    snap = ArtifactSnapshot(
        "Window", ("2024-01-01",), ArtifactState.STALE, "t", True, (W0, W1), W1
    )
    query = _planner().plan_query(DGMQuery().anchor("events", "e"))
    sink = SinkTarget[i["sink"]]
    catalog = None
    if sink is SinkTarget.ICEBERG:
        catalog = IcebergCatalog(CATALOG.name, i["engine"], i["merge_on_read"])
    try:
        plan = _planner(sink, catalog).apply_rule_10(
            artifact, snap, now=NOW, target=ArtifactTarget("daily", "ts"), query=query
        )
    except SemanticError:
        return REJECTED
    return plan.write.operation.name  # type: ignore[union-attr]


_QUESTIONS: dict[str, Callable[[Mapping[str, Any]], Any]] = {
    "target": _target,
    "partition": _partition,
    "append": _append,
    "iceberg_catalog": _iceberg_catalog,
    "merge": _merge,
}


def _run(case: Case) -> Any:
    return _QUESTIONS[case.input["question"]](case.input)


BACKEND = Backend(
    "dgm-write-plan",
    "DGMPlanner.apply_rule_8 / apply_rule_10: the write plan per sink",
    frozenset({"write_plan"}),
    _run,
)
