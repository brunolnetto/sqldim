"""Write planning per sink: planner Rules 8 and 10 (§6.2).

``dgm-write-plan`` asks ``DGMPlanner.apply_rule_8`` and ``apply_rule_10`` the
questions of :mod:`tests.conformance.cases.export`.  Rule 8 returns a
descriptive plan string (``"COPY TO (FORMAT PARQUET); PARTITION_BY …"``), not
SQL; the runner reads the statement form and the flags from it.  Rule 10
returns a typed :class:`ArtifactPlan` whose ``write.operation`` is read.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from sqldim.core.query.dgm import DGMPlanner, DGMQuery
from sqldim.core.query.dgm.annotations import PipelineArtifact, WriteModeKind
from sqldim.core.query.dgm.planner import QueryTarget, SinkTarget
from sqldim.core.query.dgm.planner._artifact_plan import ArtifactSnapshot, ArtifactState
from sqldim.core.query.dgm.planner._rule10 import ArtifactTarget
from sqldim.exceptions import SemanticError
from tests.conformance.cases import Case
from tests.conformance.runners import REJECTED, Backend

W0, W1, NOW = datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 2, 1)
_SCAN = re.compile(r"\b\w+_scan\b")


def _planner(sink: SinkTarget | None = None) -> DGMPlanner:
    return DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB, sink)  # type: ignore[arg-type]


def _rule8(sink: str, *, temporal_agg: bool = False, q_delta: bool = False) -> str:
    return _planner().apply_rule_8(SinkTarget[sink], temporal_agg, q_delta, None)


def _target(i: Mapping[str, Any]) -> str:
    return "SCAN" if _SCAN.search(_rule8(i["sink"])) else "WRITABLE"


def _partition(i: Mapping[str, Any]) -> bool:
    return "PARTITION" in _rule8(i["sink"], temporal_agg=i["temporal_agg"])


def _append(i: Mapping[str, Any]) -> bool:
    return "APPEND" in _rule8(i["sink"], q_delta=i["q_delta"])


def _iceberg_catalog(i: Mapping[str, Any]) -> str:
    # SinkTarget.ICEBERG carries no catalog: the plan is asked for as is
    try:
        _rule8(i["sink"])
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
    try:
        plan = _planner(SinkTarget[i["sink"]]).apply_rule_10(
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
