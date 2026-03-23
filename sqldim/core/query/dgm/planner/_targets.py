"""DGM Planner support types and static rule helpers (DGM v0.16 §6.2).

Threshold constants, enum types, and dataclasses used by DGMPlanner, plus
module-level helper functions extracted from DGMPlanner's @staticmethod rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.bdd import DGMPredicateBDD
    from sqldim.core.query.dgm.graph import GraphStatistics, NodeExpr, SubgraphExpr
    from sqldim.core.query.dgm.annotations import AnnotationSigma, GrainKind

__all__ = [
    "QueryTarget",
    "SinkTarget",
    "PreComputation",
    "CostEstimate",
    "ExportPlan",
    "SMALL",
    "CLOSURE_THRESHOLD",
    "SMALL_GRAPH_THRESHOLD",
    "DENSE",
]

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

#: Path-cardinality threshold.  Bound→Bound paths ≤ SMALL use recursive CTE.
SMALL: int = 100

#: Node-count above which hierarchy execution uses a pre-materialised closure.
CLOSURE_THRESHOLD: int = 1_000

#: Node-count below which SubgraphExpr algorithms are emitted inline.
SMALL_GRAPH_THRESHOLD: int = 500

#: Temporal-density above which TemporalAgg is pre-aggregated daily.
DENSE: float = 0.8


# ---------------------------------------------------------------------------
# QueryTarget
# ---------------------------------------------------------------------------


class QueryTarget(Enum):
    """Supported output query targets (§6.3)."""

    SQL_DUCKDB = "SQL_DUCKDB"
    SQL_POSTGRESQL = "SQL_POSTGRESQL"
    SQL_MOTHERDUCK = "SQL_MOTHERDUCK"
    CYPHER = "CYPHER"
    SPARQL = "SPARQL"
    DGM_JSON = "DGM_JSON"
    DGM_YAML = "DGM_YAML"


# ---------------------------------------------------------------------------
# SinkTarget
# ---------------------------------------------------------------------------


class SinkTarget(Enum):
    """Supported write sinks (§6.3)."""

    DUCKDB = "DUCKDB"
    POSTGRESQL = "POSTGRESQL"
    MOTHERDUCK = "MOTHERDUCK"
    PARQUET = "PARQUET"
    DELTA = "DELTA"
    ICEBERG = "ICEBERG"


# ---------------------------------------------------------------------------
# PreComputation
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class PreComputation:
    """A named pre-computation step emitted before the main query.

    Parameters
    ----------
    name:
        Identifier for the pre-computation (e.g. ``"gt_bfs"``).
    query:
        SQL or Python snippet that materialises the result.
    kind:
        ``"sql"`` (default) or ``"py"`` (Python pre-computation).
    """

    name: str
    query: str
    kind: str = "sql"


# ---------------------------------------------------------------------------
# CostEstimate
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class CostEstimate:
    """Mechanical cost estimate for an ExportPlan.

    Parameters
    ----------
    cpu_ops:
        Estimated CPU operations (dimensionless).
    io_ops:
        Estimated I/O operations (dimensionless).
    note:
        Optional auditable note (no natural language).
    """

    cpu_ops: int
    io_ops: int
    note: str = ""


# ---------------------------------------------------------------------------
# ExportPlan
# ---------------------------------------------------------------------------


@dataclass
class ExportPlan:
    """Structured, auditable query plan produced by DGMPlanner.

    Parameters
    ----------
    query_target:
        Target query language / engine.
    query_text:
        The compiled query string.
    pre_compute:
        Ordered list of pre-computation steps.
    sink_target:
        Optional write sink.
    write_plan:
        Optional write-plan string (CREATE TABLE AS, COPY TO, …).
    cost_estimate:
        Optional mechanical cost estimate.
    alternatives:
        Alternative ``(ExportPlan, CostEstimate)`` pairs for audit.
    cone_containment_applied:
        True when Rule 9 fired and collapsed REACHABLE_FROM ∩ REACHABLE_TO.
    """

    query_target: QueryTarget
    query_text: str
    pre_compute: list[PreComputation] = field(default_factory=list)
    sink_target: SinkTarget | None = None
    write_plan: str | None = None
    cost_estimate: CostEstimate | None = None
    alternatives: list[tuple[ExportPlan, CostEstimate]] = field(default_factory=list)
    cone_containment_applied: bool = False


# ---------------------------------------------------------------------------
# Rule 6 static helpers (annotation-driven optimisations)
# ---------------------------------------------------------------------------


def _r6_role_playing(ann: object, _c: "set[str]") -> list[str]:
    return [f"RolePlaying({ann.dim}): single scan under multiple aliases {ann.roles}"]  # type: ignore[attr-defined]


def _r6_projects_from(ann: object, c: "set[str]") -> list[str]:
    if ann.dim_full in c:  # type: ignore[attr-defined]
        return [
            f"ProjectsFrom: eliminate mini join {ann.dim_mini!r} — full table {ann.dim_full!r} present"  # type: ignore[attr-defined]
        ]
    return []


def _r6_derived_fact(ann: object, c: "set[str]") -> list[str]:
    if c.intersection(ann.sources):  # type: ignore[attr-defined]
        return [f"DerivedFact({ann.fact}): inline — srcs ∩ C ≠ ∅"]  # type: ignore[attr-defined]
    return []


def _r6_weight_constraint(ann: object, _c: "set[str]") -> list[str]:
    if ann.is_allocative:  # type: ignore[attr-defined]
        return [f"WeightConstraint(ALLOCATIVE) on {ann.bridge}: PathAgg without weight → use weighted form"]  # type: ignore[attr-defined]
    return []


def _r6_bridge_semantics(ann: object, _c: "set[str]") -> list[str]:
    from sqldim.core.query.dgm.annotations import BridgeSemanticsKind

    if ann.sem is BridgeSemanticsKind.CAUSAL:  # type: ignore[attr-defined]
        return [f"BridgeSemantics(CAUSAL) on {ann.bridge}: drop cycle guard; dag_bfs on G and G^T"]  # type: ignore[attr-defined]
    if ann.sem is BridgeSemanticsKind.SUPERSESSION:  # type: ignore[attr-defined]
        return [f"BridgeSemantics(SUPERSESSION) on {ann.bridge}: CASE WHEN superseded THEN -1*measure ELSE measure"]  # type: ignore[attr-defined]
    return []


def _r6_degenerate(ann: object, _c: "set[str]") -> list[str]:
    return [f"Degenerate({ann.dim}): exclusion from GroupBy candidates"]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Rule 8 static helpers (sink-aware write planning)
# ---------------------------------------------------------------------------


def _r8_partition_flag(plan: str, sink_target: SinkTarget, has_temporal_agg: bool) -> str:
    _file_sinks = (SinkTarget.PARQUET, SinkTarget.DELTA, SinkTarget.ICEBERG)
    if has_temporal_agg and sink_target in _file_sinks:
        plan += "; PARTITION_BY timestamp_unit"
    return plan


def _r8_delta_append_flag(plan: str, sink_target: SinkTarget, has_q_delta: bool) -> str:
    if has_q_delta and sink_target is SinkTarget.DELTA:
        plan += "; APPEND mode"
    return plan


def _r8_grain_append_flag(plan: str, sink_target: SinkTarget, grain_kind: object, GrainKind: type) -> str:
    if grain_kind is GrainKind.ACCUMULATING and sink_target is SinkTarget.DELTA:
        plan += "; APPEND mode"
    return plan


# ---------------------------------------------------------------------------
# Rule 10 static helpers (PipelineArtifact state-aware write planning)
# ---------------------------------------------------------------------------


def _r10_ttl_check(completed_at_s: float, now_s: float, ttl_s: int) -> bool:
    """Return True when the artifact has exceeded its TTL (Complete → Stale)."""
    return (now_s - completed_at_s) > ttl_s


def _r10_adaptive_write_plan(state: object) -> str:
    """Return ``"APPEND"`` or ``"MERGE"`` based on P(f).state for ADAPTIVE mode."""
    from sqldim.core.query.dgm.annotations import PipelineStateKind

    if state in (PipelineStateKind.MISSING, PipelineStateKind.FAILED):
        return "APPEND"
    if state is PipelineStateKind.STALE:
        return "MERGE"
    return "APPEND"


def _r10_backfill_predicate(fact_alias: str, backfill_horizon_days: int) -> str:
    """Return a SQL-fragment predicate for the backfill gap filter (injected by Rule 10)."""
    return (
        f"{fact_alias}.state IN ('Missing', 'Failed')"
        f" AND {fact_alias}.window_end < (NOW() - INTERVAL {backfill_horizon_days} DAY)"
    )
