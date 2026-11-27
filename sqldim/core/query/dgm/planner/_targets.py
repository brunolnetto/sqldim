"""DGM Planner support types and static rule helpers (DGM v0.16 §6.2).

Threshold constants, enum types, and dataclasses used by DGMPlanner, plus
module-level helper functions extracted from DGMPlanner's @staticmethod rules.
"""

from __future__ import annotations

from sqldim.exceptions import SemanticError

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from sqldim.core.query.dgm.planner._steps import (
    ApproximateVariant,
    CostEstimate,
    PreComputation,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._deps import Dependency, RelationCatalog
    from sqldim.core.query.dgm.planner._ir import PlanIR

__all__ = [
    "QueryTarget",
    "SinkTarget",
    "PreComputation",
    "ApproximateVariant",
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


class UnsupportedSink(SemanticError, ValueError):
    """Rule 8 has no write plan for the sink: it is never defaulted (fail-closed)."""


class SinkTarget(Enum):
    """Supported write sinks (§6.3)."""

    DUCKDB = "DUCKDB"
    POSTGRESQL = "POSTGRESQL"
    MOTHERDUCK = "MOTHERDUCK"
    PARQUET = "PARQUET"
    DELTA = "DELTA"
    ICEBERG = "ICEBERG"


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
    estimated_rows:
        Estimated result size; ``None`` when unknown.  The gate (§6.3)
        paginates a SYNC plan whose size is unknown or over the limit.
    ir:
        The physical plan IR the query text was emitted from (issue #148),
        or ``None`` for a plan built from text.  When present, ``query_text``
        must equal the SQL emitted from it and ``pre_compute`` the IR's
        steps; build such plans with :meth:`from_ir`.
    """

    query_target: QueryTarget
    query_text: str
    pre_compute: list[PreComputation] = field(default_factory=list)
    sink_target: SinkTarget | None = None
    write_plan: str | None = None
    cost_estimate: CostEstimate | None = None
    alternatives: list[tuple[ExportPlan, CostEstimate]] = field(default_factory=list)
    cone_containment_applied: bool = False
    estimated_rows: int | None = None
    ir: "PlanIR | None" = None

    def __post_init__(self) -> None:
        if self.ir is not None:
            from sqldim.core.query.dgm.planner._ir_emit import emit_sql

            if self.query_text != emit_sql(self.ir):
                raise ValueError(
                    "query_text does not match the SQL emitted from the plan IR"
                )
            if tuple(self.pre_compute) != self.ir.pre_compute:
                raise ValueError("pre_compute does not match the plan IR's steps")

    @classmethod
    def from_ir(
        cls, ir: "PlanIR", query_target: QueryTarget, **kwargs: Any
    ) -> "ExportPlan":
        """Build a plan whose ``query_text`` and ``pre_compute`` come from *ir*."""
        from sqldim.core.query.dgm.planner._ir_emit import emit_sql

        if "pre_compute" in kwargs:
            raise TypeError("pre_compute comes from the IR; put the steps in ir")
        return cls(
            query_target,
            emit_sql(ir),
            pre_compute=list(ir.pre_compute),
            ir=ir,
            **kwargs,
        )

    def _identity_ir(self) -> "PlanIR":
        from sqldim.core.query.dgm.planner._ir import PlanIdentityUnavailable

        if self.ir is None:
            raise PlanIdentityUnavailable(
                "the plan was built from text and has no IR; it has no "
                "canonical identity or declared dependencies"
            )
        if tuple(self.pre_compute) != self.ir.pre_compute:
            raise PlanIdentityUnavailable(
                "pre_compute was changed after the plan was built and no "
                "longer matches the plan IR; the plan has no canonical "
                "identity or declared dependencies"
            )
        return self.ir

    def question_key(
        self, *, schema_version: str, params: Mapping[str, object] | None = None
    ) -> str:
        """``question_key`` of §8.14 for this plan's IR."""
        from sqldim.core.query.dgm.planner._ir_identity import question_key

        return question_key(
            self._identity_ir(), schema_version=schema_version, params=params
        )

    def deps(self, catalog: "RelationCatalog") -> "tuple[Dependency, ...]":
        """``deps(plan)`` of §8.14 for this plan's IR."""
        from sqldim.core.query.dgm.planner._ir_identity import deps

        return deps(self._identity_ir(), catalog)


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


def _r6_weight_constraint(ann: Any, _c: "set[str]") -> list[str]:
    if ann.is_allocative:  # type: ignore[attr-defined]
        return [
            f"WeightConstraint(ALLOCATIVE) on {ann.bridge}: PathAgg without weight → use weighted form"
        ]  # type: ignore[attr-defined]
    return []


def _r6_bridge_semantics(ann: Any, _c: "set[str]") -> list[str]:
    from sqldim.core.query.dgm.annotations import BridgeSemanticsKind

    if ann.sem is BridgeSemanticsKind.CAUSAL:  # type: ignore[attr-defined]
        return [
            f"BridgeSemantics(CAUSAL) on {ann.bridge}: drop cycle guard; dag_bfs on G and G^T"
        ]  # type: ignore[attr-defined]
    if ann.sem is BridgeSemanticsKind.SUPERSESSION:  # type: ignore[attr-defined]
        return [
            f"BridgeSemantics(SUPERSESSION) on {ann.bridge}: CASE WHEN superseded THEN -1*measure ELSE measure"
        ]  # type: ignore[attr-defined]
    return []


def _r6_degenerate(ann: Any, _c: "set[str]") -> list[str]:
    key = f"{ann.fact}.{ann.dim}" if ann.fact else ann.dim
    return [
        f"Degenerate({key}): no join — the key is read from the fact row; "
        "GroupBy and Where on it are valid"
    ]


# ---------------------------------------------------------------------------
# Rule 8 static helpers (sink-aware write planning)
# ---------------------------------------------------------------------------


def _r8_partition_flag(
    plan: str, sink_target: SinkTarget, has_temporal_agg: bool
) -> str:
    _file_sinks = (SinkTarget.PARQUET, SinkTarget.DELTA, SinkTarget.ICEBERG)
    if has_temporal_agg and sink_target in _file_sinks:
        plan += "; PARTITION_BY timestamp_unit"
    return plan


def _r8_delta_append_flag(plan: str, sink_target: SinkTarget, has_q_delta: bool) -> str:
    if has_q_delta and sink_target is SinkTarget.DELTA:
        plan += "; APPEND mode"
    return plan


def _r8_grain_append_flag(
    plan: str, sink_target: SinkTarget, grain_kind: object, GrainKind: Any
) -> str:
    if grain_kind is GrainKind.ACCUMULATING and sink_target is SinkTarget.DELTA:
        plan += "; APPEND mode"
    return plan
