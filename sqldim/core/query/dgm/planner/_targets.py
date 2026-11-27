"""DGM Planner support types and static rule helpers (DGM v0.16 §6.2).

Threshold constants, enum types, and dataclasses used by DGMPlanner, plus
module-level helper functions extracted from DGMPlanner's @staticmethod rules.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._ir import PlanIR
    from sqldim.core.query.dgm.planner._ir_identity import (
        Dependency,
        RelationCatalog,
    )

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


def _check_duration(value: timedelta | None, what: str) -> None:
    if value is None:
        return
    if not isinstance(value, timedelta) or value < timedelta(0):
        raise ValueError(f"{what} must be a non-negative timedelta, got {value!r}")


def _check_reads(value: "frozenset[str] | None") -> None:
    if value is None:
        return
    if not isinstance(value, frozenset) or not all(isinstance(r, str) for r in value):
        raise ValueError(f"reads must be a frozenset of relation names, got {value!r}")
    from sqldim.core.query.dgm.planner._ir import check_relation_name

    for r in value:
        check_relation_name(r)


@dataclass(frozen=True)
class ApproximateVariant:
    """The declared approximate variant of a PreComputation (§6.3 Step 1b).

    Used only under ``Substitution.APPROX_ALGORITHM`` or when the caller picks
    it from a ``CLARIFY`` option.  Every field is required: an approximation
    whose time, cost or accuracy is not stated is not an approximation the
    gate can offer.

    Parameters
    ----------
    name, query, kind:
        The replacement PreComputation step.
    estimated_time:
        Estimated duration of the approximate step.
    cost_estimate:
        Cost of the approximate step, in the units of the plan's cost.
    accuracy:
        The accuracy statement of the approximate algorithm (§6.3, e.g.
        ``"Monte Carlo approximation (k=200 samples)"``); it becomes the
        warning of the REWRITE or CLARIFY option.
    reads:
        Relations the approximate step reads, for ``deps(plan)`` (§8.14);
        ``None`` when undeclared, and then the approximated plan has no
        ``deps``.  It is never copied from the exact step.
    """

    name: str
    query: str
    estimated_time: timedelta
    cost_estimate: "CostEstimate"
    accuracy: str
    kind: str = "sql"
    reads: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if self.estimated_time is None:
            raise ValueError("an approximate variant needs an estimated_time")
        _check_duration(self.estimated_time, "estimated_time")
        _check_reads(self.reads)
        if not isinstance(self.accuracy, str) or not self.accuracy.strip():
            raise ValueError("an approximate variant needs an accuracy statement")


@dataclass(frozen=True)
class PreComputation:
    """A named pre-computation step emitted before the main query.

    A step is part of the plan IR (``PlanIR.pre_compute``): its ``name``,
    ``kind``, ``query``, ``reads`` and ``accuracy`` enter the plan's canonical
    identity, so an exact plan and its approximation never share a key.  Its
    estimates and declared approximation do not: they describe the cost of
    the question, not the question.

    Parameters
    ----------
    name:
        Identifier for the pre-computation (e.g. ``"gt_bfs"``).
    query:
        SQL or Python snippet that materialises the result.
    kind:
        ``"sql"`` (default) or ``"py"`` (Python pre-computation).
    estimated_time:
        Estimated duration; ``None`` when unknown.  The gate (§6.3 Step 1b)
        never starts a PreComputation whose time is unknown.
    cost_estimate:
        Cost of this step, as included in the plan's cost; needed to price
        the plan with the step replaced by its approximation.
    approximation:
        The declared approximate variant, if the algorithm has one.
    reads:
        Relations the step reads, for ``deps(plan)`` (§8.14); ``None`` when
        undeclared, and then the plan has no ``deps``.
    accuracy:
        ``None`` for an exact step; for a step that replaced an exact one by
        its approximate variant, the variant's accuracy statement.
    """

    name: str
    query: str
    kind: str = "sql"
    estimated_time: timedelta | None = None
    cost_estimate: "CostEstimate | None" = None
    approximation: ApproximateVariant | None = None
    reads: frozenset[str] | None = None
    accuracy: str | None = None

    def __post_init__(self) -> None:
        _check_duration(self.estimated_time, "estimated_time")
        _check_reads(self.reads)


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
    return [f"Degenerate({ann.dim}): exclusion from GroupBy candidates"]  # type: ignore[attr-defined]


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
