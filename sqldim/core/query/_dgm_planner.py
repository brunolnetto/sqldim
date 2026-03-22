"""DGM Query Planner (DGM v0.16 §6.2).

Implements planning rules 1a–9, wrapping each rule as a method on DGMPlanner.
Each rule returns either an ExportPlan, a list of strings (advisory messages),
a bool, or a str depending on the rule contract defined in the spec.

Constants
---------
SMALL              Path-cardinality threshold for Bound→Bound recursive CTE.
CLOSURE_THRESHOLD  Node-count above which a hierarchy uses a materialised
                   closure table instead of an inline recursive CTE.
SMALL_GRAPH_THRESHOLD
                   Node-count below which SubgraphExpr algorithms are emitted
                   inline; above → schedule PreComputation.
DENSE              Temporal-density threshold above which a TemporalAgg is
                   pre-aggregated to a daily summary (Rule 7).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqldim.core.query._dgm_bdd import DGMPredicateBDD
    from sqldim.core.query._dgm_graph import GraphStatistics, NodeExpr, SubgraphExpr
    from sqldim.core.query._dgm_annotations import AnnotationSigma, GrainKind

__all__ = [
    "QueryTarget",
    "SinkTarget",
    "PreComputation",
    "CostEstimate",
    "ExportPlan",
    "DGMPlanner",
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
# DGMPlanner
# ---------------------------------------------------------------------------


class DGMPlanner:
    """Query planner implementing planning rules 1a–9 (DGM §6.2).

    Parameters
    ----------
    cost_model:
        Pluggable cost model (may be None for unit tests).
    statistics:
        Graph-level statistics used by cost-sensitive rules.
    annotations:
        Schema annotation set Σ.
    rules:
        Explicit rule list override (None → apply all rules).
    query_target:
        Target query language / engine.
    sink_target:
        Optional write sink.
    """

    def __init__(
        self,
        cost_model: Any,
        statistics: "GraphStatistics",
        annotations: "AnnotationSigma",
        rules: Any,
        query_target: QueryTarget,
        sink_target: SinkTarget | None = None,
    ) -> None:
        self.cost_model = cost_model
        self.statistics = statistics
        self.annotations = annotations
        self.rules = rules
        self.query_target = query_target
        self.sink_target = sink_target

    # ------------------------------------------------------------------
    # Rule 1a — Path execution strategy
    # ------------------------------------------------------------------

    def apply_rule_1a(
        self,
        endpoint_case: str,
        strategy: str,
        path_card: int,
    ) -> ExportPlan:
        """Rule 1a: Map endpoint case × strategy to a concrete execution plan.

        Parameters
        ----------
        endpoint_case:
            One of ``"BB"``, ``"BF"``, ``"FB"``, ``"FF"``.
        strategy:
            One of ``"ALL"``, ``"SHORTEST"``, ``"MIN_WEIGHT"``, ``"CAUSAL"``.
        path_card:
            Estimated path cardinality for Bound→Bound decisions.
        """
        qt = self.query_target
        if endpoint_case == "BB":
            return self._rule_1a_bb(strategy, path_card, qt)
        if endpoint_case == "BF":
            return self._rule_1a_bf(strategy, qt)
        if endpoint_case == "FB":
            return self._rule_1a_fb(strategy, qt)
        # FF
        return ExportPlan(
            query_target=qt,
            query_text="WCC: weakly_connected_component from anchor",
        )

    def _rule_1a_bb(self, strategy: str, path_card: int, qt: QueryTarget) -> ExportPlan:
        if strategy == "CAUSAL":
            return ExportPlan(qt, "dag_bfs: no_cycle_guard; DAG BFS on G")
        if strategy in ("SHORTEST", "MIN_WEIGHT"):
            return ExportPlan(qt, "recursive_cte ORDER BY cost LIMIT 1")
        if path_card <= SMALL:
            return ExportPlan(qt, "recursive_cte with cycle_prevention")
        return ExportPlan(qt, "pre_materialise path_table")

    def _rule_1a_bf(self, strategy: str, qt: QueryTarget) -> ExportPlan:
        if strategy == "CAUSAL":
            return ExportPlan(qt, "dag_bfs: forward DAG BFS from A (no visited-set)")
        if strategy == "SHORTEST":
            return ExportPlan(qt, "bfs: first_visit per node from A")
        return ExportPlan(qt, "forward BFS/DFS from A on G")

    def _rule_1a_fb(self, strategy: str, qt: QueryTarget) -> ExportPlan:
        if strategy == "CAUSAL":
            return ExportPlan(
                qt,
                "reverse_topological bfs on transposed G^T from B",
            )
        return ExportPlan(
            qt,
            "forward bfs from B on transposed G^T adjacency",
        )

    # ------------------------------------------------------------------
    # Rule 1b — Grain-aware aggregation
    # ------------------------------------------------------------------

    def apply_rule_1b(self, grain: "GrainKind", fact: str) -> list[str]:
        """Rule 1b: Return advisory messages for grain-aware aggregation."""
        from sqldim.core.query._dgm_annotations import GrainKind

        if grain is GrainKind.PERIOD:
            return [
                f"reject SUM — Grain(PERIOD) on {fact}; SUM invalid across periods",
                f"suggest LAST instead of SUM on Grain(PERIOD) {fact}",
                f"suggest Q_delta for period-over-period diff on {fact}",
            ]
        if grain is GrainKind.ACCUMULATING:
            return [
                f"warn: cross-row agg on ACCUMULATING {fact}; add stage predicates",
                f"warn: NULL possible — add COALESCE on ACCUMULATING {fact}",
            ]
        return []

    # ------------------------------------------------------------------
    # Rule 1c — SCD resolution
    # ------------------------------------------------------------------

    def apply_rule_1c(self, scd_kind: "SCDKind") -> str:
        """Rule 1c: Return the SCD resolution strategy string."""
        from sqldim.core.query._dgm_annotations import SCDKind

        _map = {
            SCDKind.SCD1: "scd1: strip TemporalJoin predicate",
            SCDKind.SCD2: "scd2: standard effective_from/to join",
            SCDKind.SCD3: "scd3: PropRef(d.current_value) / PropRef(d.previous_value)",
            SCDKind.SCD6: "scd6: lateral join — SCD2 for versioned_attrs; direct for others",
        }
        return _map.get(scd_kind, f"unknown SCD kind: {scd_kind!r}")

    # ------------------------------------------------------------------
    # Rule 2 — Floyd-Warshall threshold
    # ------------------------------------------------------------------

    def apply_rule_2(
        self,
        pair_count: int,
        avg_path_len: float,
        node_count: int,
    ) -> str:
        """Rule 2: Decide Floyd-Warshall vs per-pair CTE.

        Pre-compute Floyd-Warshall when
        ``pair_count × avg_path_len > node_count²``.
        """
        if pair_count * avg_path_len > node_count ** 2:
            return "precompute Floyd-Warshall distance matrix"
        return "per_pair CTE with LIMIT 1"

    # ------------------------------------------------------------------
    # Rule 3 — GraphExpr scheduling
    # ------------------------------------------------------------------

    def apply_rule_3(self, expr: "NodeExpr | SubgraphExpr") -> str:
        """Rule 3: Schedule or inline a GraphExpr algorithm."""
        from sqldim.core.query._dgm_graph import NodeExpr, SubgraphExpr

        if isinstance(expr, NodeExpr):
            return self._rule3_node_expr(expr)
        return self._rule3_subgraph_expr(expr)

    def _rule3_node_expr(self, expr: "NodeExpr") -> str:
        from sqldim.core.query._dgm_graph import (
            OUTGOING_SIGNATURES,
            INCOMING_SIGNATURES,
            DOMINANT_OUTGOING_SIGNATURE,
            DOMINANT_INCOMING_SIGNATURE,
            SIGNATURE_DIVERSITY,
        )

        alg = expr.algorithm
        if isinstance(alg, (OUTGOING_SIGNATURES, DOMINANT_OUTGOING_SIGNATURE, SIGNATURE_DIVERSITY)):
            return "forward_bfs per anchor alias; pre-compute if stable"
        if isinstance(alg, (INCOMING_SIGNATURES, DOMINANT_INCOMING_SIGNATURE)):
            return "bfs on transposed G^T per anchor alias; pre-compute if stable"
        return f"inline NodeExpr({type(alg).__name__})"

    def _rule3_subgraph_expr(self, expr: "SubgraphExpr") -> str:
        from sqldim.core.query._dgm_graph import (
            GLOBAL_SIGNATURE_COUNT,
            GLOBAL_DOMINANT_SIGNATURE,
            SIGNATURE_ENTROPY,
        )

        alg = expr.algorithm
        node_count = self.statistics.node_count if self.statistics else 0
        if isinstance(alg, (GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE)):
            return "broadcast graph-level scalar inline to all tuples"
        if isinstance(alg, SIGNATURE_ENTROPY):
            if node_count >= SMALL_GRAPH_THRESHOLD:
                return "precompute SIGNATURE_ENTROPY: schedule PreComputation"
            return "inline SIGNATURE_ENTROPY: p(s) distribution; -Σ p log₂ p"
        return f"inline SubgraphExpr({type(alg).__name__})"

    # ------------------------------------------------------------------
    # Rule 4 — Band reordering (BDD implies)
    # ------------------------------------------------------------------

    def apply_rule_4(
        self,
        bdd: "DGMPredicateBDD",
        where_bdd: int,
        having_bdd: int,
    ) -> bool:
        """Rule 4: Return True if where_bdd ⊢ having_bdd (remove HAVING pred)."""
        return bdd.implies(where_bdd, having_bdd)

    # ------------------------------------------------------------------
    # Rule 5 — Hierarchy execution
    # ------------------------------------------------------------------

    def apply_rule_5(self, depth: object, node_count: int) -> str:
        """Rule 5: Choose hierarchy execution strategy.

        Parameters
        ----------
        depth:
            Integer depth or the ``RAGGED`` sentinel.
        node_count:
            Number of nodes in the hierarchy.
        """
        from sqldim.core.query._dgm_annotations import _Ragged

        is_ragged = isinstance(depth, _Ragged)
        if not is_ragged and isinstance(depth, int) and depth <= 4:
            return f"unroll: fixed_depth join chain (depth={depth})"
        if node_count > CLOSURE_THRESHOLD:
            return "closure_table: pre-materialised closure table"
        return "recursive_cte: depth-limited recursive CTE"

    # ------------------------------------------------------------------
    # Rule 6 — Annotation-driven optimisations
    # ------------------------------------------------------------------

    def apply_rule_6(
        self,
        ann: object,
        candidate_set: "set[str] | None" = None,
    ) -> list[str]:
        """Rule 6: Return optimisation messages for *ann* (§6.2)."""
        from sqldim.core.query._dgm_annotations import (
            RolePlaying, ProjectsFrom, DerivedFact,
            WeightConstraint, BridgeSemantics, Degenerate,
        )

        c = candidate_set or set()
        _dispatch = {
            RolePlaying: self._r6_role_playing,
            ProjectsFrom: self._r6_projects_from,
            DerivedFact: self._r6_derived_fact,
            WeightConstraint: self._r6_weight_constraint,
            BridgeSemantics: self._r6_bridge_semantics,
            Degenerate: self._r6_degenerate,
        }
        handler = _dispatch.get(type(ann))
        if handler is None:
            return []
        return handler(ann, c)

    @staticmethod
    def _r6_role_playing(ann: object, _c: "set[str]") -> list[str]:
        return [f"RolePlaying({ann.dim}): single scan under multiple aliases {ann.roles}"]  # type: ignore[attr-defined]

    @staticmethod
    def _r6_projects_from(ann: object, c: "set[str]") -> list[str]:
        if ann.dim_full in c:  # type: ignore[attr-defined]
            return [
                f"ProjectsFrom: eliminate mini join {ann.dim_mini!r} — full table {ann.dim_full!r} present"  # type: ignore[attr-defined]
            ]
        return []

    @staticmethod
    def _r6_derived_fact(ann: object, c: "set[str]") -> list[str]:
        if c.intersection(ann.sources):  # type: ignore[attr-defined]
            return [f"DerivedFact({ann.fact}): inline — srcs ∩ C ≠ ∅"]  # type: ignore[attr-defined]
        return []

    @staticmethod
    def _r6_weight_constraint(ann: object, _c: "set[str]") -> list[str]:
        if ann.is_allocative:  # type: ignore[attr-defined]
            return [f"WeightConstraint(ALLOCATIVE) on {ann.bridge}: PathAgg without weight → use weighted form"]  # type: ignore[attr-defined]
        return []

    @staticmethod
    def _r6_bridge_semantics(ann: object, _c: "set[str]") -> list[str]:
        from sqldim.core.query._dgm_annotations import BridgeSemanticsKind

        if ann.sem is BridgeSemanticsKind.CAUSAL:  # type: ignore[attr-defined]
            return [f"BridgeSemantics(CAUSAL) on {ann.bridge}: drop cycle guard; dag_bfs on G and G^T"]  # type: ignore[attr-defined]
        if ann.sem is BridgeSemanticsKind.SUPERSESSION:  # type: ignore[attr-defined]
            return [f"BridgeSemantics(SUPERSESSION) on {ann.bridge}: CASE WHEN superseded THEN -1*measure ELSE measure"]  # type: ignore[attr-defined]
        return []

    @staticmethod
    def _r6_degenerate(ann: object, _c: "set[str]") -> list[str]:
        return [f"Degenerate({ann.dim}): exclusion from GroupBy candidates"]  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Rule 7 — TemporalAgg window scheduling
    # ------------------------------------------------------------------

    def apply_rule_7(self, density: float, window_type: str) -> str:
        """Rule 7: Choose TemporalAgg scheduling strategy."""
        if density > DENSE:
            return "pre_aggregate to daily summary (density above DENSE threshold)"
        if window_type == "ROLLING":
            return "recursive window_frame for ROLLING window"
        return f"date_filter + aggfn for {window_type} window"

    # ------------------------------------------------------------------
    # Rule 8 — Sink-aware write planning
    # ------------------------------------------------------------------

    def apply_rule_8(
        self,
        sink_target: SinkTarget,
        has_temporal_agg: bool,
        has_q_delta: bool,
        grain_kind: "GrainKind | None",
    ) -> str:
        """Rule 8: Return the write plan string for *sink_target*."""
        from sqldim.core.query._dgm_annotations import GrainKind

        _base: dict[SinkTarget, str] = {
            SinkTarget.DUCKDB: "CREATE TABLE AS / INSERT INTO",
            SinkTarget.MOTHERDUCK: "CREATE TABLE AS / INSERT INTO",
            SinkTarget.POSTGRESQL: "ATTACH ... AS pg; CREATE TABLE / INSERT INTO via COPY",
            SinkTarget.PARQUET: "COPY TO (FORMAT PARQUET)",
            SinkTarget.DELTA: "CREATE OR REPLACE / INSERT INTO delta_scan",
            SinkTarget.ICEBERG: "INSERT INTO iceberg_scan",
        }
        plan = _base.get(sink_target, "INSERT INTO")
        plan = self._r8_partition_flag(plan, sink_target, has_temporal_agg)
        plan = self._r8_delta_append_flag(plan, sink_target, has_q_delta)
        if grain_kind is not None:
            plan = self._r8_grain_append_flag(plan, sink_target, grain_kind, GrainKind)
        return plan

    @staticmethod
    def _r8_partition_flag(plan: str, sink_target: SinkTarget, has_temporal_agg: bool) -> str:
        _file_sinks = (SinkTarget.PARQUET, SinkTarget.DELTA, SinkTarget.ICEBERG)
        if has_temporal_agg and sink_target in _file_sinks:
            plan += "; PARTITION_BY timestamp_unit"
        return plan

    @staticmethod
    def _r8_delta_append_flag(plan: str, sink_target: SinkTarget, has_q_delta: bool) -> str:
        if has_q_delta and sink_target is SinkTarget.DELTA:
            plan += "; APPEND mode"
        return plan

    @staticmethod
    def _r8_grain_append_flag(plan: str, sink_target: SinkTarget, grain_kind: object, GrainKind: type) -> str:
        if grain_kind is GrainKind.ACCUMULATING and sink_target is SinkTarget.DELTA:
            plan += "; APPEND mode"
        return plan

    # ------------------------------------------------------------------
    # Rule 9 — Cone containment optimisation
    # ------------------------------------------------------------------

    def apply_rule_9(
        self,
        has_reachable_from: bool,
        source_alias: "str | None",
        has_reachable_to: bool,
        target_alias: "str | None",
    ) -> bool:
        """Rule 9: Return True when both REACHABLE_FROM and REACHABLE_TO are
        present on the same Join branch — caller should call collapse_cone().
        """
        return has_reachable_from and has_reachable_to

    def collapse_cone(
        self,
        source_alias: str,
        target_alias: str,
    ) -> ExportPlan:
        """Collapse REACHABLE_FROM(A) ∩ REACHABLE_TO(B) → REACHABLE_BETWEEN(A,B).

        Implements the G_A* ∩ G_*B = G_AB lattice identity (§8.7).
        """
        return ExportPlan(
            query_target=self.query_target,
            query_text=(
                f"TrimJoin(REACHABLE_BETWEEN({source_alias!r}, {target_alias!r}))"
                " -- Rule 9 cone_containment: G_A* ∩ G_*B = G_AB"
            ),
            cone_containment_applied=True,
        )

    # ------------------------------------------------------------------
    # build_plan() — high-level entry point
    # ------------------------------------------------------------------

    def build_plan(
        self,
        query_text: str,
        table_name: "str | None" = None,
    ) -> ExportPlan:
        """Build an ExportPlan for *query_text* targeting self.query_target."""
        write_plan: str | None = None
        if self.sink_target is not None and table_name:
            write_plan = self.apply_rule_8(
                sink_target=self.sink_target,
                has_temporal_agg=False,
                has_q_delta=False,
                grain_kind=None,
            )
        return ExportPlan(
            query_target=self.query_target,
            query_text=query_text,
            sink_target=self.sink_target,
            write_plan=write_plan,
        )
