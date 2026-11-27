"""Rule 3 — GraphExpr scheduling (DGM §6.2), as typed plan steps (#148).

:func:`schedule_graph_expr` decides where a GraphExpr runs and, when it is
scheduled before the query, returns the :class:`PreComputation` that goes
into the plan IR.  The planner states nothing it was not given:

* the step's time, cost and approximate variant come from the cost model,
  through :class:`PrecomputeCostModel`; a cost model that does not implement
  it, or returns ``None``, leaves them unknown, and the gate (§6.3 Step 1b)
  rejects the plan;
* the relations the step reads are the caller's ``reads`` (the NodeTables
  and RelationTables of the graph the algorithm ranges over, from the
  mapping M of §2.6); without them the plan has no ``deps``;
* an unknown node count is not "small": the step is scheduled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from sqldim.core.query.dgm.planner._canon import canonical_hash
from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct
from sqldim.core.query.dgm.planner._targets import (
    SMALL_GRAPH_THRESHOLD,
    ApproximateVariant,
    CostEstimate,
    PreComputation,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.graph import NodeExpr, SubgraphExpr

__all__ = [
    "Placement",
    "GraphExprSchedule",
    "PrecomputeEstimate",
    "PrecomputeCostModel",
    "schedule_graph_expr",
]


class Placement(Enum):
    """Where Rule 3 runs a GraphExpr."""

    INLINE = "INLINE"  # evaluated inside the query
    PRECOMPUTE = "PRECOMPUTE"  # a PreComputation before the query


@dataclass(frozen=True)
class GraphExprSchedule:
    """The decision of Rule 3 for one GraphExpr.

    ``strategy`` is the execution strategy of §6.2 Rule 3.
    ``pre_computation`` is set exactly when ``placement`` is PRECOMPUTE.
    """

    placement: Placement
    strategy: str
    pre_computation: PreComputation | None = None

    def __post_init__(self) -> None:
        if (self.placement is Placement.PRECOMPUTE) != (
            self.pre_computation is not None
        ):
            raise ValueError("a PreComputation is present exactly when scheduled")


@dataclass(frozen=True)
class PrecomputeEstimate:
    """What a cost model declares for one scheduled GraphExpr."""

    estimated_time: timedelta
    cost_estimate: CostEstimate
    approximation: ApproximateVariant | None = None


@runtime_checkable
class PrecomputeCostModel(Protocol):
    def estimate_precompute(
        self, expr: "NodeExpr | SubgraphExpr"
    ) -> PrecomputeEstimate | None:
        """Time, cost and approximate variant of precomputing *expr*, if known."""
        ...


def _descriptor(expr: "SubgraphExpr") -> str:
    if expr.scope is not None:
        raise UnsupportedConstruct(
            "a scoped SubgraphExpr has no canonical step descriptor in this IR "
            "version; it is not scheduled"
        )
    return expr.to_sql()


def _pre_computation(
    expr: "SubgraphExpr", cost_model: object, reads: frozenset[str] | None
) -> PreComputation:
    query = _descriptor(expr)
    name = "pc_" + canonical_hash("graph", query)[:16]
    est = (
        cost_model.estimate_precompute(expr)
        if isinstance(cost_model, PrecomputeCostModel)
        else None
    )
    if est is not None and not isinstance(est, PrecomputeEstimate):
        raise TypeError(f"estimate_precompute returned {est!r}")
    return PreComputation(
        name=name,
        query=query,
        kind="graph",
        estimated_time=None if est is None else est.estimated_time,
        cost_estimate=None if est is None else est.cost_estimate,
        approximation=None if est is None else est.approximation,
        reads=reads,
    )


def _node_schedule(expr: "NodeExpr") -> GraphExprSchedule:
    from sqldim.core.query.dgm.graph import (
        DOMINANT_INCOMING_SIGNATURE,
        DOMINANT_OUTGOING_SIGNATURE,
        INCOMING_SIGNATURES,
        OUTGOING_SIGNATURES,
        SIGNATURE_DIVERSITY,
    )

    alg = expr.algorithm
    if isinstance(
        alg, (OUTGOING_SIGNATURES, DOMINANT_OUTGOING_SIGNATURE, SIGNATURE_DIVERSITY)
    ):
        text = "forward_bfs per anchor alias; pre-compute if stable"
    elif isinstance(alg, (INCOMING_SIGNATURES, DOMINANT_INCOMING_SIGNATURE)):
        text = "bfs on transposed G^T per anchor alias; pre-compute if stable"
    else:
        text = f"inline NodeExpr({type(alg).__name__})"
    return GraphExprSchedule(Placement.INLINE, text)


def _subgraph_schedule(
    expr: "SubgraphExpr",
    node_count: int | None,
    cost_model: object,
    reads: frozenset[str] | None,
) -> GraphExprSchedule:
    from sqldim.core.query.dgm.graph import (
        GLOBAL_DOMINANT_SIGNATURE,
        GLOBAL_SIGNATURE_COUNT,
        SIGNATURE_ENTROPY,
    )

    alg = expr.algorithm
    name = type(alg).__name__
    if not isinstance(
        alg, (GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE, SIGNATURE_ENTROPY)
    ):
        return GraphExprSchedule(Placement.INLINE, f"inline SubgraphExpr({name})")
    if node_count is not None and node_count < SMALL_GRAPH_THRESHOLD:
        if isinstance(alg, SIGNATURE_ENTROPY):
            text = "inline SIGNATURE_ENTROPY: p(s) distribution; -Σ p log₂ p"
        else:
            text = "broadcast graph-level scalar inline to all tuples"
        return GraphExprSchedule(Placement.INLINE, text)
    pc = _pre_computation(expr, cost_model, reads)
    text = f"precompute {name}: schedule PreComputation; broadcast scalar"
    return GraphExprSchedule(Placement.PRECOMPUTE, text, pc)


def schedule_graph_expr(
    expr: "NodeExpr | SubgraphExpr",
    *,
    node_count: int | None,
    cost_model: object,
    reads: frozenset[str] | None = None,
) -> GraphExprSchedule:
    """Rule 3 for *expr*; see the module docstring for what is declared."""
    from sqldim.core.query.dgm.graph import NodeExpr

    if isinstance(expr, NodeExpr):
        return _node_schedule(expr)
    return _subgraph_schedule(expr, node_count, cost_model, reads)
