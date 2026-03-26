"""DGM Query Planner — DGMPlanner, support types, thresholds, and gate (§6.2–6.3)."""

from sqldim.core.query.dgm.planner._targets import (  # noqa: F401
    SMALL,
    CLOSURE_THRESHOLD,
    SMALL_GRAPH_THRESHOLD,
    DENSE,
    QueryTarget,
    SinkTarget,
    PreComputation,
    CostEstimate,
    ExportPlan,
)
from sqldim.core.query.dgm.planner._core import (  # noqa: F401
    DGMPlanner,
)
from sqldim.core.query.dgm.planner._gate import (  # noqa: F401
    HARD_CEILING_FACTOR,
    BudgetDecisionKind,
    SampleMethod,
    BudgetDecision,
    ProceedDecision,
    StreamDecision,
    AsyncDecision,
    RewriteDecision,
    SampleDecision,
    PaginateDecision,
    ClarifyDecision,
    RejectDecision,
    ExecutionBudget,
    ExecutionGate,
)
