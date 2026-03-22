"""DGM Query Planner — DGMPlanner, support types, and thresholds (DGM §6.2)."""
from sqldim.core.query.dgm.planner._targets import (  # noqa: F401
    SMALL, CLOSURE_THRESHOLD, SMALL_GRAPH_THRESHOLD, DENSE,
    QueryTarget, SinkTarget, PreComputation, CostEstimate, ExportPlan,
)
from sqldim.core.query.dgm.planner._core import (  # noqa: F401
    DGMPlanner,
)
