"""DGM Supervisor Agent — composable analytical tool orchestration (§12)."""

from sqldim.core.query.dgm.nl.supervisor._auth import (
    AccessDenied,
    Action,
    AuthContext,
    DataAccess,
    DatasetGrant,
)
from sqldim.core.query.dgm.nl.supervisor._graph import (
    SupervisorRuntime,
    build_supervisor_graph,
    make_runtime,
    run_supervisor,
)
from sqldim.core.query.dgm.nl.supervisor._judge import (
    JudgeCase,
    JudgeVerdict,
    PipelineOutput,
    SupervisorJudge,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    Outcome,
    SupervisorPlan,
    SupervisorState,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolError,
    ToolErrorKind,
)

__all__ = [
    "AccessDenied",
    "Action",
    "AuthContext",
    "CallStatus",
    "CancelToken",
    "DataAccess",
    "DatasetGrant",
    "JudgeCase",
    "JudgeVerdict",
    "Outcome",
    "PipelineOutput",
    "SupervisorJudge",
    "SupervisorPlan",
    "SupervisorRuntime",
    "SupervisorState",
    "ToolCallRequest",
    "ToolCallResult",
    "ToolCapabilitySchema",
    "ToolContext",
    "ToolError",
    "ToolErrorKind",
    "build_supervisor_graph",
    "make_runtime",
    "run_supervisor",
]
