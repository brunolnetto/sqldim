"""Test doubles for the supervisor contract: scripted tools and planners."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Any

from sqldim.core.query.dgm.nl.supervisor._auth import (
    Action,
    AuthContext,
    DataAccess,
    DatasetGrant,
)
from sqldim.core.query.dgm.nl.supervisor._graph import SupervisorRuntime
from sqldim.core.query.dgm.nl.supervisor._planner import PlannerView
from sqldim.core.query.dgm.nl.supervisor._tools import (
    BaseTool,
    UnavailableTool,
    _capability,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.planner._gate import ExecutionBudget
from sqldim.core.query.dgm.planner._targets import CostEstimate

Behaviour = Callable[[ToolCallRequest, ToolContext, int], Any]


def budget(cost: int = 1000) -> ExecutionBudget:
    return ExecutionBudget(
        max_estimated_cost=CostEstimate(cpu_ops=cost, io_ops=0),
        max_result_rows=1000,
        max_wall_time=timedelta(seconds=5),
        max_precompute_time=timedelta(seconds=1),
        streaming_threshold=CostEstimate(cpu_ops=cost // 2, io_ops=0),
        async_threshold=CostEstimate(cpu_ops=cost // 2 + 1, io_ops=0),
    )


def auth(
    grants: Mapping[str, set[Action]] | None = None,
    tenant: str = "t1",
) -> AuthContext:
    grants = grants if grants is not None else {"sales": {Action.READ}}
    return AuthContext(
        principal_id="alice",
        tenant_id=tenant,
        grants=frozenset(DatasetGrant(ds, frozenset(a)) for ds, a in grants.items()),
    )


class Opener:
    """Records every dataset opened, to prove denials touch no data."""

    def __init__(self) -> None:
        self.opened: list[str] = []

    def __call__(self, dataset_id: str) -> str:
        self.opened.append(dataset_id)
        return f"handle:{dataset_id}"


def query_turn(rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "question_key": "qk",
        "decision": "PROCEED",
        "rows": rows if rows is not None else [{"n": 1}],
        "job": None,
        "explanation": "counted",
        "unresolved": [],
    }


def succeed(data: Any) -> Behaviour:
    def run(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
        return ToolCallResult(
            call_id=request.call_id,
            tool_name=request.tool_name,
            status=CallStatus.SUCCEEDED,
            data=data,
            attempts=attempt,
        )

    return run


def fail(
    kind: ToolErrorKind,
    *,
    status: CallStatus = CallStatus.FAILED,
    retryable: bool = False,
) -> Behaviour:
    def run(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
        return ToolCallResult.failure(
            request, status, kind, f"scripted {kind.value}", retryable=retryable
        )

    return run


class FakeTool(BaseTool):
    """A §12.3 tool name with scripted behaviour (one entry per attempt)."""

    def __init__(
        self,
        name: str,
        *behaviours: Behaviour,
        idempotent: bool = True,
        delay: float = 0.0,
        version: str = "test",
    ) -> None:
        base = _capability(name, available=True, version=version)
        self._schema = ToolCapabilitySchema(
            **{**base.__dict__, "idempotent": idempotent}
        )
        self._behaviours = list(behaviours) or [succeed(query_turn())]
        self._delay = delay
        self.calls: list[ToolCallRequest] = []
        self.contexts: list[ToolContext] = []

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        return CostEstimate(cpu_ops=1, io_ops=0)

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        self.calls.append(request)
        self.contexts.append(ctx)
        if self._delay:
            await asyncio.sleep(self._delay)
        attempt = len(self.calls)
        behaviour = self._behaviours[min(attempt, len(self._behaviours)) - 1]
        return behaviour(request, ctx, attempt)


def registry(*tools: BaseTool) -> Mapping[str, BaseTool]:
    from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS

    out: dict[str, BaseTool] = {n: UnavailableTool(n) for n in TOOL_SCHEMAS}
    out.update({t.capability_schema.name: t for t in tools})
    return MappingProxyType(out)


def step(
    call_id: str,
    tool: str = "dgm_query",
    arguments: dict[str, Any] | None = None,
    *,
    depends_on: list[str] | None = None,
    fraction: float = 0.5,
) -> ToolCallRequest:
    if arguments is None:
        arguments = {"dataset": "sales", "utterance": "count orders"}
    return ToolCallRequest(
        call_id=call_id,
        tool_name=tool,
        arguments=arguments,
        depends_on=depends_on or [],
        budget_fraction=fraction,
    )


def plan(
    *steps: ToolCallRequest, plan_id: str = "p1", version: int = 1
) -> SupervisorPlan:
    return SupervisorPlan(
        plan_id=plan_id, version=version, goal="g", steps=list(steps), confidence=0.9
    )


class ScriptedPlanner:
    """Returns one scripted plan per call (the last one repeats)."""

    demo = False

    def __init__(
        self, *plans: Callable[[PlannerView], SupervisorPlan] | SupervisorPlan
    ) -> None:
        self._plans = list(plans)
        self.views: list[PlannerView] = []

    def propose(self, view: PlannerView) -> SupervisorPlan:
        self.views.append(view)
        item = self._plans[min(len(self.views), len(self._plans)) - 1]
        return item(view) if callable(item) else item


class RaisingPlanner:
    demo = False

    def propose(self, view: PlannerView) -> SupervisorPlan:
        raise RuntimeError("model unreachable")


def tool_context(
    the_auth: AuthContext | None = None,
    opener: Opener | None = None,
    *,
    demo: bool = False,
    deadline: datetime | None = None,
    cancel: CancelToken | None = None,
) -> ToolContext:
    the_auth = the_auth or auth()
    return ToolContext(
        auth=the_auth,
        budget=budget(),
        deadline=deadline or datetime.now(timezone.utc) + timedelta(minutes=1),
        cancel=cancel or CancelToken(),
        data=DataAccess(the_auth, opener or Opener()),
        demo=demo,
    )


def runtime(
    tools: Mapping[str, BaseTool],
    planner: Any,
    *,
    the_auth: AuthContext | None = None,
    opener: Opener | None = None,
    demo: bool = False,
    **kwargs: Any,
) -> SupervisorRuntime:
    the_auth = the_auth or auth()
    return SupervisorRuntime(
        auth=the_auth,
        budget=budget(),
        registry=tools,
        planner=planner,
        data=DataAccess(the_auth, opener or Opener()),
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        demo=demo,
        **kwargs,
    )
