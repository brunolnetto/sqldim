"""DGM Supervisor — ``execute_tool`` and child budgets (§12.4, §12.5, §12.10).

``execute_tool`` is the only path from a plan step to a tool.  It resolves the
tool by exact name, refuses unavailable tools, validates arguments, checks the
grants immediately before the call, scopes data access to the request's
datasets, enforces the deadline and validates the output.  Every outcome is a
typed :class:`ToolCallResult`; nothing is raised for an expected failure and
nothing is substituted (D42).
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied
from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallRequest,
    ToolCallResult,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import authorization_error
from sqldim.core.query.dgm.planner._gate import ExecutionBudget
from sqldim.core.query.dgm.planner._targets import CostEstimate

if TYPE_CHECKING:
    from sqldim.core.query.dgm.nl.supervisor._tools import BaseTool

__all__ = ["child_budget", "execute_tool"]


def _scale(cost: CostEstimate, factor: float) -> CostEstimate:
    return CostEstimate(
        cpu_ops=int(cost.cpu_ops * factor),
        io_ops=int(cost.io_ops * factor),
        note=cost.note,
    )


def child_budget(
    budget: ExecutionBudget,
    fraction: float,
    deadline: datetime,
    now: datetime | None = None,
) -> ExecutionBudget:
    """§12.5: scale every cost field by *fraction*; bound time by the deadline."""
    now = now or datetime.now(timezone.utc)
    remaining = max(deadline - now, timedelta(0))
    return dataclasses.replace(
        budget,
        max_estimated_cost=_scale(budget.max_estimated_cost, fraction),
        streaming_threshold=_scale(budget.streaming_threshold, fraction),
        async_threshold=_scale(budget.async_threshold, fraction),
        max_wall_time=min(budget.max_wall_time, remaining),
    )


def _fail(
    request: ToolCallRequest,
    kind: ToolErrorKind,
    message: str,
    *,
    attempt: int,
    status: CallStatus = CallStatus.FAILED,
    retryable: bool = False,
    **context: Any,
) -> ToolCallResult:
    return ToolCallResult.failure(
        request,
        status,
        kind,
        message,
        retryable=retryable,
        context=context,
        attempts=attempt,
    )


def _precheck(
    request: ToolCallRequest,
    registry: Mapping[str, "BaseTool"],
    ctx: ToolContext,
    attempt: int,
) -> ToolCallResult | None:
    """Everything that must hold before a tool is called."""
    if request.datasets is None or request.idempotency_key is None:
        return _fail(
            request,
            ToolErrorKind.INVALID_ARGUMENTS,
            "request was not bound by plan validation",
            attempt=attempt,
        )
    tool = registry.get(request.tool_name)
    if tool is None or request.tool_name not in TOOL_SCHEMAS:
        return _fail(
            request,
            ToolErrorKind.UNKNOWN_TOOL,
            f"unknown tool {request.tool_name!r}",
            attempt=attempt,
        )
    if not tool.capability_schema.available:
        return _fail(
            request,
            ToolErrorKind.NOT_IMPLEMENTED,
            f"{request.tool_name} is not available in this deployment",
            attempt=attempt,
            missing=getattr(tool, "missing", None),
        )
    try:
        TOOL_SCHEMAS[request.tool_name].args.model_validate(request.arguments)
    except ValidationError as exc:
        return _fail(
            request,
            ToolErrorKind.INVALID_ARGUMENTS,
            f"arguments do not match {request.tool_name}",
            attempt=attempt,
            errors=[{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()],
        )
    denied = authorization_error(ctx.auth, request.tool_name, request.datasets)
    if denied is not None:
        return _fail(request, denied.kind, denied.message, attempt=attempt)
    if ctx.cancel.cancelled:
        return _fail(
            request,
            ToolErrorKind.CANCELLED,
            "the run was cancelled before the call started",
            attempt=attempt,
            status=CallStatus.CANCELLED,
        )
    return None


def _postcheck(
    request: ToolCallRequest, result: ToolCallResult, ctx: ToolContext, attempt: int
) -> ToolCallResult:
    """Validate what the tool returned (identity, demo marking, output schema)."""
    if (result.call_id, result.tool_name) != (request.call_id, request.tool_name):
        return _fail(
            request,
            ToolErrorKind.INVALID_OUTPUT,
            "tool returned a result for another call",
            attempt=attempt,
        )
    if result.demo and not ctx.demo:
        return _fail(
            request,
            ToolErrorKind.INVALID_OUTPUT,
            "a demo result was produced outside demo mode",
            attempt=attempt,
        )
    if result.status is CallStatus.SUCCEEDED:
        try:
            TOOL_SCHEMAS[request.tool_name].output.model_validate(result.data)
        except ValidationError as exc:
            failed = _fail(
                request,
                ToolErrorKind.INVALID_OUTPUT,
                f"output of {request.tool_name} does not match its schema",
                attempt=attempt,
                errors=[{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()],
            )
            return failed.model_copy(update={"demo": result.demo})
    return result


async def _call(
    request: ToolCallRequest,
    tool: "BaseTool",
    call_ctx: ToolContext,
    timeout: float,
    attempt: int,
) -> ToolCallResult:
    """Await the tool within *timeout*; a timeout or a raise is a typed failure."""
    try:
        return await asyncio.wait_for(tool.execute(request, call_ctx), timeout)
    except asyncio.TimeoutError:
        return _fail(
            request,
            ToolErrorKind.TIMEOUT,
            f"no result within {timeout:.3f}s",
            attempt=attempt,
            retryable=True,
        )
    except Exception as exc:  # noqa: BLE001 — a tool bug is reported, not hidden
        return _fail(
            request,
            ToolErrorKind.TOOL_FAILURE,
            "the tool raised instead of returning a result",
            attempt=attempt,
            exception=type(exc).__name__,
            detail=str(exc),
        )


async def execute_tool(
    request: ToolCallRequest,
    registry: Mapping[str, "BaseTool"],
    ctx: ToolContext,
    *,
    attempt: int = 1,
) -> ToolCallResult:
    """Run one bound request (§12.4 *execute_tool*)."""
    started = datetime.now(timezone.utc)
    refused = _precheck(request, registry, ctx, attempt)
    if refused is not None:
        return refused

    budget = child_budget(ctx.budget, request.budget_fraction, ctx.deadline, started)
    timeout = budget.max_wall_time.total_seconds()
    if timeout <= 0:
        return _fail(
            request,
            ToolErrorKind.TIMEOUT,
            "the run deadline has passed",
            attempt=attempt,
            retryable=True,
        )
    try:
        call_ctx = dataclasses.replace(
            ctx, budget=budget, data=ctx.data.scoped(request.datasets or [])
        )
    except AccessDenied as exc:
        return _fail(request, ToolErrorKind.UNAUTHORIZED, str(exc), attempt=attempt)

    result = await _call(
        request, registry[request.tool_name], call_ctx, timeout, attempt
    )
    checked = _postcheck(request, result, ctx, attempt)
    return checked.model_copy(
        update={
            "attempts": attempt if checked.status is not CallStatus.SKIPPED else 0,
            "datasets": list(request.datasets or []),
            "started_at": checked.started_at or started,
            "finished_at": checked.finished_at or datetime.now(timezone.utc),
        }
    )
