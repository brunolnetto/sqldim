"""DGM Supervisor — ``dgm_validate_invariant`` (§12.3, D43).

Checks one formal model invariant over every element of its scope, against the
DGM model the dataset handle declares (``dgm_model()``, see
:class:`~._invariant_model.DeclaresDGMModel`)
and its data, opened only through ``ToolContext.data`` (§12.10).

Fail closed (D42): ``holds`` is set only after a complete evaluation.  Every
other end is a typed failure — UNSUPPORTED (the dataset declares no model, the
model lacks what the invariant reads, or the invariant holds by construction),
NOT_IMPLEMENTED (a clause this build does not evaluate), INVALID_ARGUMENTS
(``PA*`` without a PipelineArtifact type), UNAUTHORIZED, CANCELLED or
TOOL_FAILURE.  ``violation_count`` is always the full count; only the list of
witnesses is cut at ``max_witnesses``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import duckdb
from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied
from sqldim.core.query.dgm.nl.supervisor._invariant_core import (
    Evaluation,
    Evaluator,
    NotEvaluable,
)
from sqldim.core.query.dgm.nl.supervisor._invariant_eval import (
    EVALUATORS,
    SCHEMA_ONLY,
)
from sqldim.core.query.dgm.nl.supervisor._invariant_model import DGMModel
from sqldim.core.query.dgm.nl.supervisor._invariant_pa import PA_EVALUATORS
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    INVARIANTS,
    ValidateInvariantArgs,
)
from sqldim.core.query.dgm.nl.supervisor._tools import (
    TOOL_VERSION,
    BaseTool,
    _capability,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
    zero_cost,
)
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = ["CostNotEstimated", "ValidateInvariantTool", "evaluate_invariant"]

_ALL = {**EVALUATORS, **PA_EVALUATORS}
if set(_ALL) != set(INVARIANTS):  # pragma: no cover — a build error, not a request
    raise RuntimeError("every §12.3 invariant needs an evaluator entry")


class CostNotEstimated(RuntimeError):
    """No statistics are available to estimate the cost without reading data."""


def evaluate_invariant(
    invariant: str,
    model: DGMModel,
    con: duckdb.DuckDBPyConnection | None,
    max_witnesses: int,
    *,
    cancel: CancelToken | None = None,
    now: datetime | None = None,
) -> Evaluation:
    """Evaluate *invariant* completely, or raise :class:`NotEvaluable`."""
    check = _ALL.get(invariant)
    if check is None:
        raise NotEvaluable(
            ToolErrorKind.INVALID_ARGUMENTS,
            f"{invariant!r} is not an invariant of §12.3",
            invariant=invariant,
        )
    ev = Evaluator(con, model, max_witnesses, cancel, now)
    check(ev)
    return ev.result


class _Run:
    """The open connection of one call, so a timeout can interrupt it."""

    con: duckdb.DuckDBPyConnection | None = None


class ValidateInvariantTool(BaseTool):
    """``dgm_validate_invariant`` — the contract tool of §12.3."""

    def __init__(self) -> None:
        self._schema = _capability(
            "dgm_validate_invariant", available=True, version=TOOL_VERSION
        )

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        # The context carries no GraphStatistics; a guessed figure would read
        # as an estimate, so none is given.
        raise CostNotEstimated(
            "dgm_validate_invariant has no statistics to estimate from"
        )

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        started = datetime.now(timezone.utc)
        run = _Run()
        try:
            args = ValidateInvariantArgs.model_validate(request.arguments)
            evaluation = await asyncio.to_thread(self._evaluate, args, ctx, run)
        except asyncio.CancelledError:
            if run.con is not None:
                run.con.interrupt()
            raise
        except Exception as exc:  # noqa: BLE001 — each end is a typed result
            return _failure(request, exc)
        return ToolCallResult(
            call_id=request.call_id,
            tool_name=request.tool_name,
            status=CallStatus.SUCCEEDED,
            data=evaluation.as_output(),
            datasets=list(request.datasets or []),
            attempts=1,
            cost_actual=zero_cost(),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _evaluate(
        args: ValidateInvariantArgs, ctx: ToolContext, run: _Run
    ) -> Evaluation:
        source = ctx.data.open(args.dataset)
        declare = getattr(source, "dgm_model", None)
        if not callable(declare):
            raise NotEvaluable(
                ToolErrorKind.UNSUPPORTED,
                "the dataset declares no DGM model (G_S, K_S, M, Σ; §2.6), so "
                "no invariant can be evaluated against it",
                dataset=args.dataset,
            )
        model = declare()
        if not isinstance(model, DGMModel):
            raise NotEvaluable(
                ToolErrorKind.UNSUPPORTED,
                "the dataset's dgm_model() did not return a DGMModel",
                dataset=args.dataset,
            )
        if args.invariant in SCHEMA_ONLY:
            return evaluate_invariant(
                args.invariant, model, None, args.max_witnesses, cancel=ctx.cancel
            )
        source.setup()
        try:
            run.con = source.get_connection()
            return evaluate_invariant(
                args.invariant, model, run.con, args.max_witnesses, cancel=ctx.cancel
            )
        finally:
            run.con = None
            source.teardown()


def _failure(request: ToolCallRequest, exc: Exception) -> ToolCallResult:
    if isinstance(exc, AccessDenied):
        return ToolCallResult.failure(
            request, CallStatus.FAILED, ToolErrorKind.UNAUTHORIZED, str(exc)
        )
    if isinstance(exc, ValidationError):
        return ToolCallResult.failure(
            request,
            CallStatus.FAILED,
            ToolErrorKind.INVALID_ARGUMENTS,
            "arguments do not match dgm_validate_invariant",
        )
    if isinstance(exc, NotEvaluable):
        status = (
            CallStatus.CANCELLED
            if exc.kind is ToolErrorKind.CANCELLED
            else CallStatus.FAILED
        )
        return ToolCallResult.failure(
            request, status, exc.kind, exc.message, context=exc.context
        )
    message = (
        "the invariant query failed"
        if isinstance(exc, duckdb.Error)
        else "the invariant evaluation failed"
    )
    return ToolCallResult.failure(
        request,
        CallStatus.FAILED,
        ToolErrorKind.TOOL_FAILURE,
        message,
        context={"exception": type(exc).__name__, "detail": str(exc)},
    )
