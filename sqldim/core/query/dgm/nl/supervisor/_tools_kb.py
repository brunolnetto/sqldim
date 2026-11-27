"""DGM Supervisor — knowledge-base tools (§12.3, §12.7, ADR 0007, D43, D44).

``dgm_store_finding`` writes one finding to the tenant's knowledge base, at
most once per ``idempotency_key`` (§12.11).  ``dgm_retrieve_related`` returns
the tenant's findings within move distance ``k`` of ``(q', π')``, in the total
order of ADR 0007, keeping only findings whose datasets are all granted READ.

The move distance ``d_S`` is a shortest path in the move graph ``M`` of
ADR 0007, whose vertices are the canonical skeletons of ``Q_valid`` named by
their ``question_key``.  This module does not build ``M``: it receives the
structural move generator of §8.16.6 as a :class:`MoveGraph`
(``_tools_lattice.LatticeMoveGraph`` in a runtime, ADR 0007).  Without one,
``dgm_retrieve_related`` is registered unavailable — distances are never
approximated by text, embeddings or key equality alone.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._auth import (
    NOT_AUTHORIZED_OR_NOT_FOUND,
    AccessDenied,
    Action,
    AuthContext,
)
from sqldim.core.query.dgm.nl.supervisor._kb import (
    FindingConflict,
    KnowledgeBaseError,
    StoredFinding,
    TenantKnowledgeBase,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    RetrieveRelatedArgs,
    StoreFindingArgs,
)
from sqldim.core.query.dgm.nl.supervisor._tools import (
    TOOL_VERSION,
    BaseTool,
    UnavailableTool,
    _capability,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
    zero_cost,
)
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = [
    "MISSING_KB",
    "MISSING_MOVE_GRAPH",
    "MoveGraph",
    "MoveGraphError",
    "MoveGraphUnsupported",
    "RetrieveRelatedTool",
    "StoreFindingTool",
    "d_P",
    "kb_tools",
    "rank_related",
    "within_distance",
]

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

MISSING_KB = "tenant-partitioned knowledge base (§12.7)"
MISSING_MOVE_GRAPH = (
    "structural move graph M over question keys: the move generator of §8.16.6 "
    "that d_S of ADR 0007 is a shortest path in"
)


class MoveGraphError(RuntimeError):
    """The move graph could not answer (reported as a tool failure)."""


class MoveGraphUnsupported(MoveGraphError):
    """The move graph cannot decide for this input (reported as UNSUPPORTED):
    the dataset declares no schema graph, or the key cannot be resolved."""


@runtime_checkable
class MoveGraph(Protocol):
    """The move graph ``M`` of ADR 0007 for the question space of a dataset.

    Vertices are ``question_key`` values of canonical skeletons of
    ``Q_valid``; ``neighbours`` returns the skeletons one structural local
    move of §8.16.6 away.  ``M`` is undirected: every move has an inverse.
    """

    def contains(self, dataset: str, question_key: str) -> bool: ...

    def neighbours(self, dataset: str, question_key: str) -> Iterable[str]: ...


# ---------------------------------------------------------------------------
# Distance and order (ADR 0007)
# ---------------------------------------------------------------------------


def within_distance(
    moves: MoveGraph, dataset: str, origin: str, k: int
) -> dict[str, int]:
    """``{q: d_S(origin, q)}`` for every skeleton with ``d_S ≤ k`` (BFS).

    Breadth-first search assigns each vertex its shortest-path length the
    first time it is reached and never expands past depth ``k``.
    """
    distance = {origin: 0}
    frontier = [origin]
    for depth in range(1, k + 1):
        reached: list[str] = []
        for vertex in frontier:
            for nxt in sorted(set(moves.neighbours(dataset, vertex))):
                if nxt not in distance:
                    distance[nxt] = depth
                    reached.append(nxt)
        if not reached:
            break
        frontier = reached
    return distance


def _slot(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def d_P(pi: dict[str, Any], pi_prime: dict[str, Any]) -> int:
    """Parameter slots on which two assignments differ (a slot bound in only
    one of them differs)."""
    slots = set(pi) | set(pi_prime)
    return sum(
        1
        for s in slots
        if s not in pi or s not in pi_prime or _slot(pi[s]) != _slot(pi_prime[s])
    )


def _micros(at: datetime) -> int:
    return (at - _EPOCH) // timedelta(microseconds=1)


def _granted(auth: AuthContext, finding: StoredFinding) -> bool:
    return bool(finding.datasets) and all(
        auth.allows(ds, {Action.READ}) for ds in finding.datasets
    )


def rank_related(
    findings: Iterable[StoredFinding],
    distance: dict[str, int],
    origin: str,
    parameters: dict[str, Any],
    auth: AuthContext,
) -> list[dict[str, Any]]:
    """Filter by grants and order by ``d_S``, ``d_P``, recency, id (ADR 0007).

    ``d_P`` is defined only for findings at ``q'`` itself; elsewhere it is
    ``None`` and does not take part in the order.
    """
    rows = []
    for f in findings:
        if f.question_key not in distance or not _granted(auth, f):
            continue
        dp = d_P(f.parameters, parameters) if f.question_key == origin else None
        rows.append((distance[f.question_key], dp, f))
    rows.sort(
        key=lambda r: (
            r[0],
            r[1] if r[1] is not None else 0,
            -_micros(r[2].stored_at),
            r[2].finding_id,
        )
    )
    return [{"finding_id": f.finding_id, "d_S": ds, "d_P": dp} for ds, dp, f in rows]


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


def _failure(
    request: ToolCallRequest,
    kind: ToolErrorKind,
    message: str,
    *,
    retryable: bool = False,
    **context: Any,
) -> ToolCallResult:
    return ToolCallResult.failure(
        request,
        CallStatus.FAILED,
        kind,
        message,
        retryable=retryable,
        context=context,
    )


def _unauthorized(request: ToolCallRequest) -> ToolCallResult:
    return _failure(request, ToolErrorKind.UNAUTHORIZED, NOT_AUTHORIZED_OR_NOT_FOUND)


def _tenant_kb(
    request: ToolCallRequest, ctx: ToolContext
) -> TenantKnowledgeBase | ToolCallResult:
    """The call's KB handle, or the failure that replaces it (fail closed)."""
    kb = ctx.kb
    if kb is None:
        return _failure(
            request,
            ToolErrorKind.NOT_IMPLEMENTED,
            "no knowledge base is configured for this run",
            missing=MISSING_KB,
        )
    if not isinstance(kb, TenantKnowledgeBase):
        return _failure(
            request,
            ToolErrorKind.TOOL_FAILURE,
            "the knowledge base handle is not scoped to a tenant",
        )
    if kb.tenant_id != ctx.auth.tenant_id:
        return _unauthorized(request)
    return kb


def _succeeded(
    request: ToolCallRequest, data: Any, started: datetime
) -> ToolCallResult:
    return ToolCallResult(
        call_id=request.call_id,
        tool_name=request.tool_name,
        status=CallStatus.SUCCEEDED,
        data=data,
        datasets=list(request.datasets or []),
        attempts=1,
        cost_actual=zero_cost(),
        started_at=started,
        finished_at=datetime.now(timezone.utc),
    )


class _KBTool(BaseTool):
    _name: str

    def __init__(self) -> None:
        self._schema = _capability(self._name, available=True, version=TOOL_VERSION)

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        return CostEstimate(cpu_ops=1, io_ops=1, note="kb: one indexed access")


class StoreFindingTool(_KBTool):
    """``dgm_store_finding``: one durable write per ``idempotency_key``."""

    _name = "dgm_store_finding"

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        started = datetime.now(timezone.utc)
        kb = _tenant_kb(request, ctx)
        if isinstance(kb, ToolCallResult):
            return kb
        try:
            finding = StoreFindingArgs.model_validate(request.arguments).finding
        except ValidationError:
            return _failure(request, ToolErrorKind.INVALID_ARGUMENTS, "invalid finding")
        if not request.idempotency_key:
            return _failure(
                request,
                ToolErrorKind.INVALID_ARGUMENTS,
                "a KB write needs the idempotency_key bound by plan validation",
            )
        # WRITE_KB plus READ on every dataset the finding derives from (§12.10).
        needed = {Action.READ, Action.WRITE_KB}
        if not all(ctx.auth.allows(ds, needed) for ds in finding.datasets):
            return _unauthorized(request)
        try:
            stored = await asyncio.to_thread(
                kb.store,
                request.idempotency_key,
                question_key=finding.question_key,
                parameters=dict(finding.parameters),
                datasets=finding.datasets,
                content=finding.content,
            )
        except FindingConflict as exc:
            return _failure(request, ToolErrorKind.INVALID_ARGUMENTS, str(exc))
        except KnowledgeBaseError as exc:
            return _failure(
                request, ToolErrorKind.TOOL_FAILURE, str(exc), retryable=True
            )
        return _succeeded(request, {"finding_id": stored.finding_id}, started)


class RetrieveRelatedTool(_KBTool):
    """``dgm_retrieve_related``: breadth-first retrieval by ``d_S ≤ k``."""

    _name = "dgm_retrieve_related"

    def __init__(self, moves: MoveGraph) -> None:
        super().__init__()
        self._moves = moves

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        started = datetime.now(timezone.utc)
        kb = _tenant_kb(request, ctx)
        if isinstance(kb, ToolCallResult):
            return kb
        try:
            args = RetrieveRelatedArgs.model_validate(request.arguments)
        except ValidationError:
            return _failure(
                request, ToolErrorKind.INVALID_ARGUMENTS, "invalid arguments"
            )
        if not ctx.auth.allows(args.dataset, {Action.READ}):
            return _unauthorized(request)
        try:
            ranked = await asyncio.to_thread(self._retrieve, kb, args, ctx.auth)
        except _NotAVertex:
            return _failure(
                request,
                ToolErrorKind.INVALID_ARGUMENTS,
                "question_key is not a skeleton of the dataset's question space",
            )
        except AccessDenied:
            return _unauthorized(request)
        except MoveGraphUnsupported as exc:
            return _failure(request, ToolErrorKind.UNSUPPORTED, str(exc))
        except MoveGraphError as exc:
            return _failure(request, ToolErrorKind.TOOL_FAILURE, str(exc))
        except KnowledgeBaseError as exc:
            return _failure(
                request, ToolErrorKind.TOOL_FAILURE, str(exc), retryable=True
            )
        return _succeeded(request, {"findings": ranked}, started)

    def _retrieve(
        self, kb: TenantKnowledgeBase, args: RetrieveRelatedArgs, auth: AuthContext
    ) -> list[dict[str, Any]]:
        if not self._moves.contains(args.dataset, args.question_key):
            raise _NotAVertex()
        distance = within_distance(self._moves, args.dataset, args.question_key, args.k)
        candidates = kb.findings_at(distance)
        return rank_related(
            candidates, distance, args.question_key, dict(args.parameters), auth
        )


class _NotAVertex(Exception):
    """``q'`` is not a vertex of the move graph."""


def kb_tools(moves: MoveGraph | None) -> dict[str, BaseTool]:
    """The KB tools of a deployment that has a knowledge base.

    Retrieval stays unavailable without a move graph: ``d_S`` is never
    replaced by another distance (ADR 0007, D42).
    """
    if moves is not None and not isinstance(moves, MoveGraph):
        raise TypeError(f"{type(moves).__name__} is not a MoveGraph")
    retrieve: BaseTool = (
        RetrieveRelatedTool(moves)
        if moves is not None
        else UnavailableTool("dgm_retrieve_related", missing=MISSING_MOVE_GRAPH)
    )
    return {"dgm_store_finding": StoreFindingTool(), "dgm_retrieve_related": retrieve}
