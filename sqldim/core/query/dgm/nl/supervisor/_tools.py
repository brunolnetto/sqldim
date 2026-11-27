"""DGM Supervisor — tool interface and registry (§12.2, §12.3).

A tool without an implementation of its D43 contract is registered with
``available = False`` and names the missing component; dispatching one
returns ``NOT_IMPLEMENTED`` without running anything (D42, ADR 0012).  The
knowledge-base tools (``_tools_kb``) are available only when the deployment
configures a knowledge base (and, for retrieval, a move graph).
``dgm_validate_invariant`` is implemented in ``_tools_invariants``.
``dgm_map_analytical_space`` is a contract tool (``_tools_space``), and so is
``dgm_what_can_i_answer`` (``_tools_lattice``).

The explicit demo mode (D47, ADR 0013) adds one heuristic stand-in:
``dgm_query`` answered by the current NL graph.  It is registered only by
``build_registry(demo=True)``, which the caller must select for the run, and
every result it produces carries ``demo = True``.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied
from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
    zero_cost,
)
from sqldim.core.query.dgm.planner._gate import BudgetDecisionKind
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = [
    "BaseTool",
    "DemoQueryTool",
    "TOOL_REGISTRY",
    "ToolUnavailable",
    "UnavailableTool",
    "build_registry",
]

TOOL_VERSION = "0.1.0"

# The component each tool still lacks (context of its NOT_IMPLEMENTED error).
_MISSING: dict[str, str] = {
    "dgm_query": "run_ask_from_source: the non-interactive NL turn of §11.10",
    "dgm_what_can_i_answer": "bounded enumeration of Q_valid skeletons (§8.11)",
    "dgm_correlate": "drill-across over conformed keys (§5.2.2, Rule 12)",
    "dgm_explore_from": "structural moves from a question key (§8.16.6)",
    "dgm_sensitivity_sweep": (
        "contract dgm_query execution of the source question (#153), to run it "
        "over Π(q) (§8.15)"
    ),
    "dgm_validate_invariant": "invariant evaluators (§2.1, §3)",
    "dgm_store_finding": "tenant-partitioned knowledge base (§12.7)",
    "dgm_retrieve_related": (
        "tenant-partitioned knowledge base (§12.7) and the structural move "
        "graph of §8.16.6 (ADR 0007)"
    ),
    "dgm_map_analytical_space": "vocabulary decomposition (§8.16)",
}


class ToolUnavailable(RuntimeError):
    """Raised when an unavailable tool is asked for a cost estimate."""


def _capability(name: str, *, available: bool, version: str) -> ToolCapabilitySchema:
    spec = TOOL_SCHEMAS[name]
    return ToolCapabilitySchema(
        name=name,
        version=version,
        description=spec.description,
        input_schema=spec.args.model_json_schema(),
        output_schema=spec.output.model_json_schema(),
        reads=spec.reads,
        writes_kb=spec.writes_kb,
        idempotent=spec.idempotent,
        composable_with=spec.composable_with,
        available=available,
    )


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------


class BaseTool(ABC):
    """The single normative tool interface (§12.2)."""

    @property
    @abstractmethod
    def capability_schema(self) -> ToolCapabilitySchema: ...

    @abstractmethod
    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        """Estimate from statistics only: reads no data, has no side effect."""

    @abstractmethod
    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        """Run the call; an expected failure is a result, never an exception."""

    def compose(self, left: ToolCallResult, right: ToolCallResult) -> ToolCallResult:
        """No tool defines composition yet: report it, never merge blindly."""
        request = ToolCallRequest(
            call_id=f"{left.call_id}+{right.call_id}",
            tool_name=self.capability_schema.name,
            budget_fraction=0.0,
            datasets=sorted(set(left.datasets) | set(right.datasets)),
        )
        return ToolCallResult.failure(
            request,
            CallStatus.FAILED,
            ToolErrorKind.COMPOSITION_FAILURE,
            f"{self.capability_schema.name} does not define composition",
            context={"left": left.call_id, "right": right.call_id},
        )

    def explain(self, arguments: dict[str, Any]) -> str:
        schema = self.capability_schema
        return f"{schema.name}: {schema.description} — arguments {arguments}"


class UnavailableTool(BaseTool):
    """A §12.3 tool whose implementation is missing in this build."""

    def __init__(self, name: str, *, missing: str | None = None) -> None:
        self._name = name
        self._missing = missing
        self._schema = _capability(name, available=False, version=TOOL_VERSION)

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    @property
    def missing(self) -> str:
        return self._missing or _MISSING[self._name]

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        raise ToolUnavailable(f"{self._name} is not available: {self.missing}")

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        return ToolCallResult.failure(
            request,
            CallStatus.FAILED,
            ToolErrorKind.NOT_IMPLEMENTED,
            f"{self._name} is not implemented in this deployment",
            context={"missing": self.missing},
        )


# ---------------------------------------------------------------------------
# Demo mode (D47)
# ---------------------------------------------------------------------------


def _rows_as_dicts(result: dict[str, Any]) -> list[dict[str, Any]]:
    columns = list(result.get("columns") or [])
    rows: list[dict[str, Any]] = []
    for row in result.get("rows") or []:
        if isinstance(row, dict):
            rows.append(dict(row))
        else:
            rows.append(dict(zip(columns, row)))
    return rows


class DemoQueryTool(BaseTool):
    """``dgm_query`` answered by the heuristic NL graph — demo mode only.

    The NL graph does not implement §11.10 (it has template and fallback
    paths), so its output is never a contract result: every result carries
    ``demo = True``.  A turn that produced no executed result is FAILED.
    """

    def __init__(self, model: Any = None) -> None:
        self._model = model
        self._schema = _capability(
            "dgm_query", available=True, version=f"{TOOL_VERSION}+demo"
        )

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        return CostEstimate(cpu_ops=0, io_ops=0, note="demo: not estimated")

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        if not ctx.demo:
            return ToolCallResult.failure(
                request,
                CallStatus.FAILED,
                ToolErrorKind.NOT_IMPLEMENTED,
                "the heuristic dgm_query runs only in demo mode",
                context={"missing": _MISSING["dgm_query"]},
            )
        started = datetime.now(timezone.utc)
        try:
            turn = await asyncio.to_thread(self._run_turn, request, ctx)
        except AccessDenied as exc:
            return _demo_failure(request, ToolErrorKind.UNAUTHORIZED, str(exc))
        except _DemoTurnFailed as exc:
            return _demo_failure(request, exc.kind, exc.message, decision=exc.decision)
        except Exception as exc:  # noqa: BLE001 — reported, never swallowed
            return _demo_failure(
                request,
                ToolErrorKind.TOOL_FAILURE,
                "the demo NL turn failed",
                {"exception": type(exc).__name__, "detail": str(exc)},
            )
        if turn is None:
            return _demo_failure(
                request,
                ToolErrorKind.TOOL_FAILURE,
                "the demo NL turn produced no executed result",
            )
        return ToolCallResult(
            call_id=request.call_id,
            tool_name=request.tool_name,
            status=CallStatus.SUCCEEDED,
            data=turn,
            decision=None,
            datasets=list(request.datasets or []),
            attempts=1,
            cost_actual=zero_cost(),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            demo=True,
        )

    def _run_turn(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> dict[str, Any] | None:
        from langgraph.checkpoint.memory import MemorySaver

        from sqldim.application.ask import build_registry_from_schema
        from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
        from sqldim.core.query.dgm.nl._graph import build_nl_graph

        dataset = request.arguments["dataset"]
        source = ctx.data.open(dataset)
        source.setup()
        try:
            con = source.get_connection()
            registry = build_registry_from_schema(con, source.get_table_names())
            nl_ctx = DGMContext(entity_registry=registry, budget=ctx.budget, con=con)
            graph = build_nl_graph(
                checkpointer=MemorySaver(),
                context=nl_ctx,
                model=self._model,
                demo=True,
            )
            out = graph.invoke(
                NLInterfaceState(utterance=request.arguments["utterance"]).model_dump(),
                config={
                    "configurable": {"thread_id": f"demo-{request.call_id}"},
                    "recursion_limit": 25,
                },
            )
        finally:
            source.teardown()
        if out.get("error"):
            raise _DemoTurnFailed(out["error"])
        result = out.get("result")
        if not isinstance(result, dict):
            return None
        return {
            "question_key": None,
            "decision": "PROCEED",
            "rows": _rows_as_dicts(result),
            "job": None,
            "explanation": out.get("explanation") or "",
            "unresolved": [],
        }


class _DemoTurnFailed(Exception):
    """The demo NL turn ended with a typed error in its state."""

    def __init__(self, error: dict[str, Any]) -> None:
        try:
            self.kind = ToolErrorKind(error.get("kind"))
        except ValueError:
            self.kind = ToolErrorKind.TOOL_FAILURE
        self.message = str(error.get("message"))
        # A gate REJECT or CLARIFY is a budget rejection (§12.11).
        raw = error.get("decision")
        self.decision = BudgetDecisionKind(raw) if raw else None
        super().__init__(self.message)


def _demo_failure(
    request: ToolCallRequest,
    kind: ToolErrorKind,
    message: str,
    context: dict[str, Any] | None = None,
    *,
    decision: BudgetDecisionKind | None = None,
) -> ToolCallResult:
    status = CallStatus.REJECTED if decision is not None else CallStatus.FAILED
    result = ToolCallResult.failure(
        request, status, kind, message, context=context, decision=decision
    )
    return result.model_copy(update={"demo": True})


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def build_registry(
    *,
    demo: bool = False,
    model: Any = None,
    kb: Any = None,
    moves: Any = None,
    skeletons: Any = None,
    policy: Any = None,
) -> Mapping[str, BaseTool]:
    """The immutable registry for one run (§12.2, *Registry*).

    ``demo=True`` must come from the caller's explicit selection (D47); it is
    never chosen because a component is missing.  The KB tools are available
    only with a configured knowledge base *kb*; retrieval also needs the move
    graph *moves* of ADR 0007.  *skeletons* is the ``SkeletonIndex`` the
    enumeration fills so that the move graph can resolve its keys, and
    *policy* the deployment's ``SupervisorPolicy`` (default policy if
    omitted).
    """
    from sqldim.core.query.dgm.nl.supervisor._tools_lattice import (
        WhatCanIAnswerTool,
    )
    from sqldim.core.query.dgm.nl.supervisor._tools_space import (
        MapAnalyticalSpaceTool,
    )

    tools: dict[str, BaseTool] = {name: UnavailableTool(name) for name in TOOL_SCHEMAS}
    from sqldim.core.query.dgm.nl.supervisor._tools_invariants import (
        ValidateInvariantTool,
    )

    tools["dgm_validate_invariant"] = ValidateInvariantTool()
    tools["dgm_map_analytical_space"] = MapAnalyticalSpaceTool()
    tools["dgm_what_can_i_answer"] = (
        WhatCanIAnswerTool(index=skeletons)
        if policy is None
        else WhatCanIAnswerTool(policy, index=skeletons)
    )
    if demo:
        tools["dgm_query"] = DemoQueryTool(model=model)
    if kb is not None:
        from sqldim.core.query.dgm.nl.supervisor._tools_kb import kb_tools

        tools.update(kb_tools(moves))
    return MappingProxyType(tools)


TOOL_REGISTRY: Mapping[str, BaseTool] = build_registry()
