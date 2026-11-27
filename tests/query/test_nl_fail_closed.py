"""#129 / ADR 0012 / D47: the NL graph never hides a failure.

Outside demo mode the graph is not built.  Inside it, each node that cannot
do its work sets a typed ``error``; nothing is replaced by a placeholder
query, by another strategy or by an empty result.
"""

from __future__ import annotations

import asyncio
from typing import Any

import duckdb
import pytest

from sqldim.application.ask import build_registry_from_schema, make_default_budget
from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
from sqldim.core.query.dgm.nl._graph import NLPipelineUnavailable, build_nl_graph
from sqldim.core.query.dgm.nl._node_impls import (
    make_llm_nodes,
    make_operational_nodes,
)
from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._tools import DemoQueryTool
from sqldim.core.query.dgm.nl.supervisor._types import CallStatus, ToolErrorKind
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import auth, plan, step, tool_context


def _ctx(tables: dict[str, str] | None = None) -> DGMContext:
    con = duckdb.connect()
    for name, ddl in (tables or {"orders": "id INTEGER, amount DOUBLE"}).items():
        con.execute(f"CREATE TABLE {name} ({ddl})")
        con.execute(f"INSERT INTO {name} VALUES (1, 2.0)")
    registry = build_registry_from_schema(con, list(tables or {"orders": ""}))
    return DGMContext(entity_registry=registry, budget=make_default_budget(), con=con)


class _Raising:
    def run_sync(self, *a: Any, **kw: Any) -> Any:
        raise RuntimeError("model unreachable")


def _run(ctx: DGMContext, utterance: str, **kw: Any) -> dict[str, Any]:
    graph = build_nl_graph(context=ctx, demo=True, **kw)
    return graph.invoke(
        {"utterance": utterance},
        config={"configurable": {"thread_id": utterance}, "recursion_limit": 25},
    )


class TestNotBuiltOutsideDemo:
    @pytest.mark.parametrize("kw", [{}, {"model": object()}, {"context": object()}])
    def test_no_selection_no_graph(self, kw: dict[str, Any]) -> None:
        with pytest.raises(NLPipelineUnavailable):
            build_nl_graph(**kw)


class TestDemoStates:
    def test_every_state_is_marked_demo(self) -> None:
        state = _run(_ctx(), "list orders")
        assert state["demo"] is True
        assert state.get("error") is None
        assert state["result"]["count"] == 1

    def test_genuine_empty_result_is_a_result(self) -> None:
        ctx = _ctx({"orders": "id INTEGER, amount DOUBLE"})
        ctx.con.execute("DELETE FROM orders")
        state = _run(ctx, "list orders")
        assert state.get("error") is None
        assert state["result"]["count"] == 0


class TestNoSubstitution:
    def test_sql_agent_failure_is_not_replaced_by_the_template(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl._node_impls.make_sql_agent",
            lambda model: _Raising(),
        )
        nodes = make_operational_nodes(_ctx(), model=object())
        out = nodes["dag_construction"](NLInterfaceState(utterance="total amount"))
        assert out["q_current"] is None
        assert out["error"]["kind"] == "TOOL_FAILURE"
        assert "sql agent failed" in out["error"]["message"]

    def test_entity_agent_failure_does_not_widen_to_every_table(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for name in ("entity", "temporal", "compositional", "ranking", "explanation"):
            monkeypatch.setattr(
                f"sqldim.core.query.dgm.nl._node_impls.make_{name}_agent",
                lambda model: _Raising(),
            )
        ctx = _ctx(
            {"orders": "id INTEGER, amount DOUBLE", "users": "id INTEGER, x INT"}
        )
        nodes = make_llm_nodes(ctx, model=object())
        out = nodes["entity_resolution"](NLInterfaceState(utterance="list orders"))
        assert out == {
            "error": {
                "kind": "TOOL_FAILURE",
                "message": "the entity_result agent failed: RuntimeError: "
                "model unreachable",
            }
        }
        # Downstream nodes do nothing once the turn has failed.
        failed = NLInterfaceState(utterance="list orders", error=out["error"])
        ops = make_operational_nodes(ctx)
        assert ops["candidate_generation"](failed) == {}
        assert ops["dag_construction"](failed) == {}


class TestNoFictitiousResult:
    def test_unresolved_table_is_not_a_placeholder_query(self) -> None:
        ctx = DGMContext(
            entity_registry=build_registry_from_schema(duckdb.connect(), []),
            budget=make_default_budget(),
            con=duckdb.connect(),
        )
        state = _run(ctx, "show me something")
        assert state["q_current"] is None
        assert state["error"]["kind"] == "NEEDS_CLARIFICATION"
        assert "budget_gate" not in state["visited_nodes"]

    def test_failed_execution_is_not_an_empty_result(self) -> None:
        nodes = make_operational_nodes(_ctx())
        out = nodes["execution"](
            NLInterfaceState(utterance="x", q_current="SELECT nope FROM orders")
        )
        assert out["result"] is None
        assert out["error"]["kind"] == "TOOL_FAILURE"

    def test_gate_refuses_to_run_without_a_query(self) -> None:
        nodes = make_operational_nodes(_ctx())
        with pytest.raises(ValueError, match="q_current"):
            nodes["budget_gate"](NLInterfaceState(utterance="x"))

    def test_sum_without_a_measure_is_not_guessed(self) -> None:
        ctx = _ctx({"orders": "id INTEGER, label VARCHAR"})
        state = _run(ctx, "total revenue by label")
        assert state["error"]["kind"] == "NEEDS_CLARIFICATION"
        assert "SUM" in state["error"]["message"]


class TestDemoQueryToolSurfacesErrors:
    def test_state_error_becomes_a_typed_demo_failure(self) -> None:
        empty = DGMContext(
            entity_registry=build_registry_from_schema(duckdb.connect(), []),
            budget=make_default_budget(),
            con=duckdb.connect(),
        )

        class Source:
            def setup(self) -> None: ...
            def teardown(self) -> None: ...
            def get_connection(self) -> Any:
                return empty.con

            def get_table_names(self) -> list[str]:
                return []

        the_auth = auth({"sales": {Action.READ}})
        ctx = tool_context(the_auth, demo=True)
        ctx = type(ctx)(**{**ctx.__dict__, "data": ctx.data.scoped(["sales"])})
        ctx.data._opener = lambda ds: Source()  # type: ignore[attr-defined]
        bound, _ = bind_plan(
            plan(step("q1", arguments={"dataset": "sales", "utterance": "x"}))
        )
        result = asyncio.run(DemoQueryTool().execute(bound.steps[0], ctx))
        assert result.status is CallStatus.FAILED
        assert result.demo is True
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.NEEDS_CLARIFICATION


class TestGateDecisionsReachTheSupervisor:
    def _run(self, decision: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
        from sqldim.core.query.dgm.planner._gate import ExecutionGate

        monkeypatch.setattr(ExecutionGate, "gate", lambda self, p, b: decision)
        ctx_nl = _ctx()

        class Source:
            def setup(self) -> None: ...
            def teardown(self) -> None: ...
            def get_connection(self) -> Any:
                return ctx_nl.con

            def get_table_names(self) -> list[str]:
                return ["orders"]

        ctx = tool_context(auth({"sales": {Action.READ}}), demo=True)
        ctx = type(ctx)(**{**ctx.__dict__, "data": ctx.data.scoped(["sales"])})
        ctx.data._opener = lambda ds: Source()  # type: ignore[attr-defined]
        bound, _ = bind_plan(
            plan(step("q1", arguments={"dataset": "sales", "utterance": "list orders"}))
        )
        return asyncio.run(DemoQueryTool().execute(bound.steps[0], ctx))

    def test_gate_reject_is_a_budget_rejection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from sqldim.core.query.dgm.planner._gate import (
            BudgetDecisionKind,
            RejectDecision,
        )

        result = self._run(RejectDecision(reason="too expensive"), monkeypatch)
        assert result.status is CallStatus.REJECTED
        assert result.decision is BudgetDecisionKind.REJECT
        assert result.error.kind is ToolErrorKind.BUDGET_REJECTED
        assert result.demo is True

    def test_gate_clarify_is_returned_not_confirmed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from sqldim.core.query.dgm.planner._gate import (
            BudgetDecisionKind,
            ClarifyDecision,
            ClarifyKind,
            ClarifyOption,
        )
        from sqldim.core.query.dgm.planner._targets import (
            CostEstimate,
            ExportPlan,
            QueryTarget,
        )

        p = ExportPlan(
            query_target=QueryTarget.SQL_DUCKDB,
            query_text="SELECT id FROM orders",
            cost_estimate=CostEstimate(cpu_ops=5, io_ops=0),
        )
        decision = ClarifyDecision(
            options=(ClarifyOption(p, p.cost_estimate, ClarifyKind.REFINEMENT),)
        )
        result = self._run(decision, monkeypatch)
        assert result.status is CallStatus.REJECTED
        assert result.decision is BudgetDecisionKind.CLARIFY
        assert result.error.kind is ToolErrorKind.NEEDS_CLARIFICATION
