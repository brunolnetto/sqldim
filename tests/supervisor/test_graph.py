"""§12.4 supervisor graph: routing, recovery, outcomes and isolation."""

from __future__ import annotations

import json

import pytest

from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._graph import (
    SUPERVISOR_NODE_NAMES,
    build_supervisor_graph,
    run_supervisor,
)
from sqldim.core.query.dgm.nl.supervisor._planner import (
    KeywordDemoPlanner,
    PlannerView,
)
from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY, DemoQueryTool
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    Outcome,
    SupervisorPlan,
    ToolErrorKind,
)
from tests.supervisor._fakes import (
    FakeTool,
    RaisingPlanner,
    ScriptedPlanner,
    auth,
    fail,
    plan,
    query_turn,
    registry,
    runtime,
    step,
    succeed,
)

CORRELATE_ARGS = {"left": "a", "right": "b", "keys": ["region"]}


def _plan_from(view: PlannerView, *steps) -> SupervisorPlan:  # type: ignore[no-untyped-def]
    return plan(*steps, plan_id=view.plan_id, version=view.version)


class TestStructure:
    def test_nodes_match_spec(self) -> None:
        rt = runtime(registry(FakeTool("dgm_query")), ScriptedPlanner(plan()))
        graph = build_supervisor_graph(rt)
        assert set(SUPERVISOR_NODE_NAMES) <= set(graph.get_graph().nodes)
        assert "reflect" not in graph.get_graph().nodes


class TestFailClosedEntry:
    def test_no_planner_fails_without_running_anything(self) -> None:
        state = run_supervisor("q", runtime(registry(FakeTool("dgm_query")), None))
        assert state.outcome is Outcome.FAILED
        assert state.planning_error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert state.visited_nodes == ["plan", "respond"]
        assert "Outcome: FAILED" in state.synthesis

    def test_no_available_tool_fails_before_planning(self) -> None:
        planner = ScriptedPlanner(plan(step("a")))
        state = run_supervisor("q", runtime(TOOL_REGISTRY, planner))
        assert state.outcome is Outcome.FAILED
        assert state.planning_error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert planner.views == []
        assert set(state.planning_error.context["missing"]) == set(TOOL_REGISTRY)

    def test_planner_failure_is_reported_not_replaced(self) -> None:
        state = run_supervisor(
            "how many orders?",
            runtime(registry(FakeTool("dgm_query")), RaisingPlanner()),
        )
        assert state.outcome is Outcome.FAILED
        assert state.planning_error.kind is ToolErrorKind.TOOL_FAILURE
        assert state.planning_error.context["exception"] == "RuntimeError"
        assert state.plan is None
        assert state.results == {}

    def test_keyword_planner_requires_demo_mode(self) -> None:
        with pytest.raises(ValueError, match="demo mode"):
            runtime(registry(FakeTool("dgm_query")), KeywordDemoPlanner())

    def test_demo_tool_requires_demo_mode(self) -> None:
        with pytest.raises(ValueError, match="demo mode"):
            runtime(registry(DemoQueryTool()), ScriptedPlanner(plan()))


class TestHappyPath:
    def test_complete(self) -> None:
        tool = FakeTool("dgm_query")
        state = run_supervisor(
            "q",
            runtime(
                registry(tool), ScriptedPlanner(lambda v: _plan_from(v, step("a")))
            ),
        )
        assert state.outcome is Outcome.COMPLETE
        assert state.coverage == 1.0
        assert state.results["a"].status is CallStatus.SUCCEEDED
        assert "Outcome: COMPLETE" in state.synthesis
        assert "counted" in state.synthesis
        assert len(tool.calls) == 1
        assert tool.calls[0].datasets == ["sales"]
        assert tool.calls[0].idempotency_key

    def test_planner_sees_only_readable_datasets_and_available_tools(self) -> None:
        planner = ScriptedPlanner(lambda v: _plan_from(v))
        grants = {"sales": {Action.READ}, "kb_only": {Action.WRITE_KB}}
        run_supervisor(
            "q",
            runtime(registry(FakeTool("dgm_query")), planner, the_auth=auth(grants)),
        )
        view = planner.views[0]
        assert view.datasets == ("sales",)
        assert [t.name for t in view.tools] == ["dgm_query"]

    def test_empty_plan_is_failed_with_reason(self) -> None:
        empty = SupervisorPlan(
            plan_id="x", version=1, goal="g", confidence=0.4, reasoning="nothing to do"
        )
        state = run_supervisor(
            "q", runtime(registry(FakeTool("dgm_query")), ScriptedPlanner(empty))
        )
        assert state.outcome is Outcome.FAILED
        assert "nothing to do" in state.synthesis

    def test_results_synthesized_in_plan_order(self) -> None:
        tool = FakeTool(
            "dgm_query",
            succeed(query_turn([{"first": 1}])),
            succeed(query_turn([{"second": 2}])),
        )
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a"), step("b")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        text = state.synthesis
        assert text.index("[a]") < text.index("[b]")


class TestFailuresAndRouting:
    def test_failed_step_skips_dependents_and_is_partial(self) -> None:
        query = FakeTool(
            "dgm_query",
            succeed(query_turn()),
            fail(ToolErrorKind.UNSUPPORTED),
        )
        correlate = FakeTool("dgm_correlate")
        planner = ScriptedPlanner(
            lambda v: _plan_from(
                v,
                step("a", fraction=0.3),
                step("b", fraction=0.3),
                step(
                    "c",
                    "dgm_correlate",
                    CORRELATE_ARGS,
                    depends_on=["a", "b"],
                    fraction=0.3,
                ),
            )
        )
        grants = auth({"sales": {Action.READ, Action.CORRELATE}})
        state = run_supervisor(
            "q", runtime(registry(query, correlate), planner, the_auth=grants)
        )
        assert state.outcome is Outcome.PARTIAL
        assert state.coverage == pytest.approx(1 / 3)
        assert state.statuses["c"] is CallStatus.SKIPPED
        assert state.results["c"].error.kind is ToolErrorKind.DEPENDENCY_NOT_MET
        assert state.results["c"].attempts == 0
        assert correlate.calls == []
        assert "Coverage 1/3" in state.synthesis
        assert "not a complete answer" in state.synthesis

    def test_all_failed_is_failed(self) -> None:
        tool = FakeTool("dgm_query", fail(ToolErrorKind.UNSUPPORTED))
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.FAILED
        assert "UNSUPPORTED" in state.synthesis

    def test_retryable_failure_is_retried_with_same_key(self) -> None:
        tool = FakeTool(
            "dgm_query",
            fail(ToolErrorKind.TIMEOUT, retryable=True),
            succeed(query_turn()),
        )
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.COMPLETE
        assert state.results["a"].attempts == 2
        assert len({c.idempotency_key for c in tool.calls}) == 1

    def test_retries_bounded_by_max_attempts(self) -> None:
        tool = FakeTool("dgm_query", fail(ToolErrorKind.TIMEOUT, retryable=True))
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert len(tool.calls) == 2  # DEFAULT_POLICY.max_attempts
        assert state.outcome is Outcome.FAILED

    def test_non_idempotent_tool_is_not_retried(self) -> None:
        tool = FakeTool(
            "dgm_query", fail(ToolErrorKind.TIMEOUT, retryable=True), idempotent=False
        )
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        run_supervisor("q", runtime(registry(tool), planner))
        assert len(tool.calls) == 1

    def test_unauthorized_is_not_retried_or_replanned(self) -> None:
        tool = FakeTool("dgm_query", fail(ToolErrorKind.UNAUTHORIZED))
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert len(tool.calls) == 1
        assert len(planner.views) == 1
        assert state.outcome is Outcome.FAILED

    def test_rejection_replans_with_new_version(self) -> None:
        tool = FakeTool(
            "dgm_query",
            fail(ToolErrorKind.BUDGET_REJECTED, status=CallStatus.REJECTED),
            succeed(query_turn()),
        )
        smaller = step("a2", arguments={"dataset": "sales", "utterance": "narrower"})
        planner = ScriptedPlanner(
            lambda v: _plan_from(v, step("a")),
            lambda v: _plan_from(v, smaller),
        )
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.COMPLETE
        assert state.plan.version == 2
        assert state.replans == 1
        replan_view = planner.views[1]
        assert replan_view.rejection.call_id == "a"
        assert replan_view.previous.version == 1
        assert [c.call_id for c in tool.calls] == ["a", "a2"]

    def test_rejected_call_repeated_unchanged_is_invalid(self) -> None:
        tool = FakeTool(
            "dgm_query", fail(ToolErrorKind.BUDGET_REJECTED, status=CallStatus.REJECTED)
        )
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.ABORTED
        assert len(tool.calls) == 1  # the repeated call never ran
        assert state.abort_diagnostic["reason"] == "replan limit reached"

    def test_rejection_after_replan_limit_aborts_with_diagnostic(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._schemas import SupervisorPolicy

        tool = FakeTool(
            "dgm_query", fail(ToolErrorKind.BUDGET_REJECTED, status=CallStatus.REJECTED)
        )
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        rt = runtime(registry(tool), planner, policy=SupervisorPolicy(max_replans=0))
        state = run_supervisor("q", rt)
        assert state.outcome is Outcome.ABORTED
        diag = state.abort_diagnostic
        assert diag["call_id"] == "a"
        assert diag["error"]["kind"] == "BUDGET_REJECTED"
        assert "Outcome: ABORTED" in state.synthesis

    def test_invalid_plan_never_runs_and_is_replanned(self) -> None:
        tool = FakeTool("dgm_query")
        planner = ScriptedPlanner(
            lambda v: _plan_from(v, step("a", tool="dgm_magic")),
            lambda v: _plan_from(v, step("a")),
        )
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.COMPLETE
        assert planner.views[1].violations[0].kind is ToolErrorKind.UNKNOWN_TOOL
        assert len(tool.calls) == 1

    def test_persistently_invalid_plan_aborts(self) -> None:
        tool = FakeTool("dgm_query")
        planner = ScriptedPlanner(
            lambda v: _plan_from(
                v, step("a", arguments={"dataset": "secret", "utterance": "x"})
            )
        )
        state = run_supervisor("q", runtime(registry(tool), planner))
        assert state.outcome is Outcome.ABORTED
        assert tool.calls == []
        kinds = {v["kind"] for v in state.abort_diagnostic["violations"]}
        assert kinds == {"UNAUTHORIZED"}

    def test_cancelled_run_marks_pending_steps(self) -> None:
        tool = FakeTool("dgm_query")
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a"), step("b")))
        rt = runtime(registry(tool), planner)
        rt.cancel.cancel()
        state = run_supervisor("q", rt)
        assert state.outcome is Outcome.CANCELLED
        assert {s.value for s in state.statuses.values()} == {"CANCELLED"}
        assert tool.calls == []


class TestIsolation:
    def test_checkpointed_state_holds_no_identity(self) -> None:
        planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
        rt = runtime(registry(FakeTool("dgm_query")), planner)
        state = run_supervisor("q", rt)
        dumped = json.dumps(state.model_dump(mode="json"))
        assert "alice" not in dumped
        assert "grants" not in dumped

    def test_threads_are_keyed_by_tenant(self) -> None:
        from langgraph.checkpoint.memory import MemorySaver

        saver = MemorySaver()
        for tenant in ("t1", "t2"):
            planner = ScriptedPlanner(lambda v: _plan_from(v, step("a")))
            rt = runtime(
                registry(FakeTool("dgm_query")),
                planner,
                the_auth=auth({"sales": {Action.READ}}, tenant=tenant),
            )
            run_supervisor("q", rt, thread_id="same", checkpointer=saver)
        threads = {c.config["configurable"]["thread_id"] for c in saver.list(None)}
        assert {"t1:same", "t2:same"} <= threads


class TestDemoMode:
    def test_demo_runs_are_marked(self) -> None:
        tool = FakeTool("dgm_query")
        planner = KeywordDemoPlanner()
        state = run_supervisor(
            "how many orders were placed?",
            runtime(
                registry(tool),
                planner,
                the_auth=auth({"commerce": {Action.READ}}),
                demo=True,
            ),
        )
        assert state.demo is True
        assert state.synthesis.startswith("[demo mode")
