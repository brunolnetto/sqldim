"""Tests for the supervisor LangGraph graph."""

from __future__ import annotations


class TestSupervisorGraphStructure:
    """The supervisor graph must compile and have expected nodes."""

    def test_graph_compiles(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        assert graph is not None

    def test_graph_has_expected_nodes(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import SUPERVISOR_NODE_NAMES

        expected = {
            "plan",
            "reflect",
            "select_tool",
            "execute_tool",
            "synthesize",
            "respond",
        }
        assert expected == set(SUPERVISOR_NODE_NAMES)


class TestSupervisorGraphStubExecution:
    """The graph runs end-to-end in stub mode (no LLM)."""

    def test_stub_invoke_returns_state(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        graph = build_supervisor_graph()
        initial = SupervisorState(utterance="Compare ecommerce and social_network")
        result = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-supervisor"},
                "recursion_limit": 25,
            },
        )
        assert "utterance" in result
        assert "visited_nodes" in result

    def test_stub_visits_all_nodes(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        graph = build_supervisor_graph()
        initial = SupervisorState(utterance="test question")
        result = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-supervisor-nodes"},
                "recursion_limit": 25,
            },
        )
        visited = result.get("visited_nodes", [])
        assert "plan" in visited
        assert "respond" in visited


class TestRouteAfterExecution:
    """route_after_execution defensive branches."""

    def test_returns_done_when_plan_is_none(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import route_after_execution
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test")  # plan is None
        assert route_after_execution(state) == "done"


class TestStubPlan:
    """_stub_plan produces a single dgm_query step."""

    def test_stub_plan_creates_dgm_query_step(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_plan
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="top products")
        out = _stub_plan(state)
        plan = out["plan"]
        assert out["current_step"] == 0
        assert plan.steps[0].tool_name == "dgm_query"
        assert plan.steps[0].arguments["question"] == "top products"


class TestStubExecuteTool:
    """_stub_execute_tool covers known-tool, no-plan, and unknown-tool branches."""

    def test_no_plan_returns_empty_dict(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_execute_tool
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test")  # plan is None
        assert _stub_execute_tool(state) == {}

    def test_known_tool_succeeds(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_execute_tool
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            SupervisorState,
            ToolCallRequest,
        )

        plan = SupervisorPlan(
            goal="test",
            steps=[
                ToolCallRequest(
                    tool_name="dgm_what_can_i_answer",
                    arguments={"dataset": "__all__"},
                    budget_fraction=1.0,
                )
            ],
        )
        state = SupervisorState(utterance="test", plan=plan)
        out = _stub_execute_tool(state)
        assert len(out["results"]) == 1
        assert out["results"][0].success is True

    def test_unknown_tool_returns_failure(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_execute_tool
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            SupervisorState,
            ToolCallRequest,
        )

        plan = SupervisorPlan(
            goal="test",
            steps=[
                ToolCallRequest(
                    tool_name="dgm_nonexistent_tool",
                    arguments={},
                    budget_fraction=1.0,
                )
            ],
        )
        state = SupervisorState(utterance="test", plan=plan)
        out = _stub_execute_tool(state)
        assert len(out["results"]) == 1
        assert out["results"][0].success is False


class TestStubSynthesize:
    """_stub_synthesize covers all render branches."""

    def test_failed_result_renders_warning(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        state = SupervisorState(
            utterance="test",
            results=[
                ToolCallResult(tool_name="dgm_query", success=False, error="timeout")
            ],
        )
        out = _stub_synthesize(state)
        assert "timeout" in out["synthesis"]

    def test_single_dataset_with_questions(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        state = SupervisorState(
            utterance="test",
            results=[
                ToolCallResult(
                    tool_name="dgm_what_can_i_answer",
                    success=True,
                    data={
                        "datasets": ["ecommerce"],
                        "questions": ["top products", "revenue?"],
                    },
                )
            ],
        )
        out = _stub_synthesize(state)
        assert "Questions for 'ecommerce'" in out["synthesis"]
        assert "top products" in out["synthesis"]

    def test_single_dataset_no_questions_with_model(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        state = SupervisorState(
            utterance="test",
            results=[
                ToolCallResult(
                    tool_name="dgm_what_can_i_answer",
                    success=True,
                    data={"datasets": ["ecommerce"], "questions": []},
                )
            ],
        )
        out = _stub_synthesize(state, has_model=True)
        assert "Dataset 'ecommerce' is available." in out["synthesis"]
        assert "--model" not in out["synthesis"]

    def test_empty_tool_list_registered(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        state = SupervisorState(
            utterance="test",
            results=[
                ToolCallResult(
                    tool_name="dgm_map_analytical_space",
                    success=True,
                    data={"tools": []},
                )
            ],
        )
        out = _stub_synthesize(state)
        assert "No tools registered." in out["synthesis"]

    def test_dgm_query_with_model_omits_stub_hint(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        state = SupervisorState(
            utterance="top products",
            results=[
                ToolCallResult(
                    tool_name="dgm_query",
                    success=True,
                    data={"dataset": "ecommerce", "question": "top products"},
                )
            ],
        )
        out = _stub_synthesize(state, has_model=True)
        assert "completed." in out["synthesis"]
        assert "stub" not in out["synthesis"]


class TestRunPipelineDefaultContext:
    """run_pipeline auto-discovers context when none is passed."""

    def test_run_without_context_uses_known_datasets(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import run_pipeline

        output = run_pipeline("help")  # no context → triggers KNOWN_DATASETS discovery
        assert output.utterance == "help"
        assert len(output.visited_nodes) > 0


class TestConversationHistory:
    """The graph threads conversation history through turns."""

    def test_synthesize_appends_to_history(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        graph = build_supervisor_graph()
        initial = SupervisorState(utterance="What datasets do you have?")
        result = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-history-1"},
                "recursion_limit": 25,
            },
        )
        history = result.get("conversation_history", [])
        assert len(history) >= 2
        assert history[0]["role"] == "user"
        assert history[1]["role"] == "assistant"

    def test_history_carries_across_invocations(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        graph = build_supervisor_graph()

        # First turn
        result1 = graph.invoke(
            SupervisorState(utterance="first question").model_dump(),
            config={
                "configurable": {"thread_id": "test-history-2a"},
                "recursion_limit": 25,
            },
        )
        history1 = result1.get("conversation_history", [])

        # Second turn — carry forward history
        result2 = graph.invoke(
            SupervisorState(
                utterance="follow-up",
                conversation_history=list(history1),
            ).model_dump(),
            config={
                "configurable": {"thread_id": "test-history-2b"},
                "recursion_limit": 25,
            },
        )
        history2 = result2.get("conversation_history", [])
        # Should have entries from both turns
        assert len(history2) >= 4
        user_entries = [h for h in history2 if h["role"] == "user"]
        assert len(user_entries) >= 2

    def test_context_memory_updated_by_plan_node(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorContext,
            SupervisorState,
        )

        context = SupervisorContext(available_datasets=["ecommerce"])
        graph = build_supervisor_graph(context=context)
        previous = [{"role": "user", "content": "earlier"}]
        graph.invoke(
            SupervisorState(
                utterance="test",
                conversation_history=previous,
            ).model_dump(),
            config={
                "configurable": {"thread_id": "test-ctx-mem"},
                "recursion_limit": 25,
            },
        )
        # Context memory should have been populated by plan node
        assert context.memory == previous
