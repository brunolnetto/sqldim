"""Tests for the supervisor LangGraph graph."""

from __future__ import annotations


class TestSupervisorGraphStructure:
    """The supervisor graph must compile and have expected nodes."""

    def test_graph_compiles(self) -> None:
        from sqldim.core.query.dgm.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        assert graph is not None

    def test_graph_has_expected_nodes(self) -> None:
        from sqldim.core.query.dgm.supervisor._graph import SUPERVISOR_NODE_NAMES

        expected = {"plan", "select_tool", "execute_tool", "synthesize", "respond"}
        assert expected == set(SUPERVISOR_NODE_NAMES)


class TestSupervisorGraphStubExecution:
    """The graph runs end-to-end in stub mode (no LLM)."""

    def test_stub_invoke_returns_state(self) -> None:
        from sqldim.core.query.dgm.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.supervisor._types import SupervisorState

        graph = build_supervisor_graph()
        initial = SupervisorState(utterance="Compare ecommerce and social_network")
        result = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-supervisor"},
                "recursion_limit": 10,
            },
        )
        assert "utterance" in result
        assert "visited_nodes" in result

    def test_stub_visits_all_nodes(self) -> None:
        from sqldim.core.query.dgm.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.supervisor._types import SupervisorState

        graph = build_supervisor_graph()
        initial = SupervisorState(utterance="test question")
        result = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-supervisor-nodes"},
                "recursion_limit": 10,
            },
        )
        visited = result.get("visited_nodes", [])
        assert "plan" in visited
        assert "respond" in visited
