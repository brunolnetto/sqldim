"""Tests for supervisor agent type definitions."""

from __future__ import annotations

import pytest


class TestToolCapabilitySchema:
    """Each tool must declare a ToolCapabilitySchema."""

    def test_capability_schema_has_required_fields(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCapabilitySchema

        schema = ToolCapabilitySchema(
            name="dgm_query",
            description="Execute a DGM query against a dataset",
            input_schema={"dataset": "str", "question": "str"},
            output_schema={"result": "Any", "explanation": "str"},
            composable_with=["dgm_correlate", "dgm_explore_from"],
        )
        assert schema.name == "dgm_query"
        assert "dataset" in schema.input_schema

    def test_capability_schema_frozen(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCapabilitySchema

        schema = ToolCapabilitySchema(
            name="dgm_query",
            description="test",
            input_schema={},
            output_schema={},
            composable_with=[],
        )
        with pytest.raises(AttributeError):
            schema.name = "changed"  # type: ignore[misc]


class TestToolCallRequest:
    """ToolCallRequest is the supervisor's instruction to invoke a tool."""

    def test_request_creation(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallRequest

        req = ToolCallRequest(
            tool_name="dgm_query",
            arguments={"dataset": "ecommerce", "question": "top products"},
            budget_fraction=0.25,
        )
        assert req.tool_name == "dgm_query"
        assert req.budget_fraction == 0.25

    def test_budget_fraction_range(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallRequest

        with pytest.raises(ValueError, match="budget_fraction"):
            ToolCallRequest(
                tool_name="t",
                arguments={},
                budget_fraction=1.5,
            )


class TestToolCallResult:
    """ToolCallResult wraps a single tool invocation output."""

    def test_success_result(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        result = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={"rows": 42},
            cost_actual=100,
        )
        assert result.success is True
        assert result.data == {"rows": 42}

    def test_failure_result_has_error(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        result = ToolCallResult(
            tool_name="dgm_query",
            success=False,
            error="Budget exceeded",
            cost_actual=0,
        )
        assert result.success is False
        assert result.error == "Budget exceeded"


class TestSupervisorPlan:
    """SupervisorPlan decomposes a user question into ordered tool calls."""

    def test_plan_creation(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            ToolCallRequest,
        )

        plan = SupervisorPlan(
            goal="Compare ecommerce and social_network",
            steps=[
                ToolCallRequest(
                    tool_name="dgm_query",
                    arguments={"dataset": "ecommerce", "question": "top segments"},
                    budget_fraction=0.4,
                ),
                ToolCallRequest(
                    tool_name="dgm_query",
                    arguments={"dataset": "social_network", "question": "influencers"},
                    budget_fraction=0.4,
                ),
                ToolCallRequest(
                    tool_name="dgm_correlate",
                    arguments={"left": "$0", "right": "$1"},
                    budget_fraction=0.2,
                ),
            ],
        )
        assert len(plan.steps) == 3
        assert plan.total_budget_fraction == pytest.approx(1.0)

    def test_plan_budget_exceeds_one_raises(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            ToolCallRequest,
        )

        with pytest.raises(ValueError, match="budget"):
            SupervisorPlan(
                goal="oops",
                steps=[
                    ToolCallRequest(
                        tool_name="a",
                        arguments={},
                        budget_fraction=0.8,
                    ),
                    ToolCallRequest(
                        tool_name="b",
                        arguments={},
                        budget_fraction=0.8,
                    ),
                ],
            )


class TestSupervisorState:
    """SupervisorState threads through the LangGraph supervisor."""

    def test_initial_state(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="Compare ecommerce and social")
        assert state.utterance == "Compare ecommerce and social"
        assert state.plan is None
        assert state.results == []
        assert state.synthesis is None

    def test_state_tracks_visited_nodes(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(
            utterance="test",
            visited_nodes=["plan", "execute"],
        )
        assert len(state.visited_nodes) == 2

    def test_state_has_conversation_history(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(
            utterance="follow-up question",
            conversation_history=[
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "first answer"},
            ],
        )
        assert len(state.conversation_history) == 2
        assert state.conversation_history[0]["role"] == "user"

    def test_state_conversation_history_defaults_empty(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test")
        assert state.conversation_history == []

    def test_confidence_out_of_range_raises(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            ToolCallRequest,
        )

        with pytest.raises(ValueError, match="confidence"):
            SupervisorPlan(
                goal="test",
                steps=[
                    ToolCallRequest(tool_name="t", arguments={}, budget_fraction=1.0)
                ],
                confidence=1.5,
            )


class TestSupervisorContext:
    """SupervisorContext carries runtime deps."""

    def test_context_lists_available_datasets(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        ctx = SupervisorContext(
            available_datasets=["ecommerce", "social_network"],
            budget=None,
        )
        assert "social_network" in ctx.available_datasets

    def test_context_has_memory(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        ctx = SupervisorContext(
            available_datasets=["ecommerce"],
            memory=[{"role": "user", "content": "earlier question"}],
        )
        assert len(ctx.memory) == 1

    def test_context_memory_defaults_empty(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        ctx = SupervisorContext(available_datasets=[])
        assert ctx.memory == []
