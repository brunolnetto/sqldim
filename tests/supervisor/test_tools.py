"""Tests for the 8 supervisor DGM tools."""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.supervisor._types import (
    ToolCallRequest,
    ToolCallResult,
)


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

_EXPECTED_TOOLS = {
    "dgm_query",
    "dgm_what_can_i_answer",
    "dgm_correlate",
    "dgm_explore_from",
    "dgm_sensitivity_sweep",
    "dgm_validate_invariant",
    "dgm_store_finding",
    "dgm_retrieve_related",
    "dgm_map_analytical_space",
}


class TestToolRegistry:
    """All 9 tools must be registered in TOOL_REGISTRY."""

    def test_all_tools_registered(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        assert set(TOOL_REGISTRY.keys()) == _EXPECTED_TOOLS

    def test_each_tool_has_capability_schema(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        for name, tool in TOOL_REGISTRY.items():
            schema = tool.capability_schema
            assert schema.name == name, f"Tool {name} schema mismatch"

    def test_each_tool_has_cost_estimate(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        for name, tool in TOOL_REGISTRY.items():
            est = tool.cost_estimate({})
            assert isinstance(est, (int, float)), f"Tool {name} cost not numeric"
            assert est >= 0, f"Tool {name} cost negative"


class TestToolContract:
    """Each tool fulfils the 5-point formal contract."""

    def test_tools_have_execute(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        for name, tool in TOOL_REGISTRY.items():
            assert callable(getattr(tool, "execute", None)), f"{name} missing execute"

    def test_tools_have_explain(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        for name, tool in TOOL_REGISTRY.items():
            assert callable(getattr(tool, "explain", None)), f"{name} missing explain"

    def test_tools_have_compose(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        for name, tool in TOOL_REGISTRY.items():
            assert callable(getattr(tool, "compose", None)), f"{name} missing compose"


class TestDgmQueryTool:
    """dgm_query wraps run_ask_from_source."""

    def test_stub_execute_returns_result(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        tool = TOOL_REGISTRY["dgm_query"]
        result = tool.execute(
            {"dataset": "social_network", "question": "who follows whom"},
            context=None,
        )
        assert isinstance(result, ToolCallResult)
        assert result.tool_name == "dgm_query"

    def test_explain_returns_string(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        tool = TOOL_REGISTRY["dgm_query"]
        explanation = tool.explain(
            {"dataset": "social_network", "question": "test"},
        )
        assert isinstance(explanation, str)
        assert len(explanation) > 0


class TestDgmWhatCanIAnswerTool:
    """dgm_what_can_i_answer lists answerable questions."""

    def test_stub_returns_list(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        tool = TOOL_REGISTRY["dgm_what_can_i_answer"]
        result = tool.execute({"dataset": "social_network"}, context=None)
        assert isinstance(result, ToolCallResult)


class TestDgmMapAnalyticalSpaceTool:
    """dgm_map_analytical_space maps the vocabulary decomposition."""

    def test_stub_returns_result(self) -> None:
        from sqldim.core.query.dgm.supervisor._tools import TOOL_REGISTRY

        tool = TOOL_REGISTRY["dgm_map_analytical_space"]
        result = tool.execute({"dataset": "social_network"}, context=None)
        assert isinstance(result, ToolCallResult)
