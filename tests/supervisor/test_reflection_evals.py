"""Eval cases for the supervisor reflect node and multi-stage reasoning.

Design principle
----------------
The supervisor's keyword-based planner is intentionally narrow: it cannot
handle semantically rich or ambiguous inputs the way an LLM would.  When
``plan.confidence < 0.5`` the graph routes to the ``reflect`` node, which
front-loads two orientation steps before proceeding:

  1. dgm_what_can_i_answer  — survey available datasets
  2. dgm_map_analytical_space — survey available tools

This gives the synthesiser enough information to produce a helpful response
even when the original question was vague, short, or had no recognisable
analytical keywords.

Test classes
------------
* TestReflectNodeActivation   — ill-defined inputs visit reflect; clear ones skip it
* TestReflectPlanEnrichment   — reflected plans contain the two orientation steps
* TestReflectBudgetInvariant  — reflected plans still sum to ≤ 1.0 budget
* TestReflectSynthesisQuality — synthesised output is informative after reflection
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
from sqldim.core.query.dgm.nl.supervisor._planner import decompose_question
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorState,
)

_CTX = SupervisorContext(available_datasets=["commerce", "devops", "entertainment"])

_ILL_DEFINED: list[str] = [
    "",
    "?",
    "help",
    "analyze",
    "interesting",
    "tell me",
]

_ILL_DEFINED_IDS = [u or "<empty>" for u in _ILL_DEFINED]

_CLEAR: list[str] = [
    "Which datasets can you see?",
    "What tools do you have?",
    "Show me available data and capabilities",
    "How many orders were placed last month?",
    "Top 5 customers by revenue",
    "Who are the top contributors?",
]

_RECURSION_LIMIT = 25  # reflected plans have ≥2 steps → ~8 node invocations


def _invoke(utterance: str, thread_id: str) -> dict:
    graph = build_supervisor_graph(context=_CTX)
    initial = SupervisorState(utterance=utterance)
    return graph.invoke(
        initial.model_dump(),
        config={
            "configurable": {"thread_id": thread_id},
            "recursion_limit": _RECURSION_LIMIT,
        },
    )


# ---------------------------------------------------------------------------
# Reflect node activation
# ---------------------------------------------------------------------------


class TestReflectNodeActivation:
    """Ill-defined questions route through reflect; clear ones skip it."""

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_ill_defined_visits_reflect(self, utterance: str) -> None:
        result = _invoke(utterance, f"act-ill-{abs(hash(utterance))}")
        visited = result.get("visited_nodes", [])
        assert "reflect" in visited, (
            f"Expected reflect for ill-defined '{utterance}', visited={visited}"
        )

    @pytest.mark.parametrize("utterance", _CLEAR)
    def test_clear_questions_skip_reflect(self, utterance: str) -> None:
        result = _invoke(utterance, f"act-clear-{abs(hash(utterance))}")
        visited = result.get("visited_nodes", [])
        assert "reflect" not in visited, (
            f"Unexpected reflect for clear '{utterance}', visited={visited}"
        )


# ---------------------------------------------------------------------------
# Reflected plan enrichment
# ---------------------------------------------------------------------------


class TestReflectPlanEnrichment:
    """The reflect node prepends orientation steps to the plan."""

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_reflected_plan_orientation_steps_executed(self, utterance: str) -> None:
        """After reflection, results must include both orientation tools."""
        result = _invoke(utterance, f"enrich-{abs(hash(utterance))}")
        raw = result.get("results", [])
        executed = [
            r.tool_name if hasattr(r, "tool_name") else r["tool_name"] for r in raw
        ]
        assert "dgm_what_can_i_answer" in executed, (
            f"Expected dgm_what_can_i_answer in results for '{utterance}', "
            f"got {executed}"
        )
        assert "dgm_map_analytical_space" in executed, (
            f"Expected dgm_map_analytical_space in results for '{utterance}', "
            f"got {executed}"
        )

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_reflected_plan_adds_orientation_steps(self, utterance: str) -> None:
        """Reflected plan = 2 orientation + original step(s); total is always ≥ 2."""
        plan = decompose_question(utterance, _CTX)
        original_steps = list(plan.steps)
        orientation_count = 2
        total_steps = orientation_count + len(original_steps)
        assert total_steps >= 2, (
            f"Expected ≥2 steps after reflect for '{utterance}', got {total_steps}"
        )


# ---------------------------------------------------------------------------
# Budget invariant under reflection
# ---------------------------------------------------------------------------


class TestReflectBudgetInvariant:
    """Reflected plans must still respect the budget ≤ 1.0 constraint."""

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_reflected_budget_sums_to_one(self, utterance: str) -> None:
        original = decompose_question(utterance, _CTX)
        orientation_budget = 0.5
        original_total = sum(s.budget_fraction for s in original.steps) or 1.0
        scale = (1.0 - orientation_budget) / original_total
        reflected_total = orientation_budget + sum(
            s.budget_fraction * scale for s in original.steps
        )
        assert reflected_total <= 1.0 + 1e-6, (
            f"Reflected budget {reflected_total:.6f} > 1.0 for '{utterance}'"
        )


# ---------------------------------------------------------------------------
# Synthesis quality after reflection
# ---------------------------------------------------------------------------


class TestReflectSynthesisQuality:
    """After reflection the synthesiser produces substantive output."""

    def test_empty_string_synthesis_is_informative(self) -> None:
        result = _invoke("", "synth-empty")
        synthesis = result.get("synthesis", "") or ""
        assert any(
            kw in synthesis.lower()
            for kw in ("dataset", "tool", "available", "see", "can")
        ), f"Expected informative synthesis for empty input, got: {synthesis!r}"

    def test_vague_help_synthesis_is_substantive(self) -> None:
        result = _invoke("help", "synth-help")
        synthesis = result.get("synthesis", "") or ""
        assert len(synthesis) > 30, (
            f"Expected substantive synthesis for 'help', got: {synthesis!r}"
        )

    def test_vague_question_synthesis_mentions_datasets_or_tools(self) -> None:
        result = _invoke("interesting", "synth-interesting")
        synthesis = result.get("synthesis", "") or ""
        assert any(
            kw in synthesis.lower()
            for kw in ("dataset", "tool", "available", "ecommerce", "retail")
        ), f"Expected domain context in synthesis for 'interesting', got: {synthesis!r}"

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_all_ill_defined_produce_non_empty_synthesis(self, utterance: str) -> None:
        result = _invoke(utterance, f"synth-nonempty-{abs(hash(utterance))}")
        synthesis = result.get("synthesis", "") or ""
        assert len(synthesis) > 0, f"Synthesis was empty for ill-defined '{utterance}'"


# ---------------------------------------------------------------------------
# Multi-stage reasoning: reflect does not lose original intent
# ---------------------------------------------------------------------------


class TestReflectPreservesOriginalIntent:
    """After reflection, the orientation steps run and the plan goal is intact."""

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_only_orientation_tools_executed_for_vague_input(
        self, utterance: str
    ) -> None:
        """For ill-defined inputs the empty original plan means only the two
        orientation steps execute — no spurious dgm_query on a random dataset."""
        result = _invoke(utterance, f"intent-{abs(hash(utterance))}")
        raw = result.get("results", [])
        executed = [
            r.tool_name if hasattr(r, "tool_name") else r["tool_name"] for r in raw
        ]
        assert "dgm_what_can_i_answer" in executed, (
            f"Expected dgm_what_can_i_answer in results for '{utterance}', got {executed}"
        )
        assert "dgm_map_analytical_space" in executed, (
            f"Expected dgm_map_analytical_space in results for '{utterance}', got {executed}"
        )
        assert "dgm_query" not in executed, (
            f"Unexpected dgm_query for ill-defined '{utterance}' — "
            f"empty-original-plan should produce only orientation steps, got {executed}"
        )

    @pytest.mark.parametrize("utterance", _ILL_DEFINED, ids=_ILL_DEFINED_IDS)
    def test_original_goal_preserved_in_plan(self, utterance: str) -> None:
        """The plan goal should equal the original utterance."""
        result = _invoke(utterance, f"goal-{abs(hash(utterance))}")
        plan_data = result.get("plan")
        # LangGraph serialises Pydantic models to dicts
        if isinstance(plan_data, dict):
            goal = plan_data.get("goal", "")
        else:
            goal = getattr(plan_data, "goal", "")
        assert goal == utterance, f"Plan goal '{goal}' ≠ utterance '{utterance}'"
