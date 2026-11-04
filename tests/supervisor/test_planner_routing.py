"""Keyword-planner routing tests — one test per intent route.

Covers all 9 tool targets plus:
- Fallback (reflect) path
- Budget-aware downgrade
- Two-step composition
- `reasoning` field population
- Short / empty utterance confidence penalties
"""

from __future__ import annotations


import pytest

from sqldim.core.query.dgm.nl.supervisor._planner import decompose_question
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorPlan,
)

# ── Shared fixtures ───────────────────────────────────────────────────────

_DATASETS = [
    "commerce",
    "data_models",
    "devops",
    "entertainment",
    "fintech",
    "growth",
    "nba_analytics",
    "telemetry",
]

_CTX = SupervisorContext(available_datasets=_DATASETS)
_CTX_EMPTY = SupervisorContext(available_datasets=[])


def _plan(utterance: str, ctx: SupervisorContext = _CTX) -> SupervisorPlan:
    return decompose_question(utterance, ctx)


def _tools(plan: SupervisorPlan) -> list[str]:
    return [s.tool_name for s in plan.steps]


# ── Parametrized happy-path intent routing ────────────────────────────────


@pytest.mark.parametrize(
    "utterance,expected_tool",
    [
        ("what datasets can you see?", "dgm_what_can_i_answer"),
        ("which datasets are available?", "dgm_what_can_i_answer"),
        ("what can you answer for the dataset?", "dgm_what_can_i_answer"),
        ("what tools do you have?", "dgm_map_analytical_space"),
        ("list your capabilities", "dgm_map_analytical_space"),
        ("how many orders were placed last week?", "dgm_query"),
        ("show total revenue by country", "dgm_query"),
        ("validate that revenue is always positive", "dgm_validate_invariant"),
        ("check that no null values exist in orders", "dgm_validate_invariant"),
        ("sensitivity sweep of days_window for orders", "dgm_sensitivity_sweep"),
        ("how sensitive is the churn model to the parameter?", "dgm_sensitivity_sweep"),
        ("correlate ecommerce with retail trends", "dgm_correlate"),
        ("what is the relationship between churn and revenue?", "dgm_correlate"),
        ("what else can I explore from this result?", "dgm_explore_from"),
        ("navigate to related questions", "dgm_explore_from"),
        ("store this finding about the revenue drop", "dgm_store_finding"),
        ("save the result about churn", "dgm_store_finding"),
        ("retrieve previous findings about churn", "dgm_retrieve_related"),
        ("what do we know from the knowledge base?", "dgm_retrieve_related"),
    ],
)
class TestIntentRouting:
    """Each utterance routes to the expected tool via the keyword planner."""

    def test_first_step(self, utterance: str, expected_tool: str) -> None:
        plan = _plan(utterance)
        tools = _tools(plan)
        assert tools, f"Expected step with {expected_tool!r}, got empty plan"
        assert tools[0] == expected_tool, (
            f"{utterance!r} → {tools} (expected {expected_tool!r})"
        )

    def test_confidence_acceptable(self, utterance: str, expected_tool: str) -> None:
        plan = _plan(utterance)
        assert plan.confidence >= 0.5, (
            f"{expected_tool!r}: confidence {plan.confidence} below 0.5"
        )

    def test_reasoning_populated(self, utterance: str, expected_tool: str) -> None:
        plan = _plan(utterance)
        assert plan.reasoning, (
            f"{expected_tool!r}: reasoning is empty for {utterance!r}"
        )


# ── Dual-intent: datasets + tools ────────────────────────────────────────


class TestDualIntent:
    def test_both_tools_present(self) -> None:
        plan = _plan("what datasets and tools do you have?")
        tools = _tools(plan)
        assert "dgm_what_can_i_answer" in tools
        assert "dgm_map_analytical_space" in tools

    def test_high_confidence(self) -> None:
        plan = _plan("which data and capabilities are available?")
        assert plan.confidence >= 0.85


# ── Reflect / fallback path ───────────────────────────────────────────────


class TestReflectFallback:
    def test_empty_utterance_returns_empty_steps(self) -> None:
        plan = _plan("", _CTX)
        assert plan.steps == []

    def test_empty_utterance_very_low_confidence(self) -> None:
        plan = _plan("", _CTX)
        assert plan.confidence <= 0.15

    def test_vague_utterance_routes_to_reflect(self) -> None:
        plan = _plan("blah", _CTX)
        # short → confidence ≤ 0.3, no steps
        assert plan.confidence <= 0.3

    def test_unknown_intent_empty_steps(self) -> None:
        plan = _plan("xyzzy frobnicate frobble", _CTX)
        assert plan.steps == []
        assert plan.confidence < 0.5


# ── Short-utterance confidence penalty ────────────────────────────────────


class TestShortUtterance:
    def test_short_utterance_low_confidence(self) -> None:
        plan = _plan("hi", _CTX)
        assert plan.confidence <= 0.3

    def test_reasoning_explains_short(self) -> None:
        plan = _plan("hi", _CTX)
        assert "short" in plan.reasoning.lower() or "char" in plan.reasoning.lower()


# ── Budget-aware downgrade ────────────────────────────────────────────────


class _TightBudget:
    """Minimal budget object: forces `_budget_allows` to return False."""

    max_cost: float = 0.0  # blocks all real tools


class TestBudgetDowngrade:
    def test_exhausted_budget_degrades_analytical_query(self) -> None:
        ctx = SupervisorContext(
            available_datasets=["commerce"],
            budget=_TightBudget(),
        )
        plan = decompose_question("how many orders are open?", ctx)
        # Should degrade from dgm_query → dgm_what_can_i_answer
        tools = _tools(plan)
        assert tools == ["dgm_what_can_i_answer"], (
            f"Expected discovery fallback, got {tools}"
        )

    def test_budget_degradation_reasoning(self) -> None:
        ctx = SupervisorContext(
            available_datasets=["commerce"],
            budget=_TightBudget(),
        )
        plan = decompose_question("what is the total revenue?", ctx)
        assert "budget" in plan.reasoning.lower() or "degrad" in plan.reasoning.lower()


# ── Composition routing ───────────────────────────────────────────────────


class TestCompositionRouting:
    def test_composition_produces_two_steps(self) -> None:
        plan = _plan("show orders and then explore related questions")
        # At minimum expect 2 steps
        assert len(plan.steps) >= 2, (
            f"Expected 2+ steps for composition, got {len(plan.steps)}: {_tools(plan)}"
        )

    def test_composition_first_step_is_query(self) -> None:
        plan = _plan("count revenue and then dig deeper")
        tools = _tools(plan)
        assert tools[0] == "dgm_query"

    def test_composition_second_step_explores(self) -> None:
        plan = _plan("list top products then explore from results")
        tools = _tools(plan)
        assert "dgm_explore_from" in tools


# ── Dataset-mention binding ───────────────────────────────────────────────


class TestDatasetBinding:
    def test_legacy_dataset_alias_resolves_to_canonical_domain(self) -> None:
        plan = _plan("how many returns in ecommerce last month?")
        assert plan.steps
        assert plan.steps[0].arguments["dataset"] == "commerce"

    @pytest.mark.parametrize(
        "utterance,expected",
        [
            ("how many orders were placed last week?", "commerce"),
            ("What is the average order value?", "commerce"),
            ("Count users who signed up in Q1", "growth"),
            ("what is the churn rate?", "growth"),
            ("how many payments were processed?", "fintech"),
            ("how many commits were merged?", "devops"),
            ("how many errors occurred last week?", "telemetry"),
            ("how many movie ratings were submitted?", "entertainment"),
            ("how many rebounds did NBA players get?", "nba_analytics"),
            ("show the star schema dimensions", "data_models"),
        ],
    )
    def test_topic_inference_selects_canonical_domain(
        self, utterance: str, expected: str
    ) -> None:
        plan = _plan(utterance)
        assert plan.steps
        assert plan.steps[0].arguments["dataset"] == expected

    def test_generic_query_does_not_select_first_dataset(self) -> None:
        plan = _plan("what is the trend?", _CTX)
        assert plan.steps == []
        assert plan.confidence < 0.5

    def test_generic_query_is_independent_of_dataset_order(self) -> None:
        first = SupervisorContext(
            available_datasets=["commerce", "growth", "telemetry"]
        )
        second = SupervisorContext(
            available_datasets=["telemetry", "growth", "commerce"]
        )

        plan_a = _plan("what is the trend?", first)
        plan_b = _plan("what is the trend?", second)

        assert plan_a.steps == plan_b.steps == []
        assert plan_a.confidence == plan_b.confidence

    def test_conflicting_topics_do_not_select_arbitrary_domain(self) -> None:
        plan = _plan("what is the revenue trend for churn?", _CTX)
        assert plan.steps == []
        assert plan.confidence < 0.5

    def test_unavailable_topic_does_not_select_an_unrelated_domain(self) -> None:
        ctx = SupervisorContext(available_datasets=["growth"])
        plan = _plan("how many orders were placed?", ctx)

        assert plan.steps == []
        assert plan.confidence < 0.5

    def test_conflicting_legacy_aliases_route_to_reflect(self) -> None:
        plan = _plan(
            "How many teams exist in the enterprise hierarchy?",
            _CTX,
        )
        assert plan.steps == []
        assert plan.confidence < 0.5

    def test_no_datasets_does_not_fabricate_identifier(self) -> None:
        plan = _plan("what is the trend?", _CTX_EMPTY)
        assert plan.steps == []
        assert plan.confidence < 0.5


# ── SupervisorPlan schema ─────────────────────────────────────────────────


class TestPlanSchema:
    def test_all_plans_return_supervisor_plan(self) -> None:
        utterances = [
            "validate data",
            "correlate",
            "store result",
            "explore",
            "retrieve findings",
            "sweep parameter",
        ]
        for utt in utterances:
            plan = _plan(utt)
            assert isinstance(plan, SupervisorPlan), (
                f"{utt!r} did not return SupervisorPlan"
            )

    def test_goal_matches_utterance(self) -> None:
        utt = "how many orders exceeded quota?"
        plan = _plan(utt)
        assert plan.goal == utt

    def test_confidence_in_range(self) -> None:
        for utt in ["validate X", "correlate A and B", "retrieve old findings", ""]:
            plan = _plan(utt)
            assert 0.0 <= plan.confidence <= 1.0, (
                f"{utt!r}: confidence {plan.confidence} out of range"
            )
