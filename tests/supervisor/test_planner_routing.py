"""Keyword demo planner (D47) — routing and dataset binding.

The keyword planner is the heuristic of the explicit demo mode.  It plans only
tools that are available in the view, never substitutes one tool for another,
and never invents a dataset.
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.nl.supervisor._planner import (
    KeywordDemoPlanner,
    PlannerView,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._types import SupervisorPlan

_DATASETS = (
    "commerce",
    "data_models",
    "devops",
    "entertainment",
    "fintech",
    "growth",
    "nba_analytics",
    "telemetry",
)

_DEMO_TOOLS = tuple(
    t.capability_schema
    for t in build_registry(demo=True).values()
    if t.capability_schema.available
)


def _view(utterance: str, datasets: tuple[str, ...] = _DATASETS) -> PlannerView:
    return PlannerView(
        goal=utterance,
        plan_id="p",
        version=1,
        datasets=datasets,
        tools=_DEMO_TOOLS,
    )


def _plan(utterance: str, datasets: tuple[str, ...] = _DATASETS) -> SupervisorPlan:
    return KeywordDemoPlanner().propose(_view(utterance, datasets))


def test_demo_registry_exposes_only_dgm_query() -> None:
    assert [t.name for t in _DEMO_TOOLS] == ["dgm_query"]


class TestAnalyticalRouting:
    @pytest.mark.parametrize(
        "utterance",
        ["how many orders were placed last week?", "show total revenue by country"],
    )
    def test_analytical_question_plans_one_query(self, utterance: str) -> None:
        plan = _plan(utterance)
        assert [s.tool_name for s in plan.steps] == ["dgm_query"]
        step = plan.steps[0]
        assert step.call_id == "q1"
        assert step.arguments == {"dataset": "commerce", "utterance": utterance}
        assert step.depends_on == []
        assert step.budget_fraction == 1.0
        assert plan.confidence >= 0.5

    def test_planner_does_not_set_datasets_or_keys(self) -> None:
        step = _plan("how many orders were placed?").steps[0]
        assert step.datasets is None
        assert step.idempotency_key is None


class TestUnavailableCapabilities:
    """Intents whose tool is not implemented are reported, never substituted."""

    @pytest.mark.parametrize(
        "utterance,tool",
        [
            ("validate that revenue is always positive", "dgm_validate_invariant"),
            ("sensitivity sweep of days_window for orders", "dgm_sensitivity_sweep"),
            ("correlate ecommerce with retail trends", "dgm_correlate"),
            ("what is the relationship between churn and revenue?", "dgm_correlate"),
            ("what else can I explore from this result?", "dgm_explore_from"),
            ("store this finding about the revenue drop", "dgm_store_finding"),
            ("retrieve previous findings about churn", "dgm_retrieve_related"),
            ("count orders then explore related questions", "dgm_explore_from"),
        ],
    )
    def test_no_step_and_reason_names_the_tool(self, utterance: str, tool: str) -> None:
        plan = _plan(utterance)
        assert plan.steps == []
        assert "not available" in plan.reasoning
        assert tool in plan.reasoning

    def test_no_fallback_to_query(self) -> None:
        plan = _plan("validate that orders are non-negative in commerce")
        assert all(s.tool_name != "dgm_query" for s in plan.steps)


class TestDiscovery:
    def test_dataset_discovery_lists_view_datasets_without_tools(self) -> None:
        plan = _plan("which datasets are available?", ("commerce", "growth"))
        assert plan.steps == []
        assert "commerce, growth" in plan.reasoning

    def test_dataset_discovery_never_mentions_ungranted_datasets(self) -> None:
        plan = _plan("which datasets are available?", ("growth",))
        assert "commerce" not in plan.reasoning

    def test_tool_discovery_lists_available_tools_only(self) -> None:
        plan = _plan("what tools do you have?")
        assert plan.steps == []
        assert "dgm_query" in plan.reasoning
        assert "dgm_correlate" not in plan.reasoning


class TestShortAndVague:
    def test_empty_utterance(self) -> None:
        plan = _plan("")
        assert plan.steps == []
        assert plan.confidence <= 0.1

    def test_short_utterance_low_confidence(self) -> None:
        plan = _plan("hi")
        assert plan.steps == []
        assert plan.confidence <= 0.3
        assert "short" in plan.reasoning.lower()

    def test_no_analytical_intent_plans_nothing(self) -> None:
        plan = _plan("the weather is nice today")
        assert plan.steps == []


class TestDatasetBinding:
    def test_legacy_dataset_alias_resolves_to_canonical_domain(self) -> None:
        plan = _plan("how many returns in ecommerce last month?")
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
        assert _plan(utterance).steps[0].arguments["dataset"] == expected

    @pytest.mark.parametrize(
        "utterance",
        [
            "what is the trend?",
            "what is the revenue trend for churn?",
            "How many teams exist in the enterprise hierarchy?",
        ],
    )
    def test_ambiguous_question_plans_nothing(self, utterance: str) -> None:
        plan = _plan(utterance)
        assert plan.steps == []
        assert plan.confidence < 0.5

    def test_independent_of_dataset_order(self) -> None:
        a = _plan("what is the trend?", ("commerce", "growth", "telemetry"))
        b = _plan("what is the trend?", ("telemetry", "growth", "commerce"))
        assert a.steps == b.steps == []
        assert a.confidence == b.confidence

    def test_unavailable_topic_does_not_select_an_unrelated_domain(self) -> None:
        assert _plan("how many orders were placed?", ("growth",)).steps == []

    def test_no_datasets_does_not_fabricate_identifier(self) -> None:
        assert _plan("how many orders were placed?", ()).steps == []

    def test_single_dataset_is_an_explicit_constraint(self) -> None:
        plan = _plan("how many rows are there in total?", ("growth",))
        assert plan.steps[0].arguments["dataset"] == "growth"


class TestPlanShape:
    def test_plan_id_and_version_come_from_the_view(self) -> None:
        plan = _plan("how many orders were placed?")
        assert (plan.plan_id, plan.version) == ("p", 1)

    def test_goal_matches_utterance(self) -> None:
        utt = "how many orders exceeded quota?"
        assert _plan(utt).goal == utt

    @pytest.mark.parametrize("utt", ["validate X", "correlate A and B", "", "orders"])
    def test_confidence_in_range(self, utt: str) -> None:
        assert 0.0 <= _plan(utt).confidence <= 1.0

    def test_planner_is_marked_demo(self) -> None:
        assert KeywordDemoPlanner.demo is True
