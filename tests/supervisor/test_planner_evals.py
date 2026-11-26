"""Planners: the LLM planner fails closed; the demo planner is deterministic."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sqldim.core.query.dgm.nl.supervisor._planner import (
    KeywordDemoPlanner,
    LLMPlanner,
    PlannerView,
    _PlannerStep,
    _PlanResult,
    _render_view,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._types import (
    ToolError,
    ToolErrorKind,
)
from tests.supervisor._fakes import plan, step

_TOOLS = tuple(
    t.capability_schema
    for t in build_registry(demo=True).values()
    if t.capability_schema.available
)


def _view(goal: str = "top revenue products", **kw: Any) -> PlannerView:
    defaults: dict[str, Any] = {
        "plan_id": "p1",
        "version": 1,
        "datasets": ("commerce",),
        "tools": _TOOLS,
    }
    return PlannerView(goal=goal, **{**defaults, **kw})


def _agent_returning(result: _PlanResult) -> MagicMock:
    run = MagicMock()
    run.output = result
    agent = MagicMock()
    agent.run_sync.return_value = run
    return agent


class TestLLMPlanner:
    def test_requires_a_model(self) -> None:
        with pytest.raises(ValueError):
            LLMPlanner(None)

    def test_converts_model_output(self) -> None:
        out = _PlanResult(
            steps=[
                _PlannerStep(
                    call_id="q1",
                    tool_name="dgm_query",
                    arguments={"dataset": "commerce", "utterance": "top revenue"},
                    budget_fraction=0.7,
                )
            ],
            confidence=1.4,
            reasoning="one query",
        )
        with patch("pydantic_ai.Agent", return_value=_agent_returning(out)):
            p = LLMPlanner(MagicMock()).propose(_view())
        assert [s.call_id for s in p.steps] == ["q1"]
        assert p.steps[0].budget_fraction == 0.7  # not renormalised
        assert p.steps[0].datasets is None  # never taken from the model
        assert (p.plan_id, p.version) == ("p1", 1)
        assert p.confidence == 1.0

    def test_failure_propagates_without_fallback(self) -> None:
        with patch("pydantic_ai.Agent", side_effect=RuntimeError("api down")):
            with pytest.raises(RuntimeError, match="api down"):
                LLMPlanner(MagicMock()).propose(_view("How many orders?"))

    def test_empty_plan_is_kept_empty(self) -> None:
        out = _PlanResult(steps=[], confidence=0.2, reasoning="cannot answer")
        with patch("pydantic_ai.Agent", return_value=_agent_returning(out)):
            p = LLMPlanner(MagicMock()).propose(_view("?"))
        assert p.steps == []
        assert p.reasoning == "cannot answer"

    def test_prompt_shows_only_view_contents(self) -> None:
        text = _render_view(_view(datasets=("commerce",)))
        assert "Datasets: commerce" in text
        assert "dgm_query" in text
        assert "dgm_correlate" not in text

    def test_replan_prompt_carries_results_and_violations(self) -> None:
        violation = ToolError(
            kind=ToolErrorKind.UNKNOWN_TOOL,
            message="unknown tool 'x'",
            context={"rule": "P3"},
        )
        text = _render_view(
            _view(
                version=2,
                previous=plan(step("a")),
                violations=(violation,),
                remaining_fraction=0.25,
            )
        )
        assert "Current plan (version 1)" in text
        assert "P3: unknown tool 'x'" in text
        assert "0.2500" in text


@pytest.mark.parametrize(
    "utterance",
    [
        "How many orders were placed last month?",
        "Which datasets can you see?",
        "correlate revenue and churn",
        "",
    ],
)
class TestKeywordDemoPlannerInvariants:
    def test_deterministic(self, utterance: str) -> None:
        a = KeywordDemoPlanner().propose(_view(utterance))
        b = KeywordDemoPlanner().propose(_view(utterance))
        assert a == b

    def test_budget_at_most_one(self, utterance: str) -> None:
        assert (
            KeywordDemoPlanner().propose(_view(utterance)).total_budget_fraction <= 1.0
        )

    def test_only_view_tools(self, utterance: str) -> None:
        p = KeywordDemoPlanner().propose(_view(utterance))
        assert {s.tool_name for s in p.steps} <= {t.name for t in _TOOLS}
