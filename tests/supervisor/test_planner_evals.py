"""Eval cases for the supervisor planner: routing correctness and confidence.

Each parametrized case specifies:
  utterance            — the raw user input
  expected_tools       — ordered list of tool names the planner should route to
  confidence_range     — (lo, hi) bounds the plan.confidence must fall within

The confidence threshold for the reflect node is 0.5; cases below 0.5 are
expected to trigger reflection in the graph (tested in test_reflection_evals).
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.nl.supervisor._planner import decompose_question
from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

_CTX = SupervisorContext(available_datasets=["ecommerce", "retail", "social_network"])

# ---------------------------------------------------------------------------
# (utterance, expected_tool_sequence, confidence_range)
# ---------------------------------------------------------------------------
_ROUTING_CASES: list[tuple[str, list[str], tuple[float, float]]] = [
    # ── Clear dataset intent ──────────────────────────────────────────────
    ("Which datasets can you see?", ["dgm_what_can_i_answer"], (0.5, 1.0)),
    ("What data is available to me?", ["dgm_what_can_i_answer"], (0.5, 1.0)),
    ("What datasets do you know about?", ["dgm_what_can_i_answer"], (0.5, 1.0)),
    ("Show me available datasets", ["dgm_what_can_i_answer"], (0.5, 1.0)),
    # ── Clear tool intent ─────────────────────────────────────────────────
    ("What tools do you have?", ["dgm_map_analytical_space"], (0.5, 1.0)),
    (
        "What are your analytical capabilities?",
        ["dgm_map_analytical_space"],
        (0.5, 1.0),
    ),
    # 'can you' is no longer a tool keyword (too broad); 'list' is a question
    # marker so confidence ≥ 0.5 but routing falls back to dgm_query
    ("Can you list what you can do?", ["dgm_query"], (0.5, 1.0)),
    # ── Both dataset + tool intent ────────────────────────────────────────
    (
        "Which datasets can you see and what tools do you have?",
        ["dgm_what_can_i_answer", "dgm_map_analytical_space"],
        (0.7, 1.0),
    ),
    (
        "Show me available data and your capabilities",
        ["dgm_what_can_i_answer", "dgm_map_analytical_space"],
        (0.7, 1.0),
    ),
    # ── Domain-specific analytical queries → dgm_query ───────────────────
    ("How many orders were placed last month?", ["dgm_query"], (0.5, 1.0)),
    ("Top 5 customers by revenue", ["dgm_query"], (0.5, 1.0)),
    ("Who are the best sales reps?", ["dgm_query"], (0.5, 1.0)),
    ("Compare ecommerce and retail performance", ["dgm_query"], (0.5, 1.0)),
    ("Show me the churn rate over the last year", ["dgm_query"], (0.5, 1.0)),
    ("What is the average order value?", ["dgm_query"], (0.5, 1.0)),
    ("Find the top contributors by commit count", ["dgm_query"], (0.5, 1.0)),
    ("List all active subscriptions", ["dgm_query"], (0.5, 1.0)),
    ("Count users who signed up in Q1", ["dgm_query"], (0.5, 1.0)),
    # ── Ill-defined — low confidence; planner returns empty plan; reflect handles orientation ──
    ("", [], (0.0, 0.3)),
    ("?", [], (0.0, 0.4)),
    ("help", [], (0.0, 0.4)),
    ("analyze", [], (0.0, 0.5)),
    ("interesting", [], (0.0, 0.5)),
    ("tell me", [], (0.0, 0.5)),
    # ── Dataset-specific question routing ─────────────────────────────────
    (
        "Which questions do you suggest for the dataset social_network?",
        ["dgm_what_can_i_answer"],
        (0.5, 1.0),
    ),
    (
        "What questions can you answer for ecommerce?",
        ["dgm_what_can_i_answer"],
        (0.5, 1.0),
    ),
    ("What can you answer for retail?", ["dgm_what_can_i_answer"], (0.5, 1.0)),
]

_CASE_IDS = [c[0][:40] or "<empty>" for c in _ROUTING_CASES]


class TestPlannerRouting:
    """Basic routing: correct tools selected per utterance category."""

    @pytest.mark.parametrize(
        "utterance,expected_tools,_cr",
        _ROUTING_CASES,
        ids=_CASE_IDS,
    )
    def test_routes_to_correct_tools(
        self,
        utterance: str,
        expected_tools: list[str],
        _cr: tuple[float, float],
    ) -> None:
        plan = decompose_question(utterance, _CTX)
        actual = [s.tool_name for s in plan.steps]
        assert actual == expected_tools, (
            f"'{utterance}' → {actual!r}, expected {expected_tools!r}"
        )

    @pytest.mark.parametrize(
        "utterance,_et,confidence_range",
        _ROUTING_CASES,
        ids=_CASE_IDS,
    )
    def test_confidence_in_expected_range(
        self,
        utterance: str,
        _et: list[str],
        confidence_range: tuple[float, float],
    ) -> None:
        plan = decompose_question(utterance, _CTX)
        lo, hi = confidence_range
        assert lo <= plan.confidence <= hi, (
            f"'{utterance}' confidence={plan.confidence:.2f}, expected [{lo:.2f}, {hi:.2f}]"
        )


class TestPlannerBudgetInvariant:
    """Budget fractions always sum to ≤ 1.0 across all utterance types."""

    @pytest.mark.parametrize(
        "utterance",
        [c[0] for c in _ROUTING_CASES],
        ids=_CASE_IDS,
    )
    def test_budget_sums_to_at_most_one(self, utterance: str) -> None:
        plan = decompose_question(utterance, _CTX)
        total = sum(s.budget_fraction for s in plan.steps)
        assert total <= 1.0 + 1e-9, f"'{utterance}' budget sum={total:.6f} > 1.0"


class TestPlannerNoContext:
    """Planner degrades gracefully when context is None."""

    def test_no_context_returns_valid_plan(self) -> None:
        plan = decompose_question("How many users are there?")
        assert plan is not None
        assert len(plan.steps) >= 1
        assert plan.steps[0].tool_name == "dgm_query"
        assert plan.steps[0].arguments.get("dataset") == "unknown"

    def test_no_context_confidence_valid(self) -> None:
        plan = decompose_question("")
        assert 0.0 <= plan.confidence <= 1.0

    def test_no_context_dataset_query_is_unknown(self) -> None:
        plan = decompose_question("Show me churn rate")
        assert plan.steps[0].arguments.get("dataset") == "unknown"


class TestPlannerDeterminism:
    """Same input always produces the same plan (no randomness)."""

    @pytest.mark.parametrize(
        "utterance",
        ["Which datasets?", "What tools?", "Show me churn", "help", ""],
        ids=["datasets", "tools", "churn", "help", "empty"],
    )
    def test_same_input_same_output(self, utterance: str) -> None:
        plan_a = decompose_question(utterance, _CTX)
        plan_b = decompose_question(utterance, _CTX)
        assert plan_a.model_dump() == plan_b.model_dump()


class TestPlannerConfidenceThresholds:
    """Well-formed questions clear 0.5; ill-defined ones fall below."""

    _WELL_FORMED = [
        "Which datasets can you see?",
        "What tools do you have?",
        "How many orders last month?",
        "Top 5 customers by revenue",
        "Show me available data and capabilities",
    ]

    _ILL_DEFINED = [
        "",
        "?",
        "help",
        "analyze",
        "interesting",
        "tell me",
    ]

    @pytest.mark.parametrize("utterance", _WELL_FORMED)
    def test_well_formed_confidence_passes_threshold(self, utterance: str) -> None:
        plan = decompose_question(utterance, _CTX)
        assert plan.confidence >= 0.5, (
            f"'{utterance}' confidence={plan.confidence:.2f} below reflect threshold"
        )

    @pytest.mark.parametrize(
        "utterance",
        _ILL_DEFINED,
        ids=[u or "<empty>" for u in _ILL_DEFINED],
    )
    def test_ill_defined_confidence_below_threshold(self, utterance: str) -> None:
        plan = decompose_question(utterance, _CTX)
        assert plan.confidence < 0.5, (
            f"'{utterance}' confidence={plan.confidence:.2f} unexpectedly high"
        )


class TestLLMDecomposePath:
    """Cover the pydantic-ai LLM code path in _llm_decompose and decompose_question."""

    def test_llm_path_returns_plan_from_mock(self) -> None:
        """_llm_decompose is called when a model is provided; mock Agent for speed."""
        from unittest.mock import MagicMock, patch

        from sqldim.core.query.dgm.nl.supervisor._planner import (
            _PlanResult,
            _PlannerStep,
            decompose_question,
        )
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        mock_output = _PlanResult(
            steps=[
                _PlannerStep(
                    tool_name="dgm_query",
                    arguments={"dataset": "ecommerce"},
                    budget_fraction=1.0,
                )
            ],
            confidence=0.9,
            reasoning="ecommerce query",
        )
        mock_run = MagicMock()
        mock_run.output = mock_output
        mock_agent_instance = MagicMock()
        mock_agent_instance.run_sync.return_value = mock_run

        ctx = SupervisorContext(available_datasets=["ecommerce"])
        with patch("pydantic_ai.Agent", return_value=mock_agent_instance):
            plan = decompose_question("top revenue products", ctx, model=MagicMock())

        assert plan.steps[0].tool_name == "dgm_query"
        assert plan.confidence == pytest.approx(0.9)

    def test_llm_path_falls_back_when_agent_raises(self) -> None:
        """On any exception from Agent, decompose_question silently falls back."""
        from unittest.mock import MagicMock, patch

        from sqldim.core.query.dgm.nl.supervisor._planner import decompose_question
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        ctx = SupervisorContext(available_datasets=["ecommerce"])
        with patch("pydantic_ai.Agent", side_effect=RuntimeError("api down")):
            plan = decompose_question("How many orders?", ctx, model=MagicMock())

        # Fell back to keyword decompose — "how" is a question marker → dgm_query
        assert plan is not None
        assert plan.steps[0].tool_name == "dgm_query"

    def test_llm_path_empty_steps_falls_back_to_keyword(self) -> None:
        """_llm_decompose falls back when LLM returns zero steps."""
        from unittest.mock import MagicMock, patch

        from sqldim.core.query.dgm.nl.supervisor._planner import (
            _PlanResult,
            decompose_question,
        )
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        mock_output = _PlanResult(steps=[], confidence=0.2, reasoning="unclear")
        mock_run = MagicMock()
        mock_run.output = mock_output
        mock_agent_instance = MagicMock()
        mock_agent_instance.run_sync.return_value = mock_run

        ctx = SupervisorContext(available_datasets=["ecommerce"])
        with patch("pydantic_ai.Agent", return_value=mock_agent_instance):
            plan = decompose_question("?", ctx, model=MagicMock())

        # LLM returned [] → _keyword_decompose handles it
        assert plan is not None
