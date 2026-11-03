"""Coverage tests for supervisor _judge.py and _tools.py uncovered branches.

Targets:
- sqldim/core/query/dgm/nl/supervisor/_judge.py
    line 131:      _score_in_range validator
    lines 220-221: run_pipeline first_ds = ds; break
    lines 329-344: SupervisorJudge.__init__ and judge() method
    lines 349-358: SupervisorJudge._build_prompt() method

- sqldim/core/query/dgm/nl/supervisor/_tools.py
    lines 115-158: DgmQueryTool.execute real-execution path
"""

from __future__ import annotations

import json
from unittest import mock

import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# _judge.py — JudgeVerdict._score_in_range validator (line 131)
# ---------------------------------------------------------------------------


class TestJudgeVerdictScoreValidator:
    """Constructing a JudgeVerdict exercises the _score_in_range validator."""

    def test_score_clamped_to_1(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import JudgeVerdict

        v = JudgeVerdict(
            planning_score=1.5,  # above 1.0 → clamped to 1.0
            execution_score=1.0,
            synthesis_score=0.8,
            overall_score=0.9,
            reasoning="test",
            passed=True,
        )
        assert v.planning_score == 1.0

    def test_score_clamped_to_0(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import JudgeVerdict

        v = JudgeVerdict(
            planning_score=-0.5,  # below 0.0 → clamped to 0.0
            execution_score=0.0,
            synthesis_score=0.0,
            overall_score=0.0,
            reasoning="test",
            passed=False,
        )
        assert v.planning_score == 0.0

    def test_valid_score_unchanged(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import JudgeVerdict

        v = JudgeVerdict(
            planning_score=0.7,
            execution_score=0.8,
            synthesis_score=0.9,
            overall_score=0.8,
            reasoning="all good",
            passed=True,
        )
        assert v.planning_score == 0.7
        assert v.execution_score == 0.8


# ---------------------------------------------------------------------------
# _judge.py — load_judge_cases with custom path (lines 165-174)
# ---------------------------------------------------------------------------


class TestLoadJudgeCasesCustomPath:
    def test_loads_cases_from_custom_json(self, tmp_path) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import load_judge_cases

        case_data = {
            "cases": [
                {
                    "id": "test.custom.01",
                    "utterance": "show revenue",
                    "tags": ["smoke"],
                    "expected": {
                        "planning": {
                            "tools": ["dgm_query"],
                            "dataset": "ecommerce",
                            "confidence_min": 0.5,
                        },
                        "execution": {"all_succeed": True, "result_count": 1},
                        "synthesis": {
                            "must_mention": ["revenue"],
                            "must_not_mention": [],
                            "semantic_criteria": "mentions revenue data",
                        },
                    },
                }
            ]
        }
        p = tmp_path / "custom_judge_evals.json"
        p.write_text(json.dumps(case_data))

        cases = load_judge_cases(path=p)
        assert len(cases) == 1
        assert cases[0].id == "test.custom.01"
        assert cases[0].utterance == "show revenue"
        assert cases[0].planning.tools == ["dgm_query"]

    def test_empty_cases_array(self, tmp_path) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import load_judge_cases

        p = tmp_path / "empty.json"
        p.write_text(json.dumps({"cases": []}))
        cases = load_judge_cases(path=p)
        assert cases == []


# ---------------------------------------------------------------------------
# _judge.py — run_pipeline with context=None (uses KNOWN_DATASETS, lines 191-195)
# and first_ds extraction (lines 220-221)
# ---------------------------------------------------------------------------


class TestRunPipeline:
    def test_run_pipeline_no_context_uses_known_datasets(self) -> None:
        """run_pipeline(context=None) constructs context from KNOWN_DATASETS."""
        from sqldim.core.query.dgm.nl.supervisor._judge import run_pipeline

        output = run_pipeline("show me revenue", context=None, thread_id="test-no-ctx")
        assert output.utterance == "show me revenue"
        assert output.synthesis  # should produce some synthesis
        assert output.plan_tools  # should have at least one tool
        assert output.all_succeeded

    def test_run_pipeline_specific_dataset_sets_first_ds(self) -> None:
        """Plans containing a specific dataset (not __all__) set first_ds (lines 220-221)."""
        from sqldim.core.query.dgm.nl.supervisor._judge import run_pipeline
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        # Use a specific dataset utterance to force the planner to use a specific dataset
        context = SupervisorContext(available_datasets=["ecommerce"])

        output = run_pipeline(
            "what is the total revenue by category in ecommerce",
            context=context,
            thread_id="test-specific-ds",
        )
        assert output.utterance == "what is the total revenue by category in ecommerce"
        # plan_dataset might or might not be set depending on planner keyword matching
        # The call itself exercises lines 217-221
        assert output.plan_tools  # should have at least one tool

    def test_run_pipeline_output_structure(self) -> None:
        """Verify PipelineOutput fields are populated correctly."""
        from sqldim.core.query.dgm.nl.supervisor._judge import run_pipeline
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        context = SupervisorContext(available_datasets=["ecommerce"])
        output = run_pipeline("show revenue", context=context, thread_id="test-output")

        assert isinstance(output.plan_tools, list)
        assert isinstance(output.plan_confidence, float)
        assert isinstance(output.plan_budget_total, float)
        assert isinstance(output.results, list)
        assert isinstance(output.visited_nodes, list)
        assert isinstance(output.node_timings, dict)
        assert isinstance(output.synthesis, str)
        assert isinstance(output.all_succeeded, bool)


# ---------------------------------------------------------------------------
# _judge.py — SupervisorJudge.__init__ and judge() + _build_prompt() (lines 329-358)
# ---------------------------------------------------------------------------


class TestSupervisorJudge:
    """SupervisorJudge with mocked pydantic_ai.Agent."""

    def _make_case(self):
        from sqldim.core.query.dgm.nl.supervisor._judge import (
            JudgeCase,
            _PlanningExpectation,
            _ExecutionExpectation,
            _SynthesisExpectation,
        )

        return JudgeCase(
            id="test.01",
            utterance="show revenue",
            tags=[],
            planning=_PlanningExpectation(
                tools=["dgm_query"], dataset="ecommerce", confidence_min=0.5
            ),
            execution=_ExecutionExpectation(all_succeed=True, result_count=1),
            synthesis=_SynthesisExpectation(
                must_mention=["revenue"],
                must_not_mention=[],
                semantic_criteria="mentions revenue data",
            ),
        )

    def _make_pipeline_output(self):
        from sqldim.core.query.dgm.nl.supervisor._judge import PipelineOutput

        return PipelineOutput(
            utterance="show revenue",
            plan_tools=["dgm_query"],
            plan_dataset="ecommerce",
            plan_confidence=0.8,
            plan_budget_total=1.0,
            results=[
                {
                    "tool_name": "dgm_query",
                    "success": True,
                    "data": {"dataset": "ecommerce", "stub": True},
                    "error": None,
                }
            ],
            all_succeeded=True,
            synthesis="DGM query on 'ecommerce' completed (stub).",
            visited_nodes=["plan", "execute_tool", "synthesize"],
            node_timings={"plan": 2.0, "synthesize": 5.0},
        )

    def test_init_stores_model(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import SupervisorJudge

        judge = SupervisorJudge("fake-model")
        assert judge._model == "fake-model"

    def test_build_prompt_contains_utterance(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import SupervisorJudge

        case = self._make_case()
        output = self._make_pipeline_output()
        prompt = SupervisorJudge._build_prompt(case, output)
        assert "show revenue" in prompt
        assert "dgm_query" in prompt
        assert "ecommerce" in prompt

    def test_build_prompt_with_no_results(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import (
            PipelineOutput,
            SupervisorJudge,
        )

        case = self._make_case()
        output = PipelineOutput(
            utterance="test",
            plan_tools=[],
            plan_dataset=None,
            plan_confidence=0.0,
            plan_budget_total=0.0,
            results=[],
            all_succeeded=True,
            synthesis="",
            visited_nodes=[],
            node_timings={},
        )
        prompt = SupervisorJudge._build_prompt(case, output)
        assert "(no results)" in prompt

    def test_judge_calls_agent_and_returns_verdict(self, monkeypatch) -> None:
        from sqldim.core.query.dgm.nl.supervisor._judge import (
            JudgeVerdict,
            SupervisorJudge,
        )

        expected_verdict = JudgeVerdict(
            planning_score=0.9,
            execution_score=1.0,
            synthesis_score=0.8,
            overall_score=0.9,
            reasoning="All criteria met",
            passed=True,
        )

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                class R:
                    output = expected_verdict

                return R()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        judge = SupervisorJudge("fake-model")
        case = self._make_case()
        pipeline = self._make_pipeline_output()

        verdict = judge.judge(case, pipeline)
        assert verdict.passed is True
        assert verdict.overall_score == 0.9

    def test_judge_respects_timeout_env_var(self, monkeypatch) -> None:
        """SQLDIM_JUDGE_TIMEOUT env var is read by judge()."""
        from sqldim.core.query.dgm.nl.supervisor._judge import (
            JudgeVerdict,
            SupervisorJudge,
        )

        timeouts_used = []

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, model_settings=None, **kw):
                if model_settings:
                    timeouts_used.append(model_settings.get("timeout"))

                class R:
                    output = JudgeVerdict(
                        planning_score=1.0,
                        execution_score=1.0,
                        synthesis_score=1.0,
                        overall_score=1.0,
                        reasoning="ok",
                        passed=True,
                    )

                return R()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)
        monkeypatch.setenv("SQLDIM_JUDGE_TIMEOUT", "42")

        judge = SupervisorJudge("fake-model")
        judge.judge(self._make_case(), self._make_pipeline_output())
        assert 42.0 in timeouts_used

    def test_build_prompt_with_failed_result(self) -> None:
        """Results with data=None don't crash _build_prompt."""
        from sqldim.core.query.dgm.nl.supervisor._judge import (
            PipelineOutput,
            SupervisorJudge,
        )

        case = self._make_case()
        output = PipelineOutput(
            utterance="test",
            plan_tools=["dgm_query"],
            plan_dataset="ecommerce",
            plan_confidence=0.5,
            plan_budget_total=1.0,
            results=[
                {
                    "tool_name": "dgm_query",
                    "success": False,
                    "data": None,
                    "error": "conn fail",
                }
            ],
            all_succeeded=False,
            synthesis="",
            visited_nodes=[],
            node_timings={},
        )
        prompt = SupervisorJudge._build_prompt(case, output)
        assert "dgm_query" in prompt


# ---------------------------------------------------------------------------
# _tools.py — DgmQueryTool.execute real-execution path (lines 115-158)
# ---------------------------------------------------------------------------


class TestDgmQueryToolRealExecution:
    """DgmQueryTool.execute with a non-None context.model and valid dataset."""

    def _make_context(self, model="fake-model", available=("ecommerce",)):
        """Return a SupervisorContext-like object with model set."""
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        return SupervisorContext(
            available_datasets=list(available),
            model=model,
        )

    def test_load_dataset_exception_returns_failure(self, monkeypatch) -> None:
        """Lines 150-156: when load_dataset raises, returns success=False."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool

        monkeypatch.setattr(
            "sqldim.application.ask.load_dataset",
            lambda name: (_ for _ in ()).throw(RuntimeError("dataset not found")),
        )

        tool = DgmQueryTool()
        ctx = self._make_context(available=["ecommerce"])
        result = tool.execute(
            {"dataset": "ecommerce", "question": "show revenue"}, context=ctx
        )
        assert result.success is False
        assert "dataset not found" in result.error

    def test_dataset_not_in_available_returns_stub(self) -> None:
        """dataset not in context.available_datasets → stub result."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool

        tool = DgmQueryTool()
        ctx = self._make_context(available=["fintech"])  # doesn't have ecommerce
        result = tool.execute(
            {"dataset": "ecommerce", "question": "show revenue"}, context=ctx
        )
        assert result.success is True
        assert result.data.get("stub") is True

    def test_no_model_returns_stub(self) -> None:
        """When model is None, immediately returns stub."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        tool = DgmQueryTool()
        ctx = SupervisorContext(available_datasets=["ecommerce"], model=None)
        result = tool.execute(
            {"dataset": "ecommerce", "question": "show revenue"}, context=ctx
        )
        assert result.success is True
        assert result.data.get("stub") is True

    def test_successful_execution_returns_result(self, monkeypatch) -> None:
        """Lines 115-149, 158-167: successful execution path."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool

        # Mock the full NL pipeline
        class FakeSource:
            def setup(self): ...
            def get_connection(self):
                return mock.MagicMock()

            def get_table_names(self):
                return ["orders", "customers"]

            def teardown(self): ...

        class FakeGraph:
            def invoke(self, state, config=None):
                return {
                    "explanation": "Total revenue is $1M",
                    "result": {"count": 3, "columns": ["cat"], "rows": [["A"]]},
                }

        monkeypatch.setattr(
            "sqldim.application.ask.load_dataset",
            lambda name: mock.MagicMock(),
        )
        monkeypatch.setattr(
            "sqldim.application._pipeline_sources.DatasetPipelineSource",
            lambda ds: FakeSource(),
        )
        monkeypatch.setattr(
            "sqldim.application.ask.build_registry_from_schema",
            lambda con, tables: mock.MagicMock(),
        )
        monkeypatch.setattr(
            "sqldim.application.ask.make_default_budget",
            lambda: mock.MagicMock(),
        )
        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl._graph.build_nl_graph",
            lambda *a, **kw: FakeGraph(),
        )

        tool = DgmQueryTool()
        ctx = self._make_context()
        result = tool.execute(
            {"dataset": "ecommerce", "question": "show revenue"},
            context=ctx,
        )
        assert result.success is True
        assert result.data.get("explanation") == "Total revenue is $1M"
        assert result.cost_actual == 500

    def test_pipeline_exception_returns_failure(self, monkeypatch) -> None:
        """When the NL pipeline itself raises, returns success=False."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool

        def raise_exc(name):
            raise RuntimeError("internal error")

        monkeypatch.setattr("sqldim.application.ask.load_dataset", raise_exc)

        tool = DgmQueryTool()
        ctx = self._make_context()
        result = tool.execute(
            {"dataset": "ecommerce", "question": "test"},
            context=ctx,
        )
        assert result.success is False
        assert "internal error" in result.error

    def test_no_context_returns_stub(self) -> None:
        """No context at all → immediate stub return."""
        from sqldim.core.query.dgm.nl.supervisor._tools import DgmQueryTool

        tool = DgmQueryTool()
        result = tool.execute(
            {"dataset": "ecommerce", "question": "test"}, context=None
        )
        assert result.success is True
        assert result.data.get("stub") is True
