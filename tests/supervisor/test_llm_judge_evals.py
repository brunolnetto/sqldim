"""LLM judge eval suite for the DGM supervisor pipeline.

This test suite evaluates the full supervisor pipeline (planning → execution →
synthesis) using an LLM-as-judge.  It is gated behind the ``SQLDIM_LLM_JUDGE``
environment variable and never runs during default ``pytest`` sweeps.

Usage::

    # Run all judge eval cases (requires an API key for the chosen provider)
    SQLDIM_LLM_JUDGE=1 python -m pytest tests/supervisor/test_llm_judge_evals.py -x -v

    # Run a single case
    SQLDIM_LLM_JUDGE=1 python -m pytest tests/supervisor/test_llm_judge_evals.py -x -k "judge.01"

    # Override provider/model via environment
    SQLDIM_LLM_JUDGE=1 SQLDIM_JUDGE_PROVIDER=anthropic SQLDIM_JUDGE_MODEL=claude-3-5-haiku-latest \\
        python -m pytest tests/supervisor/test_llm_judge_evals.py -x

Environment variables
---------------------
``SQLDIM_LLM_JUDGE``
    Set to ``1`` to enable the external LLM judge evaluations. Deterministic\npre-checks always run.
``SQLDIM_JUDGE_PROVIDER``
    LLM provider for the judge agent (default: ``openai``).
``SQLDIM_JUDGE_MODEL``
    Model name for the judge agent (default: ``gpt-4o-mini``).
``SQLDIM_JUDGE_TIMEOUT``
    Per-call timeout in seconds for judge LLM requests (default: ``30``).  If
    any judge call exceeds this, the LLM judge tests are skipped rather than
    hanging indefinitely.

Test classes
------------
* TestDeterministicPreChecks  — fast-fail routing, budget, and confidence assertions
* TestLLMJudgeFullPipeline   — per-case LLM judge evaluation across all 3 dimensions
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

from sqldim.core.query.dgm.nl.supervisor._judge import (
    JudgeCase,
    PipelineOutput,
    load_judge_cases,
    run_pipeline,
)
from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

# ---------------------------------------------------------------------------
# Gate: only the external LLM judge evaluations require explicit opt-in
# ---------------------------------------------------------------------------

_LLM_JUDGE_ENABLED = os.environ.get("SQLDIM_LLM_JUDGE", "0") == "1"

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_JUDGE_CASES: list[JudgeCase] = load_judge_cases()


def _case_id(c: JudgeCase) -> str:
    return c.id


# Build a context using all available domains
_ALL_DATASETS = [
    "devops",
    "data_models",
    "commerce",
    "growth",
    "fintech",
    "entertainment",
    "nba_analytics",
    "telemetry",
]

_CTX = SupervisorContext(available_datasets=_ALL_DATASETS)

_PASS_THRESHOLD = 0.6  # planning and execution
_SYNTHESIS_THRESHOLD = 0.4  # stubs produce minimal synthesis by design
_OVERALL_THRESHOLD = 0.5  # realistic target for a stub pipeline


# Provider → required env-var name for cloud APIs.
_PROVIDER_API_KEY_VARS: dict[str, str] = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GOOGLE_API_KEY",
    "groq": "GROQ_API_KEY",
    "mistral": "MISTRAL_API_KEY",
}


@pytest.fixture(scope="module")
def judge() -> Any:
    """Build the LLM judge agent once per module.

    Skips all LLM judge tests when the required API key for the configured
    provider is absent, avoiding indefinite hangs on machines without
    cloud credentials.
    """
    from sqldim.core.query.dgm.nl._agents import make_model
    from sqldim.core.query.dgm.nl.supervisor._judge import SupervisorJudge

    provider = os.environ.get("SQLDIM_JUDGE_PROVIDER", "openai")
    model_name = os.environ.get("SQLDIM_JUDGE_MODEL", "gpt-4o-mini")

    key_var = _PROVIDER_API_KEY_VARS.get(provider.lower())
    if key_var and not os.environ.get(key_var):
        pytest.skip(
            f"SQLDIM_LLM_JUDGE=1 but {key_var} is not set — "
            f"skipping LLM judge tests (provider={provider!r})"
        )

    model = make_model(provider, model_name)
    return SupervisorJudge(model)


@pytest.fixture(scope="module")
def pipeline_outputs() -> dict[str, PipelineOutput]:
    """Run the supervisor pipeline once per case and cache the outputs.

    This avoids re-running the (deterministic, stub) pipeline for every
    test class that needs the same output.
    """
    cache: dict[str, PipelineOutput] = {}
    for case in _JUDGE_CASES:
        cache[case.id] = run_pipeline(
            case.utterance,
            context=_CTX,
            thread_id=f"judge-{case.id}-{uuid.uuid4().hex[:8]}",
        )
    return cache


@pytest.fixture(scope="module")
def judge_verdicts(
    judge: Any, pipeline_outputs: dict[str, PipelineOutput]
) -> dict[str, Any]:
    """Compute all LLM judge verdicts once per module, in parallel.

    Parallelising the 30 × ~3.5s LLM calls with a thread pool cuts setup
    from ~100s (sequential) to roughly 100s / pool_size seconds.
    Without this caching each of the 4 test methods per case would also
    call judge.judge() independently (120 total calls vs 30).

    If any call exceeds ``SQLDIM_JUDGE_TIMEOUT`` seconds (default: 30) the
    entire batch is cancelled and the tests are skipped.
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from concurrent.futures import as_completed

    from sqldim.core.query.dgm.nl.supervisor._judge import JudgeVerdict

    _per_call_timeout = float(os.environ.get("SQLDIM_JUDGE_TIMEOUT", "30"))
    _total_timeout = _per_call_timeout * len(_JUDGE_CASES) + 30

    cache: dict[str, JudgeVerdict] = {}

    def _run_one(case: Any) -> tuple[str, JudgeVerdict]:
        return case.id, judge.judge(case, pipeline_outputs[case.id])

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_run_one, case): case for case in _JUDGE_CASES}
        try:
            for future in as_completed(futures, timeout=_total_timeout):
                case_id, verdict = future.result(timeout=_per_call_timeout)
                cache[case_id] = verdict
        except (FuturesTimeout, TimeoutError):
            executor.shutdown(wait=False, cancel_futures=True)
            pytest.skip(
                f"LLM judge timed out (>{_per_call_timeout}s per call) — "
                "check LLM availability or increase SQLDIM_JUDGE_TIMEOUT"
            )

    return cache


# ---------------------------------------------------------------------------
# Deterministic pre-checks (no LLM required — fast-fail gate)
# ---------------------------------------------------------------------------


class TestDeterministicPreChecks:
    """Fast structural assertions that catch obvious failures before LLM judge."""

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_plan_routes_expected_tools(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        """Full pipeline (including reflect) selects the expected tools."""
        output = pipeline_outputs[case.id]
        assert output.plan_tools == case.planning.tools, (
            f"[{case.id}] Expected tools {case.planning.tools}, got {output.plan_tools}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_plan_confidence_above_minimum(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        output = pipeline_outputs[case.id]
        assert output.plan_confidence >= case.planning.confidence_min, (
            f"[{case.id}] Confidence {output.plan_confidence:.2f} "
            f"< expected min {case.planning.confidence_min}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_plan_budget_within_bounds(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        output = pipeline_outputs[case.id]
        assert output.plan_budget_total <= 1.0 + 1e-9, (
            f"[{case.id}] Budget sum {output.plan_budget_total:.6f} > 1.0"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_pipeline_result_count(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        output = pipeline_outputs[case.id]
        assert len(output.results) == case.execution.result_count, (
            f"[{case.id}] Expected {case.execution.result_count} results, "
            f"got {len(output.results)}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_pipeline_all_succeeded(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        if not case.execution.all_succeed:
            pytest.skip("case does not expect all_succeed")
        output = pipeline_outputs[case.id]
        assert output.all_succeeded, f"[{case.id}] Expected all tools to succeed"

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_synthesis_must_mention(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        output = pipeline_outputs[case.id]
        synth_lower = output.synthesis.lower()
        for keyword in case.synthesis.must_mention:
            assert keyword.lower() in synth_lower, (
                f"[{case.id}] Synthesis missing required keyword '{keyword}': "
                f"{output.synthesis!r}"
            )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_synthesis_must_not_mention(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        output = pipeline_outputs[case.id]
        synth_lower = output.synthesis.lower()
        for keyword in case.synthesis.must_not_mention:
            assert keyword.lower() not in synth_lower, (
                f"[{case.id}] Synthesis contains forbidden keyword '{keyword}': "
                f"{output.synthesis!r}"
            )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_dataset_selection(
        self,
        case: JudgeCase,
        pipeline_outputs: dict[str, PipelineOutput],
    ) -> None:
        """When a specific dataset is expected, the plan selects it.
        When no dataset is expected, verify a valid one was chosen (or None)."""
        output = pipeline_outputs[case.id]
        if case.planning.dataset is not None:
            assert output.plan_dataset == case.planning.dataset, (
                f"[{case.id}] Expected dataset '{case.planning.dataset}', "
                f"got '{output.plan_dataset}'"
            )
        else:
            # No specific dataset mandated — plan_dataset should be either
            # None (vague / exploratory) or a member of the available list.
            if output.plan_dataset is not None:
                assert output.plan_dataset in _ALL_DATASETS, (
                    f"[{case.id}] Unexpected dataset '{output.plan_dataset}'"
                )


# ---------------------------------------------------------------------------
# LLM Judge — full pipeline evaluation
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _LLM_JUDGE_ENABLED,
    reason="SQLDIM_LLM_JUDGE=1 not set — skipping LLM judge eval suite",
)
class TestLLMJudgeFullPipeline:
    """Per-case LLM judge evaluation of planning + execution + synthesis."""

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_planning_score(
        self,
        case: JudgeCase,
        judge_verdicts: dict[str, Any],
    ) -> None:
        verdict = judge_verdicts[case.id]
        assert verdict.planning_score >= _PASS_THRESHOLD, (
            f"[{case.id}] Planning score {verdict.planning_score:.2f} "
            f"< {_PASS_THRESHOLD}: {verdict.reasoning}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_execution_score(
        self,
        case: JudgeCase,
        judge_verdicts: dict[str, Any],
    ) -> None:
        verdict = judge_verdicts[case.id]
        assert verdict.execution_score >= _PASS_THRESHOLD, (
            f"[{case.id}] Execution score {verdict.execution_score:.2f} "
            f"< {_PASS_THRESHOLD}: {verdict.reasoning}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_synthesis_score(
        self,
        case: JudgeCase,
        judge_verdicts: dict[str, Any],
    ) -> None:
        verdict = judge_verdicts[case.id]
        assert verdict.synthesis_score >= _SYNTHESIS_THRESHOLD, (
            f"[{case.id}] Synthesis score {verdict.synthesis_score:.2f} "
            f"< {_SYNTHESIS_THRESHOLD}: {verdict.reasoning}"
        )

    @pytest.mark.parametrize("case", _JUDGE_CASES, ids=_case_id)
    def test_overall_pass(
        self,
        case: JudgeCase,
        judge_verdicts: dict[str, Any],
    ) -> None:
        verdict = judge_verdicts[case.id]
        assert verdict.overall_score >= _OVERALL_THRESHOLD, (
            f"[{case.id}] Judge FAILED (overall={verdict.overall_score:.2f} "
            f"< {_OVERALL_THRESHOLD}): {verdict.reasoning}"
        )
