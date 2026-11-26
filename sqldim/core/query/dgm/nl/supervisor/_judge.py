"""DGM Supervisor Agent — LLM-as-judge evaluation framework (§12 evals).

Evaluates the full supervisor pipeline (planning → execution → synthesis)
against structured (query, expected output) pairs using a pydantic-ai
``Agent`` that produces a typed ``JudgeVerdict``.

Three scoring dimensions
------------------------
1. **Planning** — correct tool selection, dataset choice, confidence level
2. **Execution** — all tools succeed, expected result count satisfied
3. **Synthesis** — output mentions required keywords, avoids forbidden terms,
   and satisfies a semantic criterion judged by the LLM

Usage::

    from sqldim.core.query.dgm.nl.supervisor._judge import (
        SupervisorJudge, load_judge_cases, run_pipeline,
    )
    from sqldim.core.query.dgm.nl._agents import make_model
    from sqldim.application.supervisor_host import local_supervisor_runtime

    judge = SupervisorJudge(make_model("openai", "gpt-4o-mini"))
    cases = load_judge_cases()
    runtime = local_supervisor_runtime(datasets, demo=True)  # explicit (D47)
    output = run_pipeline(cases[0].utterance, runtime)
    verdict = judge.judge(cases[0], output)
    print(verdict.overall_score, verdict.reasoning)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator

from sqldim.core.query.dgm.nl.supervisor._graph import (
    SupervisorRuntime,
    run_supervisor,
)
from sqldim.core.query.dgm.nl.supervisor._types import CallStatus, SupervisorPlan

__all__ = [
    "JudgeCase",
    "JudgeVerdict",
    "PipelineOutput",
    "SupervisorJudge",
    "load_judge_cases",
    "run_pipeline",
]

# ---------------------------------------------------------------------------
# Default JSON path — tests/supervisor/judge_evals.json
# ---------------------------------------------------------------------------

_DEFAULT_EVALS_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent.parent.parent.parent
    / "tests"
    / "supervisor"
    / "judge_evals.json"
)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _PlanningExpectation:
    tools: list[str]
    dataset: str | None
    confidence_min: float


@dataclass(frozen=True)
class _ExecutionExpectation:
    all_succeed: bool
    result_count: int
    outcome: str | None = None


@dataclass(frozen=True)
class _SynthesisExpectation:
    must_mention: list[str]
    must_not_mention: list[str]
    semantic_criteria: str


@dataclass(frozen=True)
class JudgeCase:
    """A single LLM judge eval case loaded from ``judge_evals.json``."""

    id: str
    utterance: str
    tags: list[str]
    planning: _PlanningExpectation
    execution: _ExecutionExpectation
    synthesis: _SynthesisExpectation


@dataclass
class PipelineOutput:
    """Captured output from a supervisor pipeline invocation."""

    utterance: str
    plan_tools: list[str]
    plan_dataset: str | None
    plan_confidence: float
    plan_budget_total: float
    results: list[dict[str, Any]]
    all_succeeded: bool
    outcome: str
    demo: bool
    synthesis: str
    visited_nodes: list[str]
    node_timings: dict[str, float]


class JudgeVerdict(BaseModel):
    """Structured LLM judge output — one verdict per eval case."""

    planning_score: float
    execution_score: float
    synthesis_score: float
    overall_score: float
    reasoning: str
    passed: bool

    @field_validator(
        "planning_score", "execution_score", "synthesis_score", "overall_score"
    )
    @classmethod
    def _score_in_range(cls, v: float) -> float:
        return min(max(v, 0.0), 1.0)


# ---------------------------------------------------------------------------
# JSON loader
# ---------------------------------------------------------------------------


def _parse_case(d: dict[str, Any]) -> JudgeCase:
    exp = d["expected"]
    p = exp["planning"]
    e = exp["execution"]
    s = exp["synthesis"]
    return JudgeCase(
        id=d["id"],
        utterance=d["utterance"],
        tags=d.get("tags", []),
        planning=_PlanningExpectation(
            tools=p["tools"],
            dataset=p.get("dataset"),
            confidence_min=p.get("confidence_min", 0.0),
        ),
        execution=_ExecutionExpectation(
            all_succeed=e["all_succeed"],
            result_count=e["result_count"],
            outcome=e.get("outcome"),
        ),
        synthesis=_SynthesisExpectation(
            must_mention=s.get("must_mention", []),
            must_not_mention=s.get("must_not_mention", []),
            semantic_criteria=s.get("semantic_criteria", ""),
        ),
    )


def load_judge_cases(path: Path | None = None) -> list[JudgeCase]:
    """Load judge eval cases from a JSON file.

    Falls back to ``tests/supervisor/judge_evals.json`` when *path* is
    ``None``.
    """
    p = path or _DEFAULT_EVALS_PATH
    with p.open() as f:
        data = json.load(f)
    return [_parse_case(c) for c in data.get("cases", [])]


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _plan_summary(
    plan: SupervisorPlan | None,
) -> tuple[list[str], str | None, float, float]:
    if plan is None:
        return [], None, 0.0, 0.0
    first_ds = next((s.datasets[0] for s in plan.steps if s.datasets), None)
    return (
        [s.tool_name for s in plan.steps],
        first_ds,
        plan.confidence,
        plan.total_budget_fraction,
    )


def run_pipeline(
    utterance: str,
    runtime: SupervisorRuntime,
    *,
    thread_id: str = "judge",
) -> PipelineOutput:
    """Run the supervisor once and capture what the judge needs.

    The caller supplies the runtime, so demo mode is always its explicit
    choice (D47); demo runs are reported as such.
    """
    state = run_supervisor(utterance, runtime, thread_id=thread_id)
    tools, first_ds, confidence, budget = _plan_summary(state.plan)
    results = [r.model_dump(mode="json") for r in state.results.values()]
    return PipelineOutput(
        utterance=utterance,
        plan_tools=tools,
        plan_dataset=first_ds,
        plan_confidence=confidence,
        plan_budget_total=budget,
        results=results,
        all_succeeded=bool(results)
        and all(r.status is CallStatus.SUCCEEDED for r in state.results.values()),
        outcome=(state.outcome.value if state.outcome else "FAILED"),
        demo=state.demo,
        synthesis=state.synthesis or "",
        visited_nodes=state.visited_nodes,
        node_timings=state.node_timings,
    )


# ---------------------------------------------------------------------------
# LLM Judge
# ---------------------------------------------------------------------------

_JUDGE_SYSTEM_PROMPT = """\
You are an expert evaluator of an analytical supervisor agent pipeline.
You will be given:
  1. The user's input utterance
  2. The pipeline output (plan, tool results, synthesis)
  3. Expected criteria for planning, execution, and synthesis

IMPORTANT: Judge what was executed, never what was intended.  A step that
did not run (NOT_IMPLEMENTED, UNAUTHORIZED, SKIPPED, …) is a failure of that
step, not a success, and a response that presents missing results as an
answer is wrong.  When "Demo mode" is true the output is heuristic (D47):
score it, but it is never evidence of a contract result.

Score each dimension on a 0.0–1.0 scale:

## Planning Score
- 1.0: Correct tools selected, correct dataset, confidence appropriate
- 0.7: Right tools but minor issues (e.g. dataset not explicitly matched,
  confidence slightly below ideal but above the stated minimum)
- 0.3: Wrong tools or missing expected tools
- 0.0: Completely wrong or empty plan when one was expected

## Execution Score
- 1.0: All tools succeeded, result count matches expectation
- 0.7: Tools succeeded but result count differs slightly
- 0.3: Some tools failed
- 0.0: All tools failed or no results when expected

## Synthesis Score
- 1.0: States the outcome, answers from SUCCEEDED results only, mentions the
  required keywords and satisfies the semantic criteria
- 0.7: Correct outcome and content, minor omissions
- 0.3: Required keywords absent or semantic criterion not met
- 0.0: Irrelevant, empty, contains a forbidden term, or presents a step that
  did not succeed as if it had

## Overall Score
Weighted average: planning=0.3, execution=0.3, synthesis=0.4
Set passed=true when overall_score >= 0.5.

Be precise and objective.  Base your scores strictly on the criteria, not on
subjective impressions.  Return your reasoning in 2–3 sentences.
"""


class SupervisorJudge:
    """LLM-as-judge for the DGM supervisor pipeline.

    Uses a pydantic-ai ``Agent`` to evaluate pipeline output against
    expected criteria and produce a typed ``JudgeVerdict``.
    """

    def __init__(self, model: Any) -> None:
        self._model = model

    def judge(self, case: JudgeCase, output: PipelineOutput) -> JudgeVerdict:
        """Evaluate *output* against *case* expectations using the LLM judge."""
        from pydantic_ai import Agent  # lazy import

        agent: Agent[None, JudgeVerdict] = Agent(
            model=self._model,
            output_type=JudgeVerdict,
            system_prompt=_JUDGE_SYSTEM_PROMPT,
        )

        timeout = float(os.environ.get("SQLDIM_JUDGE_TIMEOUT", "30"))
        user_message = self._build_prompt(case, output)
        result = agent.run_sync(user_message, model_settings={"timeout": timeout})
        return result.output

    @staticmethod
    def _build_prompt(case: JudgeCase, output: PipelineOutput) -> str:
        """Format the judge evaluation prompt."""
        results_summary = []
        for r in output.results:
            name = r.get("tool_name", "?")
            status = r.get("status", "?")
            error = (r.get("error") or {}).get("kind")
            data_keys = (
                list(r.get("data", {}).keys())
                if isinstance(r.get("data"), dict)
                else []
            )
            results_summary.append(
                f"  - {name}: status={status}, error={error}, data_keys={data_keys}"
            )

        results_text = (
            "\n".join(results_summary) if results_summary else "  (no results)"
        )

        return f"""\
## Input
Utterance: {output.utterance!r}

## Pipeline Output
Plan tools: {output.plan_tools}
Plan dataset: {output.plan_dataset}
Plan confidence: {output.plan_confidence:.2f}
Plan budget total: {output.plan_budget_total:.3f}
Visited nodes: {output.visited_nodes}

Tool Results ({len(output.results)} total):
{results_text}

All tools succeeded: {output.all_succeeded}
Outcome: {output.outcome}
Demo mode: {output.demo}

Synthesis:
{output.synthesis}

## Expected Criteria

### Planning
Expected tools: {case.planning.tools}
Expected dataset: {case.planning.dataset}
Minimum confidence: {case.planning.confidence_min}

### Execution
Expect all succeed: {case.execution.all_succeed}
Expected result count: {case.execution.result_count}
Expected outcome: {case.execution.outcome}

### Synthesis
Must mention: {case.synthesis.must_mention}
Must NOT mention: {case.synthesis.must_not_mention}
Semantic criteria: {case.synthesis.semantic_criteria}
"""
