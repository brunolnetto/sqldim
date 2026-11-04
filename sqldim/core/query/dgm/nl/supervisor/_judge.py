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

    judge = SupervisorJudge(make_model("openai", "gpt-4o-mini"))
    cases = load_judge_cases()
    output = run_pipeline(cases[0].utterance)
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

from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorState,
)

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


def run_pipeline(
    utterance: str,
    context: SupervisorContext | None = None,
    *,
    thread_id: str = "judge",
    recursion_limit: int = 25,
) -> PipelineOutput:
    """Run the full supervisor graph and capture structured output."""
    if context is None:
        from sqldim.application.ask import KNOWN_DATASETS

        context = SupervisorContext(available_datasets=sorted(KNOWN_DATASETS.keys()))

    graph = build_supervisor_graph(context=context)
    raw = graph.invoke(
        SupervisorState(utterance=utterance).model_dump(),
        config={
            "configurable": {"thread_id": thread_id},
            "recursion_limit": recursion_limit,
        },
    )

    # Extract plan details — LangGraph may return either a Pydantic model
    # or a serialised dict depending on the graph configuration.
    plan = raw.get("plan")
    if plan is not None:
        # Normalise to dict if it's a Pydantic model
        plan_d: dict[str, Any] = plan if isinstance(plan, dict) else plan.model_dump()
        steps = plan_d.get("steps", [])
        plan_tools = [s["tool_name"] for s in steps]
        first_ds = None
        for s in steps:
            ds = s.get("arguments", {}).get("dataset")
            if ds and ds != "__all__":
                first_ds = ds
                break
        plan_confidence = plan_d.get("confidence", 0.0)
        plan_budget = sum(s.get("budget_fraction", 0) for s in steps)
    else:  # pragma: no cover  — graph always produces a plan
        plan_tools = []
        first_ds = None
        plan_confidence = 0.0
        plan_budget = 0.0

    # Extract results — normalise Pydantic models to dicts
    raw_results = raw.get("results", [])
    results = []
    all_ok = True
    for r in raw_results:
        rd = r if isinstance(r, dict) else r.model_dump()
        results.append(rd)
        if not rd.get("success", False):  # pragma: no cover  — all stubs succeed
            all_ok = False

    return PipelineOutput(
        utterance=utterance,
        plan_tools=plan_tools,
        plan_dataset=first_ds,
        plan_confidence=plan_confidence,
        plan_budget_total=plan_budget,
        results=results,
        all_succeeded=all_ok,
        synthesis=raw.get("synthesis", "") or "",
        visited_nodes=raw.get("visited_nodes", []),
        node_timings=raw.get("node_timings", {}),
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

IMPORTANT: This is a STUB pipeline used in development/testing.  Tool
executions produce minimal stub output.  Synthesis text of the form
  'DGM query on \'X\' for \'<utterance>\' completed (stub — pass …)'
is the EXPECTED output for a successful stub run — do NOT penalise it.
The embedded utterance text within synthesis counts toward satisfying
semantic criteria and must_mention keyword checks.

SPECIAL CASE — Orientation/Reflection responses:
When the utterance is empty or a single vague word (like "", "help", "?",
"analyze", "interesting") the pipeline correctly invokes the reflect node
and produces orientation content listing available datasets and tools.
If the synthesis:
  • contains a list of datasets (any of: 'Dataset', 'dataset', 'Datasets'),
  • contains a list of tools (any of: 'Tool', 'tool', 'Tools'),
  • and avoids forbidden terms,
that IS the correct and complete answer — score synthesis 0.9 or 1.0.
Do NOT score orientation synthesis as 0.3 just because the utterance was
short or empty.

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
For stub pipelines the synthesis is intentionally brief.  Apply these rules:
- 1.0: Mentions all required keywords directly and satisfies semantic criteria
- 0.9: Orientation response listing datasets and tools (see SPECIAL CASE above)
- 0.7: All must_mention keywords present (directly or via embedded utterance),
  semantic criteria largely met even if output is brief
- 0.5: Synthesis confirms the correct tool ran and the embedded utterance
  provides enough context to satisfy the semantic criterion; stub indicator
  present but not penalised
- 0.3: Required keywords absent AND semantic criterion not inferable from
  either the synthesis text or the embedded utterance
- 0.0: Irrelevant, empty, or contains a forbidden term

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
            ok = r.get("success", False)
            data_keys = (
                list(r.get("data", {}).keys())
                if isinstance(r.get("data"), dict)
                else []
            )
            results_summary.append(f"  - {name}: success={ok}, data_keys={data_keys}")

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

### Synthesis
Must mention: {case.synthesis.must_mention}
Must NOT mention: {case.synthesis.must_not_mention}
Semantic criteria: {case.synthesis.semantic_criteria}
"""
