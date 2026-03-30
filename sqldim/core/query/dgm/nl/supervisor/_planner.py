"""DGM Supervisor Agent — question decomposition planner (§12.2).

LLM-first planning
------------------
When a pydantic-ai model is supplied (via ``context.model``), the planner
runs a structured-output agent that reads the full tool registry and the
available datasets, then produces a typed ``_PlanResult`` which is
converted to a ``SupervisorPlan``.

The agent's system prompt explains:
* Which 9 DGM tools exist and their purpose
* The available datasets (injected at call time)
* Budget rules (all ``budget_fraction`` values must sum to ≤ 1.0)
* Confidence semantics

Keyword fallback
----------------
When no model is provided (``context.model is None``), or if the LLM call
fails, the planner falls back to phrase-based keyword routing.  This keeps
tests fast and deterministic, and lets CLI usage work without API keys.

Confidence
----------
* LLM path: the model sets ``confidence`` directly (0–1)
* Fallback path: heuristic scoring identical to the previous implementation

Plans with ``confidence < 0.5`` are routed to the ``reflect`` node in
the supervisor graph, which prepends orientation steps so the synthesiser
can produce a meaningful response even for vague inputs.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorPlan,
    ToolCallRequest,
)

__all__ = ["decompose_question"]

# ---------------------------------------------------------------------------
# Pydantic-AI planner output schema
# ---------------------------------------------------------------------------

_TOOL_REGISTRY_SUMMARY = """
Available DGM tools (use exactly these names in tool_name):
  - dgm_query              : Execute an analytical NL query against ONE named dataset
  - dgm_what_can_i_answer  : List which datasets exist and what questions they can answer
  - dgm_map_analytical_space: Enumerate the full tool registry with descriptions
  - dgm_correlate          : JOIN / correlate results from two different analyses
  - dgm_explore_from       : Navigate from a result to related questions (recommender)
  - dgm_sensitivity_sweep  : Sweep a parameter space to find sensitivity boundaries
  - dgm_validate_invariant : Validate a formal model invariant holds
  - dgm_store_finding      : Persist an analytical finding to the knowledge base
  - dgm_retrieve_related   : Retrieve lattice-adjacent findings from the knowledge base
""".strip()

_PLANNER_SYSTEM_PROMPT = f"""\
You are a DGM Supervisor Planner.  Your job is to decompose the user's
question into an ordered plan of DGM tool calls.

{_TOOL_REGISTRY_SUMMARY}

Rules:
- Include ONLY tools that directly contribute to answering the question.
- For "which datasets / what data do you know about" style questions → dgm_what_can_i_answer
- For "what tools / capabilities do you have" style questions → dgm_map_analytical_space
- For analytical domain questions (churn, revenue, counts, trends…) → dgm_query, picking
  the most relevant dataset from the available_datasets list.
- For comparison / cross-domain questions → two dgm_query steps or dgm_correlate.
- budget_fraction values MUST sum to exactly 1.0 across all steps.
- confidence: 0.9 for a clear question; 0.7 for a reasonable one; < 0.5 for vague/too short.
- reasoning: one sentence explaining which tool(s) you selected and why.
"""


class _PlannerStep(BaseModel):
    tool_name: str
    arguments: dict[str, Any]
    budget_fraction: float = 1.0


class _PlanResult(BaseModel):
    steps: list[_PlannerStep]
    confidence: float
    reasoning: str


# ---------------------------------------------------------------------------
# LLM planner path
# ---------------------------------------------------------------------------


def _llm_decompose(
    utterance: str,
    context: SupervisorContext,
    llm_model: Any,
) -> SupervisorPlan:
    """Run the pydantic-ai planner agent and convert its output to SupervisorPlan."""
    from pydantic_ai import Agent  # lazy import

    agent: Any = Agent(
        model=llm_model,
        output_type=_PlanResult,
        system_prompt=_PLANNER_SYSTEM_PROMPT,
    )

    dataset_list = ", ".join(context.available_datasets) if context.available_datasets else "none"
    user_message = (
        f"Available datasets: {dataset_list}\n\nQuestion: {utterance}"
    )

    result = agent.run_sync(user_message)
    plan_result: _PlanResult = result.output

    # Normalise budget fractions to sum exactly to 1.0
    raw_steps = plan_result.steps
    total = sum(s.budget_fraction for s in raw_steps) or 1.0
    steps = [
        ToolCallRequest(
            tool_name=s.tool_name,
            arguments=s.arguments,
            budget_fraction=round(s.budget_fraction / total, 6),
        )
        for s in raw_steps
        if s.tool_name
    ]
    if not steps:
        return _keyword_decompose(utterance, context)

    confidence = min(max(plan_result.confidence, 0.0), 1.0)
    return SupervisorPlan(goal=utterance, steps=steps, confidence=confidence)


# ---------------------------------------------------------------------------
# Keyword fallback (phrase-based, no LLM required)
# ---------------------------------------------------------------------------

_QUESTION_MARKERS: frozenset[str] = frozenset({
    "what", "who", "when", "where", "how", "why", "which",
    "show", "list", "get", "find", "top", "count", "sum",
    "average", "compare", "mean", "total", "trend",
})

_REFLECT_MIN_LENGTH = 5

_DATASET_PHRASES = [
    "which dataset", "what dataset", "show dataset", "list dataset",
    "available dataset", "what data", "which data", "available data",
    "what can you see", "what do you see", "what do you know",
    "can you see", "you see",
]


def _score_confidence(utterance: str, wants_datasets: bool, wants_tools: bool) -> float:
    utt = utterance.strip()
    if not utt:
        return 0.1
    if len(utt) < _REFLECT_MIN_LENGTH:
        return 0.3
    if wants_datasets and wants_tools:
        return 0.9
    if wants_datasets or wants_tools:
        return 0.7
    if set(utt.lower().split()) & _QUESTION_MARKERS:
        return 0.55
    return 0.35


def _keyword_decompose(
    utterance: str,
    context: SupervisorContext | None,
) -> SupervisorPlan:
    """Phrase/keyword routing — used when no LLM is configured."""
    datasets = context.available_datasets if context else []
    utt = utterance.lower()

    wants_datasets = any(phrase in utt for phrase in _DATASET_PHRASES)
    wants_tools = any(w in utt for w in ["tool", "capabilit"])
    confidence = _score_confidence(utterance, wants_datasets, wants_tools)

    if wants_datasets and wants_tools:
        steps: list[ToolCallRequest] = [
            ToolCallRequest(
                tool_name="dgm_what_can_i_answer",
                arguments={"dataset": "__all__"},
                budget_fraction=0.5,
            ),
            ToolCallRequest(
                tool_name="dgm_map_analytical_space",
                arguments={"dataset": "__all__"},
                budget_fraction=0.5,
            ),
        ]
    elif wants_datasets:
        steps = [
            ToolCallRequest(
                tool_name="dgm_what_can_i_answer",
                arguments={"dataset": "__all__"},
                budget_fraction=1.0,
            ),
        ]
    elif wants_tools:
        steps = [
            ToolCallRequest(
                tool_name="dgm_map_analytical_space",
                arguments={"dataset": "__all__"},
                budget_fraction=1.0,
            ),
        ]
    else:
        target = datasets[0] if datasets else "unknown"
        steps = [
            ToolCallRequest(
                tool_name="dgm_query",
                arguments={"dataset": target, "question": utterance},
                budget_fraction=1.0,
            ),
        ]

    return SupervisorPlan(goal=utterance, steps=steps, confidence=confidence)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def decompose_question(
    utterance: str,
    context: SupervisorContext | None = None,
    *,
    model: Any = None,
) -> SupervisorPlan:
    """Decompose *utterance* into an ordered plan of DGM tool calls.

    When ``context.model`` is a pydantic-ai model instance the LLM planner
    is used, which understands the full analytical intent of the question.

    When no model is available the phrase-based keyword heuristic is used as
    a deterministic, cost-free fallback.  The ``model`` keyword argument (if
    provided) takes precedence over ``context.model``.
    """
    llm_model = model or (context.model if context else None)

    if llm_model is not None:
        try:
            return _llm_decompose(
                utterance,
                context or SupervisorContext(available_datasets=[]),
                llm_model,
            )
        except Exception:
            pass  # graceful degradation to keyword routing

    return _keyword_decompose(utterance, context)
