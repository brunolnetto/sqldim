"""DGM Supervisor Agent — question decomposition planner (§12.2).

The planner takes a user utterance and decomposes it into an ordered
sequence of tool calls (SupervisorPlan).  In stub mode it uses keyword
routing to infer intent; with an LLM it uses structured output to produce
multi-step plans.
"""

from __future__ import annotations

from typing import Any

from sqldim.core.query.dgm.supervisor._types import (
    SupervisorContext,
    SupervisorPlan,
    ToolCallRequest,
)

__all__ = ["decompose_question"]


def decompose_question(
    utterance: str,
    context: SupervisorContext | None = None,
    *,
    model: Any = None,
) -> SupervisorPlan:
    """Decompose *utterance* into an ordered plan of DGM tool calls.

    Keyword routing (stub / no-LLM mode):

    * Mentions datasets / seeing / available  → ``dgm_what_can_i_answer``
    * Mentions tools / capabilities           → ``dgm_map_analytical_space``
    * Both at once                            → two-step plan at 0.5 each
    * Everything else                         → ``dgm_query`` on first dataset
    """
    datasets = context.available_datasets if context else []
    utt = utterance.lower()

    wants_datasets = any(
        w in utt for w in ["dataset", "data", "see", "available", "know"]
    )
    wants_tools = any(
        w in utt for w in ["tool", "capabilit", "can you", "have"]
    )

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

    return SupervisorPlan(goal=utterance, steps=steps)
