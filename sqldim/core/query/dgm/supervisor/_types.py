"""DGM Supervisor Agent — type definitions (§12).

Six agent-safe properties:
  1. Closed output vocabulary — all tool outputs are typed
  2. Formal question space — tools operate on Q_valid(A,B)
  3. Deterministic equivalence — same input yields same output
  4. Formal failure modes — typed errors, not exceptions
  5. Cost estimation before execution — budget gates at every level
  6. Composable via Q_algebra — tool outputs are joinable/unionable
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, field_validator, model_validator

__all__ = [
    "ToolCapabilitySchema",
    "ToolCallRequest",
    "ToolCallResult",
    "SupervisorPlan",
    "SupervisorState",
    "SupervisorContext",
]


# ---------------------------------------------------------------------------
# Tool contract types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCapabilitySchema:
    """5-point tool contract: schema declaring what a tool can do (§12.1)."""

    name: str
    description: str
    input_schema: dict[str, str]
    output_schema: dict[str, str]
    composable_with: list[str] = field(default_factory=list)


class ToolCallRequest(BaseModel):
    """Supervisor's instruction to invoke one tool (§12.2)."""

    tool_name: str
    arguments: dict[str, Any]
    budget_fraction: float = 1.0

    @field_validator("budget_fraction")
    @classmethod
    def _budget_in_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"budget_fraction must be in [0, 1], got {v}")
        return v

    model_config = {"frozen": True}


class ToolCallResult(BaseModel):
    """Output of a single tool invocation (§12.1)."""

    tool_name: str
    success: bool
    data: Any = None
    error: str | None = None
    cost_actual: int = 0

    model_config = {"arbitrary_types_allowed": True}


class SupervisorPlan(BaseModel):
    """Ordered sequence of tool calls decomposing a user question (§12.2)."""

    goal: str
    steps: list[ToolCallRequest]

    @property
    def total_budget_fraction(self) -> float:
        return sum(s.budget_fraction for s in self.steps)

    @model_validator(mode="after")
    def _budget_within_bounds(self) -> "SupervisorPlan":
        total = self.total_budget_fraction
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"Total budget fraction {total:.3f} exceeds 1.0"
            )
        return self

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# LangGraph state
# ---------------------------------------------------------------------------


class SupervisorState(BaseModel):
    """Persistent state threaded through the supervisor LangGraph (§12.2)."""

    utterance: str
    plan: SupervisorPlan | None = None
    current_step: int = 0
    results: list[ToolCallResult] = []
    synthesis: str | None = None
    visited_nodes: list[str] = []
    node_timings: dict[str, float] = {}

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


@dataclass
class SupervisorContext:
    """Carries runtime dependencies into the supervisor graph (§12.3)."""

    available_datasets: list[str]
    budget: Any = None
    model: Any = None
