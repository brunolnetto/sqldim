"""DGM Supervisor Agent — 9 tool implementations (§12.1).

8 core tools operationalizing the DGM formal machinery:
  1. dgm_query            — Run a full NL→SQL pipeline against a dataset
  2. dgm_what_can_i_answer — List answerable questions from Q_valid(A,B)
  3. dgm_correlate         — Cross-domain JOIN of two tool results
  4. dgm_explore_from      — Navigate from a result to related questions
  5. dgm_sensitivity_sweep — Parameter sweep analysis over P_Σ
  6. dgm_validate_invariant — Check a formal invariant of the model
  7. dgm_store_finding     — Persist a finding to the knowledge base
  8. dgm_retrieve_related  — Lattice-adjacent retrieval from KB
  9. dgm_map_analytical_space — Map full vocabulary decomposition

Each tool fulfils the 5-point contract:
  capability_schema, cost_estimate, execute, compose, explain
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sqldim.core.query.dgm.supervisor._types import (
    ToolCallResult,
    ToolCapabilitySchema,
)

__all__ = ["BaseTool", "TOOL_REGISTRY"]


# ---------------------------------------------------------------------------
# Base tool protocol
# ---------------------------------------------------------------------------


class BaseTool(ABC):
    """Abstract base for all supervisor tools (5-point contract)."""

    @property
    @abstractmethod
    def capability_schema(self) -> ToolCapabilitySchema: ...

    @abstractmethod
    def cost_estimate(self, arguments: dict[str, Any]) -> float: ...

    @abstractmethod
    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult: ...

    def compose(
        self, left: ToolCallResult, right: ToolCallResult
    ) -> ToolCallResult:
        """Default composition: merge data dicts."""
        merged = {}
        if isinstance(left.data, dict):
            merged.update(left.data)
        if isinstance(right.data, dict):
            merged.update(right.data)
        return ToolCallResult(
            tool_name=f"{left.tool_name}+{right.tool_name}",
            success=left.success and right.success,
            data=merged,
            cost_actual=left.cost_actual + right.cost_actual,
        )

    def explain(self, arguments: dict[str, Any]) -> str:
        """Default explanation: describe what the tool would do."""
        schema = self.capability_schema
        return (
            f"Tool '{schema.name}': {schema.description}\n"
            f"Input: {arguments}\n"
            f"Estimated cost: {self.cost_estimate(arguments)}"
        )


# ---------------------------------------------------------------------------
# Tool implementations (stubs — delegate to full pipeline when context available)
# ---------------------------------------------------------------------------


class DgmQueryTool(BaseTool):
    """Run a DGM NL→SQL pipeline against a dataset (§12.1)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_query",
            description="Execute a DGM NL query against a named dataset",
            input_schema={"dataset": "str", "question": "str"},
            output_schema={"result": "Any", "explanation": "str"},
            composable_with=["dgm_correlate", "dgm_explore_from"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 1000.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={"dataset": arguments.get("dataset"), "stub": True},
            cost_actual=0,
        )


class DgmWhatCanIAnswerTool(BaseTool):
    """List answerable questions for a dataset from Q_valid(A,B) (§12.1)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_what_can_i_answer",
            description="List answerable questions from the formal question space",
            input_schema={"dataset": "str"},
            output_schema={"questions": "list[str]"},
            composable_with=["dgm_query"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 500.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        datasets = (
            list(context.available_datasets)
            if context is not None and hasattr(context, "available_datasets")
            else []
        )
        return ToolCallResult(
            tool_name="dgm_what_can_i_answer",
            success=True,
            data={"datasets": datasets, "questions": []},
            cost_actual=0,
        )


class DgmCorrelateTool(BaseTool):
    """Cross-domain correlation of two DGM results (§12.3)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_correlate",
            description="JOIN results from two different DGM analyses",
            input_schema={"left": "ToolCallResult", "right": "ToolCallResult"},
            output_schema={"correlation": "Any"},
            composable_with=["dgm_query"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 200.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_correlate",
            success=True,
            data={"correlation": None, "stub": True},
            cost_actual=0,
        )


class DgmExploreFromTool(BaseTool):
    """Navigate from a result to related questions via recommender (§12.4)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_explore_from",
            description="Navigate from a result to related questions",
            input_schema={"result": "ToolCallResult", "direction": "str"},
            output_schema={"suggestions": "list[str]"},
            composable_with=["dgm_query", "dgm_what_can_i_answer"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 300.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_explore_from",
            success=True,
            data={"suggestions": [], "stub": True},
            cost_actual=0,
        )


class DgmSensitivitySweepTool(BaseTool):
    """Parameter sweep analysis over P_Σ (§12.5)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_sensitivity_sweep",
            description="Sweep parameter space to find sensitivity boundaries",
            input_schema={"dataset": "str", "question": "str", "param": "str"},
            output_schema={"sweep_results": "list[dict]"},
            composable_with=["dgm_query"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 2000.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_sensitivity_sweep",
            success=True,
            data={"sweep_results": [], "stub": True},
            cost_actual=0,
        )


class DgmValidateInvariantTool(BaseTool):
    """Check a formal invariant of the DGM model (§12.6)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_validate_invariant",
            description="Validate a formal invariant holds for the current model state",
            input_schema={"invariant": "str", "dataset": "str"},
            output_schema={"valid": "bool", "violations": "list[str]"},
            composable_with=[],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 100.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_validate_invariant",
            success=True,
            data={"valid": True, "violations": [], "stub": True},
            cost_actual=0,
        )


class DgmStoreFindingTool(BaseTool):
    """Persist a finding to the knowledge base (§12.7)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_store_finding",
            description="Store an analytical finding for later retrieval",
            input_schema={"finding": "str", "tags": "list[str]"},
            output_schema={"stored": "bool", "finding_id": "str"},
            composable_with=["dgm_retrieve_related"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 10.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_store_finding",
            success=True,
            data={"stored": True, "finding_id": "stub-0", "stub": True},
            cost_actual=0,
        )


class DgmRetrieveRelatedTool(BaseTool):
    """Lattice-adjacent retrieval from the knowledge base (§12.7)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_retrieve_related",
            description="Retrieve related findings via lattice-adjacent search",
            input_schema={"query": "str", "max_results": "int"},
            output_schema={"findings": "list[dict]"},
            composable_with=["dgm_store_finding", "dgm_query"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 50.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        return ToolCallResult(
            tool_name="dgm_retrieve_related",
            success=True,
            data={"findings": [], "stub": True},
            cost_actual=0,
        )


class DgmMapAnalyticalSpaceTool(BaseTool):
    """Map the full vocabulary decomposition of a dataset (§8.16 / §12.8)."""

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return ToolCapabilitySchema(
            name="dgm_map_analytical_space",
            description="Map the analytical space: vocabulary decomposition, effective size, semantic basis",
            input_schema={"dataset": "str"},
            output_schema={"vocabularies": "dict", "A_eff_estimate": "int"},
            composable_with=["dgm_what_can_i_answer", "dgm_sensitivity_sweep"],
        )

    def cost_estimate(self, arguments: dict[str, Any]) -> float:
        return 800.0

    def execute(
        self, arguments: dict[str, Any], *, context: Any = None
    ) -> ToolCallResult:
        tools_info = [
            {
                "name": t.capability_schema.name,
                "description": t.capability_schema.description,
            }
            for t in TOOL_REGISTRY.values()
        ]
        return ToolCallResult(
            tool_name="dgm_map_analytical_space",
            success=True,
            data={"tools": tools_info},
            cost_actual=0,
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

TOOL_REGISTRY: dict[str, BaseTool] = {
    "dgm_query": DgmQueryTool(),
    "dgm_what_can_i_answer": DgmWhatCanIAnswerTool(),
    "dgm_correlate": DgmCorrelateTool(),
    "dgm_explore_from": DgmExploreFromTool(),
    "dgm_sensitivity_sweep": DgmSensitivitySweepTool(),
    "dgm_validate_invariant": DgmValidateInvariantTool(),
    "dgm_store_finding": DgmStoreFindingTool(),
    "dgm_retrieve_related": DgmRetrieveRelatedTool(),
    "dgm_map_analytical_space": DgmMapAnalyticalSpaceTool(),
}
