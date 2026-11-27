"""DGM Supervisor — per-tool argument and output schemas (§12.3, D43).

Each tool's ``input_schema`` / ``output_schema`` is the JSON Schema of the
Pydantic models below.  Arguments are validated against them before anything
runs (P3, ``execute_tool``) and ``data`` of a SUCCEEDED result is validated
against the output model (``INVALID_OUTPUT`` otherwise).

Requests above a policy bound are ``INVALID_ARGUMENTS``; nothing is truncated
silently (§12.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

__all__ = ["SupervisorPolicy", "DEFAULT_POLICY", "TOOL_SCHEMAS", "ToolSchemas"]


@dataclass(frozen=True)
class SupervisorPolicy:
    """Policy constants of the deployment (§12.3–§12.5)."""

    max_enum_hops: int = 4
    max_enum_questions: int = 200
    max_anchors: int = 3
    max_explore: int = 50
    max_sweep_points: int = 50
    max_witnesses: int = 100
    max_attempts: int = 2
    max_replans: int = 2
    max_concurrent_calls: int = 4


DEFAULT_POLICY = SupervisorPolicy()

_STRICT = {"extra": "forbid", "frozen": True}

INVARIANTS = (
    "H1", "H2", "H3", "PARTITION",
    "M1", "M2", "M3", "M4", "M5", "M6", "M7",
    "PA1", "PA2", "PA3", "PA4",
)  # fmt: skip


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------


class QueryArgs(BaseModel):
    dataset: str = Field(min_length=1)
    utterance: str = Field(min_length=1)
    model_config = _STRICT  # type: ignore[assignment]


class WhatCanIAnswerArgs(BaseModel):
    dataset: str = Field(min_length=1)
    anchors: list[str] = Field(min_length=1)
    max_hops: int = Field(ge=0, le=DEFAULT_POLICY.max_enum_hops)
    limit: int = Field(ge=1, le=DEFAULT_POLICY.max_enum_questions)
    model_config = _STRICT  # type: ignore[assignment]


class MapAnalyticalSpaceArgs(BaseModel):
    dataset: str = Field(min_length=1)
    utterance: str | None = None
    model_config = _STRICT  # type: ignore[assignment]


class CorrelateArgs(BaseModel):
    left: str = Field(min_length=1, description="call_id of a dgm_query step")
    right: str = Field(min_length=1, description="call_id of a dgm_query step")
    keys: list[str] = Field(min_length=1)
    model_config = _STRICT  # type: ignore[assignment]


class ExploreFromArgs(BaseModel):
    source: str = Field(min_length=1, description="call_id of the source step")
    limit: int = Field(ge=1, le=DEFAULT_POLICY.max_explore)
    model_config = _STRICT  # type: ignore[assignment]


class SensitivitySweepArgs(BaseModel):
    source: str = Field(min_length=1, description="call_id of the source step")
    parameter: str = Field(min_length=1)
    values: list[Any] = Field(min_length=1, max_length=DEFAULT_POLICY.max_sweep_points)
    model_config = _STRICT  # type: ignore[assignment]


class ValidateInvariantArgs(BaseModel):
    dataset: str = Field(min_length=1)
    invariant: Literal[INVARIANTS]  # type: ignore[valid-type]
    max_witnesses: int = Field(ge=0, le=DEFAULT_POLICY.max_witnesses)
    model_config = _STRICT  # type: ignore[assignment]


class Finding(BaseModel):
    question_key: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    datasets: list[str] = Field(min_length=1)
    content: str = Field(min_length=1)
    model_config = _STRICT  # type: ignore[assignment]


class StoreFindingArgs(BaseModel):
    finding: Finding
    model_config = _STRICT  # type: ignore[assignment]


class RetrieveRelatedArgs(BaseModel):
    dataset: str = Field(min_length=1)
    question_key: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    k: int = Field(default=2, ge=0)
    model_config = _STRICT  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------


class NLTurnResult(BaseModel):
    """One non-interactive NL turn (§12.2, ``run_ask_from_source``)."""

    question_key: str | None
    decision: str
    rows: list[dict[str, Any]] | None
    job: dict[str, Any] | None
    explanation: str
    unresolved: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _async_has_job_not_rows(self) -> "NLTurnResult":
        if self.decision == "ASYNC" and (self.job is None or self.rows is not None):
            raise ValueError("an ASYNC turn carries a job and no rows")
        return self


class QuestionSummary(BaseModel):
    question_key: str
    skeleton: str


class WhatCanIAnswerOutput(BaseModel):
    questions: list[QuestionSummary]
    complete: bool
    max_hops: int
    limit: int
    expr_depth: int
    pred_depth: int


class MapAnalyticalSpaceOutput(BaseModel):
    vocabularies: dict[str, list[str]]
    sizes: dict[str, int]
    anchor_sets: list[list[str]]
    grounding: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _schema_has_nodes(self) -> "MapAnalyticalSpaceOutput":
        if not self.vocabularies.get("V_N"):
            raise ValueError("a dataset with a schema has non-empty V_N")
        return self


class CorrelateOutput(BaseModel):
    keys: list[str]
    rows: list[dict[str, Any]]


class Suggestion(BaseModel):
    question_key: str
    move: str
    parameters: dict[str, Any]
    reason: str


class ExploreFromOutput(BaseModel):
    suggestions: list[Suggestion]


class SweepPoint(BaseModel):
    value: Any
    rows: list[dict[str, Any]] | None = None
    job: dict[str, Any] | None = None


class SensitivitySweepOutput(BaseModel):
    points: list[SweepPoint] = Field(min_length=1)


class ValidateInvariantOutput(BaseModel):
    holds: bool
    checked: int = Field(ge=0)
    violation_count: int = Field(ge=0)
    violations: list[dict[str, Any]]


class StoreFindingOutput(BaseModel):
    finding_id: str = Field(min_length=1)


class RelatedFinding(BaseModel):
    finding_id: str
    d_S: int = Field(ge=0)
    # d_P is defined only when the finding's skeleton is q' itself (ADR 0007).
    d_P: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _d_p_only_at_origin(self) -> "RelatedFinding":
        if (self.d_P is None) != (self.d_S != 0):
            raise ValueError("d_P is present iff d_S = 0 (ADR 0007)")
        return self


class RetrieveRelatedOutput(BaseModel):
    findings: list[RelatedFinding]


# ---------------------------------------------------------------------------
# Catalogue (§12.3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolSchemas:
    """Static facts of one tool: schemas and §12.3 table columns.

    ``reads`` are JSON pointers to argument fields that name datasets;
    ``references`` are argument fields holding the ``call_id`` of an earlier
    step, whose datasets the request inherits (§12.10, *Results*).
    """

    args: type[BaseModel]
    output: type[BaseModel]
    reads: tuple[str, ...]
    references: tuple[str, ...]
    writes_kb: bool
    idempotent: bool
    composable_with: tuple[str, ...]
    description: str


TOOL_SCHEMAS: dict[str, ToolSchemas] = {
    "dgm_query": ToolSchemas(
        QueryArgs,
        NLTurnResult,
        reads=("/dataset",),
        references=(),
        writes_kb=False,
        idempotent=True,
        composable_with=(
            "dgm_correlate",
            "dgm_explore_from",
            "dgm_sensitivity_sweep",
            "dgm_store_finding",
        ),
        description="One NL question against a dataset",
    ),
    "dgm_what_can_i_answer": ToolSchemas(
        WhatCanIAnswerArgs,
        WhatCanIAnswerOutput,
        reads=("/dataset",),
        references=(),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_query",),
        description="List answerable questions from Q_valid, within bounds",
    ),
    "dgm_correlate": ToolSchemas(
        CorrelateArgs,
        CorrelateOutput,
        reads=(),
        references=("left", "right"),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_explore_from", "dgm_store_finding"),
        description="Drill-across of two query results on conformed keys",
    ),
    "dgm_explore_from": ToolSchemas(
        ExploreFromArgs,
        ExploreFromOutput,
        reads=(),
        references=("source",),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_store_finding",),
        description="Structural moves from a result to related valid questions",
    ),
    "dgm_sensitivity_sweep": ToolSchemas(
        SensitivitySweepArgs,
        SensitivitySweepOutput,
        reads=(),
        references=("source",),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_store_finding",),
        description="Run a question at every listed parameter value",
    ),
    "dgm_validate_invariant": ToolSchemas(
        ValidateInvariantArgs,
        ValidateInvariantOutput,
        reads=("/dataset",),
        references=(),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_store_finding",),
        description="Check one formal model invariant over its whole scope",
    ),
    "dgm_store_finding": ToolSchemas(
        StoreFindingArgs,
        StoreFindingOutput,
        reads=("/finding/datasets/*",),
        references=(),
        writes_kb=True,
        idempotent=True,
        composable_with=("dgm_retrieve_related",),
        description="Persist a finding to the tenant's knowledge base",
    ),
    "dgm_retrieve_related": ToolSchemas(
        RetrieveRelatedArgs,
        RetrieveRelatedOutput,
        reads=("/dataset",),
        references=(),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_query",),
        description="Retrieve findings by move distance in the question lattice",
    ),
    "dgm_map_analytical_space": ToolSchemas(
        MapAnalyticalSpaceArgs,
        MapAnalyticalSpaceOutput,
        reads=("/dataset",),
        references=(),
        writes_kb=False,
        idempotent=True,
        composable_with=("dgm_query", "dgm_what_can_i_answer"),
        description="Vocabulary decomposition of a dataset and grounding check",
    ),
}
