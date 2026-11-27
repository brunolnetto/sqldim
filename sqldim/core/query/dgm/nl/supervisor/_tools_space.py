"""``dgm_map_analytical_space`` — vocabulary decomposition of a dataset (§12.3, §8.16).

The tool reads ``G_S``, ``K_S`` and ``Σ`` from the model the dataset
*declares* — the same :class:`~sqldim.core.query.dgm.model.DGMModel` that
``dgm_validate_invariant`` reads — through ``ToolContext.data`` and nothing
else (§12.10).  It never reads instance data (§8.11, *Schema determinacy*),
never infers a model and never looks at another dataset.

Outcomes, kept apart (D42):

* an ``utterance`` asks for the grounding check of §12.8, which needs the
  reading of the NL interface (§11.10) — not implemented → ``NOT_IMPLEMENTED``;
* a dataset that declares no model → ``UNSUPPORTED``;
* a declaration that is ill-formed, raises, or violates §2.1 → ``TOOL_FAILURE``;
* a model that does not state a kind of Σ the decomposition reads
  (``FactlessFact``, ``MedallionLayer``, ``RolePlaying``) → ``UNSUPPORTED``;
* otherwise SUCCEEDED with the decomposition.  It is never empty: a
  well-formed model has at least one node type.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqldim.core.query.dgm.analytical_space import (
    VOCABULARIES,
    admissible_anchor_sets,
    decompose,
)
from sqldim.core.query.dgm.model import AnnotationNotDeclared, DGMModel
from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied
from sqldim.core.query.dgm.nl.supervisor._declared_model import (
    DeclarationUnavailable,
    declared_model,
    undeclared_annotation,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    DEFAULT_POLICY,
    SupervisorPolicy,
)
from sqldim.core.query.dgm.nl.supervisor._tools import (
    TOOL_VERSION,
    BaseTool,
    _capability,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallRequest,
    ToolCallResult,
    ToolCapabilitySchema,
    ToolContext,
    ToolErrorKind,
    zero_cost,
)
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = ["MapAnalyticalSpaceTool"]

NAME = "dgm_map_analytical_space"
GROUNDING_MISSING = (
    "grounding check of §12.8: needs the reading of an utterance by the NL "
    "interface (§11.10, run_ask_from_source), which is not implemented"
)


def _partition_violations(model: DGMModel) -> None:
    problems = [
        f"{e.term}: {p}" for e in model.edge_types for p in model.partition_problems(e)
    ]
    if problems:
        raise DeclarationUnavailable(
            ToolErrorKind.TOOL_FAILURE,
            "the declared schema graph violates §2.1",
            detail="; ".join(problems),
        )


def map_space(schema: DGMModel, policy: SupervisorPolicy) -> dict[str, Any]:
    """The SUCCEEDED payload: vocabularies, sizes and admissible anchor sets.

    Raises :class:`AnnotationNotDeclared` if *schema* does not state a kind
    of Σ the decomposition reads.
    """
    vocabularies = decompose(schema)
    return {
        "vocabularies": vocabularies,
        "sizes": {name: len(members) for name, members in vocabularies.items()},
        "anchor_sets": admissible_anchor_sets(
            schema, max_hops=policy.max_enum_hops, max_anchors=policy.max_anchors
        ),
        "grounding": None,
    }


class MapAnalyticalSpaceTool(BaseTool):
    """Contract implementation of ``dgm_map_analytical_space`` (D43)."""

    def __init__(self, policy: SupervisorPolicy = DEFAULT_POLICY) -> None:
        self._policy = policy
        self._schema = _capability(NAME, available=True, version=TOOL_VERSION)

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        return CostEstimate(
            cpu_ops=0, io_ops=0, note="schema only: reads no instance data"
        )

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        started = datetime.now(timezone.utc)
        try:
            data = self._run(request, ctx)
        except DeclarationUnavailable as exc:
            return ToolCallResult.failure(
                request, CallStatus.FAILED, exc.kind, exc.message, context=exc.context
            )
        except AccessDenied as exc:
            return ToolCallResult.failure(
                request, CallStatus.FAILED, ToolErrorKind.UNAUTHORIZED, str(exc)
            )
        except Exception as exc:  # noqa: BLE001 — reported, never swallowed
            return ToolCallResult.failure(
                request,
                CallStatus.FAILED,
                ToolErrorKind.TOOL_FAILURE,
                "the analytical space of the dataset could not be computed",
                context={"exception": type(exc).__name__, "detail": str(exc)},
            )
        return ToolCallResult(
            call_id=request.call_id,
            tool_name=request.tool_name,
            status=CallStatus.SUCCEEDED,
            data=data,
            datasets=list(request.datasets or []),
            attempts=1,
            cost_actual=zero_cost(),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
        )

    def _run(self, request: ToolCallRequest, ctx: ToolContext) -> dict[str, Any]:
        if request.arguments.get("utterance") is not None:
            raise DeclarationUnavailable(
                ToolErrorKind.NOT_IMPLEMENTED,
                "the grounding check is not implemented in this deployment",
                missing=GROUNDING_MISSING,
            )
        dataset = request.arguments["dataset"]
        model = declared_model(ctx.data.open(dataset), dataset)
        _partition_violations(model)
        try:
            return map_space(model, self._policy)
        except AnnotationNotDeclared as exc:
            raise undeclared_annotation(exc) from exc

    def explain(self, arguments: dict[str, Any]) -> str:
        return (
            f"{NAME}: the vocabularies {', '.join(VOCABULARIES)} and the "
            f"anchor sets of dataset {arguments.get('dataset')!r}, from its "
            "declared model"
        )
