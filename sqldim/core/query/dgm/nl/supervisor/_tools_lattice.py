"""``dgm_what_can_i_answer`` and the move graph over question keys (§12.3, §12.7).

Both read the model the dataset *declares* (``dgm_model()`` on the
handle ``ctx.data.open`` returns) and nothing else: no instance data, no
inferred schema, no other dataset.

* :class:`WhatCanIAnswerTool` emits the first ``limit`` skeletons of the
  bounded enumeration of ``Q_valid(anchors)`` (§8.11) in its deterministic
  order, with ``complete`` true only when nothing follows them.
* :class:`LatticeMoveGraph` is the :class:`MoveGraph` of ADR 0007 over the
  same ``question_key`` values: its edges are the structural moves of
  §8.16.6.  A key is a hash, so the graph resolves it through a
  :class:`SkeletonIndex` filled by the enumeration and by the moves; a key
  the index has never seen is reported, never guessed.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqldim.core.query.dgm.analytical_space import DGMSchema
from sqldim.core.query.dgm.lattice import (
    EXPR_DEPTH,
    PRED_DEPTH,
    InvalidAnchors,
    LatticeUnsupported,
    QuestionLattice,
    Skeleton,
    first,
)
from sqldim.core.query.dgm.model import AnnotationNotDeclared
from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied, DataAccess
from sqldim.core.query.dgm.nl.supervisor._declared_model import (
    DeclarationUnavailable,
    declared_model,
    undeclared_annotation,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    DEFAULT_POLICY,
    SupervisorPolicy,
    WhatCanIAnswerArgs,
)
from sqldim.core.query.dgm.nl.supervisor._tools import (
    TOOL_VERSION,
    BaseTool,
    _capability,
)
from sqldim.core.query.dgm.nl.supervisor._tools_kb import (
    MoveGraphError,
    MoveGraphUnsupported,
)
from sqldim.core.query.dgm.nl.supervisor._tools_space import (
    _partition_violations,
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

__all__ = [
    "LatticeMoveGraph",
    "SkeletonIndex",
    "WhatCanIAnswerTool",
    "lattice_move_graph",
]

NAME = "dgm_what_can_i_answer"


# ---------------------------------------------------------------------------
# Skeleton index
# ---------------------------------------------------------------------------


class SkeletonIndex:
    """``question_key → skeleton`` for every skeleton the lattice has emitted.

    With a file path the index survives the process; ``":memory:"`` lives as
    long as the object.  A row is only ever inserted, never changed: a key
    names one skeleton.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._lock = threading.Lock()
        self._con = sqlite3.connect(str(path), check_same_thread=False)
        with self._lock, self._con:
            self._con.execute(
                "CREATE TABLE IF NOT EXISTS skeletons ("
                "question_key TEXT PRIMARY KEY, skeleton TEXT NOT NULL)"
            )

    def put(self, question_key: str, skeleton: Skeleton) -> None:
        with self._lock, self._con:
            self._con.execute(
                "INSERT OR IGNORE INTO skeletons VALUES (?, ?)",
                (question_key, skeleton.to_json()),
            )

    def get(self, question_key: str) -> Skeleton | None:
        with self._lock:
            row = self._con.execute(
                "SELECT skeleton FROM skeletons WHERE question_key = ?",
                (question_key,),
            ).fetchone()
        return None if row is None else Skeleton.from_json(row[0])

    def close(self) -> None:
        with self._lock:
            self._con.close()


# ---------------------------------------------------------------------------
# dgm_what_can_i_answer
# ---------------------------------------------------------------------------


def _model(source: Any, dataset: str) -> DGMSchema:
    """The declared model, checked against §2.1 (as dgm_map_analytical_space)."""
    model = declared_model(source, dataset)
    _partition_violations(model)
    return model


def _lattice(schema: DGMSchema, policy: SupervisorPolicy) -> QuestionLattice:
    return QuestionLattice(
        schema, max_anchors=policy.max_anchors, max_hops=policy.max_enum_hops
    )


def enumerate_questions(
    schema: DGMSchema,
    args: WhatCanIAnswerArgs,
    policy: SupervisorPolicy,
    index: SkeletonIndex | None = None,
) -> dict[str, Any]:
    """The SUCCEEDED payload; raises :class:`DeclarationUnavailable` for a typed failure."""
    if args.max_hops > policy.max_enum_hops or args.limit > policy.max_enum_questions:
        raise DeclarationUnavailable(
            ToolErrorKind.INVALID_ARGUMENTS,
            "max_hops or limit exceeds the deployment's policy bound",
            max_enum_hops=policy.max_enum_hops,
            max_enum_questions=policy.max_enum_questions,
        )
    lattice = _lattice(schema, policy)
    try:
        questions, complete = first(
            lattice.enumerate(args.anchors, max_hops=args.max_hops), args.limit
        )
    except InvalidAnchors as exc:
        raise DeclarationUnavailable(
            ToolErrorKind.INVALID_ARGUMENTS,
            "anchors are not an anchor set (AS1-AS3); no other anchor set is tried",
            problems=exc.problems,
        ) from exc
    except LatticeUnsupported as exc:
        raise DeclarationUnavailable(ToolErrorKind.UNSUPPORTED, str(exc)) from exc
    except AnnotationNotDeclared as exc:
        raise undeclared_annotation(exc) from exc
    if index is not None:
        for q in questions:
            index.put(q.question_key, q.skeleton)
    return {
        "questions": [
            {"question_key": q.question_key, "skeleton": q.summary} for q in questions
        ],
        "complete": complete,
        "max_hops": args.max_hops,
        "limit": args.limit,
        "expr_depth": EXPR_DEPTH,
        "pred_depth": PRED_DEPTH,
    }


class WhatCanIAnswerTool(BaseTool):
    """Contract implementation of ``dgm_what_can_i_answer`` (D43)."""

    def __init__(
        self,
        policy: SupervisorPolicy = DEFAULT_POLICY,
        index: SkeletonIndex | None = None,
    ) -> None:
        self._policy = policy
        self._index = index
        self._schema = _capability(NAME, available=True, version=TOOL_VERSION)

    @property
    def capability_schema(self) -> ToolCapabilitySchema:
        return self._schema

    def cost_estimate(
        self, arguments: dict[str, Any], ctx: ToolContext
    ) -> CostEstimate:
        return CostEstimate(
            cpu_ops=int(arguments.get("limit", 0)),
            io_ops=0,
            note="schema only: reads no instance data",
        )

    async def execute(
        self, request: ToolCallRequest, ctx: ToolContext
    ) -> ToolCallResult:
        started = datetime.now(timezone.utc)
        try:
            args = WhatCanIAnswerArgs.model_validate(request.arguments)
            schema = _model(ctx.data.open(args.dataset), args.dataset)
            data = await asyncio.to_thread(
                enumerate_questions, schema, args, self._policy, self._index
            )
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
                "the enumeration of the question space failed",
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

    def explain(self, arguments: dict[str, Any]) -> str:
        return (
            f"{NAME}: the first {arguments.get('limit')} skeletons of "
            f"Q_valid({arguments.get('anchors')}) within "
            f"{arguments.get('max_hops')} hops, from the declared schema graph "
            f"of dataset {arguments.get('dataset')!r}"
        )


# ---------------------------------------------------------------------------
# Move graph (ADR 0007)
# ---------------------------------------------------------------------------

UNKNOWN_KEY = (
    "question_key is not in this deployment's skeleton index: a key is a hash "
    "and membership cannot be decided from it; obtain the key from "
    "dgm_what_can_i_answer first"
)


class LatticeMoveGraph:
    """The move graph ``M`` of the declared schema of each dataset."""

    def __init__(
        self,
        schema_of: Callable[[str], DGMSchema],
        index: SkeletonIndex,
        policy: SupervisorPolicy = DEFAULT_POLICY,
    ) -> None:
        self._schema_of = schema_of
        self._index = index
        self._policy = policy
        self._lattices: dict[str, QuestionLattice] = {}
        self._lock = threading.Lock()

    def _lattice(self, dataset: str) -> QuestionLattice:
        with self._lock:
            if dataset not in self._lattices:
                self._lattices[dataset] = _lattice(
                    self._schema_of(dataset), self._policy
                )
            return self._lattices[dataset]

    def _resolve(
        self, dataset: str, question_key: str
    ) -> tuple[QuestionLattice, Skeleton]:
        lattice = self._lattice(dataset)
        skeleton = self._index.get(question_key)
        if skeleton is None:
            raise MoveGraphUnsupported(UNKNOWN_KEY)
        return lattice, skeleton

    def contains(self, dataset: str, question_key: str) -> bool:
        lattice, skeleton = self._resolve(dataset, question_key)
        try:
            return lattice.key(skeleton) == question_key and lattice.contains(skeleton)
        except (LatticeUnsupported, AnnotationNotDeclared) as exc:
            raise MoveGraphUnsupported(str(exc)) from exc

    def neighbours(self, dataset: str, question_key: str) -> list[str]:
        lattice, skeleton = self._resolve(dataset, question_key)
        if lattice.key(skeleton) != question_key:
            raise MoveGraphError("question_key is not a skeleton of this dataset")
        out: list[str] = []
        for q in lattice.neighbours(skeleton):
            self._index.put(q.question_key, q.skeleton)
            out.append(q.question_key)
        return out


def lattice_move_graph(
    data: DataAccess,
    index: SkeletonIndex,
    policy: SupervisorPolicy = DEFAULT_POLICY,
) -> LatticeMoveGraph:
    """The move graph of a runtime: each dataset's declared schema, opened
    through the runtime's own :class:`DataAccess` (grants re-checked)."""

    def schema_of(dataset: str) -> DGMSchema:
        try:
            return _model(data.scoped([dataset]).open(dataset), dataset)
        except DeclarationUnavailable as exc:
            if exc.kind is ToolErrorKind.UNSUPPORTED:
                raise MoveGraphUnsupported(exc.message) from exc
            raise MoveGraphError(exc.message) from exc

    return LatticeMoveGraph(schema_of, index, policy)
