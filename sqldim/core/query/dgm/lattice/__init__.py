"""The question lattice of a declared schema graph (§8.11, §8.14, §8.16.6).

* :meth:`QuestionLattice.enumerate` — the bounded enumeration of
  ``Q_valid(𝒜)`` behind ``dgm_what_can_i_answer`` (§12.3), in its
  deterministic order, each skeleton with its persistent ``question_key``;
* :meth:`QuestionLattice.contains` — membership (§8.11, theorem part 2);
* :meth:`QuestionLattice.neighbours` — the structural moves of §8.16.6, the
  edges of the move graph ``M`` of ADR 0007.

Everything is computed from the declared ``G_S``, ``K_S`` and ``Σ``; no
instance data is read and nothing is inferred.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from sqldim.core.query.dgm.analytical_space import DGMSchema
from sqldim.core.query.dgm.lattice._enumerate import first, skeletons
from sqldim.core.query.dgm.lattice._moves import neighbours
from sqldim.core.query.dgm.lattice._skeleton import (
    Skeleton,
    check,
    question_key,
    summary,
)
from sqldim.core.query.dgm.lattice._space import (
    InvalidAnchors,
    LatticeUnsupported,
    Space,
    build_space,
    schema_version,
)

__all__ = [
    "EXPR_DEPTH",
    "PRED_DEPTH",
    "InvalidAnchors",
    "LatticeUnsupported",
    "Question",
    "QuestionLattice",
    "Skeleton",
    "first",
    "schema_version",
]

EXPR_DEPTH = 0
"""``d`` of QH3 used by the enumeration: operands are Refs and slots (§12.3)."""

PRED_DEPTH = 0
"""Predicate nesting of the enumeration: PathPred ``sub_filter`` and the
argument of UNTIL / SINCE are ``TRUE`` (§12.3)."""


@dataclass(frozen=True)
class Question:
    """One emitted skeleton."""

    question_key: str
    skeleton: Skeleton
    summary: str


class QuestionLattice:
    """``L(G_𝒜)`` for every anchor set of one declared schema graph.

    *max_hops* is the hop bound of the move graph ``M`` (``MAX_ENUM_HOPS``);
    an enumeration may use any smaller bound.
    """

    def __init__(self, schema: DGMSchema, *, max_anchors: int, max_hops: int) -> None:
        self.schema = schema
        self.version = schema_version(schema)
        self._max_anchors = max_anchors
        self._max_hops = max_hops
        self._spaces: dict[tuple[tuple[str, ...], int], Space] = {}

    def space(self, anchors: Sequence[str], *, max_hops: int | None = None) -> Space:
        """``G_𝒜``; raises :class:`InvalidAnchors` or :class:`LatticeUnsupported`."""
        h = self._max_hops if max_hops is None else max_hops
        key = (tuple(anchors), h)
        if key not in self._spaces:
            self._spaces[key] = build_space(
                self.schema, anchors, h=h, max_anchors=self._max_anchors
            )
        return self._spaces[key]

    def key(self, sk: Skeleton) -> str:
        return question_key(sk, self.version)

    def question(self, space: Space, sk: Skeleton) -> Question:
        types = [a.type for a in space.anchors]
        return Question(self.key(sk), sk, summary(sk, types))

    def enumerate(self, anchors: Sequence[str], *, max_hops: int) -> Iterator[Question]:
        """``Q_valid(anchors)`` with connecting paths of ≤ *max_hops* hops."""
        space = self.space(anchors, max_hops=max_hops)
        seen: set[str] = set()
        for sk in skeletons(space):
            q = self.question(space, sk)
            if q.question_key not in seen:  # QH6: each canonical form once
                seen.add(q.question_key)
                yield q

    def contains(self, sk: Skeleton) -> bool:
        """Whether *sk* is a vertex of ``M`` (``Q_valid`` at ``max_hops``)."""
        try:
            space = self.space(sk.anchors)
        except InvalidAnchors:
            return False
        return not check(space, sk)

    def neighbours(self, sk: Skeleton) -> list[Question]:
        space = self.space(sk.anchors)
        return [self.question(space, n) for n in neighbours(space, sk)]
