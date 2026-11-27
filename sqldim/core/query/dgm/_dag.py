"""DGM Rule 11 Extended — Query DAG Minimisation (DGM §6.2 v0.20).

Applies the full semiring elimination laws to a :class:`QuestionAlgebra`
before CTE emission, producing a strictly minimal query DAG.

Semiring elimination rules applied during ``QueryDAGManager.make()``:

  UNION laws:
    Q ∪ Q            = Q           (idempotence; same DAG node id)
    Q ∪ ∅            = Q           (identity)
    ∅ ∪ Q            = Q           (identity)
    Q₁ ∪ Q₂ where Q₁⊆Q₂ = Q₂    (containment absorption; Band 1 only)

  INTERSECT laws:
    Q ∩ Q            = Q           (idempotence)
    Q ∩ Q_top        = Q           (identity)
    Q_top ∩ Q        = Q           (identity)
    Q₁ ∩ Q₂ where Q₁⊆Q₂ = Q₁    (containment selection; Band 1 only)

  Containment check:
    Q₁ ⊆ Q₂  iff  bdd.implies(Q₁.where_bdd, Q₂.where_bdd)  [Band 1]

Containment is applied only between compositions over one Band 1 skeleton
(same anchor, joins and projection; no GroupBy, aggregate, Having, window or
Qualify): only there does implication of the Where predicates prove
containment of the results (§8.14 ``contains``).

The laws are set laws (§8.13): the algebra emits SQL ``UNION``, ``INTERSECT``
and ``EXCEPT``.  When an eliminated set operation's survivor may hold
duplicate rows (a leaf, JOIN or WITH CTE), the eliminated name denotes the
survivor's distinct rows: a JOIN/WITH consumer reads a ``SELECT DISTINCT``
CTE and a final read selects ``DISTINCT``, so a minimised algebra returns the
same rows as the original.

After elimination each node becomes one CTE.  The CTE count is minimal within
the canonical form — no composition reachable by these rules has fewer nodes —
not over every equivalent query (§8.14).

API
---
:class:`QueryDAGNode`          — dataclass: (node_id, op, left_id, right_id).
:class:`QueryDAGManager`       — ``leaf()``, ``make()``, sentinel ids.
:func:`apply_semiring_minimisation` — public entry point.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqldim.core.query.dgm.algebra import ComposeOp, ComposedQuery, QuestionAlgebra
from sqldim.core.query.dgm.bdd import TRUE_NODE_ID

if TYPE_CHECKING:
    from sqldim.core.query.dgm.bdd import DGMPredicateBDD

__all__ = [
    "QueryDAGNode",
    "QueryDAGManager",
    "apply_semiring_minimisation",
]

# ---------------------------------------------------------------------------
# Sentinel IDs
# ---------------------------------------------------------------------------

_EMPTY_ID: int = 0  # Additive identity for UNION  (Q ∪ ∅ = Q)
_TOP_ID: int = 1  # Multiplicative identity for INTERSECT (Q ∩ Q_top = Q)


# ---------------------------------------------------------------------------
# QueryDAGNode
# ---------------------------------------------------------------------------


@dataclass
class QueryDAGNode:
    """Immutable reference keeping DAG topology for one node.

    Leaf nodes: ``op=None``, ``left_id=None``, ``right_id=None``.
    Internal nodes: ``op`` is a :class:`ComposeOp`; operand ids set.
    """

    node_id: int
    op: ComposeOp | None
    left_id: int | None
    right_id: int | None


# ---------------------------------------------------------------------------
# QueryDAGManager
# ---------------------------------------------------------------------------


class QueryDAGManager:
    """Manages the query DAG unique table with inline semiring elimination.

    Sentinel IDs (fixed):
      ``empty_id`` = 0 — identity for UNION.
      ``top_id``   = 1 — identity for INTERSECT.

    Leaf node IDs start at 2.

    ``make(op, left, right)`` implements structural elimination (idempotence
    and identity laws) without requiring a BDD.  Containment-based elimination
    is handled upstream in :func:`apply_semiring_minimisation`.
    """

    def __init__(self) -> None:
        # Pre-allocate sentinel slots at positions 0 and 1.
        self._nodes: list[QueryDAGNode] = [
            QueryDAGNode(node_id=_EMPTY_ID, op=None, left_id=None, right_id=None),
            QueryDAGNode(node_id=_TOP_ID, op=None, left_id=None, right_id=None),
        ]
        # Unique table: (op.value, left_id, right_id) → node_id
        self._unique: dict[tuple[str, int, int], int] = {}
        # Leaf registry: name → node_id
        self._leaf_ids: dict[str, int] = {}

    # -- Sentinel properties -------------------------------------------------

    @property
    def empty_id(self) -> int:
        """Sentinel node ID for EMPTY_Q (additive identity for UNION)."""
        return _EMPTY_ID

    @property
    def top_id(self) -> int:
        """Sentinel node ID for TOP_Q (multiplicative identity for INTERSECT)."""
        return _TOP_ID

    def __len__(self) -> int:
        """Total number of allocated DAG nodes (sentinels + leaves + internal)."""
        return len(self._nodes)

    # -- Node allocation -----------------------------------------------------

    def leaf(self, name: str) -> int:
        """Return the node ID for leaf CTE *name*, allocating if needed."""
        if name in self._leaf_ids:
            return self._leaf_ids[name]
        node_id = len(self._nodes)
        self._nodes.append(
            QueryDAGNode(node_id=node_id, op=None, left_id=None, right_id=None)
        )
        self._leaf_ids[name] = node_id
        return node_id

    # -- Semiring elimination (structural only) ------------------------------

    def _elim_union(self, left: int, right: int) -> int | None:
        if left == right:  # idempotence: Q ∪ Q = Q
            return left
        if left == _EMPTY_ID:  # identity: ∅ ∪ Q = Q
            return right
        if right == _EMPTY_ID:  # identity: Q ∪ ∅ = Q
            return left
        return None

    def _elim_intersect(self, left: int, right: int) -> int | None:
        if left == right:  # idempotence: Q ∩ Q = Q
            return left
        if left == _TOP_ID:  # identity: Q_top ∩ Q = Q
            return right
        if right == _TOP_ID:  # identity: Q ∩ Q_top = Q
            return left
        return None

    def make(self, op: ComposeOp, left: int, right: int) -> int:
        """Return the canonical node ID for composition (op, left, right).

        Structural elimination rules (idempotence and identity) are applied
        inline.  No BDD is needed for these rules.  Containment-based
        elimination requires a BDD and is handled in
        :func:`apply_semiring_minimisation`.
        """
        if op is ComposeOp.UNION:
            canon = self._elim_union(left, right)
        elif op is ComposeOp.INTERSECT:
            canon = self._elim_intersect(left, right)
        else:
            canon = None
        if canon is not None:
            return canon
        key = (op.value, left, right)
        if key in self._unique:
            return self._unique[key]
        node_id = len(self._nodes)
        self._nodes.append(
            QueryDAGNode(node_id=node_id, op=op, left_id=left, right_id=right)
        )
        self._unique[key] = node_id
        return node_id


# ---------------------------------------------------------------------------
# BDD-id helpers
# ---------------------------------------------------------------------------


def _leaf_bdd_id(cq: ComposedQuery, bdd: "DGMPredicateBDD") -> int:
    """Return the compiled BDD node ID for a leaf CTE's WHERE predicate.

    Leaf CTEs without a WHERE predicate map to ``TRUE_NODE_ID`` (tautology).
    """
    if cq.query is None:
        return TRUE_NODE_ID
    pred = getattr(cq.query, "_where_pred", None)
    if pred is None:
        return TRUE_NODE_ID
    return bdd.compile(pred)


_BAND23_ATTRS = (
    "_group_by_cols",
    "_agg_exprs",
    "_having_pred",
    "_window_exprs",
    "_qualify_pred",
    "_branches",
    "_temporal_context",
)


def _band1_skeleton(cq: ComposedQuery) -> str | None:
    """The SQL of a Band 1 leaf without its Where, or None if not Band 1.

    Two leaves with one skeleton differ only in their Where, so implication
    of the Where predicates is containment of their results.
    """
    q = cq.query
    if q is None or any(getattr(q, a, None) for a in _BAND23_ATTRS):
        return None
    bare = copy.copy(q)
    bare._where_pred = None
    try:
        return bare.to_sql()
    except Exception:  # an unrenderable leaf has no skeleton to compare
        return None


def _composed_skeleton(
    op: ComposeOp, left: str | None, right: str | None
) -> str | None:
    """A UNION/INTERSECT of two compositions over one skeleton keeps it."""
    if op in (ComposeOp.UNION, ComposeOp.INTERSECT) and left == right:
        return left
    return None


def _build_bdd_ids(
    algebra: QuestionAlgebra,
    composition: dict[str, tuple[str, ComposeOp, str]],
    bdd: "DGMPredicateBDD",
) -> tuple[dict[str, int], dict[str, str | None]]:
    """Build ``{name: bdd_id}`` and ``{name: skeleton}`` (topological pass).

    Leaf CTEs: compiled from their WHERE predicate (or TRUE_NODE_ID if none).
    Composed UNION CTEs: OR of operands' BDD IDs.
    Composed INTERSECT CTEs: AND of operands' BDD IDs.
    Other ops: TRUE_NODE_ID and no skeleton, so no containment check.
    """
    bdd_ids: dict[str, int] = {}
    skel: dict[str, str | None] = {}
    for name, cq in algebra._ctes.items():
        if name not in composition:
            bdd_ids[name] = _leaf_bdd_id(cq, bdd)
            skel[name] = _band1_skeleton(cq)
            continue
        left_name, op, right_name = composition[name]
        l_id = bdd_ids.get(left_name, TRUE_NODE_ID)
        r_id = bdd_ids.get(right_name, TRUE_NODE_ID)
        if op is ComposeOp.UNION:
            bdd_ids[name] = bdd.manager.apply("OR", l_id, r_id)
        elif op is ComposeOp.INTERSECT:
            bdd_ids[name] = bdd.manager.apply("AND", l_id, r_id)
        else:
            bdd_ids[name] = TRUE_NODE_ID
        skel[name] = _composed_skeleton(op, skel.get(left_name), skel.get(right_name))
    return bdd_ids, skel


# ---------------------------------------------------------------------------
# Containment-based elimination helpers
# ---------------------------------------------------------------------------


def _union_survivor(
    left: str,
    right: str,
    bdd_ids: dict[str, int],
    bdd: "DGMPredicateBDD",
) -> str | None:
    """Return the surviving CTE name for UNION containment, or None."""
    left_id = bdd_ids.get(left, TRUE_NODE_ID)
    r = bdd_ids.get(right, TRUE_NODE_ID)
    if bdd.implies(left_id, r):  # left ⊆ right → right dominates union
        return right
    if bdd.implies(r, left_id):  # right ⊆ left → left dominates union
        return left
    return None


def _intersect_survivor(
    left: str,
    right: str,
    bdd_ids: dict[str, int],
    bdd: "DGMPredicateBDD",
) -> str | None:
    """Return the surviving CTE name for INTERSECT containment, or None."""
    left_id = bdd_ids.get(left, TRUE_NODE_ID)
    r = bdd_ids.get(right, TRUE_NODE_ID)
    if bdd.implies(left_id, r):  # left ⊆ right → intersection equals left
        return left
    if bdd.implies(r, left_id):  # right ⊆ left → intersection equals right
        return right
    return None


def _contained(x: str, y: str, eff: dict[str, tuple[str, ComposeOp, str]]) -> bool:
    """``x ⊆ y`` by structure alone: y is a UNION with x below it, or x an
    INTERSECT with y below it (set semantics, §8.13)."""
    if x == y:
        return True
    if y in eff and eff[y][1] is ComposeOp.UNION:
        if _contained(x, eff[y][0], eff) or _contained(x, eff[y][2], eff):
            return True
    if x in eff and eff[x][1] is ComposeOp.INTERSECT:
        return _contained(eff[x][0], y, eff) or _contained(eff[x][2], y, eff)
    return False


def _absorbed(
    left: str, right: str, op: ComposeOp, eff: dict[str, tuple[str, ComposeOp, str]]
) -> str | None:
    """The survivor of a structural containment, or None."""
    if _contained(left, right, eff):
        return right if op is ComposeOp.UNION else left
    if _contained(right, left, eff):
        return left if op is ComposeOp.UNION else right
    return None


def _try_eliminate(
    left_eff: str,
    right_eff: str,
    op: ComposeOp,
    bdd_ids: dict[str, int],
    bdd: "DGMPredicateBDD",
    skel: dict[str, str | None] | None = None,
    eff: dict[str, tuple[str, ComposeOp, str]] | None = None,
) -> str | None:
    """Return the surviving CTE if any elimination rule fires, else None.

    *eff* holds the kept compositions over their surviving operands; with it
    the absorption laws ``Q ∩ (Q ∪ R) = Q`` and ``Q ∪ (Q ∩ R) = Q`` apply to
    any operands, since containment is then structural.

    Idempotence applies to UNION and INTERSECT only.  Containment applies
    only when *skel* gives both operands one Band 1 skeleton; without
    *skel* the BDD ids are taken to be over one skeleton.
    """
    if op not in (ComposeOp.UNION, ComposeOp.INTERSECT):
        return None
    if left_eff == right_eff:  # structural idempotence
        return left_eff
    if eff is not None:
        structural = _absorbed(left_eff, right_eff, op, eff)
        if structural is not None:
            return structural
    if skel is not None and (
        skel.get(left_eff) is None or skel.get(left_eff) != skel.get(right_eff)
    ):
        return None
    if op is ComposeOp.UNION:
        return _union_survivor(left_eff, right_eff, bdd_ids, bdd)
    return _intersect_survivor(left_eff, right_eff, bdd_ids, bdd)


# ---------------------------------------------------------------------------
# Cascade substitution resolution
# ---------------------------------------------------------------------------


def _resolve(name: str, subs: dict[str, str]) -> str:
    """Follow the substitution chain to the canonical name (cycle-safe)."""
    seen: set[str] = set()
    while name in subs:
        if name in seen:
            break  # cycle guard — should not occur in valid DAG
        seen.add(name)
        name = subs[name]
    return name


# ---------------------------------------------------------------------------
# Algebra reconstruction helpers
# ---------------------------------------------------------------------------


def _copy_algebra(algebra: QuestionAlgebra) -> QuestionAlgebra:
    """Return a shallow copy of *algebra* sharing the same CTE objects."""
    new_alg = QuestionAlgebra()
    for name, cq in algebra._ctes.items():
        new_alg._ctes[name] = cq
    new_alg._composition.update(algebra._composition)
    new_alg._on.update(algebra._on)
    new_alg._aliases.update(algebra._aliases)
    return new_alg


_SET_OPS = (ComposeOp.UNION, ComposeOp.INTERSECT, ComposeOp.EXCEPT)


class _Rebuild:
    """Rebuild *algebra* without the names in *subs* (eliminated → survivor).

    A survivor that is not a set operation may hold duplicate rows, while the
    eliminated set operation returned distinct rows.  A set-operation
    consumer is insensitive to that and reads the survivor; a JOIN or WITH
    consumer reads a ``SELECT DISTINCT`` CTE kept under the eliminated name.
    """

    def __init__(self, algebra: QuestionAlgebra, subs: dict[str, str]) -> None:
        self.alg = algebra
        self.subs = subs
        self.comp = algebra._composition
        self.coerced = {
            operand
            for name, (left, op, right) in self.comp.items()
            if name not in subs and op not in _SET_OPS
            for operand in (left, right)
            if operand in subs and self._bag(_resolve(operand, subs))
        }

    def _bag(self, name: str) -> bool:
        """True when *name*'s rows may repeat (not a set operation)."""
        return name not in self.comp or self.comp[name][1] not in _SET_OPS

    def _ref(self, operand: str, consumer_op: ComposeOp) -> str:
        if operand not in self.subs:
            return operand
        if consumer_op not in _SET_OPS and operand in self.coerced:
            return operand
        return _resolve(operand, self.subs)

    def run(self) -> QuestionAlgebra:
        new_alg = QuestionAlgebra()
        new_alg._aliases.update(self.alg._aliases)
        for name, cq in self.alg._ctes.items():
            if name in self.coerced:
                body = f"SELECT DISTINCT * FROM {_resolve(name, self.subs)}"
                new_alg._ctes[name] = ComposedQuery(name=name, sql=body)
            elif name in self.subs:
                survivor = _resolve(name, self.subs)
                new_alg._aliases[name] = (survivor, self._bag(survivor))
            elif name not in self.comp:
                new_alg._ctes[name] = cq
            else:
                self._recompose(new_alg, name)
        return new_alg

    def _recompose(self, new_alg: QuestionAlgebra, name: str) -> None:
        left, op, right = self.comp[name]
        left, right = self._ref(left, op), self._ref(right, op)
        on = self.alg._on.get(name)
        sql = QuestionAlgebra._compose_sql(
            new_alg._ctes[left], new_alg._ctes[right], op, on=on
        )
        new_alg._ctes[name] = ComposedQuery(name=name, sql=sql)
        new_alg._composition[name] = (left, op, right)
        if on is not None:
            new_alg._on[name] = on


# ---------------------------------------------------------------------------
# apply_semiring_minimisation — public entry point
# ---------------------------------------------------------------------------


def apply_semiring_minimisation(
    algebra: QuestionAlgebra,
    bdd: "DGMPredicateBDD",
) -> QuestionAlgebra:
    """Apply Rule 11 Extended semiring elimination laws to *algebra*.

    Returns a **new** :class:`QuestionAlgebra` with the minimum number of CTEs
    needed to represent the same query family.  The original is never mutated.

    The topological processing order (insertion order of ``_ctes``) guarantees
    cascade substitution correctness without multi-pass iteration.

    Parameters
    ----------
    algebra:
        Source algebra (read-only).
    bdd:
        :class:`~sqldim.core.query.dgm.bdd.DGMPredicateBDD` for containment
        checks.

    Returns
    -------
    QuestionAlgebra
        Minimized algebra.  ``len(result) ≤ len(algebra)``.
    """
    composition: dict[str, tuple[str, ComposeOp, str]] = getattr(
        algebra, "_composition", {}
    )
    if not composition:
        return _copy_algebra(algebra)  # fast path: no compositions

    bdd_ids, skel = _build_bdd_ids(algebra, composition, bdd)

    # cascade substitution: eliminated_name → canonical_name
    substitutions: dict[str, str] = {}
    eff: dict[str, tuple[str, ComposeOp, str]] = {}

    for name, (left_name, op, right_name) in composition.items():
        left_eff = _resolve(left_name, substitutions)
        right_eff = _resolve(right_name, substitutions)
        survivor = _try_eliminate(left_eff, right_eff, op, bdd_ids, bdd, skel, eff)
        if survivor is not None:
            substitutions[name] = survivor
        else:
            eff[name] = (left_eff, op, right_eff)

    if not substitutions:
        return _copy_algebra(algebra)

    return _Rebuild(algebra, substitutions).run()
