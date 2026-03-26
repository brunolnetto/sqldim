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

After elimination the canonical query DAG is topologically sorted.  Each node
becomes exactly one CTE in the SQL output — the minimum CTE count equals the
number of nodes in the canonical DAG (tight lower bound: no correct query with
fewer CTEs exists).

API
---
:class:`QueryDAGNode`          — dataclass: (node_id, op, left_id, right_id).
:class:`QueryDAGManager`       — ``leaf()``, ``make()``, sentinel ids.
:func:`apply_semiring_minimisation` — public entry point.
"""

from __future__ import annotations

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


def _build_bdd_ids(
    algebra: QuestionAlgebra,
    composition: dict[str, tuple[str, ComposeOp, str]],
    bdd: "DGMPredicateBDD",
) -> dict[str, int]:
    """Build ``{name: bdd_id}`` for every CTE in *algebra* (topological pass).

    Leaf CTEs: compiled from their WHERE predicate (or TRUE_NODE_ID if none).
    Composed UNION CTEs: OR of operands' BDD IDs.
    Composed INTERSECT CTEs: AND of operands' BDD IDs.
    Other ops: TRUE_NODE_ID (no Band-1 containment check).
    """
    bdd_ids: dict[str, int] = {}
    for name, cq in algebra._ctes.items():
        if name not in composition:
            bdd_ids[name] = _leaf_bdd_id(cq, bdd)
        else:
            left_name, op, right_name = composition[name]
            l_id = bdd_ids.get(left_name, TRUE_NODE_ID)
            r_id = bdd_ids.get(right_name, TRUE_NODE_ID)
            if op is ComposeOp.UNION:
                bdd_ids[name] = bdd.manager.apply("OR", l_id, r_id)
            elif op is ComposeOp.INTERSECT:
                bdd_ids[name] = bdd.manager.apply("AND", l_id, r_id)
            else:
                bdd_ids[name] = TRUE_NODE_ID
    return bdd_ids


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


def _try_eliminate(
    left_eff: str,
    right_eff: str,
    op: ComposeOp,
    bdd_ids: dict[str, int],
    bdd: "DGMPredicateBDD",
) -> str | None:
    """Return the surviving CTE if any elimination rule fires, else None."""
    if left_eff == right_eff:  # structural idempotence
        return left_eff
    if op is ComposeOp.UNION:
        return _union_survivor(left_eff, right_eff, bdd_ids, bdd)
    if op is ComposeOp.INTERSECT:
        return _intersect_survivor(left_eff, right_eff, bdd_ids, bdd)
    return None


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
    return new_alg


def _rebuild(
    algebra: QuestionAlgebra,
    eliminated: set[str],
) -> QuestionAlgebra:
    """Return a new algebra with all *eliminated* CTEs removed."""
    new_alg = QuestionAlgebra()
    for name, cq in algebra._ctes.items():
        if name not in eliminated:
            new_alg._ctes[name] = cq
    new_alg._composition.update(
        {k: v for k, v in algebra._composition.items() if k not in eliminated}
    )
    return new_alg


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

    bdd_ids = _build_bdd_ids(algebra, composition, bdd)

    # cascade substitution: eliminated_name → canonical_name
    substitutions: dict[str, str] = {}

    for name, (left_name, op, right_name) in composition.items():
        left_eff = _resolve(left_name, substitutions)
        right_eff = _resolve(right_name, substitutions)
        survivor = _try_eliminate(left_eff, right_eff, op, bdd_ids, bdd)
        if survivor is not None:
            substitutions[name] = survivor

    if not substitutions:
        return _copy_algebra(algebra)

    return _rebuild(algebra, set(substitutions.keys()))
