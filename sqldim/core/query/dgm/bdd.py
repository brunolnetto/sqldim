"""DGM BDD canonical predicate layer (DGM v0.16 §6.1 / §8.3).

Provides O(1) equivalence, satisfiability/tautology, implication, minterm
enumeration, and the foundation for the query planner and recommender.

Architecture
------------
BDDNode      — immutable (var, low, high) node; terminals FALSE_NODE_ID=0 /
               TRUE_NODE_ID=1.
BDDManager   — unique table (var,low,high)→id; computed cache (op,u,v)→result;
               make(), apply() (Shannon), negate() operations.
DGMPredicateBDD — compiles DGM predicate trees into BDD IDs; implements
               satisfiability, tautology, implication, equivalence, minterm
               enumeration; 8-step logical optimisation pipeline.

Variable ordering (DGM §6.1): PropRef → PathPred → SignaturePred atoms, ordered
by first occurrence.  BDD correctness is independent of ordering; ordering
determines diagram size.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.preds import AND, OR, PathPred

__all__ = [
    "BDDNode",
    "FALSE_NODE_ID",
    "TRUE_NODE_ID",
    "BDDManager",
    "DGMPredicateBDD",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FALSE_NODE_ID: int = 0
TRUE_NODE_ID: int = 1


# ---------------------------------------------------------------------------
# BDDNode
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=True)
class BDDNode:
    """Immutable BDD node.

    Terminal nodes have ``var = -1`` and their ``id`` is ``FALSE_NODE_ID`` or
    ``TRUE_NODE_ID``.  All other nodes have a non-negative variable index with
    ``low`` (var=False branch) and ``high`` (var=True branch) referring to
    other node IDs.
    """

    id: int
    var: int  # -1 for terminal nodes
    low: int  # branch taken when var=False
    high: int  # branch taken when var=True


# Pre-built terminal singletons (referenced by ID only at runtime).
_FALSE_NODE = BDDNode(id=FALSE_NODE_ID, var=-1, low=FALSE_NODE_ID, high=FALSE_NODE_ID)
_TRUE_NODE = BDDNode(id=TRUE_NODE_ID, var=-1, low=TRUE_NODE_ID, high=TRUE_NODE_ID)


# ---------------------------------------------------------------------------
# BDDManager
# ---------------------------------------------------------------------------


class BDDManager:
    """Manages the BDD unique table and computed cache.

    Implements:
    * ``make(var, low, high)`` — elimination + sharing; returns node ID.
    * ``apply(op, u, v)``     — Shannon expansion with memoisation.
    * ``negate(u)``            — single-pass complement.

    Unique table: ``(var, low, high) → id``.
    Computed cache: ``(op, u, v) → result_id``.
    """

    def __init__(self) -> None:
        # Index 0 = FALSE_NODE, index 1 = TRUE_NODE
        self._nodes: list[BDDNode] = [_FALSE_NODE, _TRUE_NODE]
        self._unique: dict[tuple[int, int, int], int] = {}
        self._computed: dict[tuple[str, int, int], int] = {}

    # -- Node access ---------------------------------------------------------

    def get_node(self, uid: int) -> BDDNode:
        """Return the BDDNode for *uid*."""
        return self._nodes[uid]

    # -- make ----------------------------------------------------------------

    def make(self, var: int, low: int, high: int) -> int:
        """Return the ID of the node (var, low, high), creating if necessary.

        Applies the elimination rule: if low == high, returns low directly
        (variable is irrelevant and is eliminated).  Uses the unique table for
        sharing (identical nodes get the same ID).
        """
        if low == high:
            return low  # elimination rule
        key = (var, low, high)
        if key in self._unique:
            return self._unique[key]
        uid = len(self._nodes)
        node = BDDNode(id=uid, var=var, low=low, high=high)
        self._nodes.append(node)
        self._unique[key] = uid
        return uid

    # -- apply ---------------------------------------------------------------

    def apply(self, op: str, u: int, v: int) -> int:
        """Apply binary Boolean operator *op* to nodes *u* and *v*.

        Uses Shannon expansion and the computed cache for memoisation.
        Supported ops: ``"AND"``, ``"OR"``.
        """
        cache_key = (op, u, v)
        if cache_key in self._computed:
            return self._computed[cache_key]

        result = self._apply_uncached(op, u, v)
        self._computed[cache_key] = result
        return result

    def _sc_and(self, u: int, v: int) -> "int | None":
        """Short-circuit terminals for AND; return None to continue Shannon."""
        if u == FALSE_NODE_ID or v == FALSE_NODE_ID:
            return FALSE_NODE_ID
        if u == TRUE_NODE_ID:
            return v
        if v == TRUE_NODE_ID:
            return u
        return None

    def _sc_or(self, u: int, v: int) -> "int | None":
        """Short-circuit terminals for OR; return None to continue Shannon."""
        if u == TRUE_NODE_ID or v == TRUE_NODE_ID:
            return TRUE_NODE_ID
        if u == FALSE_NODE_ID:
            return v
        if v == FALSE_NODE_ID:
            return u
        return None

    def _apply_uncached(self, op: str, u: int, v: int) -> int:
        sc = self._sc_and(u, v) if op == "AND" else self._sc_or(u, v)
        if sc is not None:
            return sc

        nu = self._nodes[u]
        nv = self._nodes[v]

        # Shannon expansion on the top variable
        if nu.var == nv.var:
            top = nu.var
            rlo = self.apply(op, nu.low, nv.low)
            rhi = self.apply(op, nu.high, nv.high)
        elif nu.var < nv.var:
            top = nu.var
            rlo = self.apply(op, nu.low, v)
            rhi = self.apply(op, nu.high, v)
        else:
            top = nv.var
            rlo = self.apply(op, u, nv.low)
            rhi = self.apply(op, u, nv.high)

        return self.make(top, rlo, rhi)

    # -- negate --------------------------------------------------------------

    def negate(self, u: int) -> int:
        """Return the BDD ID for NOT *u* (single-pass complement)."""
        if u == FALSE_NODE_ID:
            return TRUE_NODE_ID
        if u == TRUE_NODE_ID:
            return FALSE_NODE_ID
        node = self._nodes[u]
        neg_low = self.negate(node.low)
        neg_high = self.negate(node.high)
        return self.make(node.var, neg_low, neg_high)


# ---------------------------------------------------------------------------
# DGMPredicateBDD
# ---------------------------------------------------------------------------


class DGMPredicateBDD:
    """Compiles DGM predicate trees into reduced ordered BDDs.

    Atoms (ScalarPred, PathPred, SignaturePred) are assigned integer variable
    indices on first occurrence and treated as opaque Boolean atoms inside the
    BDD.  The BDD structure captures all AND/OR/NOT structure.
    """

    MINTERM_THRESHOLD = 8  # UNION ALL below; CASE expression above

    def __init__(self, manager: BDDManager) -> None:
        self.manager = manager
        self._atom_to_var: dict[int, int] = {}  # id(atom) → var index
        self._var_to_atom: dict[int, object] = {}  # var index → atom
        self._next_var: int = 0  # variable index counter

    # -- Convenience sentinels -----------------------------------------------

    def compile_true(self) -> int:
        """Return the TRUE terminal ID."""
        return TRUE_NODE_ID

    def compile_false(self) -> int:
        """Return the FALSE terminal ID."""
        return FALSE_NODE_ID

    def compile(self, pred: object) -> int:
        """Compile a DGM predicate tree to a BDD node ID."""
        return self._compile(pred)

    def _compile_and(self, pred: "AND") -> int:
        result = TRUE_NODE_ID
        for sub in pred.preds:
            result = self.manager.apply("AND", result, self._compile(sub))
        return result

    def _compile_or(self, pred: "OR") -> int:
        result = FALSE_NODE_ID
        for sub in pred.preds:
            result = self.manager.apply("OR", result, self._compile(sub))
        return result

    def _compile_forall(self, pred: "PathPred") -> int:
        from sqldim.core.query.dgm.preds import NOT, PathPred, Quantifier

        inner = NOT(pred.sub_filter)
        exists_pred = PathPred(
            pred.anchor,
            pred.path,
            inner,
            quantifier=Quantifier.EXISTS,
            strategy=pred.strategy,
            temporal_mode=pred.temporal_mode,
            promote=pred.promote,
        )
        return self.manager.negate(self._compile(exists_pred))

    def _compile(self, pred: object) -> int:
        from sqldim.core.query.dgm.preds import AND, OR

        _dispatch = {AND: self._compile_and, OR: self._compile_or}
        handler = _dispatch.get(type(pred))
        if handler is not None:
            return handler(pred)  # type: ignore[operator]
        return self._compile_structural(pred)

    def _compile_structural(self, pred: object) -> int:
        from sqldim.core.query.dgm.preds import NOT, PathPred, Quantifier

        if isinstance(pred, NOT):
            return self.manager.negate(self._compile(pred.pred))
        if isinstance(pred, PathPred) and pred.quantifier is Quantifier.FORALL:
            return self._compile_forall(pred)
        return self._atom_id(pred)

    def _atom_id(self, atom: object) -> int:
        """Return the BDD ID for an opaque atom, assigning a new var if needed."""
        key = id(atom)
        if key not in self._atom_to_var:
            var = self._next_var
            self._next_var += 1
            self._atom_to_var[key] = var
            self._var_to_atom[var] = atom
        var = self._atom_to_var[key]
        return self.manager.make(var, FALSE_NODE_ID, TRUE_NODE_ID)

    # -- Queries -------------------------------------------------------------

    def is_satisfiable(self, uid: int) -> bool:
        """Return True if the predicate is satisfiable (not identically False)."""
        return uid != FALSE_NODE_ID

    def is_tautology(self, uid: int) -> bool:
        """Return True if the predicate is a tautology (identically True)."""
        return uid == TRUE_NODE_ID

    def implies(self, u: int, v: int) -> bool:
        """Return True if u → v (u implies v).

        u → v iff NOT u OR v is a tautology, i.e. AND(u, NOT v) == FALSE.
        """
        not_v = self.manager.negate(v)
        return self.manager.apply("AND", u, not_v) == FALSE_NODE_ID

    def equivalent(self, u: int, v: int) -> bool:
        """Return True if u ↔ v (same BDD canonical form)."""
        return u == v

    def minterms(self, uid: int) -> list[dict[int, bool]]:
        """Enumerate all satisfying variable assignments (minterms).

        Returns a list of dicts mapping ``var_index → bool`` for each minterm.
        The TRUE node has one minterm (empty dict — all vars unconstrained).
        The FALSE node has no minterms.
        """
        results: list[dict[int, bool]] = []
        self._collect_minterms(uid, {}, results)
        return results

    def _collect_minterms(
        self,
        uid: int,
        assignment: dict[int, bool],
        results: list[dict[int, bool]],
    ) -> None:
        if uid == FALSE_NODE_ID:
            return
        if uid == TRUE_NODE_ID:
            results.append(dict(assignment))
            return
        node = self.manager.get_node(uid)
        # Take low branch (var = False)
        assignment[node.var] = False
        self._collect_minterms(node.low, assignment, results)
        # Take high branch (var = True)
        assignment[node.var] = True
        self._collect_minterms(node.high, assignment, results)
        del assignment[node.var]

    # -- Logical optimisation pipeline (DGM §6.1, 8 steps) ------------------

    def optimize(self, uid: int) -> int:
        """Apply the 8-step logical optimisation pipeline.

        Steps:
        1. Tautology / empty-result short-circuit
        2. Redundant AND-with-TRUE elimination
        3. Redundant OR-with-FALSE elimination
        4. (Cross-band implication — planner concern; no-op here)
        5. (PathPred context propagation — no-op at BDD level)
        6. (TemporalMode template selection — delegated to to_sql)
        7. (Minterm count → SQL strategy selection — happens in to_sql)
        8. FORALL De Morgan — already applied during compile()
        """
        # Step 1 — short-circuit
        if uid == FALSE_NODE_ID or uid == TRUE_NODE_ID:
            return uid
        # Steps 2–3 — identity element elimination is already baked into
        # BDDManager.apply() terminal short-circuits; the canonical BDD form
        # is already optimised after compilation.
        return uid

    # -- SQL translation -----------------------------------------------------

    def to_sql(self, uid: int) -> str:
        """Translate a compiled BDD to SQL.

        Uses UNION ALL when |minterms| ≤ THRESHOLD (step 7); otherwise
        returns a BDD-structured CASE expression placeholder.
        """
        if uid == FALSE_NODE_ID:
            return "FALSE"
        if uid == TRUE_NODE_ID:
            return "TRUE"
        terms = self.minterms(uid)
        if len(terms) <= self.MINTERM_THRESHOLD:
            return self._terms_to_union_all(terms)
        return self._bdd_case_sql(uid)

    @staticmethod
    def _minterm_to_clause(term: "dict[int, bool]") -> str:
        """Render one minterm dict as a SQL AND clause."""
        if not term:
            return "TRUE"
        return " AND ".join(
            f"var_{v} = {str(b).upper()}" for v, b in sorted(term.items())
        )

    def _terms_to_union_all(self, terms: "list[dict[int, bool]]") -> str:
        """Render a list of minterms as UNION ALL of SELECT 1 WHERE …."""
        parts = [self._minterm_to_clause(t) for t in terms]
        return " UNION ALL ".join(f"SELECT 1 WHERE {p}" for p in parts)

    def _bdd_case_sql(self, uid: int) -> str:
        """Render a CASE expression that walks the BDD at query time."""
        if uid == FALSE_NODE_ID:
            return "FALSE"
        if uid == TRUE_NODE_ID:
            return "TRUE"
        node = self.manager.get_node(uid)
        low_sql = self._bdd_case_sql(node.low)
        high_sql = self._bdd_case_sql(node.high)
        return f"CASE WHEN var_{node.var} THEN ({high_sql}) ELSE ({low_sql}) END"
