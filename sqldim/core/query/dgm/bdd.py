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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqldim.core.query.dgm._bdd_theory import Formula
    from sqldim.core.query.dgm.preds import PathPred

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
    """Compiles DGM predicate trees into reduced ordered BDDs (§6.1).

    A variable is an atom in canonical form (:mod:`._bdd_theory`), never a
    Python object: two separately built ``x > 5`` are one variable.  Filters
    are three-valued (Kleene); :meth:`compile` returns ``T⁺(φ)``, the BDD of
    "φ is TRUE", over two variables ``p⁺``/``p⁻`` for every nullable atom and
    one for a two-valued atom (IsNull, PathPred, SignaturePred, and a
    comparison whose Refs are all declared non-nullable).  Every property is
    decided relative to the axioms Δ = Δ_excl ∧ Δ_null ∧ Δ_ord of the atoms
    compiled so far.

    *non_nullable* names the Refs declared non-nullable, by their SQL
    (``"t.x"``); every other Ref is nullable, so nothing is inferred from
    the absence of NULL unless it is declared.

    FORALL is eliminated by CTL duality with ``NotTrue`` and the
    REQUIRE_WITNESS guard (§4.1).  A FORALL whose dual the specification
    does not state (a PREVIOUSLY or a strategy other than ALL) stays an
    opaque atom: no inference is made about it.
    """

    MINTERM_THRESHOLD = 8  # UNION ALL below; CASE expression above

    def __init__(
        self, manager: BDDManager, *, non_nullable: "Iterable[str]" = ()
    ) -> None:
        self.manager = manager
        self._non_nullable = frozenset(non_nullable)
        self._atoms: dict[tuple, tuple[int, int | None]] = {}  # key → (v⁺, v⁻)
        self._var_to_atom: dict[int, object] = {}  # var index → atom
        self._next_var: int = 0  # variable index counter
        self._delta_cache: tuple[int, int] | None = None  # (|atoms|, Δ)

    # -- Convenience sentinels -----------------------------------------------

    def compile_true(self) -> int:
        """Return the TRUE terminal ID."""
        return TRUE_NODE_ID

    def compile_false(self) -> int:
        """Return the FALSE terminal ID."""
        return FALSE_NODE_ID

    def compile(self, pred: object) -> int:
        """``T⁺(pred)``: the BDD of "pred is TRUE"."""
        return self._tr(pred)[0]

    # -- Translation T⁺ / T⁻ -------------------------------------------------

    def _tr(self, pred: object) -> tuple[int, int]:
        """``(T⁺(pred), T⁻(pred))``."""
        from sqldim.core.query.dgm.preds import AND, NOT, OR, NotTrue

        if isinstance(pred, AND):
            return self._junction([self._tr(p) for p in pred.preds], "AND")
        if isinstance(pred, OR):
            return self._junction([self._tr(p) for p in pred.preds], "OR")
        if isinstance(pred, NOT):
            pos, neg = self._tr(pred.pred)
            return neg, pos
        if isinstance(pred, NotTrue):
            pos = self._tr(pred.pred)[0]
            return self.manager.negate(pos), pos
        return self._tr_atom(pred)

    def _junction(self, parts: "list[tuple[int, int]]", op: str) -> tuple[int, int]:
        dual = "OR" if op == "AND" else "AND"
        pos = TRUE_NODE_ID if op == "AND" else FALSE_NODE_ID
        neg = FALSE_NODE_ID if op == "AND" else TRUE_NODE_ID
        for p, n in parts:
            pos = self.manager.apply(op, pos, p)
            neg = self.manager.apply(dual, neg, n)
        return pos, neg

    def _tr_formula(self, f: "Formula", atom: object) -> tuple[int, int]:
        tag = f[0]
        if tag == "atom":
            return self._literal(f[1], atom)
        if tag == "const":
            return (
                (TRUE_NODE_ID, FALSE_NODE_ID) if f[1] else (FALSE_NODE_ID, TRUE_NODE_ID)
            )
        if tag == "not":
            pos, neg = self._tr_formula(f[1], atom)
            return neg, pos
        return self._junction([self._tr_formula(g, atom) for g in f[1:]], tag.upper())

    def _tr_atom(self, pred: object) -> tuple[int, int]:
        from sqldim.core.query.dgm._bdd_theory import canonical
        from sqldim.core.query.dgm.preds import PathPred, Quantifier, SignaturePred

        f = canonical(pred)
        if f is not None:
            return self._tr_formula(f, pred)
        if isinstance(pred, PathPred):
            if pred.quantifier is Quantifier.FORALL:
                eliminated = self._eliminate_forall(pred)
                if eliminated is not None:
                    return self._tr(eliminated)
            return self._literal(self._path_key(pred), pred, two_valued=True)
        if isinstance(pred, SignaturePred):
            key: tuple = ("sig", pred.path_alias, repr(pred.sequence), repr(pred.match))
            return self._literal(key, pred, two_valued=True)
        to_sql = getattr(pred, "to_sql", None)
        key = ("raw", to_sql()) if to_sql is not None else ("obj", id(pred))
        return self._literal(key, pred)

    # -- Atoms ---------------------------------------------------------------

    def _nullable(self, key: tuple) -> bool:
        from sqldim.core.query.dgm._bdd_theory import exprs_of

        if key[0] in ("isnull", "path", "sig"):
            return False
        exprs = exprs_of(key)
        if not exprs:
            return True  # a raw or unknown atom: nothing is known
        return any(self._expr_nullable(e) for e in exprs)

    def _expr_nullable(self, e: tuple) -> bool:
        return e[1] not in self._non_nullable

    def _new_var(self, atom: object) -> int:
        var = self._next_var
        self._next_var += 1
        self._var_to_atom[var] = atom
        return var

    def _register(self, key: tuple, atom: object, two_valued: bool) -> None:
        from sqldim.core.query.dgm._bdd_theory import basis_companions

        for k in basis_companions(key):
            if k not in self._atoms:
                pos = self._new_var(atom)
                null = not two_valued and self._nullable(k)
                self._atoms[k] = (pos, self._new_var(atom) if null else None)

    def _literal(
        self, key: tuple, atom: object, *, two_valued: bool = False
    ) -> tuple[int, int]:
        """``(T⁺, T⁻)`` of one basis atom, registering it on first use."""
        self._register(key, atom, two_valued)
        return self._lits(key)

    def _lits(self, key: tuple) -> tuple[int, int]:
        pos_var, neg_var = self._atoms[key]
        pos = self.manager.make(pos_var, FALSE_NODE_ID, TRUE_NODE_ID)
        if neg_var is None:
            return pos, self.manager.negate(pos)
        return pos, self.manager.make(neg_var, FALSE_NODE_ID, TRUE_NODE_ID)

    def _path_key(self, pred: "PathPred") -> tuple:
        """(anchor, path, strategy, temporal_mode, BDD id of sub_filter)."""
        from sqldim.core.query.dgm.preds import _flatten_path, _HopBase

        def hop(h: object) -> tuple:
            if isinstance(h, _HopBase):
                return (
                    type(h).__name__,
                    h.from_alias,
                    h.label,
                    h.to_alias,
                    h.table,
                    h.on,
                )
            return ("obj", id(h))

        strategy = pred.strategy
        strat = (
            None
            if strategy is None
            else (type(strategy).__name__, getattr(strategy, "k", None))
        )
        return (
            "path",
            pred.anchor,
            tuple(hop(h) for h in _flatten_path(pred.path)),
            strat,
            self._mode_key(pred.temporal_mode),
            self.compile(pred.sub_filter),
        )

    def _mode_key(self, mode: object) -> object:
        if mode is None:
            return None
        if isinstance(mode, type):
            return mode.__name__
        inner = getattr(mode, "pred", None)
        return (type(mode).__name__, None if inner is None else self.compile(inner))

    # -- FORALL elimination (§4.1) ---------------------------------------------

    def _eliminate_forall(self, pred: "PathPred") -> object | None:
        """The EXISTS form of a FORALL PathPred, or ``None`` when undefined."""
        from sqldim.core.query.dgm.preds import ALL, AND, NOT, NotTrue, RawPred
        from sqldim.core.query.dgm.temporal import (
            EVENTUALLY,
            GLOBALLY,
            NEXT,
            UntilMode,
        )

        if pred.strategy is not None and not isinstance(pred.strategy, ALL):
            return None
        mode: Any = pred.temporal_mode
        phi: Any = pred.sub_filter

        def exists(m: object, sub: object) -> Any:
            return self._exists_like(pred, m, sub)

        true = RawPred("TRUE")
        dual: dict[Any, Any] = {
            None: None,
            GLOBALLY: EVENTUALLY,
            EVENTUALLY: GLOBALLY,
            NEXT: NEXT,
        }
        if mode in dual:
            guard = exists(
                NEXT if mode is NEXT else (None if mode is None else EVENTUALLY), true
            )
            return AND(guard, NOT(exists(dual[mode], NotTrue(phi))))
        if isinstance(mode, UntilMode):
            psi: Any = mode.pred
            until = exists(UntilMode(AND(NotTrue(phi), NotTrue(psi))), NotTrue(psi))
            never = exists(GLOBALLY, NotTrue(psi))
            return AND(exists(EVENTUALLY, true), NOT(until), NOT(never))
        return None

    @staticmethod
    def _exists_like(pred: "PathPred", mode: object, sub: object) -> object:
        from sqldim.core.query.dgm.preds import PathPred, Quantifier

        return PathPred(
            pred.anchor,
            pred.path,
            sub,  # type: ignore[arg-type]
            quantifier=Quantifier.EXISTS,
            strategy=pred.strategy,
            temporal_mode=mode,
            promote=pred.promote,
        )

    # -- Δ -------------------------------------------------------------------

    def _delta(self) -> int:
        """Δ_excl ∧ Δ_null ∧ Δ_ord over every atom compiled so far."""
        from sqldim.core.query.dgm._bdd_theory import delta_clauses

        size = len(self._atoms)
        if self._delta_cache is not None and self._delta_cache[0] == size:
            return self._delta_cache[1]
        out = TRUE_NODE_ID
        clauses = delta_clauses(self._atoms, self._nullable, self._expr_nullable)
        for clause in clauses:
            out = self.manager.apply("AND", out, self._clause(clause))
        self._delta_cache = (size, out)
        return out

    def _lit(self, lit: "tuple[tuple, bool]") -> int:
        pos, neg = self._lits(lit[0])
        return pos if lit[1] else neg

    def _all(self, lits: "Iterable[tuple[tuple, bool]]") -> int:
        out = TRUE_NODE_ID
        for lit in lits:
            out = self.manager.apply("AND", out, self._lit(lit))
        return out

    def _defined(self, key: tuple) -> int:
        pos, neg = self._lits(key)
        return self.manager.apply("OR", pos, neg)

    def _clause(self, clause: "tuple[str, tuple, tuple]") -> int:
        kind, left, right = clause
        m = self.manager
        if kind == "excl":
            return m.negate(self._all(left))
        if kind == "iff_def":
            a, b = (self._defined(k) for k, _ in left)
            return m.negate(self._xor(a, b))
        premise = self._all(left)
        if kind == "undef":
            body = TRUE_NODE_ID
            for k, _ in right:
                body = m.apply("AND", body, m.negate(self._defined(k)))
        else:
            body = self._all(right)
        return m.apply("OR", m.negate(premise), body)

    def _xor(self, u: int, v: int) -> int:
        m = self.manager
        return m.apply(
            "OR", m.apply("AND", u, m.negate(v)), m.apply("AND", m.negate(u), v)
        )

    # -- Queries -------------------------------------------------------------

    def is_satisfiable(self, uid: int) -> bool:
        """``logical_sat``: Δ ∧ T⁺(φ) is not the FALSE node."""
        if uid in (FALSE_NODE_ID, TRUE_NODE_ID):
            return uid == TRUE_NODE_ID
        return self.manager.apply("AND", self._delta(), uid) != FALSE_NODE_ID

    def is_tautology(self, uid: int) -> bool:
        """``tautology``: Δ → T⁺(φ) is the TRUE node."""
        if uid in (FALSE_NODE_ID, TRUE_NODE_ID):
            return uid == TRUE_NODE_ID
        m = self.manager
        return m.apply("OR", m.negate(self._delta()), uid) == TRUE_NODE_ID

    def implies(self, u: int, v: int) -> bool:
        """``implies``: Δ ∧ T⁺(u) ∧ ¬T⁺(v) is the FALSE node."""
        m = self.manager
        both = m.apply("AND", u, m.negate(v))
        if both == FALSE_NODE_ID:
            return True
        return m.apply("AND", self._delta(), both) == FALSE_NODE_ID

    def equivalent(self, u: int, v: int) -> bool:
        """``equivalent``: under Δ the two filters keep the same tuples."""
        if u == v:
            return True
        diff = self._xor(u, v)
        return self.manager.apply("AND", self._delta(), diff) == FALSE_NODE_ID

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
