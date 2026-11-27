"""Joint BDD over plan-IR predicates: feasibility and implication (§6.1, §7.1 Layer 2).

Atoms are the canonical atoms of :mod:`..planner._ir_identity` (a comparison,
a raw condition, a path predicate), each one an opaque Boolean variable, as in
:class:`~sqldim.core.query.dgm.bdd.DGMPredicateBDD`.  The answers are
therefore propositional: *infeasible* means contradictory by Boolean
structure alone (``x > 5 AND NOT x > 5``), and an implication is proved only
when it holds for every assignment of the atoms.  Nothing is claimed beyond
that — two atoms the theory would relate (``x > 5`` and ``x > 3``) are
independent here, so feasibility errs on the side of keeping a candidate and
implication on the side of "not proved".
"""

from __future__ import annotations

from sqldim.core.query.dgm.bdd import FALSE_NODE_ID, TRUE_NODE_ID, BDDManager
from sqldim.core.query.dgm.planner._canon import canonical_encode
from sqldim.core.query.dgm.planner._ir import Conj, Disj, Neg, Pred
from sqldim.core.query.dgm.planner._ir_identity import _atom

__all__ = ["PredicateBDD"]


class PredicateBDD:
    """One BDD manager shared by every predicate compiled through it."""

    def __init__(self) -> None:
        self._mgr = BDDManager()
        self._vars: dict[bytes, int] = {}

    def compile(self, p: Pred | None) -> int:
        """The BDD of *p*; ``None`` (no filter) is TRUE."""
        if p is None:
            return TRUE_NODE_ID
        if isinstance(p, Conj):
            return self._fold("AND", p.preds, TRUE_NODE_ID)
        if isinstance(p, Disj):
            return self._fold("OR", p.preds, FALSE_NODE_ID)
        if isinstance(p, Neg):
            return self._mgr.negate(self.compile(p.pred))
        key = canonical_encode(_atom(p))
        var = self._vars.setdefault(key, len(self._vars))
        return self._mgr.make(var, FALSE_NODE_ID, TRUE_NODE_ID)

    def _fold(self, op: str, preds: tuple[Pred, ...], unit: int) -> int:
        acc = unit
        for sub in preds:
            acc = self._mgr.apply(op, acc, self.compile(sub))
        return acc

    def satisfiable(self, p: Pred | None) -> bool:
        return self.compile(p) != FALSE_NODE_ID

    def implies(self, p: Pred | None, q: Pred | None) -> bool:
        """``p → q`` for every assignment of the atoms."""
        u, v = self.compile(p), self.compile(q)
        return self._mgr.apply("AND", u, self._mgr.negate(v)) == FALSE_NODE_ID

    def equivalent(self, p: Pred | None, q: Pred | None) -> bool:
        return self.compile(p) == self.compile(q)
