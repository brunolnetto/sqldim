"""The §6.1 BDD theory (issue #173): canonical atoms, Kleene encoding, Δ.

The conformance suite (``tests/conformance``, area ``bdd``) checks the
verdicts against the oracle; these tests pin the API and the fail-closed
choices that the oracle does not exercise.
"""

from __future__ import annotations

import datetime

from sqldim.core.query.dgm import (
    NOT,
    OR,
    PREVIOUSLY,
    NotTrue,
    PathPred,
    PropRef,
    Quantifier,
    RawPred,
    ScalarPred,
    BridgeHop,
)
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD


def _bdd(*non_nullable: str) -> DGMPredicateBDD:
    return DGMPredicateBDD(BDDManager(), non_nullable=non_nullable)


def _x(op: str, c: object, ref: str = "t.x") -> ScalarPred:
    return ScalarPred(PropRef(ref), op, c)


def test_atoms_are_identified_by_canonical_form():
    bdd = _bdd()
    assert bdd.compile(_x(">", 5)) == bdd.compile(_x(">", 5))
    assert bdd.compile(_x("<", 5)) == bdd.compile(ScalarPred(PropRef("t.x"), "<", 5))


def test_refs_are_nullable_unless_declared():
    p = OR(_x("=", 1), NOT(_x("=", 1)))
    bdd = _bdd()
    assert not bdd.is_tautology(bdd.compile(p))
    bdd = _bdd("t.x")
    assert bdd.is_tautology(bdd.compile(p))


def test_not_true_is_two_valued():
    bdd = _bdd()
    p = _x(">", 5)
    assert bdd.is_tautology(bdd.compile(OR(p, NotTrue(p))))
    assert NotTrue(p).to_sql() == "(t.x > 5) IS NOT TRUE"


def test_raw_true_and_false_are_constants():
    bdd = _bdd()
    assert bdd.is_tautology(bdd.compile(RawPred("TRUE")))
    assert not bdd.is_satisfiable(bdd.compile(RawPred(" false ")))


def test_order_theory_on_dates():
    bdd = _bdd()
    later = bdd.compile(_x(">", datetime.date(2024, 6, 1), "t.d"))
    early = bdd.compile(_x(">", datetime.date(2024, 1, 1), "t.d"))
    assert bdd.implies(later, early)
    assert not bdd.implies(early, later)


def test_strings_get_no_order_or_equality_axiom():
    """Their order and equality depend on the backend's collation."""
    bdd = _bdd("t.s")
    a, b = bdd.compile(_x("=", "a", "t.s")), bdd.compile(_x("=", "A", "t.s"))
    both = bdd.manager.apply("AND", a, b)
    assert bdd.is_satisfiable(both)
    assert not bdd.implies(
        bdd.compile(_x(">", "b", "t.s")), bdd.compile(_x(">", "a", "t.s"))
    )


def test_forall_previously_stays_an_opaque_atom():
    """§4.1 states no guard for FORALL PREVIOUSLY: no elimination, no inference."""
    hop = BridgeHop("c", "next", "s", table="t", on="s.prev_id = c.id")
    pp = PathPred(
        "c", hop, _x(">", 1, "s.v"), quantifier=Quantifier.FORALL,
        temporal_mode=PREVIOUSLY,
    )  # fmt: skip
    bdd = _bdd()
    uid = bdd.compile(pp)
    node = bdd.manager.get_node(uid)
    assert (node.low, node.high) == (0, 1)
    assert bdd._var_to_atom[node.var] is pp
