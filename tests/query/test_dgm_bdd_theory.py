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
import pytest

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


def test_string_equality_always_gets_its_axioms():
    """Equality does not depend on the collation (§6.1)."""
    bdd = _bdd("t.s")
    a, b = bdd.compile(_x("=", "a", "t.s")), bdd.compile(_x("=", "A", "t.s"))
    assert not bdd.is_satisfiable(bdd.manager.apply("AND", a, b))


def test_string_order_is_opaque_without_a_collation():
    bdd = _bdd("t.s")
    assert not bdd.implies(
        bdd.compile(_x(">", "b", "t.s")), bdd.compile(_x(">", "a", "t.s"))
    )


def test_string_order_under_the_binary_collation():
    bdd = DGMPredicateBDD(BDDManager(), collations={"t.s": "binary"})
    gt_b, gt_a = bdd.compile(_x(">", "b", "t.s")), bdd.compile(_x(">", "a", "t.s"))
    assert bdd.implies(gt_b, gt_a) and not bdd.implies(gt_a, gt_b)
    # code point order: 'B' < 'a'
    assert bdd.implies(
        bdd.compile(_x("=", "a", "t.s")), bdd.compile(_x(">", "B", "t.s"))
    )
    # another expression is not collated
    assert not bdd.implies(
        bdd.compile(_x(">", "b", "t.u")), bdd.compile(_x(">", "a", "t.u"))
    )


def test_an_unknown_collation_is_refused():
    with pytest.raises(ValueError, match="collation"):
        DGMPredicateBDD(BDDManager(), collations={"t.s": "nocase"})


def test_forall_previously_is_eliminated_with_a_one_hop_guard():
    """§4.1: self-dual, guarded by PathPred(EXISTS, PREVIOUSLY, TRUE)."""
    hop = BridgeHop("c", "next", "s", table="t", on="s.prev_id = c.id")
    pp = PathPred(
        "c", hop, _x(">", 1, "s.v"), quantifier=Quantifier.FORALL,
        temporal_mode=PREVIOUSLY,
    )  # fmt: skip
    bdd = _bdd()
    uid = bdd.compile(pp)
    atoms = set()
    while uid not in (0, 1):
        node = bdd.manager.get_node(uid)
        atom = bdd._var_to_atom[node.var]
        assert atom is not pp and atom.quantifier is Quantifier.EXISTS
        assert atom.temporal_mode is PREVIOUSLY
        positive = node.low == 0
        atoms.add((positive, type(atom.sub_filter).__name__))
        uid = node.high if positive else node.low
    assert atoms == {(True, "RawPred"), (False, "NotTrue")}
