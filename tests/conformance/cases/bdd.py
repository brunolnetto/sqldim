"""The §6.1 BDD: Kleene encoding, order theory, canonical atoms, and §8.2/§8.3."""

from __future__ import annotations

import random
from itertools import product
from typing import Any

from tests.conformance.cases import DERIVED, Case

KLEENE = "§6.1 Three-valued logic (T⁺/T⁻ encoding, Δ_excl, Δ_null)"
ORDER = "§6.1 Order theory of atoms (Δ_ord, O1–O6)"
CANON = "§6.1 Variables (canonical form of atoms)"
PROP = "§6.1 / §8.3 propositional equivalence, tautology, implication"

P_NULL = {"p": ("opaque", True)}
N_ONLY = {"n": ("opaque", False)}
PQ = {"p": ("opaque", True), "q": ("opaque", True)}
X = {"x": {"nullable": True}}
XY = {"x": {"nullable": True}, "y": {"nullable": True}}
XN = {"x": {"nullable": False}}


def A(name: str) -> tuple:
    return ("atom", name)


def NOT(f: tuple) -> tuple:
    return ("not", f)


def AND(*fs: tuple) -> tuple:
    return ("and",) + fs


def OR(*fs: tuple) -> tuple:
    return ("or",) + fs


def _b(cid, clause, rule, question, formula, atoms, exp, exprs=None, other=None, **kw):
    inp: dict[str, Any] = {"question": question, "formula": formula, "atoms": atoms,
                           "exprs": exprs or {}, **kw}  # fmt: skip
    if other is not None:
        inp["other"] = other
    return Case(cid, "bdd", clause, rule, "atoms", inp, exp)


KLEENE_CASES = [
    _b("BDD-K-01", KLEENE, "p OR NOT p over a nullable atom is not a tautology",
       "tautology", OR(A("p"), NOT(A("p"))), P_NULL, False),
    _b("BDD-K-02", KLEENE, "n OR NOT n over a non-nullable atom is a tautology",
       "tautology", OR(A("n"), NOT(A("n"))), N_ONLY, True),
    _b("BDD-K-03", KLEENE, "p AND NOT p is a contradiction (Δ_excl)", "logical_sat",
       AND(A("p"), NOT(A("p"))), P_NULL, False),
    _b("BDD-K-04", KLEENE, "NOT (p OR NOT p) is a contradiction", "logical_sat",
       NOT(OR(A("p"), NOT(A("p")))), P_NULL, False),
    _b("BDD-K-05", KLEENE, "p OR NotTrue(p) is a tautology", "tautology",
       OR(A("p"), ("nottrue", A("p"))), P_NULL, True),
    _b("BDD-K-06", KLEENE, "IsNull(x) AND x > 5 is a contradiction (Δ_null)",
       "logical_sat", AND(A("isnull"), A("gt5")),
       {"isnull": ("isnull", "x"), "gt5": ("cmp", "x", ">", 5)}, False, X),
    _b("BDD-K-07", KLEENE, "p AND q is satisfiable", "logical_sat", AND(A("p"), A("q")),
       PQ, True),
    _b("BDD-K-08", KLEENE, "x > 5 OR NOT (x > 5) is not a tautology for nullable x",
       "tautology", OR(A("gt5"), NOT(A("gt5"))), {"gt5": ("cmp", "x", ">", 5)},
       DERIVED, X),
]  # fmt: skip

GT5 = ("cmp", "x", ">", 5)
ORDER_CASES = [
    _b("BDD-O-01", ORDER, "x > 5 implies x > 3", "implies", A("a"),
       {"a": GT5, "b": ("cmp", "x", ">", 3)}, True, X, other=A("b")),
    _b("BDD-O-02", ORDER, "x > 5 AND x < 3 is not satisfiable", "logical_sat",
       AND(A("a"), A("b")), {"a": GT5, "b": ("cmp", "x", "<", 3)}, False, X),
    _b("BDD-O-03", ORDER, "x > 5 does not imply y > 3", "implies", A("a"),
       {"a": GT5, "b": ("cmp", "y", ">", 3)}, False, XY, other=A("b")),
    _b("BDD-O-04", ORDER, "x > y AND y > x is satisfiable (outside the theory)",
       "logical_sat", AND(A("a"), A("b")),
       {"a": ("cmp2", "x", ">", "y"), "b": ("cmp2", "y", ">", "x")}, True, XY),
    _b("BDD-O-05", ORDER, "x > 5 AND x < 6 is satisfiable (no integer discreteness)",
       "logical_sat", AND(A("a"), A("b")), {"a": GT5, "b": ("cmp", "x", "<", 6)},
       DERIVED, X),
    _b("BDD-O-06", ORDER, "x = 5 implies x < 7 (O3)", "implies", A("a"),
       {"a": ("cmp", "x", "=", 5), "b": ("cmp", "x", "<", 7)}, DERIVED, X,
       other=A("b")),
    _b("BDD-O-07", ORDER, "(x = 1 OR x = 2) AND x = 3 is not satisfiable",
       "logical_sat", AND(OR(A("a"), A("b")), A("c")),
       {"a": ("cmp", "x", "=", 1), "b": ("cmp", "x", "=", 2), "c": ("cmp", "x", "=", 3)},
       DERIVED, X),
    _b("BDD-O-08", ORDER, "x >= 5 implies NOT (x < 3)", "implies", A("a"),
       {"a": ("cmp", "x", ">=", 5), "b": ("cmp", "x", "<", 3)}, DERIVED, X,
       other=NOT(A("b"))),
]  # fmt: skip

STR = "§6.1 Order theory of atoms: String equality always, order under a collation"
S = {"s": {"nullable": True}}
SB = {"s": {"nullable": True, "collation": "binary"}}


def _s(op: str, c: str) -> tuple:
    return ("cmp", "s", op, c)


STRING_CASES = [
    _b("BDD-S-01", STR, "s = 'a' AND s = 'b' is not satisfiable (no collation)",
       "logical_sat", AND(A("a"), A("b")), {"a": _s("=", "a"), "b": _s("=", "b")},
       DERIVED, S),
    _b("BDD-S-02", STR, "(s = 'a' OR s = 'b') AND s = 'c' is not satisfiable",
       "logical_sat", AND(OR(A("a"), A("b")), A("c")),
       {"a": _s("=", "a"), "b": _s("=", "b"), "c": _s("=", "c")}, DERIVED, S),
    _b("BDD-S-03", STR, "s = 'a' implies s ≠ 'b' (no collation)", "implies", A("a"),
       {"a": _s("=", "a"), "b": _s("!=", "b")}, DERIVED, S, other=A("b")),
    _b("BDD-S-04", STR, "s = 'a' AND s = 'A' is not satisfiable (equality by value)",
       "logical_sat", AND(A("a"), A("b")), {"a": _s("=", "a"), "b": _s("=", "A")},
       DERIVED, S),
    _b("BDD-S-05", STR, "without a collation s > 'b' does not imply s > 'a'",
       "implies", A("a"), {"a": _s(">", "b"), "b": _s(">", "a")}, DERIVED, S,
       other=A("b")),
    _b("BDD-S-06", STR, "without a collation s > 'b' AND s < 'a' is satisfiable",
       "logical_sat", AND(A("a"), A("b")), {"a": _s(">", "b"), "b": _s("<", "a")},
       DERIVED, S),
    _b("BDD-S-07", STR, "without a collation s = 'a' does not imply s < 'b'",
       "implies", A("a"), {"a": _s("=", "a"), "b": _s("<", "b")}, DERIVED, S,
       other=A("b")),
    _b("BDD-S-08", STR, "under binary s > 'b' implies s > 'a'", "implies", A("a"),
       {"a": _s(">", "b"), "b": _s(">", "a")}, DERIVED, SB, other=A("b")),
    _b("BDD-S-09", STR, "under binary s > 'b' AND s < 'a' is not satisfiable",
       "logical_sat", AND(A("a"), A("b")), {"a": _s(">", "b"), "b": _s("<", "a")},
       DERIVED, SB),
    _b("BDD-S-10", STR, "under binary s = 'a' implies s < 'b' (O3)", "implies",
       A("a"), {"a": _s("=", "a"), "b": _s("<", "b")}, DERIVED, SB, other=A("b")),
    _b("BDD-S-11", STR, "under binary s = 'a' implies s > 'B' (code point order)",
       "implies", A("a"), {"a": _s("=", "a"), "b": _s(">", "B")}, DERIVED, SB,
       other=A("b")),
    _b("BDD-S-12", STR, "under binary s ≥ 'b' implies NOT (s < 'a')", "implies",
       A("a"), {"a": _s(">=", "b"), "b": _s("<", "a")}, DERIVED, SB,
       other=NOT(A("b"))),
]  # fmt: skip

CANON_CASES = [
    _b("BDD-C-01", CANON, "two separately built x > 5 atoms are one variable",
       "equivalent", A("a"), {"a": GT5, "b": GT5}, DERIVED, X, other=A("b")),
    _b("BDD-C-02", CANON, "x ≥ 5 is NOT (x < 5)", "equivalent", A("a"),
       {"a": ("cmp", "x", ">=", 5), "b": ("cmp", "x", "<", 5)}, DERIVED, XN,
       other=NOT(A("b"))),
    _b("BDD-C-03", CANON, "x ≠ 5 is NOT (x = 5)", "equivalent", A("a"),
       {"a": ("cmp", "x", "!=", 5), "b": ("cmp", "x", "=", 5)}, DERIVED, XN,
       other=NOT(A("b"))),
]  # fmt: skip

PQR = {"p": ("opaque", False), "q": ("opaque", False), "r": ("opaque", False)}
p, q, r = A("p"), A("q"), A("r")

PROP_CASES = [
    _b("BDD-P-01", PROP, "AND is commutative", "equivalent", AND(p, q), PQR, True,
       other=AND(q, p)),
    _b("BDD-P-02", PROP, "De Morgan", "equivalent", NOT(AND(p, q)), PQR, True,
       other=OR(NOT(p), NOT(q))),
    _b("BDD-P-03", PROP, "absorption p ∨ (p ∧ q) ≡ p", "equivalent", OR(p, AND(p, q)),
       PQR, True, other=p),
    _b("BDD-P-04", PROP, "p ∧ q implies p", "implies", AND(p, q), PQR, True, other=p),
    _b("BDD-P-05", PROP, "p does not imply p ∧ q", "implies", p, PQR, False,
       other=AND(p, q)),
    _b("BDD-P-06", PROP, "(¬p ∨ q) ∨ (¬q ∨ p) is a tautology", "tautology",
       OR(OR(NOT(p), q), OR(NOT(q), p)), PQR, True),
    _b("BDD-P-07", PROP, "distributivity", "equivalent", AND(p, OR(q, r)), PQR, True,
       other=OR(AND(p, q), AND(p, r))),
]  # fmt: skip


def random_formula(rng: random.Random, depth: int) -> tuple:
    if depth == 0 or rng.random() < 0.25:
        return A(rng.choice("pqr"))
    op = rng.choice(("and", "or", "not"))
    if op == "not":
        return NOT(random_formula(rng, depth - 1))
    return (op, random_formula(rng, depth - 1), random_formula(rng, depth - 1))


def _gen(seed: int) -> Case:
    rng = random.Random(seed)
    question = ("tautology", "logical_sat", "implies", "equivalent")[seed % 4]
    f, g = random_formula(rng, 4), random_formula(rng, 4)
    return _b(f"BDD-P-GEN-{seed:02d}", PROP, f"seeded formula {seed}: {question}",
              question, f, PQR, DERIVED, other=g)  # fmt: skip


GENERATED = [_gen(s) for s in range(24)]

ELIM = "§4.1 FORALL elimination (CTL duality, NotTrue, REQUIRE_WITNESS guard)"
GUARD_EF = ("+", "EXISTS", "EVENTUALLY", "TRUE")
GUARD_EX = ("+", "EXISTS", "NEXT", "TRUE")

ELIM_CASES = [
    Case("BDD-F-01", "forall_elim", ELIM, "AG φ = guard ∧ ¬EF NotTrue(φ)", "atoms",
         {"mode": "GLOBALLY"},
         sorted([GUARD_EF, ("-", "EXISTS", "EVENTUALLY", "NotTrue(φ)")])),
    Case("BDD-F-02", "forall_elim", ELIM, "AF φ = guard ∧ ¬EG NotTrue(φ)", "atoms",
         {"mode": "EVENTUALLY"},
         sorted([GUARD_EF, ("-", "EXISTS", "GLOBALLY", "NotTrue(φ)")])),
    Case("BDD-F-03", "forall_elim", ELIM, "AX φ = guard ∧ ¬EX NotTrue(φ)", "atoms",
         {"mode": "NEXT"},
         sorted([GUARD_EX, ("-", "EXISTS", "NEXT", "NotTrue(φ)")])),
    Case("BDD-F-04", "forall_elim", ELIM,
         "FORALL PREVIOUSLY φ = guard ∧ ¬EXISTS PREVIOUSLY NotTrue(φ) (self-dual)",
         "atoms", {"mode": "PREVIOUSLY"},
         sorted([("+", "EXISTS", "PREVIOUSLY", "TRUE"),
                 ("-", "EXISTS", "PREVIOUSLY", "NotTrue(φ)")])),
]  # fmt: skip

THM = "§8.2 Propositional completeness (proof by minterm construction)"
THEOREM_CASES = [
    Case(f"THM-PC-{''.join('1' if b else '0' for b in bits)}", "completeness", THM,
         "the filter tree built by the proof computes the function", "atoms",
         {"bits": bits}, True)
    for bits in product([False, True], repeat=4)
]  # fmt: skip

CUBES = "§8.3 A BDD's paths to TRUE are disjoint cubes covering its minterms"
CUBE_CASES = [
    Case(f"THM-CUBE-{s:02d}", "cubes", CUBES, f"seeded formula {s}", "atoms",
         {"formula": random_formula(random.Random(1000 + s), 4)}, DERIVED)
    for s in range(8)
]  # fmt: skip

CASES = (
    KLEENE_CASES
    + ORDER_CASES
    + STRING_CASES
    + CANON_CASES
    + PROP_CASES
    + GENERATED
    + ELIM_CASES
    + THEOREM_CASES
    + CUBE_CASES
)
