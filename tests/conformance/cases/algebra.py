"""The question algebra (§8.13) and Rule 11 minimisation (§6.2, §8.14).

Every element of ``Q_algebra`` denotes a set of tuples; a minimised
composition denotes the same set as the original.  The leaves read
``orders``, which stores one row twice, so a bag-valued result is told apart.
"""

from __future__ import annotations

from tests.conformance.cases import DERIVED, Case

SET = "§8.13 Semiring structure (elements are sets); §6.2 Rule 11 extended"

TABLES = {
    "orders": [(1, "a"), (1, "a"), (2, "b"), (3, "c")],
    "returns": [(1, "a"), (9, "z")],
}
Q = ("orders", None)
R = ("returns", None)
STRICT = ("orders", ("x", "=", "b"))


def _alg(cid, rule, leaves, compose, final, minimise=True):
    inp = {"tables": TABLES, "leaves": leaves, "compose": compose, "final": final,
           "minimise": minimise}  # fmt: skip
    return Case(cid, "algebra", SET, rule, "orders_bag", inp, DERIVED)


CASES = [
    _alg("ALG-01", "Q ∪ Q is the set of Q's rows", {"q": Q},
         [("qq", "q", "UNION", "q")], "qq", minimise=False),
    _alg("ALG-02", "minimised Q ∪ Q (idempotence) returns the rows of Q ∪ Q",
         {"q": Q}, [("qq", "q", "UNION", "q")], "qq"),
    _alg("ALG-03", "minimised Q ∩ Q (idempotence) returns the rows of Q ∩ Q",
         {"q": Q}, [("qq", "q", "INTERSECT", "q")], "qq"),
    _alg("ALG-04", "minimised Q₁ ∪ Q₂ with Q₁ ⊆ Q₂ (containment absorption)",
         {"s": STRICT, "q": Q}, [("u", "s", "UNION", "q")], "u"),
    _alg("ALG-05", "a JOIN over a minimised Q ∪ Q joins its distinct rows",
         {"q": Q, "r": R}, [("qq", "q", "UNION", "q"), ("j", "qq", "JOIN", "r")], "j"),
    _alg("ALG-06", "EXCEPT over a minimised Q ∪ Q", {"q": Q, "r": R},
         [("qq", "q", "UNION", "q"), ("e", "qq", "EXCEPT", "r")], "e"),
    _alg("ALG-07", "no containment between leaves over different tables",
         {"q": Q, "r": R}, [("u", "q", "UNION", "r")], "u"),
    _alg("ALG-08", "minimised Q ∩ (Q ∪ R) (absorption)", {"q": Q, "r": R},
         [("u", "q", "UNION", "r"), ("i", "q", "INTERSECT", "u")], "i"),
]  # fmt: skip
