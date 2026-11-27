"""``DGMPredicateBDD`` (§6.1, §8.2, §8.3).

An atom of the case becomes one ``sqldim`` predicate object per atom *name*,
so two occurrences of one name are one object; two names with the same
definition are two objects built separately (the canonical-form cases).
An expression or opaque atom the case declares non-nullable is passed to the
BDD as ``non_nullable``; every other Ref is nullable for it.  An expression's
declared collation is passed as ``collations``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from itertools import product
from typing import Any

from sqldim.core.query.dgm import (
    AND,
    PREVIOUSLY,
    BridgeHop,
    EVENTUALLY,
    GLOBALLY,
    NEXT,
    NOT,
    OR,
    NotTrue,
    PathPred,
    PropRef,
    Quantifier,
    RawPred,
    ScalarPred,
    VerbHop,
)
from sqldim.core.query.dgm.bdd import (
    FALSE_NODE_ID,
    TRUE_NODE_ID,
    BDDManager,
    DGMPredicateBDD,
)
from tests.conformance.cases import Case
from tests.conformance.reference import completeness_tree
from tests.conformance.runners import Backend, NotExpressible


def _atom(spec: Sequence, name: str) -> Any:
    kind = spec[0]
    if kind == "opaque":
        return ScalarPred(PropRef(f"t.{name}"), "=", True)
    if kind == "cmp":
        return ScalarPred(PropRef(f"t.{spec[1]}"), spec[2], spec[3])
    if kind == "isnull":
        return ScalarPred(PropRef(f"t.{spec[1]}"), "IS", None)
    if kind == "cmp2":
        return RawPred(f"t.{spec[1]} {spec[2]} t.{spec[3]}")
    raise NotExpressible(f"atom form {kind!r}")


def _tree(f: Sequence, atoms: Mapping[str, Any]) -> Any:
    tag = f[0]
    if tag == "atom":
        return atoms[f[1]]
    if tag == "not":
        return NOT(_tree(f[1], atoms))
    if tag == "and":
        return AND(*(_tree(g, atoms) for g in f[1:]))
    if tag == "or":
        return OR(*(_tree(g, atoms) for g in f[1:]))
    if tag == "nottrue":
        return NotTrue(_tree(f[1], atoms))
    raise NotExpressible(f"sqldim has no {tag!r} filter node")


def _bdd(
    non_nullable: Sequence[str] = (), collations: Mapping[str, str] | None = None
) -> DGMPredicateBDD:
    return DGMPredicateBDD(
        BDDManager(), non_nullable=non_nullable, collations=collations
    )


def _collations(i: Mapping[str, Any]) -> dict[str, str]:
    exprs = i.get("exprs", {})
    return {f"t.{e}": d["collation"] for e, d in exprs.items() if d.get("collation")}


def _non_nullable(i: Mapping[str, Any]) -> list[str]:
    exprs = [f"t.{e}" for e, d in i.get("exprs", {}).items() if not d["nullable"]]
    opaque = [f"t.{n}" for n, s in i["atoms"].items() if s[0] == "opaque" and not s[1]]
    return exprs + opaque


def _decide(case: Case) -> Any:
    i = case.input
    atoms = {n: _atom(s, n) for n, s in i["atoms"].items()}
    f = _tree(i["formula"], atoms)
    g = _tree(i["other"], atoms) if "other" in i else None
    bdd = _bdd(_non_nullable(i), _collations(i))
    u = bdd.compile(f)
    q = i["question"]
    if q == "tautology":
        return bdd.is_tautology(u)
    if q == "logical_sat":
        return bdd.is_satisfiable(u)
    v = bdd.compile(g)
    if q == "implies":
        return bdd.implies(u, v)
    return bdd.equivalent(u, v)


def _value(bdd: DGMPredicateBDD, uid: int, env: Mapping[int, bool]) -> bool:
    while uid not in (FALSE_NODE_ID, TRUE_NODE_ID):
        node = bdd.manager.get_node(uid)
        uid = node.high if env[node.var] else node.low
    return uid == TRUE_NODE_ID


def _with_vars(names: Sequence[str]) -> tuple[DGMPredicateBDD, dict[str, Any]]:
    """A BDD whose variables 0…n-1 are the atoms *names*, in order."""
    bdd = _bdd([f"t.{n}" for n in names])  # two-valued, as §8.2/§8.3 read them
    atoms = {n: ScalarPred(PropRef(f"t.{n}"), "=", True) for n in names}
    for n in names:
        bdd.compile(atoms[n])
    return bdd, atoms


def _completeness(case: Case) -> Any:
    bdd, atoms = _with_vars(("p", "q"))
    u = bdd.compile(_tree(completeness_tree(case.input["bits"]), atoms))
    table = tuple(
        _value(bdd, u, dict(enumerate(bits)))
        for bits in product([False, True], repeat=2)
    )
    return table == tuple(case.input["bits"])


def _cubes(case: Case) -> Any:
    bdd, atoms = _with_vars(("p", "q", "r"))
    u = bdd.compile(_tree(case.input["formula"], atoms))
    cubes = bdd.minterms(u)
    for a, b in product(range(len(cubes)), repeat=2):
        if a < b and all(cubes[a][v] == cubes[b][v] for v in cubes[a] if v in cubes[b]):
            return "OVERLAPPING CUBES"
    covered = {
        bits
        for c in cubes
        for bits in product([False, True], repeat=3)
        if all(bits[v] == val for v, val in c.items())
    }
    return sorted(covered)


_MODES = {
    "GLOBALLY": GLOBALLY,
    "EVENTUALLY": EVENTUALLY,
    "NEXT": NEXT,
    "PREVIOUSLY": PREVIOUSLY,
}


def _sub(sub: Any, phi: Any) -> str:
    if sub is phi:
        return "φ"
    if isinstance(sub, NOT) and sub.pred is phi:
        return "NOT(φ)"
    if isinstance(sub, NotTrue) and sub.pred is phi:
        return "NotTrue(φ)"
    if isinstance(sub, RawPred) and sub.to_sql().upper() == "TRUE":
        return "TRUE"
    return repr(sub)


def _forall_elim(case: Case) -> Any:
    """The literals the BDD holds for ``PathPred(FORALL, mode, φ)``."""
    phi = ScalarPred(PropRef("fs.revenue"), ">", 500)
    hop: Any = VerbHop("c", "placed", "fs", table="t_sale", on="fs.placed_src = c.id")
    if case.input["mode"] == "PREVIOUSLY":  # a past mode needs a time-ordered path
        hop = BridgeHop("c", "next", "fs", table="t_sale", on="fs.prev_id = c.id")
    pp = PathPred("c", hop, phi, quantifier=Quantifier.FORALL,
                  temporal_mode=_MODES[case.input["mode"]])  # fmt: skip
    bdd = _bdd()
    uid = bdd.compile(pp)
    lits = []
    while uid not in (FALSE_NODE_ID, TRUE_NODE_ID):  # one path: a conjunction
        node = bdd.manager.get_node(uid)
        if node.low != FALSE_NODE_ID and node.high != FALSE_NODE_ID:
            return "NOT A CONJUNCTION OF LITERALS"
        positive = node.low == FALSE_NODE_ID
        atom = bdd._var_to_atom[node.var]  # the atom FORALL elimination built
        quant = atom.quantifier.value if atom.quantifier else "EXISTS"
        mode = atom.temporal_mode.__name__ if atom.temporal_mode else "-"
        lits.append(("+" if positive else "-", quant, mode, _sub(atom.sub_filter, phi)))
        uid = node.high if positive else node.low
    return sorted(lits)


_RUN: dict[str, Callable[[Case], Any]] = {
    "bdd": _decide,
    "completeness": _completeness,
    "cubes": _cubes,
    "forall_elim": _forall_elim,
}

BACKEND = Backend(
    "dgm-bdd",
    "DGMPredicateBDD (sqldim.core.query.dgm.bdd)",
    frozenset(_RUN),
    lambda case: _RUN[case.area](case),
)
