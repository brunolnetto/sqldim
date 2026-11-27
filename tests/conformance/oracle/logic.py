"""What the §6.1 BDD must decide, by direct evaluation over the atoms' meaning.

§6.1 states that the BDD with the encoding ``T⁺``/``T⁻`` and the axioms
``Δ = Δ_excl ∧ Δ_null ∧ Δ_ord`` is sound for Kleene logic, complete for
propositional reasoning and complete for comparisons of one expression with
constants.  Within that scope its verdicts therefore equal the semantic ones
computed here by enumerating every assignment of values to the expressions
and of truth values to opaque atoms:

* an expression ranges over a dense ordered domain — one value below, at,
  between and above the constants compared with it — plus NULL when nullable
  (so integer discreteness is not recognised, as §6.1 says);
* an atom comparing two expressions is opaque: free TRUE/FALSE, and UNKNOWN
  exactly when one of its expressions is NULL (``Δ_null``);
* an opaque atom with no expression is free over TRUE/FALSE (and UNKNOWN if
  declared nullable);
* a String expression compares by equality whatever its collation (§6.1):
  it ranges over its constants plus one value equal to none of them.  Its
  order comparisons are decided only under a declared collation — ``binary``,
  by code point, where the constants are ranked in that order and the
  expression ranges over a dense domain as above — and are otherwise opaque:
  free TRUE/FALSE, UNKNOWN exactly when the expression is NULL.

Formulas are data: ``("atom", name)``, ``("not", f)``, ``("and", f, ...)``,
``("or", f, ...)``, ``("nottrue", f)``.  Atoms: ``("cmp", e, op, c)``,
``("isnull", e)``, ``("cmp2", e, op, e2)``, ``("opaque", nullable)``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from itertools import product
from typing import Any

from tests.conformance.oracle import Undefined
from tests.conformance.oracle.kleene import K, and_, compare, is_true, not_, or_


_EQ = ("=", "!=")
_OTHER = object()  # a String equal to none of the constants


def _consts(expr: str, atoms: Mapping[str, Sequence]) -> list:
    return sorted({a[3] for a in atoms.values() if a[0] == "cmp" and a[1] == expr})


def _string(expr: str, atoms: Mapping[str, Sequence]) -> bool:
    return any(isinstance(c, str) for c in _consts(expr, atoms))


def _collated(expr: str, exprs: Mapping[str, Any]) -> bool:
    collation = exprs.get(expr, {}).get("collation")
    if collation not in (None, "binary"):
        raise Undefined(f"collation {collation!r}: its order is not defined here")
    return collation == "binary"


def _domain(expr: str, atoms: Mapping[str, Sequence], nullable: bool, exprs) -> list:
    consts = _consts(expr, atoms)
    if _string(expr, atoms):
        if not _collated(expr, exprs):
            return consts + [_OTHER] + ([None] if nullable else [])
        consts = list(range(len(consts)))  # ranks, in code point order
    vals: list = []
    if consts:
        vals.append(consts[0] - 1)
        for a, b in zip(consts, consts[1:]):
            vals += [a, (a + b) / 2]
        vals += [consts[-1], consts[-1] + 1]
    else:
        vals = [0]
    return vals + ([None] if nullable else [])


def assignments(case_input: Mapping[str, Any]) -> Iterator[dict[str, K]]:
    """Every consistent assignment of truth values to the atoms."""
    atoms, exprs = case_input["atoms"], case_input.get("exprs", {})
    names = sorted(exprs)
    domains = [_domain(e, atoms, exprs[e].get("nullable", False), exprs) for e in names]
    opaque = {a for a in atoms if _opaque_order(atoms[a], atoms, exprs)}
    free = [
        a for a in sorted(atoms) if atoms[a][0] in ("cmp2", "opaque") or a in opaque
    ]
    for values in product(*domains):
        env = dict(zip(names, values))
        fixed: dict[str, K] = {}
        choices: list[list[K]] = []
        for name in sorted(atoms):
            atom = atoms[name]
            if name in opaque:
                choices.append([None] if env[atom[1]] is None else [True, False])
            elif atom[0] == "cmp":
                fixed[name] = _cmp(env[atom[1]], atom, atoms, exprs)
            elif atom[0] == "isnull":
                fixed[name] = env[atom[1]] is None
            elif atom[0] == "cmp2":
                null = env[atom[1]] is None or env[atom[3]] is None
                choices.append([None] if null else [True, False])
            elif atom[0] == "opaque":
                choices.append([True, False, None] if atom[1] else [True, False])
            else:
                raise Undefined(f"unknown atom form {atom[0]!r}")
        for picks in product(*choices):
            yield {**fixed, **dict(zip(free, picks))}


def _opaque_order(atom: Sequence, atoms, exprs) -> bool:
    """An order comparison on a String with no declared collation."""
    return (
        atom[0] == "cmp"
        and atom[2] not in _EQ
        and _string(atom[1], atoms)
        and not _collated(atom[1], exprs)
    )


def _cmp(v: Any, atom: Sequence, atoms, exprs) -> K:
    e, op, c = atom[1], atom[2], atom[3]
    if v is None:
        return None
    if not _string(e, atoms):
        return compare(v, op, c)
    if not _collated(e, exprs):  # equality only; _OTHER equals no constant
        return (v == c) if op == "=" else (v != c)
    return compare(v, op, _consts(e, atoms).index(c))


def value(f: Sequence, env: Mapping[str, K]) -> K:
    tag = f[0]
    if tag == "atom":
        return env[f[1]]
    if tag == "not":
        return not_(value(f[1], env))
    if tag == "and":
        return and_(value(g, env) for g in f[1:])
    if tag == "or":
        return or_(value(g, env) for g in f[1:])
    if tag == "nottrue":
        return not is_true(value(f[1], env))
    raise Undefined(f"unknown formula form {tag!r}")


def decide(case_input: Mapping[str, Any]) -> bool:
    """The §6.1 property ``question`` of the case's formula(s)."""
    q, f = case_input["question"], case_input["formula"]
    envs = list(assignments(case_input))
    if q == "tautology":
        return all(is_true(value(f, e)) for e in envs)
    if q == "logical_sat":
        return any(is_true(value(f, e)) for e in envs)
    g = case_input["other"]
    if q == "implies":
        return all(is_true(value(g, e)) for e in envs if is_true(value(f, e)))
    if q == "equivalent":
        return all(is_true(value(f, e)) == is_true(value(g, e)) for e in envs)
    raise Undefined(f"unknown question {q!r}")


def truth_table(f: Sequence, names: Sequence[str]) -> tuple[bool, ...]:
    """Two-valued truth table of *f* over non-nullable opaque atoms (§8.2, §8.3)."""
    return tuple(
        bool(value(f, dict(zip(names, bits))))
        for bits in product([False, True], repeat=len(names))
    )
