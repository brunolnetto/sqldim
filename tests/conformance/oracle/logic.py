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
  declared nullable).

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


def _domain(expr: str, atoms: Mapping[str, Sequence], nullable: bool) -> list:
    consts = sorted({a[3] for a in atoms.values() if a[0] == "cmp" and a[1] == expr})
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
    domains = [_domain(e, atoms, exprs[e].get("nullable", False)) for e in names]
    free = [a for a in sorted(atoms) if atoms[a][0] in ("cmp2", "opaque")]
    for values in product(*domains):
        env = dict(zip(names, values))
        fixed: dict[str, K] = {}
        choices: list[list[K]] = []
        for name in sorted(atoms):
            atom = atoms[name]
            if atom[0] == "cmp":
                fixed[name] = compare(env[atom[1]], atom[2], atom[3])
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
