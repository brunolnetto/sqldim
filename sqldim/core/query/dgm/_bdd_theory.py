"""Canonical atoms and the axioms Δ of the §6.1 BDD.

A predicate is first rewritten into a formula over **basis atoms**, each
identified by its canonical form (never by Python object identity):

==========================  ===================================================
``("L", e, c)``             ``e < c``  — an expression compared with a constant
``("E", e, c)``             ``e = c``
``("lt", a, b)``            ``a < b``  — two expressions (outside the theory)
``("eq", a, b)``            ``a = b``  with ``a ≤ b`` in canonical order
``("isnull", e)``           ``e IS NULL`` (two-valued)
``("cmp", e, op, v)``       any other comparison, opaque
``("raw", sql)``            a raw SQL predicate, opaque
``("path", …)``             an EXISTS PathPred (two-valued)
``("sig", …)``              a SignaturePred (two-valued)
==========================  ===================================================

An expression is ``(Ref kind, SQL)``, so atoms over different Ref kinds are
never the same.  Comparisons with a constant are rewritten over L and E
(§6.1 *Basis*); the other forms are normalised as §6.1 *Variables* says.

:func:`delta_clauses` yields Δ = Δ_excl ∧ Δ_null ∧ Δ_ord as implications
over the literals ``(key, sign)``.  Δ_ord is instantiated for adjacent
constants of one expression.  It is stated for numbers, dates and
timestamps; a Bool admits the equality axioms only.  Strings get **no**
order or equality axiom: their order and equality depend on the backend's
collation, so no inference is made (sound, incomplete).
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable, Iterator
from typing import Any

__all__ = ["Formula", "canonical", "delta_clauses", "expr_key", "theory_group"]

Formula = tuple[Any, ...]
"""``("atom", key)``, ``("not", f)``, ``("and", f, …)``, ``("or", f, …)``,
``("nottrue", f)``, ``("const", bool)``."""

_SWAP = {"<": ">", ">": "<", "<=": ">=", ">=": "<=", "=": "=", "!=": "!=", "<>": "<>"}
_ORDER_OPS = frozenset(_SWAP)


def expr_key(ref: Any) -> tuple[str, str] | None:
    """``(Ref kind, SQL)`` of a Ref, or ``None`` for a literal."""
    to_sql = getattr(ref, "to_sql", None)
    if to_sql is None or isinstance(ref, (str, bytes)):
        return None
    return (type(ref).__name__, to_sql())


def theory_group(c: Any) -> str | None:
    """Which Δ_ord applies to a constant: ``ordered``, ``eq`` or none."""
    if isinstance(c, bool):
        return "eq"
    if isinstance(c, (int, float)):
        return "ordered:num"
    if isinstance(c, datetime.datetime):
        return "ordered:ts"
    if isinstance(c, datetime.date):
        return "ordered:date"
    return None


def _atom(key: tuple) -> Formula:
    return ("atom", key)


def _not(f: Formula) -> Formula:
    return ("not", f)


def _with_constant(e: tuple, op: str, c: Any) -> Formula:
    """§6.1 *Basis*: ``e op c`` over L(e,c) and E(e,c)."""
    if theory_group(c) is None:
        return _atom(("cmp", e, op, repr(c)))
    if theory_group(c) == "eq" and op not in ("=", "!=", "<>"):
        return _atom(("cmp", e, op, repr(c)))
    lt, eq = _atom(("L", e, c)), _atom(("E", e, c))
    table = {
        "<": lt,
        "=": eq,
        "<=": ("or", lt, eq),
        ">=": _not(lt),
        "!=": _not(eq),
        "<>": _not(eq),
        ">": ("and", _not(lt), _not(eq)),
    }
    return table[op]


def _two_exprs(a: tuple, op: str, b: tuple) -> Formula:
    """§6.1 *Variables*: ``a > b`` ↦ ``b < a``, ``a ≥ b`` ↦ NOT(a < b), …"""
    if op in ("=", "!=", "<>"):
        eq = _atom(("eq",) + tuple(sorted((a, b))))
        return eq if op == "=" else _not(eq)
    table = {
        "<": _atom(("lt", a, b)),
        ">": _atom(("lt", b, a)),
        ">=": _not(_atom(("lt", a, b))),
        "<=": _not(_atom(("lt", b, a))),
    }
    return table[op]


def _in(e: tuple, values: Iterable[Any]) -> Formula:
    return ("or",) + tuple(_with_constant(e, "=", c) for c in values)


def _null_test(e: tuple, op_u: str) -> Formula:
    f = _atom(("isnull", e))
    return f if op_u == "IS" else _not(f)


def _in_test(e: tuple, op_u: str, values: Any) -> Formula:
    f = _in(e, sorted(values, key=repr))
    return f if op_u == "IN" else _not(f)


def scalar(ref: Any, op: str, value: Any) -> Formula:
    """The canonical formula of ``ScalarPred(ref, op, value)``."""
    op_u = str(op).upper()
    e = expr_key(ref)
    if e is None:
        return _atom(("cmp", repr(ref), op_u, repr(value)))
    if op_u in ("IS", "IS NOT") and value is None:
        return _null_test(e, op_u)
    if op_u in ("IN", "NOT IN") and isinstance(value, (list, tuple, set, frozenset)):
        return _in_test(e, op_u, value)
    other = expr_key(value)
    if op_u not in _ORDER_OPS:
        return _atom(("cmp", e, op_u, repr(value) if other is None else other))
    if other is not None:
        return _two_exprs(e, op_u, other)
    return _with_constant(e, op_u, value)


def canonical(pred: Any) -> Formula | None:
    """The formula of a ScalarPred or RawPred; ``None`` for other atoms."""
    from sqldim.core.query.dgm.preds import RawPred, ScalarPred

    if isinstance(pred, ScalarPred):
        if isinstance(pred.ref, (int, float, str)) and expr_key(pred.value):
            # a constant on the left is moved to the right first
            return scalar(pred.value, _SWAP.get(pred.op, pred.op), pred.ref)
        return scalar(pred.ref, pred.op, pred.value)
    if isinstance(pred, RawPred):
        text = " ".join(pred.to_sql().split())
        if text.upper() in ("TRUE", "FALSE"):
            return ("const", text.upper() == "TRUE")
        return _atom(("raw", text))
    return None


def exprs_of(key: tuple) -> tuple[tuple, ...]:
    """The expressions an atom speaks of (for Δ_null and nullability)."""
    kind = key[0]
    if kind in ("L", "E", "isnull", "cmp"):
        return (key[1],) if isinstance(key[1], tuple) else ()
    if kind in ("lt", "eq"):
        return (key[1], key[2])
    return ()


# ---------------------------------------------------------------------------
# Δ
# ---------------------------------------------------------------------------

Literal = tuple[tuple, bool]
"""``(key, True)`` is ``key⁺`` ("TRUE"), ``(key, False)`` is ``key⁻``."""
Clause = tuple[str, tuple[Literal, ...], tuple[Literal, ...]]
"""``("imp", premises, conclusions)``: ∧ premises → ∧ conclusions;
``("excl", (a, b), ())``: ¬(a ∧ b); ``("iff_def", (a, b), ())``: the atoms
of a and b are defined (TRUE or FALSE) together; ``("undef", (p,), (a,))``:
p → the atom of a is UNKNOWN (¬a⁺ ∧ ¬a⁻)."""


def _excl(keys: Iterable[tuple], nullable: Any) -> Iterator[Clause]:
    for k in keys:
        if nullable(k):
            yield ("excl", ((k, True), (k, False)), ())


def _null(keys: Iterable[tuple], nullable: Any) -> Iterator[Clause]:
    keys = list(keys)
    for k in keys:
        if k[0] != "isnull":
            continue
        for a in keys:
            if nullable(a) and k[1] in exprs_of(a):
                yield ("undef", ((k, True),), ((a, True),))


def _by_expression(keys: Iterable[tuple]) -> dict[tuple, dict[str, list]]:
    out: dict[tuple, dict[str, list]] = {}
    for k in keys:
        if k[0] in ("L", "E"):
            group = theory_group(k[2])
            assert group is not None
            consts = out.setdefault(k[1], {}).setdefault(group, [])
            if k[2] not in consts:
                consts.append(k[2])
    return out


def _ordered(e: tuple, consts: list, nullable: bool) -> Iterator[Clause]:
    cs = sorted(consts)
    for c in cs:  # O5, and O6 between L(e,c) and E(e,c)
        yield ("imp", ((("E", e, c), True),), ((("L", e, c), False),))
        if nullable:
            yield ("iff_def", ((("L", e, c), True), (("E", e, c), True)), ())
    for c, d in zip(cs, cs[1:]):
        lc, ld, ec = ("L", e, c), ("L", e, d), ("E", e, c)
        yield ("imp", ((lc, True),), ((ld, True),))  # O1
        yield ("imp", ((ld, False),), ((lc, False),))  # O2
        yield ("imp", ((ec, True),), ((ld, True),))  # O3
        yield ("imp", ((ld, False),), ((ec, False),))  # O4
        if nullable:
            yield ("iff_def", ((lc, True), (ld, True)), ())  # O6


def _equality(e: tuple, consts: list, nullable: bool) -> Iterator[Clause]:
    cs = sorted(consts, key=repr)
    for i, c in enumerate(cs):
        for d in cs[i + 1 :]:
            yield ("imp", ((("E", e, c), True),), ((("E", e, d), False),))
            yield ("imp", ((("E", e, d), True),), ((("E", e, c), False),))
    if nullable:
        for c, d in zip(cs, cs[1:]):
            yield ("iff_def", ((("E", e, c), True), (("E", e, d), True)), ())


def delta_clauses(
    keys: Iterable[tuple], nullable: Any, expr_nullable: Any
) -> Iterator[Clause]:
    """Δ_excl, Δ_null and Δ_ord over the registered atoms *keys*."""
    keys = list(keys)
    yield from _excl(keys, nullable)
    yield from _null(keys, nullable)
    for e, groups in _by_expression(keys).items():
        n = expr_nullable(e)
        for group, consts in groups.items():
            if group.startswith("ordered"):
                yield from _ordered(e, consts, n)
            else:
                yield from _equality(e, consts, n)


def basis_companions(key: tuple) -> tuple[tuple, ...]:
    """Atoms registered together with *key*: L and E of one ordered constant,
    so that Δ_ord chains through every constant compared with ``e``."""
    if key[0] in ("L", "E") and str(theory_group(key[2])).startswith("ordered"):
        return (("L", key[1], key[2]), ("E", key[1], key[2]))
    return (key,)
