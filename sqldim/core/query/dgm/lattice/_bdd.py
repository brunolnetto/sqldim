"""Reduced ordered BDDs as immutable values (§6.1, §8.3, §8.14).

A Boolean function over variables ``0 … n-1`` is ``False``, ``True`` or a
node ``(var, low, high)`` whose children are functions over larger
variables and differ from each other.  Reduced and ordered, the
representation is canonical: two values are equal iff they denote the same
function, which is what QH6 of §8.11 needs.  Values are plain tuples, so a
skeleton holding one is hashable, comparable and serialisable as is.

:func:`functions_of_size` lists every function with exactly *t* internal
nodes, in a fixed order — the Where, Having and Qualify loops of the
enumeration (§8.11 (c)–(e)).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import Union

from sqldim.core.query.dgm.planner._ir_identity import (
    bdd_node_hash,
    bdd_terminal_hash,
)

__all__ = [
    "BDD",
    "conjoin_var",
    "functions_of_size",
    "is_canonical",
    "max_size",
    "merkle",
    "relabel",
    "remap",
    "render",
    "restrict",
    "size",
    "support",
]

BDD = Union[bool, tuple[int, "BDD", "BDD"]]


def _node(var: int, low: BDD, high: BDD) -> BDD:
    return low if low == high else (var, low, high)


def _top(f: BDD) -> int | None:
    return None if isinstance(f, bool) else f[0]


def _nodes(f: BDD, acc: set[tuple[int, BDD, BDD]]) -> None:
    if isinstance(f, bool) or f in acc:
        return
    acc.add(f)
    _nodes(f[1], acc)
    _nodes(f[2], acc)


def size(f: BDD) -> int:
    """Number of internal nodes (shared subgraphs counted once)."""
    acc: set[tuple[int, BDD, BDD]] = set()
    _nodes(f, acc)
    return len(acc)


def _union_size(a: BDD, b: BDD) -> int:
    acc: set[tuple[int, BDD, BDD]] = set()
    _nodes(a, acc)
    _nodes(b, acc)
    return len(acc)


def support(f: BDD) -> frozenset[int]:
    acc: set[tuple[int, BDD, BDD]] = set()
    _nodes(f, acc)
    return frozenset(n[0] for n in acc)


def is_canonical(f: BDD, n: int, *, lo: int = 0) -> bool:
    """Reduced, ordered and over variables ``lo … n-1`` only."""
    if isinstance(f, bool):
        return True
    if not (isinstance(f, tuple) and len(f) == 3 and isinstance(f[0], int)):
        return False
    var, low, high = f
    if isinstance(var, bool) or not lo <= var < n or low == high:
        return False
    return is_canonical(low, n, lo=var + 1) and is_canonical(high, n, lo=var + 1)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _cofactors(f: BDD, var: int) -> tuple[BDD, BDD]:
    if isinstance(f, bool) or f[0] != var:
        return f, f
    return f[1], f[2]


def _and(u: BDD, v: BDD, memo: dict[tuple[BDD, BDD], BDD]) -> BDD:
    if u is False or v is False:
        return False
    if u is True:
        return v
    if v is True or u == v:
        return u
    key = (u, v)
    if key not in memo:
        var = min(x for x in (_top(u), _top(v)) if x is not None)
        u0, u1 = _cofactors(u, var)
        v0, v1 = _cofactors(v, var)
        memo[key] = _node(var, _and(u0, v0, memo), _and(u1, v1, memo))
    return memo[key]


def _not(f: BDD) -> BDD:
    if isinstance(f, bool):
        return not f
    return (f[0], _not(f[1]), _not(f[2]))


def _or(u: BDD, v: BDD) -> BDD:
    return _not(_and(_not(u), _not(v), {}))


def conjoin_var(f: BDD, var: int) -> BDD:
    """``f ∧ x_var``."""
    return _and(f, (var, False, True), {})


def restrict(f: BDD, var: int, value: bool) -> BDD:
    """``f`` with ``x_var`` fixed to *value*."""
    if isinstance(f, bool) or f[0] > var:
        return f
    if f[0] == var:
        return f[2] if value else f[1]
    return _node(f[0], restrict(f[1], var, value), restrict(f[2], var, value))


def remap(f: BDD, mapping: Sequence[int]) -> BDD:
    """Rename variable ``i`` to ``mapping[i]`` (any injective renaming)."""
    memo: dict[BDD, BDD] = {}

    def go(g: BDD) -> BDD:
        if isinstance(g, bool):
            return g
        if g not in memo:
            var = mapping[g[0]]
            pos = _and((var, False, True), go(g[2]), {})
            neg = _and((var, True, False), go(g[1]), {})
            memo[g] = _or(pos, neg)
        return memo[g]

    return go(f)


# ---------------------------------------------------------------------------
# Enumeration by size
# ---------------------------------------------------------------------------


def _level_bound(i: int, rest: int) -> int:
    """At most ``min(2^i, 2^(2^r) - 2^(2^(r-1)))`` nodes test variable ``i``."""
    if (1 << rest) > i + 1:  # the second term exceeds 2^i
        return 1 << i
    return min(1 << i, (1 << (1 << rest)) - (1 << (1 << (rest - 1))))


_HUGE = 1 << 62


def max_size(n: int) -> int:
    """An upper bound on the size of a function of *n* variables.

    Exact level bound for ``n ≤ 60``; beyond that, a value no enumeration
    within the policy limits can reach.
    """
    if n > 60:
        return _HUGE
    return sum(_level_bound(i, n - i) for i in range(n))


def functions_of_size(t: int, n: int, lo: int = 0) -> Iterator[BDD]:
    """Every function over ``lo … n-1`` with exactly *t* nodes, once each.

    Order: ``True``, ``False`` for ``t = 0``; otherwise by root variable,
    then by the size of the low child and its position in this order, then
    the same for the high child.
    """
    if t == 0:
        yield True
        yield False
        return
    if t > max_size(n - lo):
        return
    for var in range(lo, n):
        for low, high in _children(t - 1, n, var + 1):
            yield (var, low, high)


def _children(t: int, n: int, lo: int) -> Iterator[tuple[BDD, BDD]]:
    for a in range(t + 1):
        for low in functions_of_size(a, n, lo):
            for b in range(max(t - a, 0), t + 1):
                yield from _highs(low, b, t, n, lo)


def _highs(low: BDD, b: int, t: int, n: int, lo: int) -> Iterator[tuple[BDD, BDD]]:
    for high in functions_of_size(b, n, lo):
        if high != low and _union_size(low, high) == t:
            yield low, high


# ---------------------------------------------------------------------------
# Identity and rendering
# ---------------------------------------------------------------------------


def merkle(f: BDD, atoms: Sequence[bytes]) -> str:
    """``predicate_hash`` of §8.14: ``H(atom, hash(low), hash(high))``."""
    memo: dict[BDD, str] = {}

    def go(g: BDD) -> str:
        if isinstance(g, bool):
            return bdd_terminal_hash(g)
        if g not in memo:
            memo[g] = bdd_node_hash(atoms[g[0]], go(g[1]), go(g[2]))
        return memo[g]

    return go(f)


def render(f: BDD, name: Callable[[int], str]) -> str:
    """A readable formula (Shannon form, simplified at terminals)."""
    if isinstance(f, bool):
        return "TRUE" if f else "FALSE"
    var, low, high = f
    x = name(var)
    if (low, high) == (False, True):
        return x
    if (low, high) == (True, False):
        return f"NOT {x}"
    return _render_branches(x, render(low, name), render(high, name), low, high)


def _render_branches(x: str, lo: str, hi: str, low: BDD, high: BDD) -> str:
    if low is False:
        return f"({x} AND {hi})"
    if high is False:
        return f"(NOT {x} AND {lo})"
    if low is True:
        return f"(NOT {x} OR {hi})"
    if high is True:
        return f"({x} OR {lo})"
    return f"(({x} AND {hi}) OR (NOT {x} AND {lo}))"


def relabel(f: BDD, mapping: Sequence[int]) -> BDD:
    """Rename variables by an order-preserving *mapping* (no re-ordering)."""
    if isinstance(f, bool):
        return f
    return (mapping[f[0]], relabel(f[1], mapping), relabel(f[2], mapping))
