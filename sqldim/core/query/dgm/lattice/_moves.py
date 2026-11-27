"""Structural local moves of §8.16.6 — the edges of the move graph (ADR 0007).

Four kinds, each with an inverse of the same kind, so the graph is
undirected:

* **Where atom** — add an atom ``x`` not in the support of ``W ≠ FALSE``
  (``W ∧ x``), or remove an atom that is a conjunct of ``W`` (``W|x=1``,
  when ``W|x=0 = FALSE``);
* **GroupBy key** — replace one key by a key not in GroupBy;
* **hop** — replace one hop of one PathJoin by another hop of ``G_𝒜``;
* **AggFn** — replace the function of one aggregate.

A replaced key or aggregate is replaced wherever Band 2 and Band 3 refer to
it.  Only results that are members of ``Q_valid(𝒜)`` are neighbours.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Sequence
from typing import Any

from sqldim.core.query.dgm.analytical_space import AGG_FNS
from sqldim.core.query.dgm.lattice import _bdd
from sqldim.core.query.dgm.lattice._context import (
    MIRROR,
    JoinCtx,
    join_ctx,
    Term,
    enc,
    sort_terms,
)
from sqldim.core.query.dgm.lattice._skeleton import (
    Filter,
    Skeleton,
    check,
    compress,
    expand,
)
from sqldim.core.query.dgm.lattice._space import Hop, Space

__all__ = ["neighbours"]


def neighbours(space: Space, sk: Skeleton) -> Iterator[Skeleton]:
    """Skeletons one structural move away, in move order, each once."""
    ctx = join_ctx(space, sk.join)
    seen: set[Skeleton] = {sk}
    moves = (
        _where_moves(ctx, sk),
        _key_moves(ctx, sk),
        _hop_moves(space, sk),
        _agg_moves(ctx, sk),
    )
    for kind in moves:
        for cand in kind:
            if cand is not None and cand not in seen and not check(space, cand):
                seen.add(cand)
                yield cand


def _where_moves(ctx: JoinCtx, sk: Skeleton) -> Iterator[Skeleton]:
    universe = ctx.where_universe
    f = expand(sk.where, universe)
    used = _bdd.support(f)
    for i in range(len(universe)):
        if i not in used:
            if f is False:
                continue
            g = _bdd.conjoin_var(f, i)
        elif _bdd.restrict(f, i, False) is False:
            g = _bdd.restrict(f, i, True)
        else:
            continue
        yield dataclasses.replace(sk, where=compress(universe, g))


def _key_moves(ctx: JoinCtx, sk: Skeleton) -> Iterator[Skeleton | None]:
    if sk.band2 is None:
        return
    group_by = sk.band2[0]
    for old in group_by:
        for new in ctx.group_keys:
            if new not in group_by:
                yield _substitute(ctx, sk, old, new)


def _all_hops(space: Space) -> list[Hop]:
    return sorted(
        (e.source, e.label, e.target, d) for e in space.edges for d in ("+", "-")
    )


def _hop_moves(space: Space, sk: Skeleton) -> Iterator[Skeleton]:
    alternatives = _all_hops(space)
    for t, (start, hops) in enumerate(sk.join):
        for j, hop in enumerate(hops):
            for alt in alternatives:
                if alt != hop:
                    new = (*hops[:j], alt, *hops[j + 1 :])
                    join = (*sk.join[:t], (start, new), *sk.join[t + 1 :])
                    yield dataclasses.replace(sk, join=join)


def _agg_moves(ctx: JoinCtx, sk: Skeleton) -> Iterator[Skeleton | None]:
    aggs = sk.aggregates
    for old in aggs:
        if old[2][0] == "node":
            continue
        for fn in AGG_FNS:
            new = ("agg", fn, old[2])
            if fn != old[1] and new not in aggs:
                yield _substitute(ctx, sk, old, new)


# ---------------------------------------------------------------------------
# Substitution and re-canonicalisation
# ---------------------------------------------------------------------------


def _replace(value: Any, old: Term, new: Term) -> Any:
    if value == old:
        return new
    if isinstance(value, tuple):
        return tuple(_replace(v, old, new) for v in value)
    return value


def _canon_ref(ref: Term) -> Term:
    if ref[0] == "win":
        return ("win", ref[1], sort_terms(ref[2]), ref[3])
    return ref


def _canon_atom(ctx: JoinCtx, atom: Term) -> Term:
    if atom[0] == "path":
        return atom
    lhs, op, rhs = _canon_ref(atom[1]), atom[2], atom[3]
    if rhs[0] == "param":
        return ("cmp", lhs, op, ("param", ctx.type_of(lhs)))
    rhs = _canon_ref(rhs)
    if enc(lhs) > enc(rhs):
        return ("cmp", rhs, MIRROR[op], lhs)
    return ("cmp", lhs, op, rhs)


def _refilter(ctx: JoinCtx, flt: Filter, old: Term, new: Term) -> Filter | None:
    atoms = [_canon_atom(ctx, _replace(a, old, new)) for a in flt[0]]
    keys = [enc(a) for a in atoms]
    if len(set(keys)) != len(keys):
        return None
    order = sorted(range(len(atoms)), key=lambda i: keys[i])
    position = [0] * len(atoms)
    for new_pos, old_pos in enumerate(order):
        position[old_pos] = new_pos
    return tuple(atoms[i] for i in order), _bdd.remap(flt[1], position)


def _canon_set(terms: Sequence[Term]) -> tuple[Term, ...] | None:
    out = sort_terms(_canon_ref(t) for t in terms)
    return out if len(set(out)) == len(out) else None


def _substitute(ctx: JoinCtx, sk: Skeleton, old: Term, new: Term) -> Skeleton | None:
    """*sk* with *old* replaced by *new* in Band 2 and Band 3, canonical again."""
    assert sk.band2 is not None
    group_by, aggs, having = sk.band2
    group_by2 = _canon_set(_replace(group_by, old, new))
    aggs2 = _canon_set(_replace(aggs, old, new))
    having2 = _refilter(ctx, having, old, new)
    if group_by2 is None or aggs2 is None or having2 is None:
        return None
    band3 = None
    if sk.band3 is not None:
        windows2 = _canon_set(_replace(sk.band3[0], old, new))
        qualify2 = _refilter(ctx, sk.band3[1], old, new)
        if windows2 is None or qualify2 is None:
            return None
        band3 = (windows2, qualify2)
    return dataclasses.replace(sk, band2=(group_by2, aggs2, having2), band3=band3)
