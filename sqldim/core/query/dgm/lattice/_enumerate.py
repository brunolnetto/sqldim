"""The bounded enumeration of ``Q_valid(𝒜)`` (§8.11 theorem, part 1; §12.3).

Order (the deterministic order of §12.3, *question lattice*):

1. Joins by total number of hops, then by the hop counts of the PathJoins,
   then by start alias in binding order and hop sequence in hop order.
2. Within a Join, by grade ``|Where| + |B₂| + |B₃|`` — BDD nodes, plus keys,
   aggregates and Having nodes, plus window sizes and Qualify nodes;
3. within a grade, by the split ``(|B₃|, |B₂|)`` ascending, and then in
   nested loops Where → Band 2 → Band 3, each component in its own fixed
   order (:func:`_bdd.functions_of_size` for the three filters).

Every component is drawn lazily, so a prefix costs only what it emits.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from itertools import combinations, islice
from typing import TypeVar

from sqldim.core.query.dgm.lattice import _bdd
from sqldim.core.query.dgm.lattice._context import (
    JoinCtx,
    PathJoin,
    Term,
    anchor_alias,
    sort_terms,
    win_size,
)
from sqldim.core.query.dgm.lattice._skeleton import (
    Band2,
    Band3,
    Skeleton,
    compress,
)
from sqldim.core.query.dgm.lattice._space import Hop, Space

__all__ = ["first", "joins", "patterns", "skeletons"]

_T = TypeVar("_T")


def first(items: Iterator[_T], limit: int) -> tuple[list[_T], bool]:
    """The first *limit* items and whether nothing follows them."""
    head = list(islice(items, limit))
    return head, next(items, _END) is _END


_END = object()


# ---------------------------------------------------------------------------
# (a) Joins
# ---------------------------------------------------------------------------


def _compositions(total: int, parts: int, h: int) -> Iterator[tuple[int, ...]]:
    if parts == 0:
        if total == 0:
            yield ()
        return
    for first_part in range(1, h + 1):
        rest = total - first_part
        if parts - 1 <= rest <= (parts - 1) * h:
            for tail in _compositions(rest, parts - 1, h):
                yield (first_part, *tail)


def patterns(
    space: Space, cur: str, target: str, left: int
) -> Iterator[tuple[Hop, ...]]:
    """Hop sequences of exactly *left* hops from *cur* to *target* in ``G_𝒜``."""
    if left == 0:
        if cur == target:
            yield ()
        return
    for hop in space.hops.get(cur, ()):
        nxt = hop[2] if hop[3] == "+" else hop[0]
        if space.dist[nxt].get(target, left) <= left - 1:
            for rest in patterns(space, nxt, target, left - 1):
                yield (hop, *rest)


def _bound(t: int, start_type: str, hops: tuple[Hop, ...]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    cur = start_type
    for j, hop in enumerate(hops):
        cur = hop[2] if hop[3] == "+" else hop[0]
        last = j == len(hops) - 1
        out.append((anchor_alias(t + 1) if last else f"p{t + 2}_{j + 1}", cur))
    return out


def _extend(
    space: Space,
    acc: tuple[PathJoin, ...],
    bound: list[tuple[str, str]],
    lengths: tuple[int, ...],
) -> Iterator[tuple[PathJoin, ...]]:
    t = len(acc)
    if t == len(lengths):
        yield acc
        return
    target = space.anchors[t + 1].type
    for start, start_type in bound:
        for hops in patterns(space, start_type, target, lengths[t]):
            new = bound + _bound(t, start_type, hops)
            yield from _extend(space, (*acc, (start, hops)), new, lengths)


def joins(space: Space) -> Iterator[tuple[PathJoin, ...]]:
    """Every Join of the space, once, in Join order."""
    k = len(space.anchors)
    root = [(anchor_alias(0), space.anchors[0].type)]
    for total in range(k - 1, (k - 1) * space.h + 1):
        for lengths in _compositions(total, k - 1, space.h):
            yield from _extend(space, (), root, lengths)


# ---------------------------------------------------------------------------
# (c)–(e) Components by size
# ---------------------------------------------------------------------------


def _band2(ctx: JoinCtx, s: int) -> Iterator[Band2 | None]:
    if s == 0:
        yield None
        return
    keys, aggs = ctx.group_keys, ctx.agg_terms
    for g in range(min(s - 1, len(keys)) + 1):
        for a in range(1, min(s - g, len(aggs)) + 1):
            for group_by in combinations(keys, g):
                for chosen in combinations(aggs, a):
                    yield from _with_having(ctx, group_by, chosen, s - g - a)


def _with_having(
    ctx: JoinCtx, group_by: tuple[Term, ...], aggs: tuple[Term, ...], h: int
) -> Iterator[Band2]:
    universe = ctx.having_universe(aggs)
    for f in _bdd.functions_of_size(h, len(universe)):
        yield (group_by, aggs, compress(universe, f))


def _term_sets(terms: Sequence[Term], s: int, start: int) -> Iterator[tuple[Term, ...]]:
    """Sets of distinct terms (sorted by size) whose sizes sum to *s*."""
    for i in range(start, len(terms)):
        u = win_size(terms[i])
        if u > s:
            return
        if u == s:
            yield (terms[i],)
        else:
            for rest in _term_sets(terms, s - u, i + 1):
                yield (terms[i], *rest)


class _WindowCache:
    def __init__(self) -> None:
        self._terms: dict[tuple[tuple[Term, ...], int], list[Term]] = {}

    def upto(self, ctx: JoinCtx, scope: tuple[Term, ...], s: int) -> list[Term]:
        key = (scope, s)
        if key not in self._terms:
            self._terms[key] = [
                w for u in range(1, s + 1) for w in ctx.window_terms(scope, u)
            ]
        return self._terms[key]


def _band3(
    ctx: JoinCtx, b2: Band2 | None, s: int, cache: _WindowCache
) -> Iterator[Band3 | None]:
    if s == 0:
        yield None
        return
    scope = ctx.b3_scope(None if b2 is None else b2[0], () if b2 is None else b2[1])
    terms = cache.upto(ctx, scope, s)
    for sw in range(1, s + 1):
        for chosen in _term_sets(terms, sw, 0):
            windows = sort_terms(chosen)
            universe = ctx.qualify_universe(windows)
            for f in _bdd.functions_of_size(s - sw, len(universe)):
                yield (windows, compress(universe, f))


def _grade_bound(ctx: JoinCtx) -> int:
    """Largest grade a skeleton over this Join can have (an upper bound)."""
    where = _bdd.max_size(len(ctx.where_universe))
    aggs = len(ctx.agg_terms)
    having = _bdd.max_size(6 * aggs + 3 * aggs * (aggs - 1))
    band3 = _bdd.max_size(10**3) if aggs or ctx.prop_refs else 0
    return where + len(ctx.group_keys) + aggs + having + band3


def _graded(ctx: JoinCtx) -> Iterator[Skeleton]:
    space = ctx.space
    universe = ctx.where_universe
    cache = _WindowCache()
    for s in range(_grade_bound(ctx) + 1):
        for s3, s2 in _splits(s):
            for f in _bdd.functions_of_size(s - s2 - s3, len(universe)):
                where = compress(universe, f)
                for b2 in _band2(ctx, s2):
                    for b3 in _band3(ctx, b2, s3, cache):
                        yield Skeleton(space.anchor_names, ctx.join, where, b2, b3)


def _splits(s: int) -> Iterator[tuple[int, int]]:
    """``(|B₃|, |B₂|)`` for grade *s*: fewer bands first."""
    for s3 in range(s + 1):
        for s2 in range(s - s3 + 1):
            yield s3, s2


def skeletons(space: Space) -> Iterator[Skeleton]:
    """``Q_valid(𝒜)`` within the space's hop bound, in enumeration order."""
    for join in joins(space):
        yield from _graded(JoinCtx(space, join))
