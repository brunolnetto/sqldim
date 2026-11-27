"""One Join and what is in scope of it (§4.1, §4.2, §5.2, §8.11 (b)–(e)).

Terms are tuples of strings, so their canonical encoding (§8.14) is defined
and their byte order is the canonical order:

==========================================  =====================================
``("prop", alias, key)``                    PropRef
``("node", alias)``                         NodeArg (argument of COUNT only)
``("agg", fn, arg)``                        aggregate ``fn(arg)``; AggRef in B₂/B₃
``("win", head, partition, order)``         window expression; WinRef in B₃
``("cmp", ref, op, ("param", type))``       Compare(ref, op, parameter slot)
``("cmp", ref, op, ref')``                  Compare of two Refs, ``ref < ref'``
``("path", i, mode)``                       PathPred(EXISTS) over the pattern of
                                            the i-th PathJoin, sub_filter TRUE
==========================================  =====================================

Window heads: ``("rank", fn)``, ``("offset", fn, ref)``, ``("running", fn,
arg)``, ``("stat", fn, arg | None)``.  An order is a tuple of ``(ref, dir)``.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from functools import cached_property, lru_cache
from itertools import combinations, permutations, product
from typing import Any

from sqldim.core.query.dgm.analytical_space import AGG_FNS, TEMPORAL_MODES
from sqldim.core.query.dgm.lattice._space import (
    NUMERIC,
    ORDERED,
    Hop,
    Space,
    compatible,
    ops_for,
)
from sqldim.core.query.dgm.planner._canon import canonical_encode

__all__ = [
    "InvalidSkeleton",
    "JoinCtx",
    "PathJoin",
    "Term",
    "enc",
    "join_ctx",
    "sort_terms",
]

Term = tuple[Any, ...]
PathJoin = tuple[str, tuple[Hop, ...]]

RANK_FNS = ("ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE")
OFFSET_FNS = ("LEAD", "LAG")
ORDER_STATS = ("PERCENT_RANK", "CUME_DIST")
PAST_MODES = frozenset({"SINCE", "ONCE", "PREVIOUSLY"})
DIRS = ("ASC", "DESC")
MIRROR = {"=": "=", "≠": "≠", "<": ">", ">": "<", "≤": "≥", "≥": "≤"}


class InvalidSkeleton(ValueError):
    """A component is not drawn from its finite set or does not type-check."""


@lru_cache(maxsize=1 << 16)
def enc(term: Any) -> bytes:
    return canonical_encode(term)


def sort_terms(terms: Iterable[Term]) -> tuple[Term, ...]:
    return tuple(sorted(terms, key=enc))


def anchor_alias(i: int) -> str:
    """Alias of the i-th anchor (0-based) in canonical order."""
    return f"a{i + 1}"


def agg_type(fn: str, arg: str | None) -> str | None:
    """Result type of ``fn(arg)`` (§4.2); ``arg=None`` is a NodeArg."""
    if fn == "COUNT":
        return "Int"
    if arg is None:
        return None
    if fn == "AVG":
        return "Real" if arg in NUMERIC else None
    if fn in ("SUM", "PROD"):
        return arg if arg in NUMERIC else None
    return arg if arg in ORDERED else None  # MIN, MAX


# ---------------------------------------------------------------------------
# The Join
# ---------------------------------------------------------------------------


def _walk(space: Space, join: Sequence[PathJoin]) -> list[tuple[str, str]]:
    """Aliases bound by *join*, in binding order, with their types."""
    if len(join) != len(space.anchors) - 1:
        raise InvalidSkeleton("a Join has one PathJoin per anchor after the first")
    bound = [(anchor_alias(0), space.anchors[0].type)]
    for t, (start, hops) in enumerate(join):
        types = dict(bound)
        if start not in types:
            raise InvalidSkeleton(f"PathJoin {t} starts at unbound alias {start}")
        if not 1 <= len(hops) <= space.h:
            raise InvalidSkeleton(f"PathJoin {t} has {len(hops)} hops, not 1…{space.h}")
        bound += _positions(space, t, types[start], hops)
    return bound


def _positions(space: Space, t: int, cur: str, hops: Sequence[Hop]):
    out: list[tuple[str, str]] = []
    target = space.anchors[t + 1].type
    for j, hop in enumerate(hops):
        if hop not in space.hops.get(cur, ()):
            raise InvalidSkeleton(f"hop {hop} does not leave {cur} within G_𝒜")
        cur = hop[2] if hop[3] == "+" else hop[0]
        last = j == len(hops) - 1
        out.append((anchor_alias(t + 1) if last else f"p{t + 2}_{j + 1}", cur))
    if cur != target:
        raise InvalidSkeleton(f"PathJoin {t} ends at {cur}, not {target}")
    return out


def _edges(join: Sequence[PathJoin]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for t, (start, hops) in enumerate(join):
        names = [start] + [f"p{t + 2}_{j + 1}" for j in range(len(hops) - 1)]
        names.append(anchor_alias(t + 1))
        out += list(zip(names, names[1:]))
    return out


def join_ctx(space: Space, join: Sequence[PathJoin]) -> "JoinCtx":
    """The (cached) context of *join*; raises :class:`InvalidSkeleton`."""
    key = tuple(join)
    cache = space.cache
    if key not in cache:
        if len(cache) > 4096:
            cache.clear()
        cache[key] = JoinCtx(space, key)
    return cache[key]


class JoinCtx:
    """Aliases, Refs, typing and Σ / DA rules for one Join over a :class:`Space`."""

    def __init__(self, space: Space, join: Sequence[PathJoin]) -> None:
        self.space = space
        self.join = tuple(join)
        self.aliases = _walk(space, self.join)
        self.alias_type = dict(self.aliases)
        self._branches = _Branches(self)
        self._universes: dict[tuple[Term, ...], tuple[Term, ...]] = {}

    # -- Refs and types -------------------------------------------------------

    @cached_property
    def prop_refs(self) -> tuple[Term, ...]:
        return tuple(
            ("prop", alias, key)
            for alias, type_ in self.aliases
            for key, _ in self.space.props.get(type_, ())
        )

    def type_of(self, ref: Term) -> str | None:
        kind = ref[0] if ref else None
        if kind == "prop":
            type_ = self.alias_type.get(ref[1])
            return None if type_ is None else self.space.prop_type(type_, ref[2])
        if kind == "agg":
            return self.agg_result(ref)
        if kind == "win":
            return _win_type(self, ref)
        return None

    def agg_result(self, term: Term) -> str | None:
        """Type of an aggregate term, or ``None`` if Σ or §4.2 rejects it."""
        _, fn, arg = term
        if arg[0] == "node":
            ok = fn == "COUNT" and arg[1] in self.alias_type
            return "Int" if ok else None
        if not self.fn_admitted(fn, arg):
            return None
        return agg_type(fn, self.type_of(arg))

    def fn_admitted(self, fn: str, ref: Term, *, per_tuple: bool = False) -> bool:
        """Σ admits ``fn`` over the PropRef *ref*: FactlessFact, AD1–AD4 and
        the rule for a numeric dimension attribute (§2.4); *per_tuple* for a
        window, which reads every tuple of C."""
        if ref[0] != "prop":
            return True
        type_ = self.alias_type.get(ref[1], "")
        if self.space.factless(type_) and fn != "COUNT":
            return False
        allowed = self.space.agg_fns(type_, ref[2], per_tuple=per_tuple)
        return allowed is None or fn in allowed

    def alias_of(self, ref: Term) -> str | None:
        return ref[1] if ref and ref[0] in ("prop", "node") else None

    def path_time_ordered(self, t: int) -> bool:
        return all(self.space.time_ordered(h) for h in self.join[t][1])

    # -- Band 1 ---------------------------------------------------------------

    @cached_property
    def where_universe(self) -> tuple[Term, ...]:
        atoms = [
            *_param_atoms(self, self.prop_refs),
            *_pair_atoms(self, self.prop_refs),
        ]
        atoms += [
            ("path", t, mode)
            for t in range(len(self.join))
            for mode in TEMPORAL_MODES
            if mode not in PAST_MODES or self.path_time_ordered(t)
        ]
        return sort_terms(a for a in atoms if self.da_atom_ok(a))

    # -- Band 2 ---------------------------------------------------------------

    @cached_property
    def group_keys(self) -> tuple[Term, ...]:
        return sort_terms(r for r in self.prop_refs if self.key_ok(r))

    @cached_property
    def agg_terms(self) -> tuple[Term, ...]:
        args = [("node", a) for a, _ in self.aliases] + list(self.prop_refs)
        terms = (("agg", fn, arg) for arg in args for fn in AGG_FNS)
        return sort_terms(t for t in terms if self.agg_ok(t))

    def key_ok(self, ref: Term) -> bool:
        type_ = self.alias_type.get(ref[1])
        if type_ is None or self.type_of(ref) is None or self.space.bronze(type_):
            return False
        return self._branches.key_ok(ref[1])

    def agg_ok(self, term: Term) -> bool:
        return self.agg_result(term) is not None and self._branches.agg_ok(term[2][1])

    def da_atom_ok(self, atom: Term) -> bool:
        return self._branches.atom_ok(_atom_aliases(self, atom))

    def having_universe(self, aggs: Sequence[Term]) -> tuple[Term, ...]:
        return self._compare_universe(tuple(aggs))

    def _compare_universe(self, refs: tuple[Term, ...]) -> tuple[Term, ...]:
        if refs not in self._universes:
            self._universes[refs] = sort_terms(
                [*_param_atoms(self, refs), *_pair_atoms(self, refs)]
            )
        return self._universes[refs]

    # -- Band 3 ---------------------------------------------------------------

    def b3_scope(self, group_by: Sequence[Term] | None, aggs: Sequence[Term]):
        """Refs a window may use: B₂ keys and AggRefs, else the PropRefs of C."""
        if group_by is None:
            return sort_terms(self.prop_refs)
        return sort_terms([*group_by, *aggs])

    def window_terms(self, scope: Sequence[Term], u: int) -> Iterator[Term]:
        """Window terms of size ``u = 1 + |partition| + |order|`` (fixed order)."""
        ordered = [r for r in scope if self.type_of(r) in ORDERED]
        for p in range(u):
            o = u - 1 - p
            for head, needs_order in _heads(self, scope):
                if o == 0 and needs_order:
                    continue
                for part in combinations(scope, p):
                    for order in _orders(ordered, o):
                        yield ("win", head, part, order)

    def qualify_universe(self, windows: Sequence[Term]) -> tuple[Term, ...]:
        return self._compare_universe(tuple(windows))


def win_size(term: Term) -> int:
    return 1 + len(term[2]) + len(term[3])


def _orders(ordered: Sequence[Term], o: int) -> Iterator[tuple[tuple[Term, str], ...]]:
    for keys in permutations(ordered, o):
        for dirs in product(DIRS, repeat=o):
            yield tuple(zip(keys, dirs))


def _heads(ctx: JoinCtx, scope: Sequence[Term]) -> Iterator[tuple[Term, bool]]:
    """Window heads in order, each with whether it requires an ORDER BY."""
    yield from ((("rank", fn), True) for fn in RANK_FNS)
    yield from ((("offset", fn, r), True) for fn in OFFSET_FNS for r in scope)
    yield from (
        (("running", fn, r), True)
        for fn in AGG_FNS
        for r in scope
        if _running_type(ctx, fn, r) is not None
    )
    yield from ((("stat", fn, None), True) for fn in ORDER_STATS)
    yield from _percentile_heads(ctx, scope)


def _percentile_heads(
    ctx: JoinCtx, scope: Sequence[Term]
) -> Iterator[tuple[Term, bool]]:
    # A statistic over a numeric PropRef takes a measure (MIN/MAX admitted),
    # never a Degenerate or dimension attribute.
    scope = [r for r in scope if ctx.fn_admitted("MIN", r, per_tuple=True)]
    types = [ctx.type_of(r) for r in scope]
    yield from (
        (("stat", "PERCENTILE_CONT", r), False)
        for r, t in zip(scope, types)
        if t in NUMERIC
    )
    yield from (
        (("stat", "PERCENTILE_DISC", r), False)
        for r, t in zip(scope, types)
        if t in ORDERED
    )


def _running_type(ctx: JoinCtx, fn: str, ref: Term) -> str | None:
    if not ctx.fn_admitted(fn, ref, per_tuple=True):
        return None
    return agg_type(fn, ctx.type_of(ref))


def _win_type(ctx: JoinCtx, term: Term) -> str | None:
    head = term[1]
    kind, fn = head[0], head[1]
    if kind == "rank":
        return "Int"
    if kind == "offset":
        return ctx.type_of(head[2])
    if kind == "running":
        return _running_type(ctx, fn, head[2])
    if fn in ORDER_STATS:
        return "Real"
    arg = ctx.type_of(head[2])
    if fn == "PERCENTILE_CONT":
        return "Real" if arg in NUMERIC else None
    return arg if arg in ORDERED else None


def _param_atoms(ctx: JoinCtx, refs: Iterable[Term]) -> Iterator[Term]:
    for ref in refs:
        type_ = ctx.type_of(ref)
        if type_ is not None:
            for op in ops_for(type_):
                yield ("cmp", ref, op, ("param", type_))


def _pair_atoms(ctx: JoinCtx, refs: Iterable[Term]) -> Iterator[Term]:
    for a, b in combinations(sort_terms(refs), 2):
        ta, tb = ctx.type_of(a), ctx.type_of(b)
        common = None if ta is None or tb is None else compatible(ta, tb)
        if common is not None:
            for op in ops_for(common):
                yield ("cmp", a, op, b)


def _atom_aliases(ctx: JoinCtx, atom: Term) -> set[str]:
    if atom[0] == "path":
        return {ctx.join[atom[1]][0]}
    refs = [atom[1]] + ([atom[3]] if atom[3][0] != "param" else [])
    return {a for a in (ctx.alias_of(r) for r in refs) if a is not None}


# ---------------------------------------------------------------------------
# Fact branches and drill-across (§5.2.2, DA1–DA3)
# ---------------------------------------------------------------------------


def _adjacency(ctx: JoinCtx) -> dict[str, set[str]]:
    adj: dict[str, set[str]] = {a: set() for a, _ in ctx.aliases}
    for u, v in _edges(ctx.join):
        adj[u].add(v)
        adj[v].add(u)
    return adj


class _Branches:
    """Fact branches of a Join; DA1–DA3 apply only to a constellation."""

    def __init__(self, ctx: JoinCtx) -> None:
        self._ctx = ctx
        self._adj = _adjacency(ctx)
        facts = [a for a, t in ctx.aliases if ctx.space.kind(t) == "fact"]
        self._id: dict[str, int] = {}
        for i, f in enumerate(facts):
            if f not in self._id:
                self._id.update(dict.fromkeys(self._reach(f), i))
        self._member = {
            a: frozenset(self._id[f] for f in self._reach(a)) for a in self._adj
        }
        self.constellation = len(set(self._id.values())) >= 2
        self._fact_types = {ctx.alias_type[f] for f in facts}

    def _splits(self, prev: str, cur: str, nxt: str) -> bool:
        t = self._ctx.alias_type
        space = self._ctx.space
        return (
            prev != nxt  # a pass relates two fact aliases, not one and itself
            and space.kind(t[cur]) == "dim"
            and space.kind(t[prev]) == "fact"
            and space.kind(t[nxt]) == "fact"
            and space.conformed_for(t[cur], {t[prev], t[nxt]})
        )

    def _reach(self, start: str) -> set[str]:
        """Fact aliases reached from *start* without a constellation pass."""
        seen: set[tuple[str | None, str]] = {(None, start)}
        queue: deque[tuple[str | None, str]] = deque(seen)
        while queue:
            prev, cur = queue.popleft()
            for nxt in sorted(self._adj[cur]):
                state = (cur, nxt)
                if state in seen or (prev is not None and self._splits(prev, cur, nxt)):
                    continue
                seen.add(state)
                queue.append(state)
        kinds = self._ctx.space.kind
        return {c for _, c in seen if kinds(self._ctx.alias_type[c]) == "fact"}

    def _single(self, alias: str) -> int | None:
        member = self._member.get(alias, frozenset())
        return next(iter(member)) if len(member) == 1 else None

    def atom_ok(self, aliases: set[str]) -> bool:
        """DA3: conformed dimensions and at most one fact branch."""
        if not self.constellation:
            return True
        return len({b for a in aliases if (b := self._single(a)) is not None}) <= 1

    def agg_ok(self, alias: str) -> bool:
        """DA1: the argument belongs to exactly one fact branch."""
        return not self.constellation or self._single(alias) is not None

    def key_ok(self, alias: str) -> bool:
        """DA2: a key is an attribute of a dimension conformed for every branch."""
        if not self.constellation:
            return True
        type_ = self._ctx.alias_type[alias]
        space = self._ctx.space
        return space.kind(type_) == "dim" and space.conformed_for(
            type_, self._fact_types
        )
