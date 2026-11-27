"""An enumeration of ``Q_valid(𝒜)`` written from the text of the spec (#139).

It reads §8.11 (the construction (a)–(e)), §12.3 (*``dgm_what_can_i_answer``
enumeration*: the clauses, the Σ rules and the grade), §4.1 (types,
ComparisonOp), §4.2 (AggFn typing) and §5.2.3 (Band 3 scope and WinExpr
typing).  Nothing here imports ``sqldim``: the set it returns is the
expected value that the lattice's output is compared with.

Scope, so that no reading has to be chosen:

* one Join — two anchors ``A`` and ``B`` and one edge type ``A → B`` with
  ``h = 1``, so the only pattern is one forward hop;
* skeletons of grade ≤ :data:`GRADE` — the grade of §12.3 is the internal BDD
  nodes of the three filters plus the GroupBy keys, the aggregates and the
  size ``1 + |partition| + |order|`` of each window.  A function of at most
  two variables has the same reduced BDD size under either variable order,
  so the set does not depend on the byte order of §8.14, which the spec
  leaves to the encoding of the atoms;
* no numeric attribute of a dimension (§2.4 admits MIN, MAX and AVG over it;
  the §12.3 sentence on Degenerate attributes reads "no aggregate other than
  COUNT"), no property of a FactlessFact, no time-ordered bridge, one fact at
  most (no constellation, so DA1–DA3 are vacuous).

A schema is ``{"nodes": {type: (kind, {key: declared type})}, "edge":
(source, label, target), "measures": {(fact, key): kind}, "bronze": set}``,
where a measure kind is ``ADDITIVE``, ``NON_ADDITIVE`` or ``DEGENERATE``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from itertools import combinations, permutations, product
from typing import Any

GRADE = 2

TYPES = {"String": "String", "Integer": "Int", "Real": "Real", "Boolean": "Bool"}
ORDERED = {"Int", "Real", "String", "Date", "Timestamp", "Duration"}
NUMERIC = {"Int", "Real"}
OPS = ("=", "≠", "<", "≤", ">", "≥")
MIRROR = {"=": "=", "≠": "≠", "<": ">", ">": "<", "≤": "≥", "≥": "≤"}
AGG_FNS = ("SUM", "COUNT", "AVG", "MIN", "MAX", "PROD")
FUTURE_MODES = ("EVENTUALLY", "GLOBALLY", "NEXT", "UNTIL")
RANK_FNS = ("ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE")
MEASURE_FNS = {
    "ADDITIVE": set(AGG_FNS),
    "NON_ADDITIVE": {"COUNT", "MIN", "MAX"},
    "DEGENERATE": {"COUNT"},
}

Term = tuple[Any, ...]
Fn = tuple[frozenset, frozenset]
"""A Boolean function: its support and the sets of true atoms that satisfy it."""

TRUE: Fn = (frozenset(), frozenset({frozenset()}))
FALSE: Fn = (frozenset(), frozenset())


def key(t: Any) -> str:
    """A total order on terms (frozensets sorted), for canonical pairs."""
    if isinstance(t, frozenset):
        return "{" + ",".join(sorted(key(x) for x in t)) + "}"
    if isinstance(t, tuple):
        return "(" + ",".join(key(x) for x in t) + ")"
    return repr(t)


def pair(a: Term, op: str, b: Term) -> Term:
    """``Compare(a, op, b)`` and ``Compare(b, mirror(op), a)`` are one atom."""
    x, y = (a, op, b), (b, MIRROR[op], a)
    return ("cmp2", *min(x, y, key=key))


# ---------------------------------------------------------------------------
# Boolean functions by reduced-BDD size
# ---------------------------------------------------------------------------


def bdd_size(table: dict[tuple[bool, ...], bool], k: int) -> int:
    """Internal nodes of the reduced ordered BDD, variables in index order."""
    total = 0
    for i in range(k):
        subs = set()
        for prefix in product((False, True), repeat=i):
            sub = tuple(
                table[prefix + rest] for rest in product((False, True), repeat=k - i)
            )
            half = len(sub) // 2
            if sub[:half] != sub[half:]:  # depends on variable i
                subs.add(sub)
        total += len(subs)
    return total


def fn_size(f: Fn) -> int:
    support = sorted(f[0], key=key)
    table = {
        bits: frozenset(a for a, b in zip(support, bits) if b) in f[1]
        for bits in product((False, True), repeat=len(support))
    }
    return bdd_size(table, len(support))


def functions(universe: Sequence[Term], s: int) -> list[Fn]:
    """Every Boolean function of the atoms with exactly *s* BDD nodes (s ≤ 2)."""
    k = (frozenset(universe), s)
    if k not in _FUNCTIONS:
        _FUNCTIONS[k] = list(_functions(universe, s))
    return _FUNCTIONS[k]


_FUNCTIONS: dict[tuple[frozenset, int], list[Fn]] = {}


def _functions(universe: Sequence[Term], s: int) -> Iterator[Fn]:
    assert s <= 2
    if s == 0:
        yield from (TRUE, FALSE)
        return
    for k in range(1, s + 1):
        for atoms in combinations(sorted(universe, key=key), k):
            rows = list(product((False, True), repeat=k))
            for values in product((False, True), repeat=len(rows)):
                table = dict(zip(rows, values))
                flipped = {r[::-1]: v for r, v in table.items()}
                size = bdd_size(table, k)
                assert size == bdd_size(flipped, k)  # order-independent for k ≤ 2
                if size != s or not _depends_on_all(table, k):
                    continue
                models = frozenset(
                    frozenset(a for a, b in zip(atoms, r) if b)
                    for r, v in table.items()
                    if v
                )
                yield frozenset(atoms), models


def _depends_on_all(table: dict, k: int) -> bool:
    return all(
        any(table[r] != table[r[:i] + (not r[i],) + r[i + 1 :]] for r in table)
        for i in range(k)
    )


# ---------------------------------------------------------------------------
# (a)–(e) for one schema
# ---------------------------------------------------------------------------


class Domain:
    def __init__(self, schema: dict) -> None:
        self.schema = schema
        src, _label, dst = schema["edge"]
        assert src < dst, "the anchors are A < B in canonical order"
        self.alias = {"a1": src, "a2": dst}  # (a): Anchor(a1), a1 -[label]-> a2
        self.props = [
            ("prop", alias, k)
            for alias, t in self.alias.items()
            for k in sorted(schema["nodes"][t][1])
        ]
        self._windows: dict[tuple, list[Term]] = {}

    # -- types and Σ ----------------------------------------------------------

    def node_kind(self, alias: str) -> str:
        return self.schema["nodes"][self.alias[alias]][0]

    def type_of(self, ref: Term) -> str | None:
        if ref[0] == "prop":
            declared = self.schema["nodes"][self.alias[ref[1]]][1][ref[2]]
            return TYPES[declared]
        if ref[0] == "agg":
            return self.agg_type(ref[1], ref[2])
        return self.win_type(ref)

    def measure(self, ref: Term) -> str | None:
        """The measure kind of a numeric fact property, else ``None``."""
        if ref[0] != "prop" or self.type_of(ref) not in NUMERIC:
            return None
        assert self.node_kind(ref[1]) == "fact", "numeric dimension attributes: out"
        return self.schema["measures"][(self.alias[ref[1]], ref[2])]

    def agg_type(self, fn: str, arg: Term, *, window: bool = False) -> str | None:
        """§4.2 typing, then the Σ rules of §2.4 / §12.3."""
        if arg[0] == "node":
            return "Int" if fn == "COUNT" and not window else None
        t = self.type_of(arg)
        if t is None:
            return None
        if fn == "COUNT":
            typed: str | None = "Int"
        elif fn == "AVG":
            typed = "Real" if t in NUMERIC else None
        elif fn in ("SUM", "PROD"):
            typed = t if t in NUMERIC else None
        else:
            typed = t if t in ORDERED else None
        kind = self.measure(arg)
        if kind is not None and fn not in MEASURE_FNS[kind]:
            return None
        return typed

    def stat_arg_ok(self, ref: Term) -> bool:
        """No StatFn over a Degenerate attribute (§12.3)."""
        return self.measure(ref) != "DEGENERATE"

    def win_type(self, w: Term) -> str | None:
        head = w[1]
        if head[0] == "rank":
            return "Int"
        if head[0] == "offset":
            return self.type_of(head[2])
        if head[0] == "running":
            return self.agg_type(head[1], head[2], window=True)
        if head[1] in ("PERCENT_RANK", "CUME_DIST"):
            return "Real"
        if head[1] == "PERCENTILE_CONT":
            return "Real"
        return self.type_of(head[2])

    # -- atoms ----------------------------------------------------------------

    def compare_atoms(self, refs: Iterable[Term]) -> list[Term]:
        refs = list(refs)
        out: list[Term] = []
        for r in refs:
            t = self.type_of(r)
            ops = OPS if t in ORDERED else ("=", "≠")
            out += [("cmp", r, op, "?") for op in ops]
        for a, b in combinations(refs, 2):
            ta, tb = self.type_of(a), self.type_of(b)
            common = ta if ta == tb else ("Real" if {ta, tb} == NUMERIC else None)
            if common is not None:
                ops = OPS if common in ORDERED else ("=", "≠")
                out += [pair(a, op, b) for op in ops]
        return out

    def where_universe(self) -> list[Term]:
        # VERB hop: not time-ordered, so SINCE, ONCE and PREVIOUSLY are out.
        return self.compare_atoms(self.props) + [("path", 0, m) for m in FUTURE_MODES]

    # -- Band 2 ---------------------------------------------------------------

    def group_keys(self) -> list[Term]:
        return [r for r in self.props if self.alias[r[1]] not in self.schema["bronze"]]

    def aggregates(self) -> list[Term]:
        out = [("agg", "COUNT", ("node", a)) for a in self.alias]
        out += [
            ("agg", fn, r)
            for r in self.props
            for fn in AGG_FNS
            if self.agg_type(fn, r) is not None
        ]
        return out

    def band2(self, budget: int) -> Iterator[tuple[Any, int]]:
        yield None, 0
        keys, aggs = self.group_keys(), self.aggregates()
        for g in range(budget + 1):
            for a in range(1, budget - g + 1):
                for gb in combinations(keys, g):
                    for ag in combinations(aggs, a):
                        universe = self.compare_atoms(ag)
                        for h in range(budget - g - a + 1):
                            for f in functions(universe, h):
                                yield (frozenset(gb), frozenset(ag), f), g + a + h

    # -- Band 3 ---------------------------------------------------------------

    def windows(self, scope: Sequence[Term], most: int) -> list[Term]:
        k = (tuple(scope), most)
        if k not in self._windows:
            self._windows[k] = self._windows_of(scope, most)
        return self._windows[k]

    def _windows_of(self, scope: Sequence[Term], most: int) -> list[Term]:
        ordered = [r for r in scope if self.type_of(r) in ORDERED]
        heads: list[tuple[Term, bool]] = [(("rank", f), True) for f in RANK_FNS]
        heads += [(("offset", f, r), True) for f in ("LEAD", "LAG") for r in scope]
        heads += [
            (("running", f, r), True)
            for f in AGG_FNS
            for r in scope
            if self.agg_type(f, r, window=True) is not None
        ]
        heads += [(("stat", f, None), True) for f in ("PERCENT_RANK", "CUME_DIST")]
        heads += [
            (("stat", "PERCENTILE_CONT", r), False)
            for r in scope
            if self.type_of(r) in NUMERIC and self.stat_arg_ok(r)
        ]
        heads += [
            (("stat", "PERCENTILE_DISC", r), False)
            for r in scope
            if self.type_of(r) in ORDERED and self.stat_arg_ok(r)
        ]
        out = []
        for head, needs_order in heads:
            for p in range(most):
                for o in range(most - p):
                    if needs_order and o == 0:
                        continue
                    for part in combinations(scope, p):
                        for keys in permutations(ordered, o):
                            for dirs in product(("ASC", "DESC"), repeat=o):
                                order = tuple(zip(keys, dirs))
                                out.append(("win", head, frozenset(part), order))
        return [w for w in out if self.win_type(w) is not None]

    def band3(self, b2: Any, budget: int) -> Iterator[tuple[Any, int]]:
        yield None, 0
        scope = list(self.props) if b2 is None else [*b2[0], *b2[1]]
        cands = self.windows(scope, budget)
        size = {w: 1 + len(w[2]) + len(w[3]) for w in cands}
        for n in range(1, budget + 1):
            for ws in combinations(cands, n):
                used = sum(size[w] for w in ws)
                if used > budget:
                    continue
                universe = self.compare_atoms(ws)
                for q in range(budget - used + 1):
                    for f in functions(universe, q):
                        yield (frozenset(ws), f), used + q

    # -- the skeletons --------------------------------------------------------

    def skeletons(self, grade: int = GRADE) -> set[tuple]:
        out: set[tuple] = set()
        where_universe = self.where_universe()
        for w in range(grade + 1):
            for where in functions(where_universe, w):
                for b2, s2 in self.band2(grade - w):
                    for b3, s3 in self.band3(b2, grade - w - s2):
                        out.add((where, b2, b3))
        return out


def grade(sk: tuple) -> int:
    where, b2, b3 = sk
    g = fn_size(where)
    if b2 is not None:
        g += len(b2[0]) + len(b2[1]) + fn_size(b2[2])
    if b3 is not None:
        g += sum(1 + len(w[2]) + len(w[3]) for w in b3[0]) + fn_size(b3[1])
    return g
