"""Question skeletons: canonical form, identity and membership (§8.11, §8.14).

A skeleton is ``(anchors, Join, Where, Band 2?, Band 3?)``.  Each filter —
Where, Having, Qualify — is ``(atoms, bdd)``: the atoms it depends on, sorted
by the byte order of their canonical encoding, and a reduced ordered BDD
over their positions.  Equal skeletons are equal values (QH6).

``question_key`` is §8.14's ``H(canon_version, schema_version,
dag_hash(root), params)`` with the planner's encoding, Merkle rule and key
rule (``planner._ir_identity``); a skeleton binds no parameter, so
``params`` is empty.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from sqldim.core.query.dgm.lattice import _bdd
from sqldim.core.query.dgm.lattice._context import (
    InvalidSkeleton,
    JoinCtx,
    join_ctx,
    PathJoin,
    Term,
    enc,
    win_size,
)
from sqldim.core.query.dgm.lattice._space import Space
from sqldim.core.query.dgm.planner._ir_identity import key_from_dag_hash

__all__ = [
    "Filter",
    "Skeleton",
    "check",
    "compress",
    "expand",
    "question_key",
    "summary",
]

Filter = tuple[tuple[Term, ...], _bdd.BDD]
Band2 = tuple[tuple[Term, ...], tuple[Term, ...], Filter]
Band3 = tuple[tuple[Term, ...], Filter]

TRUE_FILTER: Filter = ((), True)


@dataclass(frozen=True)
class Skeleton:
    """One canonical skeleton of ``Q_valid(𝒜)``."""

    anchors: tuple[str, ...]
    join: tuple[PathJoin, ...]
    where: Filter = TRUE_FILTER
    band2: Band2 | None = None
    band3: Band3 | None = None

    def payload(self) -> tuple[Any, ...]:
        return (self.anchors, self.join, self.where, self.band2, self.band3)

    def to_json(self) -> str:
        return json.dumps(self.payload(), ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> "Skeleton":
        anchors, join, where, band2, band3 = _tuples(json.loads(text))
        return cls(anchors, join, where, band2, band3)

    @property
    def group_by(self) -> tuple[Term, ...] | None:
        return None if self.band2 is None else self.band2[0]

    @property
    def aggregates(self) -> tuple[Term, ...]:
        return () if self.band2 is None else self.band2[1]


def _tuples(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_tuples(v) for v in value)
    return value


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def compress(universe: Sequence[Term], f: _bdd.BDD) -> Filter:
    """The canonical filter of *f*, a function over positions of *universe*."""
    used = sorted(_bdd.support(f))
    mapping = {old: new for new, old in enumerate(used)}
    table = [mapping.get(i, -1) for i in range(len(universe))]
    return tuple(universe[i] for i in used), _bdd.relabel(f, table)


def expand(flt: Filter, universe: Sequence[Term]) -> _bdd.BDD:
    """The filter's function over positions of *universe* (a superset)."""
    position = {enc(a): i for i, a in enumerate(universe)}
    return _bdd.relabel(flt[1], [position[enc(a)] for a in flt[0]])


def _filter_problems(flt: Filter, allowed: Iterable[Term], where: str) -> list[str]:
    atoms, f = flt
    keys = [enc(a) for a in atoms]
    if keys != sorted(set(keys)):
        return [f"{where}: atoms are not in canonical order"]
    if not set(keys) <= {enc(a) for a in allowed}:
        return [f"{where}: an atom is not a typed atom in scope"]
    if not _bdd.is_canonical(f, len(atoms)) or _bdd.support(f) != set(
        range(len(atoms))
    ):
        return [f"{where}: not a reduced ordered BDD over exactly its atoms"]
    return []


def _ph(flt: Filter) -> str:
    return _bdd.merkle(flt[1], [enc(a) for a in flt[0]])


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def dag_hash(sk: Skeleton) -> str:
    """``dag_hash`` of a leaf (§8.14): its canonical band query."""
    b2 = sk.band2
    b3 = sk.band3
    payload = (
        ("anchors", sk.anchors),
        ("join", sk.join),
        ("B1", _ph(sk.where)),
        ("group_by", () if b2 is None else b2[0]),
        ("agg", () if b2 is None else b2[1]),
        ("B2", None if b2 is None else _ph(b2[2])),
        ("window", () if b3 is None else b3[0]),
        ("B3", None if b3 is None else _ph(b3[1])),
    )
    # canonical_hash("band", payload), with the (cached) encodings of its parts.
    body = _list([enc(part) for part in payload])
    return hashlib.sha256(_list([enc("band"), body])).hexdigest()


def _list(items: list[bytes]) -> bytes:
    """The canonical encoding of a sequence from its items' encodings."""
    payload = b"".join(items)
    return b"L" + len(payload).to_bytes(8, "big") + payload


def question_key(sk: Skeleton, schema_version: str) -> str:
    return key_from_dag_hash(dag_hash(sk), schema_version=schema_version)


# ---------------------------------------------------------------------------
# Membership (§8.11, theorem part 2)
# ---------------------------------------------------------------------------


def check(space: Space, sk: Skeleton) -> list[str]:
    """Why *sk* is not in ``Q_valid(𝒜)`` within the space's bounds; ``[]`` iff it is."""
    if sk.anchors != space.anchor_names:
        return ["the anchors are not the canonical anchor set of this space"]
    try:
        ctx = join_ctx(space, sk.join)
    except (InvalidSkeleton, TypeError, ValueError, IndexError) as exc:
        return [f"Join: {exc}"]
    problems = _filter_problems(sk.where, ctx.where_universe, "Where")
    if sk.band2 is not None:
        problems += _band2_problems(ctx, sk.band2)
    if sk.band3 is not None:
        problems += _band3_problems(ctx, sk, sk.band3)
    return problems


def _set_problems(terms: tuple[Term, ...], allowed: Iterable[Term], what: str):
    keys = [enc(t) for t in terms]
    if keys != sorted(set(keys)):
        return [f"{what} is not a canonical set"]
    if not set(keys) <= {enc(t) for t in allowed}:
        return [f"{what} has a member outside its finite set"]
    return []


def _band2_problems(ctx: JoinCtx, band2: Band2) -> list[str]:
    group_by, aggs, having = band2
    problems = _set_problems(group_by, ctx.group_keys, "GroupBy")
    problems += _set_problems(aggs, ctx.agg_terms, "Agg")
    if not aggs:
        problems.append("Band 2 has at least one aggregate")
    return problems + _filter_problems(having, ctx.having_universe(aggs), "Having")


def _band3_problems(ctx: JoinCtx, sk: Skeleton, band3: Band3) -> list[str]:
    windows, qualify = band3
    scope = ctx.b3_scope(sk.group_by, sk.aggregates)
    if not windows:
        return ["Band 3 has at least one window expression"]
    allowed = [w for w in windows if _window_ok(ctx, scope, w)]
    problems = _set_problems(windows, allowed, "Window")
    return problems + _filter_problems(
        qualify, ctx.qualify_universe(windows), "Qualify"
    )


def _window_ok(ctx: JoinCtx, scope: Sequence[Term], term: Term) -> bool:
    if not (isinstance(term, tuple) and len(term) == 4 and term[0] == "win"):
        return False
    return any(t == term for t in ctx.window_terms(scope, win_size(term)))


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def _ref(term: Term) -> str:
    kind = term[0]
    if kind == "prop":
        return f"{term[1]}.{term[2]}"
    if kind == "node":
        return term[1]
    if kind == "agg":
        return f"{term[1]}({_ref(term[2])})"
    return _win(term)


def _win(term: Term) -> str:
    head, part, order = term[1], term[2], term[3]
    arg = "" if len(head) < 3 or head[2] is None else _ref(head[2])
    over = []
    if part:
        over.append("PARTITION BY " + ", ".join(_ref(r) for r in part))
    if order:
        over.append("ORDER BY " + ", ".join(f"{_ref(r)} {d}" for r, d in order))
    return f"{head[1]}({arg}) OVER ({' '.join(over)})"


def _pattern(sk: Skeleton, t: int) -> str:
    start, hops = sk.join[t]
    out = start
    for j, (_, label, _, direction) in enumerate(hops):
        nxt = f"a{t + 2}" if j == len(hops) - 1 else f"p{t + 2}_{j + 1}"
        out += f" -[{label}]-> {nxt}" if direction == "+" else f" <-[{label}]- {nxt}"
    return out


def _atom(sk: Skeleton, atom: Term) -> str:
    if atom[0] == "path":
        return f"EXISTS {atom[2]} ({_pattern(sk, atom[1])})"
    rhs = "?" if atom[3][0] == "param" else _ref(atom[3])
    return f"{_ref(atom[1])} {atom[2]} {rhs}"


def _formula(sk: Skeleton, flt: Filter) -> str:
    atoms, f = flt
    return _bdd.render(f, lambda i: _atom(sk, atoms[i]))


def summary(sk: Skeleton, types: Sequence[str]) -> str:
    """A one-line description; ``?`` marks a parameter slot."""
    anchors = ", ".join(f"{t} a{i + 1}" for i, t in enumerate(types))
    parts = [f"ANCHORS {anchors}"]
    parts += [f"JOIN {_pattern(sk, t)}" for t in range(len(sk.join))]
    parts.append(f"WHERE {_formula(sk, sk.where)}")
    if sk.band2 is not None:
        group_by, aggs, having = sk.band2
        keys = ", ".join(_ref(r) for r in group_by) or "()"
        parts += [f"GROUP BY {keys}", "AGG " + ", ".join(_ref(a) for a in aggs)]
        parts.append(f"HAVING {_formula(sk, having)}")
    if sk.band3 is not None:
        windows, qualify = sk.band3
        parts.append("WINDOW " + ", ".join(_win(w) for w in windows))
        parts.append(f"QUALIFY {_formula(sk, qualify)}")
    return " | ".join(parts)
