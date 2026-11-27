"""``deps(plan)`` of DGM §8.14: the relations and partitions a plan can read.

Every Scan of the plan — anchors, path joins, EXISTS/FORALL hops, Repeat
bodies, SCD version-resolution joins — and every relation a PreComputation
step declares it reads is a dependency.  A :class:`RelationCatalog` replaces
Derived views by their base relations and may declare how a base relation is
partitioned (:class:`Partitioning`).

**Partition pruning.**  For a relation with declared partitioning,
``Dependency.partitions`` lists the partitions the plan can read:

* An anchor Scan of the relation is pruned by the comparisons
  ``alias.column op literal`` (``op`` ∈ ``= < <= > >=``) that are top-level
  conjuncts of its band's Where, where ``column`` is the partition column.
  Such a conjunct must hold for every row of the anchor that reaches the
  result, so a partition none of whose values satisfies it is not read.
* Every other read — a joined or EXISTS hop, a Repeat body, a Derived view's
  base, a PreComputation's reads, or a comparison whose literal does not
  have the partitioning's value kind — reads **all** partitions, listed
  explicitly.  Pruning is never inferred beyond what is proven above.

A relation whose partitioning is not declared has ``partitions=None``: every
partition, as the catalog states no partitions.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Protocol

from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Compose,
    Conj,
    PlanIR,
    PlanNode,
    Pred,
    ReadRole,
    Scan,
    UndeclaredReads,
    UnknownRelation,
    check_identifier,
)

__all__ = [
    "Dependency",
    "PartitionKind",
    "Partition",
    "Partitioning",
    "RelationCatalog",
    "StaticCatalog",
    "relations_read",
    "deps",
]


# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Dependency:
    """A source the plan can read.

    ``partitions`` is ``None`` when the catalog declares no partitioning for
    the relation (every partition); otherwise it is the sorted tuple of the
    partitions the plan can read — all of them when pruning is not proven.
    """

    relation: str
    partitions: tuple[str, ...] | None = None


class PartitionKind(Enum):
    """Value kind of a partition column, which fixes how literals compare."""

    NUMBER = "NUMBER"  # int / finite float literals
    STRING = "STRING"  # str literals, compared by code point (binary collation)
    DATE = "DATE"  # 'YYYY-MM-DD' literals, compared as dates


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _number(v: object) -> object | None:
    ok = isinstance(v, (int, float)) and not isinstance(v, bool)
    return v if ok and math.isfinite(v) else None  # type: ignore[arg-type]


def _string(v: object) -> object | None:
    return v if isinstance(v, str) else None


def _date(v: object) -> object | None:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if not (isinstance(v, str) and _ISO_DATE.match(v)):
        return None
    try:
        return date.fromisoformat(v)
    except ValueError:
        return None


def _value(kind: PartitionKind, v: object) -> object | None:
    """*v* as a comparable value of *kind*, or ``None`` if it is not one."""
    return {
        PartitionKind.NUMBER: _number,
        PartitionKind.STRING: _string,
        PartitionKind.DATE: _date,
    }[kind](v)


@dataclass(frozen=True)
class Partition:
    """One partition: a list of values, or a half-open range ``[lower, upper)``.

    For a range, a bound of ``None`` is unbounded on that side.  Bounds and
    values are of the partitioning's :class:`PartitionKind`; DATE bounds are
    :class:`datetime.date` values or ``'YYYY-MM-DD'`` strings.
    """

    name: str
    values: frozenset | None = None
    lower: object = None
    upper: object = None


@dataclass(frozen=True)
class Partitioning:
    """How a base relation is partitioned: by *column*, into *partitions*."""

    column: str
    kind: PartitionKind
    partitions: tuple[Partition, ...]

    def __post_init__(self) -> None:
        check_identifier(self.column, "partition column")
        if not isinstance(self.kind, PartitionKind):
            raise ValueError(f"partition kind must be a PartitionKind: {self.kind!r}")
        if not self.partitions:
            raise ValueError("a partitioning declares at least one partition")
        names = [p.name for p in self.partitions]
        if len(set(names)) != len(names) or not all(
            isinstance(n, str) and n for n in names
        ):
            raise ValueError(f"partition names must be distinct strings: {names}")
        for p in self.partitions:
            self._check(p)

    def _bound(self, p: Partition, v: object) -> object:
        if v is None:
            return None
        out = _value(self.kind, v)
        if out is None:
            raise ValueError(f"partition {p.name!r}: {v!r} is not a {self.kind.value}")
        return out

    def _check(self, p: Partition) -> None:
        if p.values is not None:
            if p.lower is not None or p.upper is not None or not p.values:
                raise ValueError(
                    f"partition {p.name!r} is either a non-empty value list or a range"
                )
            for v in p.values:
                if v is None:
                    raise ValueError(f"partition {p.name!r}: NULL is not a value")
                self._bound(p, v)
            return
        lo, hi = self._bound(p, p.lower), self._bound(p, p.upper)
        if lo is not None and hi is not None and not lo < hi:  # type: ignore[operator]
            raise ValueError(f"partition {p.name!r}: lower must be below upper")

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(p.name for p in self.partitions))

    def touched(self, conds: list[tuple[str, object]]) -> tuple[str, ...]:
        """Partitions that can hold a row satisfying every ``(op, literal)``."""
        usable = [(op, _value(self.kind, v)) for op, v in conds]
        usable = [(op, v) for op, v in usable if v is not None]
        return tuple(
            sorted(
                p.name
                for p in self.partitions
                if all(self._may_hold(p, op, v) for op, v in usable)
            )
        )

    def _may_hold(self, p: Partition, op: str, v: object) -> bool:
        if p.values is not None:
            return any(_holds(self._bound(p, x), op, v) for x in p.values)
        lo, hi = self._bound(p, p.lower), self._bound(p, p.upper)
        below = lo is None or _holds(lo, "<=" if op in ("=", "<=") else "<", v)
        above = hi is None or _holds(v, "<", hi)
        if op == "=":
            return below and above
        return below if op in ("<", "<=") else above


def _holds(a: object, op: str, b: object) -> bool:
    if op == "=":
        return a == b
    if op == "<":
        return a < b  # type: ignore[operator]
    if op == "<=":
        return a <= b  # type: ignore[operator]
    if op == ">":
        return a > b  # type: ignore[operator]
    return a >= b  # type: ignore[operator]


class RelationCatalog(Protocol):
    def base_relations(self, name: str) -> frozenset[str] | None:
        """Base relations read through *name*; ``None`` if *name* is unknown.

        A base relation returns ``{name}``; a Derived view (§2.6) returns the
        base relations of its definition.
        """
        ...

    def partitioning(self, name: str) -> Partitioning | None:
        """The declared partitioning of base relation *name*, if any."""
        ...


@dataclass(frozen=True)
class StaticCatalog:
    """A :class:`RelationCatalog` from explicit declarations."""

    base: frozenset[str]
    derived: Mapping[str, frozenset[str]] = field(default_factory=dict)
    partitions: Mapping[str, Partitioning] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for view, bases in self.derived.items():
            if view in self.base:
                raise ValueError(f"{view!r} is declared both base and derived")
            if not bases:
                raise ValueError(f"derived view {view!r} declares no base relation")
            missing = set(bases) - self.base
            if missing:
                raise ValueError(
                    f"derived view {view!r} reads undeclared relations {sorted(missing)}"
                )
        for name, pt in self.partitions.items():
            if name not in self.base:
                raise ValueError(
                    f"partitioning declared for {name!r}, which is not a base relation"
                )
            if not isinstance(pt, Partitioning):
                raise ValueError(f"partitioning of {name!r} is not a Partitioning")

    def base_relations(self, name: str) -> frozenset[str] | None:
        if name in self.base:
            return frozenset({name})
        if name in self.derived:
            return frozenset(self.derived[name])
        return None

    def partitioning(self, name: str) -> Partitioning | None:
        return self.partitions.get(name)


# ---------------------------------------------------------------------------
# deps(plan)
# ---------------------------------------------------------------------------

_PRUNING_OPS = frozenset({"=", "<", "<=", ">", ">="})


def _nodes(ir: PlanIR) -> Iterator[PlanNode]:
    stack: list[PlanNode] = [ir.root, *(c.node for c in ir.ctes)]
    seen: set[int] = set()
    while stack:
        n = stack.pop()
        if id(n) in seen:
            continue
        seen.add(id(n))
        yield n
        if isinstance(n, Compose):
            stack += [n.left, n.right]


def _bands(ir: PlanIR) -> Iterator[BandQuery]:
    return (n for n in _nodes(ir) if isinstance(n, BandQuery))


def relations_read(ir: PlanIR) -> frozenset[str]:
    """Relation names the emitted SQL references (before view expansion)."""
    return frozenset(s.relation for q in _bands(ir) for s in q.scans())


def _conjuncts(p: Pred | None) -> Iterator[Pred]:
    if isinstance(p, Conj):
        for sub in p.preds:
            yield from _conjuncts(sub)
    elif p is not None:
        yield p


def _anchor_conds(q: BandQuery, column: str) -> list[tuple[str, object]]:
    """``(op, literal)`` of the top-level Where conjuncts on the anchor column."""
    target = f"{q.anchor.alias}.{column}"
    return [
        (c.op, c.rhs.value)
        for c in _conjuncts(q.where)
        if isinstance(c, Compare)
        and c.op in _PRUNING_OPS
        and c.lhs.sql == target
        and c.rhs.value is not None
    ]


class _Deps:
    def __init__(self, catalog: RelationCatalog) -> None:
        self.catalog = catalog
        self.acc: dict[str, set[str] | None] = {}

    def add(self, name: str, band: BandQuery | None = None) -> None:
        """Record a read of *name*; *band* is set for an anchor Scan only."""
        bases = self.catalog.base_relations(name)
        if not bases:
            raise UnknownRelation(
                f"relation {name!r} is not declared by the catalog; the plan's "
                "dependencies cannot be determined"
            )
        for b in sorted(bases):
            pt = self.catalog.partitioning(b)
            if pt is None:
                self.acc[b] = None
                continue
            direct = band is not None and b == name
            touched = pt.touched(_anchor_conds(band, pt.column)) if direct else None
            prior = self.acc.get(b) or set()
            self.acc[b] = prior | set(pt.names() if touched is None else touched)

    def result(self) -> tuple[Dependency, ...]:
        return tuple(
            sorted(
                Dependency(r, None if p is None else tuple(sorted(p)))
                for r, p in self.acc.items()
            )
        )


def _scan_reads(q: BandQuery) -> Iterator[tuple[Scan, BandQuery | None]]:
    for s in q.scans():
        yield s, (q if s.role is ReadRole.ANCHOR and s is q.anchor else None)


def deps(ir: PlanIR, catalog: RelationCatalog) -> tuple[Dependency, ...]:
    """``deps(plan)`` of §8.14, sorted; an undeclared relation is an error."""
    steps = {pc.name for pc in ir.pre_compute}
    out = _Deps(catalog)
    for q in sorted(_bands(ir), key=lambda b: b.anchor.relation):
        for scan, band in _scan_reads(q):
            if scan.relation not in steps:  # a step's output: its reads count
                out.add(scan.relation, band)
    for pc in ir.pre_compute:
        if pc.reads is None:
            raise UndeclaredReads(
                f"PreComputation {pc.name!r} declares no reads; the plan's "
                "dependencies cannot be determined"
            )
        for r in sorted(pc.reads - steps):
            out.add(r)
    return out.result()
