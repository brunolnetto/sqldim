"""Canonical identity and dependencies of a plan IR (DGM §8.14).

* :func:`predicate_hash` — Merkle hash of a filter's BDD built under the
  canonical variable order: atoms ordered by the byte order of their
  canonical serialisation, ``H(atom, hash(low), hash(high))``.
* :func:`dag_hash` — Merkle hash of a plan node, ``H(operator, payload,
  hashes of its children)``; children are sorted by hash for the commutative
  operators (UNION, INTERSECT) and kept in order otherwise.  CTE names and
  other local handles never enter the hash.
* :func:`question_key` — ``H(canon_version, schema_version, dag_hash(root),
  params)`` with ``params`` sorted by name.
* :func:`deps` — the source relations the plan can read, with Derived views
  replaced by their base relations through a :class:`RelationCatalog`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.bdd import FALSE_NODE_ID, TRUE_NODE_ID, BDDManager
from sqldim.core.query.dgm.planner._canon import (
    CANON_VERSION,
    canonical_encode,
    canonical_hash,
)
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Compose,
    Conj,
    Disj,
    Fragment,
    Join,
    Neg,
    PathExists,
    PlanIR,
    PlanNode,
    Pred,
    RawCondition,
    Scan,
    UnknownRelation,
    UnsupportedConstruct,
)

__all__ = [
    "Dependency",
    "RelationCatalog",
    "StaticCatalog",
    "predicate_hash",
    "dag_hash",
    "question_key",
    "relations_read",
    "deps",
]

_COMMUTATIVE = frozenset({ComposeOp.UNION, ComposeOp.INTERSECT})


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


def _scan(s: Scan) -> tuple:
    return ("scan", s.relation, s.alias, s.role.value)


def _frag(f: Fragment | None) -> tuple | None:
    return None if f is None else ("frag", f.sql)


def _join(j: Join) -> tuple:
    v = j.version
    ver = None if v is None else ("version", v.kind.value, v.as_of)
    return ("join", _scan(j.scan), _frag(j.on), ver)


def _atom(p: Pred) -> tuple:
    if isinstance(p, Compare):
        return ("cmp", p.lhs.sql, p.op, p.rhs.value)
    if isinstance(p, RawCondition):
        return ("raw", p.fragment.sql)
    if isinstance(p, PathExists):
        hops = tuple(
            ("hop", h.kind, h.label, _scan(h.scan), _frag(h.on)) for h in p.hops
        )
        return (
            "path",
            p.quantifier.value,
            p.strategy,
            hops,
            predicate_hash(p.sub_filter),
        )
    raise UnsupportedConstruct(f"{type(p).__name__} is not a predicate atom")


# ---------------------------------------------------------------------------
# predicate_hash
# ---------------------------------------------------------------------------


def _atoms(p: Pred) -> Iterator[Pred]:
    if isinstance(p, (Conj, Disj)):
        for sub in p.preds:
            yield from _atoms(sub)
    elif isinstance(p, Neg):
        yield from _atoms(p.pred)
    else:
        yield p


class _CanonicalBDD:
    """A BDD over the atoms of one filter, variables in canonical byte order."""

    def __init__(self, p: Pred) -> None:
        enc = {id(a): canonical_encode(_atom(a)) for a in _atoms(p)}
        self.order = sorted(set(enc.values()))
        self._var = {e: i for i, e in enumerate(self.order)}
        self._enc = enc
        self.mgr = BDDManager()
        self.root = self._build(p)

    def _fold(self, op: str, preds: tuple[Pred, ...], unit: int) -> int:
        acc = unit
        for sub in preds:
            acc = self.mgr.apply(op, acc, self._build(sub))
        return acc

    def _build(self, p: Pred) -> int:
        if isinstance(p, Conj):
            return self._fold("AND", p.preds, TRUE_NODE_ID)
        if isinstance(p, Disj):
            return self._fold("OR", p.preds, FALSE_NODE_ID)
        if isinstance(p, Neg):
            return self.mgr.negate(self._build(p.pred))
        var = self._var[self._enc[id(p)]]
        return self.mgr.make(var, FALSE_NODE_ID, TRUE_NODE_ID)

    def merkle(self) -> str:
        memo: dict[int, str] = {
            FALSE_NODE_ID: canonical_hash("bdd", "FALSE"),
            TRUE_NODE_ID: canonical_hash("bdd", "TRUE"),
        }
        return self._hash(self.root, memo)

    def _hash(self, uid: int, memo: dict[int, str]) -> str:
        if uid not in memo:
            n = self.mgr.get_node(uid)
            memo[uid] = canonical_hash(
                "bdd",
                self.order[n.var],
                self._hash(n.low, memo),
                self._hash(n.high, memo),
            )
        return memo[uid]


def predicate_hash(p: Pred) -> str:
    """``predicate_hash`` of §8.14 for one filter."""
    return _CanonicalBDD(p).merkle()


def _ph(p: Pred | None) -> str | None:
    return None if p is None else predicate_hash(p)


# ---------------------------------------------------------------------------
# dag_hash / question_key
# ---------------------------------------------------------------------------


def _band_payload(q: BandQuery) -> tuple:
    return (
        _scan(q.anchor),
        tuple(_join(j) for j in q.joins),
        ("B1", _ph(q.where)),
        tuple(g.sql for g in q.group_by),
        tuple((n, e.sql) for n, e in q.aggregates),
        ("B2", _ph(q.having)),
        tuple((n, e.sql) for n, e in q.windows),
        ("B3", _ph(q.qualify)),
    )


def _node_hash(node: PlanNode, memo: dict[int, str]) -> str:
    key = id(node)
    if key in memo:
        return memo[key]
    if isinstance(node, BandQuery):
        h = canonical_hash("band", _band_payload(node))
    else:
        kids = [_node_hash(node.left, memo), _node_hash(node.right, memo)]
        if node.op in _COMMUTATIVE:
            kids.sort()
        h = canonical_hash("compose", node.op.value, _frag(node.on), tuple(kids))
    memo[key] = h
    return h


def dag_hash(node: PlanNode | PlanIR) -> str:
    """``dag_hash`` of §8.14 for a node (or for the root of a plan)."""
    root = node.root if isinstance(node, PlanIR) else node
    return _node_hash(root, {})


def _params(params: Mapping[str, object] | None) -> tuple:
    items = dict(params or {})
    for k in items:
        if not isinstance(k, str):
            raise TypeError(f"parameter names must be strings, got {k!r}")
    return tuple((k, items[k]) for k in sorted(items))


def question_key(
    ir: PlanIR,
    *,
    schema_version: str,
    params: Mapping[str, object] | None = None,
) -> str:
    """``question_key`` of §8.14.

    *schema_version* is the hash of ``G_S``, Σ and the mapping M (§2.6) and is
    supplied by the caller; it has no default, so a key is never computed
    against an unstated schema.  *params* are the resolved π bindings.
    """
    if not isinstance(schema_version, str) or not schema_version:
        raise ValueError("question_key needs a non-empty schema_version")
    return canonical_hash(CANON_VERSION, schema_version, dag_hash(ir), _params(params))


# ---------------------------------------------------------------------------
# deps(plan)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Dependency:
    """A source the plan can read; ``partitions=None`` means every partition."""

    relation: str
    partitions: tuple[str, ...] | None = None


class RelationCatalog(Protocol):
    def base_relations(self, name: str) -> frozenset[str] | None:
        """Base relations read through *name*; ``None`` if *name* is unknown.

        A base relation returns ``{name}``; a Derived view (§2.6) returns the
        base relations of its definition.
        """
        ...


@dataclass(frozen=True)
class StaticCatalog:
    """A :class:`RelationCatalog` from explicit declarations."""

    base: frozenset[str]
    derived: Mapping[str, frozenset[str]] = field(default_factory=dict)

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

    def base_relations(self, name: str) -> frozenset[str] | None:
        if name in self.base:
            return frozenset({name})
        if name in self.derived:
            return frozenset(self.derived[name])
        return None


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


def relations_read(ir: PlanIR) -> frozenset[str]:
    """Relation names the emitted SQL references (before view expansion)."""
    return frozenset(
        s.relation for n in _nodes(ir) if isinstance(n, BandQuery) for s in n.scans()
    )


def deps(ir: PlanIR, catalog: RelationCatalog) -> tuple[Dependency, ...]:
    """``deps(plan)`` of §8.14, sorted; an undeclared relation is an error."""
    out: set[Dependency] = set()
    for name in sorted(relations_read(ir)):
        bases = catalog.base_relations(name)
        if not bases:
            raise UnknownRelation(
                f"relation {name!r} is not declared by the catalog; the plan's "
                "dependencies cannot be determined"
            )
        out.update(Dependency(b) for b in bases)
    return tuple(sorted(out))
