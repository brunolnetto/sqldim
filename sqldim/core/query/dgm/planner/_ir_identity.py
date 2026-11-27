"""Canonical identity and dependencies of a plan IR (DGM §8.14).

* :func:`predicate_hash` — Merkle hash of a filter's BDD built under the
  canonical variable order: atoms ordered by the byte order of their
  canonical serialisation, ``H(atom, hash(low), hash(high))``.
* :func:`dag_hash` — Merkle hash of a plan node, ``H(operator, payload,
  hashes of its children)``; children are sorted by hash for the commutative
  operators (UNION, INTERSECT) and kept in order otherwise.  CTE names and
  other local handles never enter the hash.  For a whole plan with
  PreComputation steps, the steps' payloads, in order, are hashed with the
  root: an exact step and its approximation have different payloads.
  Repeat bounds and mode are part of the path payload, so plans that differ
  only in them never share a ``dag_hash``.
* :func:`question_key` — ``H(canon_version, schema_version, dag_hash(root),
  params)`` with ``params`` sorted by name.
* :func:`deps` — the source relations (and partitions) the plan can read;
  see :mod:`._deps`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.bdd import FALSE_NODE_ID, TRUE_NODE_ID, BDDManager
from sqldim.core.query.dgm.planner._canon import (
    CANON_VERSION,
    canonical_encode,
    canonical_hash,
)
from sqldim.core.query.dgm.planner._deps import (
    Dependency,
    RelationCatalog,
    StaticCatalog,
    deps,
    relations_read,
)
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Conj,
    Disj,
    Fragment,
    Join,
    Neg,
    PathExists,
    PathHop,
    PathRepeat,
    PlanIR,
    PlanNode,
    Pred,
    RawCondition,
    RepeatJoin,
    Scan,
    UnsupportedConstruct,
)
from sqldim.core.query.dgm.planner._steps import PreComputation

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


def _hop(h: PathHop) -> tuple:
    return ("hop", h.kind, h.label, _scan(h.scan), _frag(h.on))


def _repeat(r: PathRepeat) -> tuple:
    return (
        "repeat",
        r.start,
        tuple(_hop(h) for h in r.body),
        r.min,
        r.max,  # None = UNBOUNDED
        r.mode.value,
        r.depth_alias,
        r.start_relation,
    )


def _path_item(h: PathHop | PathRepeat) -> tuple:
    return _repeat(h) if isinstance(h, PathRepeat) else _hop(h)


def _join(j: Join | RepeatJoin) -> tuple:
    if isinstance(j, RepeatJoin):
        return ("repeat_join", _repeat(j.repeat))
    v = j.version
    ver = None if v is None else ("version", v.kind.value, v.as_of)
    return ("join", _scan(j.scan), _frag(j.on), ver)


def _atom(p: Pred) -> tuple:
    if isinstance(p, Compare):
        return ("cmp", p.lhs.sql, p.op, p.rhs.value)
    if isinstance(p, RawCondition):
        return ("raw", p.fragment.sql)
    if isinstance(p, PathExists):
        hops = tuple(_path_item(h) for h in p.hops)
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


def _step(pc: PreComputation) -> tuple:
    reads = None if pc.reads is None else tuple(sorted(pc.reads))
    return ("precompute", pc.name, pc.kind, pc.query, reads, pc.accuracy)


def dag_hash(node: PlanNode | PlanIR) -> str:
    """``dag_hash`` of §8.14 for a node, or for a plan: its root and steps."""
    if not isinstance(node, PlanIR):
        return _node_hash(node, {})
    root = _node_hash(node.root, {})
    if not node.pre_compute:
        return root
    return canonical_hash("plan", root, tuple(_step(pc) for pc in node.pre_compute))


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
