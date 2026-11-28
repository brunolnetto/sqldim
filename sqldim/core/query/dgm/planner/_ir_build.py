"""Lowering of DGM queries to the physical plan IR (DGM §6.2; issue #148).

:func:`lower_query` lowers one :class:`DGMQuery` to a :class:`BandQuery`;
:func:`lower_algebra` lowers the sub-DAG of a :class:`QuestionAlgebra` that a
final CTE depends on to a :class:`PlanIR`.  Identical sub-plans become a
single node (the sharing rule), so each is emitted once.

Lowering fails closed: any construct whose SQL meaning or read set the IR
cannot state exactly raises :class:`UnsupportedConstruct`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Compose,
    Conj,
    Cte,
    Disj,
    DrillAcross,
    Fragment,
    Join,
    Literal,
    Neg,
    PathExists,
    PathHop,
    PathRepeat,
    PlanIR,
    PlanNode,
    Pred,
    Quantifier,
    RawCondition,
    ReadRole,
    RepeatJoin,
    Scan,
    UnsupportedConstruct,
    VersionKind,
    VersionResolution,
    check_fragment,
    check_identifier,
    check_relation_name,
    rename_aliases,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.algebra import QuestionAlgebra
    from sqldim.core.query.dgm.core import DGMQuery
    from sqldim.core.query.dgm.model import DGMModel
    from sqldim.core.query.dgm.preds import PathPred, Repeat, ScalarPred

__all__ = ["NodeIdentities", "lower_query", "lower_algebra", "lower_pred"]

_OPS = frozenset(
    {"=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "ILIKE",
     "NOT ILIKE", "IS", "IS NOT"}
)  # fmt: skip


# ---------------------------------------------------------------------------
# Node identities from a declared model (§2.6, §3.3)
# ---------------------------------------------------------------------------

#: Prefix of the names the emitter introduces (recursive CTEs, their columns
#: and aliases); an alias of the query may not start with it.
RESERVED_PREFIX = "__dgm"


class NodeIdentities:
    """Entity keys and hop weights that a declared :class:`DGMModel` states.

    A relation has a key when exactly one node type maps to it with a
    ``NodeTable`` that has no ``version_key`` (one row per entity, §2.6);
    a versioned or unmapped relation has none.  A verb hop counts 1 in
    ``weight(π)``; a bridge hop counts 1 when every bridge edge type with
    its label is declared unweighted (§3.3).
    """

    def __init__(self, model: "DGMModel") -> None:
        from sqldim.core.query.dgm.model import DGMModel

        if not isinstance(model, DGMModel):
            raise UnsupportedConstruct("node identities need a declared DGMModel")
        self.model = model

    def key(self, relation: str) -> tuple[str, ...]:
        tables = [
            n.mapping
            for n in self.model.node_types
            if n.mapping is not None and n.mapping.relation == relation
        ]
        if len(tables) != 1 or tables[0].version_key is not None:
            return ()
        return tuple(check_identifier(c, "entity key") for c in tables[0].entity_key)

    def weighted(self, kind: str, label: str) -> bool | None:
        from sqldim.core.query.dgm.model import EdgeKind

        if kind in ("verb", "verb_inverse"):
            return False
        edges = [
            e
            for e in self.model.edge_types
            if e.kind is EdgeKind.BRIDGE and e.label == label
        ]
        return any(e.weighted for e in edges) if edges else None


@dataclass(frozen=True)
class _Ctx:
    ids: NodeIdentities | None = None
    relations: Mapping[str, str] = field(default_factory=dict)


_CTX: ContextVar[_Ctx] = ContextVar("dgm_lowering", default=_Ctx())


@contextmanager
def _context(**changes: object) -> Iterator[None]:
    token = _CTX.set(replace(_CTX.get(), **changes))  # type: ignore[arg-type]
    try:
        yield
    finally:
        _CTX.reset(token)


def _identify(h: PathHop) -> PathHop:
    """*h* with the node identity and weight the model declares, if any."""
    ids = _CTX.get().ids
    if ids is None:
        return h
    return replace(
        h, key=ids.key(h.scan.relation), weighted=ids.weighted(h.kind, h.label)
    )


def _identify_repeat(r: PathRepeat) -> PathRepeat:
    return replace(r, body=tuple(_identify(h) for h in r.body))


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------


def _literal(value: object) -> Literal:
    if value is None or isinstance(value, (bool, int, str)):
        return Literal(value)
    if isinstance(value, float) and math.isfinite(value):
        return Literal(value)
    raise UnsupportedConstruct(
        f"comparison value {value!r} ({type(value).__name__}) has no literal form"
    )


def _ref_fragment(ref: object, scope: frozenset[str]) -> Fragment:
    from sqldim.core.query.dgm.refs import (
        AggRef,
        ArithExpr,
        PropRef,
        WinRef,
        _ConstExpr,
    )

    if isinstance(ref, (PropRef, AggRef, WinRef, ArithExpr, _ConstExpr)):
        return check_fragment(ref.to_sql(), scope)
    raise UnsupportedConstruct(
        f"{type(ref).__name__} is not a comparable reference in this IR version"
    )


def _lower_compare(p: "ScalarPred", scope: frozenset[str]) -> Compare:
    if p.op not in _OPS:
        raise UnsupportedConstruct(f"comparison operator {p.op!r} is not supported")
    return Compare(_ref_fragment(p.ref, scope), p.op, _literal(p.value))


def _check_alias(alias: object) -> str:
    name = check_identifier(alias, "alias")  # type: ignore[arg-type]
    if name.startswith(RESERVED_PREFIX):
        raise UnsupportedConstruct(
            f"alias {name!r} starts with {RESERVED_PREFIX!r}, which the emitter "
            "reserves for its own names"
        )
    return name


def _lower_hop(h: object, visible: set[str], role: ReadRole) -> PathHop:
    """Lower one hop; its alias joins *visible* (in place)."""
    from sqldim.core.query.dgm.preds import _HopBase

    if not isinstance(h, _HopBase):
        raise UnsupportedConstruct(f"{type(h).__name__} is not a path hop")
    if h.table is None or h.on is None:
        raise UnsupportedConstruct(
            f"hop {h.label!r} needs an explicit table and on clause"
        )
    alias = _check_alias(h.to_alias)
    if alias in visible:
        raise UnsupportedConstruct(f"path alias {alias!r} shadows an alias in scope")
    visible.add(alias)
    scan = Scan(check_relation_name(h.table), alias, role)
    on = check_fragment(h.on, frozenset(visible))
    return PathHop(getattr(h, "kind", "hop"), h.label, scan, on)


def _lower_repeat(
    r: "Repeat", visible: set[str], role: ReadRole, start_relation: str | None
) -> PathRepeat:
    """Lower a Repeat (§3.2); its body aliases join *visible* (in place)."""
    from sqldim.core.query.dgm.preds import _flatten_path

    start = check_identifier(r.from_alias, "alias")
    if start not in visible:
        raise UnsupportedConstruct(f"Repeat starts from {start!r}, not in scope")
    body = tuple(_lower_hop(h, visible, role) for h in _flatten_path(r.path))
    unrolled = re.compile(rf"^{re.escape(body[-1].scan.alias)}__r\d+$")
    clash = sorted(a for a in visible if unrolled.match(a))
    if clash:
        raise UnsupportedConstruct(
            f"aliases {clash} clash with Repeat iteration aliases"
        )
    depth = r.depth_alias
    if depth is not None:
        check_identifier(depth, "depth alias")
    upper = r.max if r.bounded else None
    return PathRepeat(start, body, r.min, upper, r.mode, depth, start_relation)  # type: ignore[arg-type]


def _lower_hops(
    path: object, scope: frozenset[str], keyed: bool
) -> tuple[PathHop | PathRepeat, ...]:
    """The hops of a PathPred; with *keyed*, each carries its node identity."""
    from sqldim.core.query.dgm.planner._ir_emit import unroll_refusal
    from sqldim.core.query.dgm.preds import Repeat, _flatten_path

    hops: list[PathHop | PathRepeat] = []
    visible = set(scope)
    relations: dict[str, str] = dict(_CTX.get().relations)
    for h in _flatten_path(path):
        if isinstance(h, Repeat):
            start = relations.get(h.from_alias)
            rep = _lower_repeat(h, visible, ReadRole.REPEAT, start)
            if keyed or unroll_refusal(rep) is not None:
                rep = _identify_repeat(rep)
            hops.append(rep)
            continue
        hop = _lower_hop(h, visible, ReadRole.EXISTS_PATH)
        relations[hop.scan.alias] = hop.scan.relation
        hops.append(_identify(hop) if keyed else hop)
    if not hops:
        raise UnsupportedConstruct("a PathPred needs at least one hop")
    return tuple(hops)


def _strategy(pp: "PathPred") -> tuple[str, int | None]:
    """The §3.3 strategy of *pp* and its k."""
    from sqldim.core.query.dgm.preds import ALL, K_SHORTEST, MIN_WEIGHT, SHORTEST

    s = pp.strategy
    if s is None or isinstance(s, ALL):
        return "ALL", None
    if isinstance(s, K_SHORTEST):
        k = s.k
        if isinstance(k, bool) or not isinstance(k, int) or k < 1:
            raise UnsupportedConstruct(f"K_SHORTEST needs k ≥ 1 (§3.3), got {k!r}")
        return "K_SHORTEST", k
    if isinstance(s, (SHORTEST, MIN_WEIGHT)):
        return type(s).__name__, None
    raise UnsupportedConstruct(f"strategy {type(s).__name__} is not one of §3.3")


def _lower_path(pp: "PathPred", scope: frozenset[str]) -> PathExists:
    from sqldim.core.query.dgm.preds import Quantifier as _Q
    from sqldim.core.query.dgm.preds._positions import trace_mode, without_mode

    if pp.promote:
        raise UnsupportedConstruct("PathPred(promote=True) is not supported")
    strategy, k = _strategy(pp)
    if strategy != "ALL" and pp.temporal_mode is not None:
        raise UnsupportedConstruct(
            f"a temporal mode with strategy {strategy}: §4.1 defines T(s) over "
            "every instance of the path, and does not state whether a strategy "
            "selects instances before their traces are formed"
        )
    mode = trace_mode(pp)
    if mode is None:
        # a mode over one position per instance is lowered by its mode-free
        # form, as to_sql() renders it
        pp = without_mode(pp)
    quant = Quantifier.FORALL if pp.quantifier is _Q.FORALL else Quantifier.EXISTS
    hops = _lower_hops(pp.path, scope, keyed=strategy != "ALL" or mode is not None)
    inner = frozenset(_path_aliases(hops)) | scope
    sub = lower_pred(pp.sub_filter, inner)
    until = None
    if mode == "UNTIL":
        until = lower_pred(pp.temporal_mode.pred, inner)  # type: ignore[attr-defined]
    return PathExists(quant, strategy, hops, sub, k, mode, until)


def _path_aliases(hops: tuple[PathHop | PathRepeat, ...]) -> list[str]:
    out: list[str] = []
    for h in hops:
        body = h.body if isinstance(h, PathRepeat) else (h,)
        out += [b.scan.alias for b in body]
    return out


def _lower_list(preds: tuple, scope: frozenset[str], what: str) -> tuple[Pred, ...]:
    if not preds:
        raise UnsupportedConstruct(f"an empty {what} has no SQL form")
    return tuple(lower_pred(s, scope) for s in preds)


def lower_pred(p: object, scope: frozenset[str]) -> Pred:
    """Lower a DGM predicate tree; *scope* is the set of visible aliases."""
    from sqldim.core.query.dgm.preds import AND, NOT, OR, PathPred, RawPred, ScalarPred

    if isinstance(p, ScalarPred):
        return _lower_compare(p, scope)
    if isinstance(p, AND):
        return Conj(_lower_list(p.preds, scope, "AND"))
    if isinstance(p, OR):
        return Disj(_lower_list(p.preds, scope, "OR"))
    if isinstance(p, NOT):
        return Neg(lower_pred(p.pred, scope))
    if isinstance(p, RawPred):
        return RawCondition(check_fragment(p.to_sql(), scope))
    if isinstance(p, str):
        return RawCondition(check_fragment(p, scope))
    if isinstance(p, PathPred):
        return _lower_path(p, scope)
    raise UnsupportedConstruct(f"{type(p).__name__} is not lowered by this IR version")


# ---------------------------------------------------------------------------
# DGMQuery → BandQuery
# ---------------------------------------------------------------------------


class _Scope:
    def __init__(self) -> None:
        self.aliases: list[str] = []
        self.relations: dict[str, str] = {}

    def add(self, alias: str, relation: str | None = None) -> frozenset[str]:
        _check_alias(alias)
        if alias in self.aliases:
            raise UnsupportedConstruct(f"alias {alias!r} is bound twice")
        self.aliases.append(alias)
        if relation is not None:
            self.relations[alias] = relation
        return self.frozen()

    def frozen(self) -> frozenset[str]:
        return frozenset(self.aliases)


def _repeat_join(q: "DGMQuery", r: "Repeat", scope: _Scope) -> RepeatJoin:
    if q._as_of:
        raise UnsupportedConstruct(
            "a Repeat PathJoin under as_of needs version resolution per "
            "iteration, which this IR version does not emit"
        )
    from sqldim.core.query.dgm.planner._ir_emit import unroll_refusal

    visible = set(scope.aliases)
    start = scope.relations.get(r.from_alias)
    repeat = _lower_repeat(r, visible, ReadRole.REPEAT, start)
    if unroll_refusal(repeat) is not None:  # emitted recursively: needs keys
        repeat = _identify_repeat(repeat)
    scope.add(repeat.end, repeat.body[-1].scan.relation)  # body-local otherwise
    return RepeatJoin(repeat)


def _path_joins(q: "DGMQuery", scope: _Scope) -> list[Join | RepeatJoin]:
    from sqldim.core.query.dgm.preds import Repeat

    version = VersionResolution(VersionKind.VALID_RANGE, q._as_of) if q._as_of else None
    out: list[Join | RepeatJoin] = []
    for entry in q._joins:
        if isinstance(entry, Repeat):
            out.append(_repeat_join(q, entry, scope))
            continue
        table, alias, on = entry
        rel = check_relation_name(table)
        visible = scope.add(alias, rel)
        scan = Scan(rel, alias, ReadRole.PATH_JOIN)
        out.append(Join(scan, check_fragment(on, visible), version))
    return out


def _dim_joins(
    q: "DGMQuery", anchor_alias: str, scope: _Scope
) -> list[Join | RepeatJoin]:
    """The joins of every dimension join, as ``DGMQuery.to_sql()`` renders them."""
    from sqldim.core.query.dgm._dim_joins import AT_TAU, CURRENT, dim_join_steps

    versions = {
        CURRENT: VersionResolution(VersionKind.IS_CURRENT),
        AT_TAU: VersionResolution(VersionKind.VALID_RANGE, q._as_of),
        None: None,
    }
    out: list[Join | RepeatJoin] = []
    for j in q._scd2_joins:
        for column in (j.fact_fk, j.dim_pk, j.version_key):
            if column is not None:
                check_identifier(column, "column")
        for step in dim_join_steps(j, anchor_alias, q._as_of):
            visible = scope.add(step.alias)
            on = check_fragment(step.on, visible)
            scan = Scan(
                check_relation_name(step.table), step.alias, ReadRole.VERSIONED_JOIN
            )
            out.append(Join(scan, on, versions[step.version]))
    return out


def _named(exprs: dict[str, str], scope: frozenset[str]) -> tuple:
    return tuple(
        (check_identifier(n, "output name"), check_fragment(e, scope))
        for n, e in exprs.items()
    )


def _opt_pred(p: object, scope: frozenset[str]) -> Pred | None:
    return None if p is None else lower_pred(p, scope)


def lower_query(
    q: "DGMQuery", model: "DGMModel | None" = None
) -> BandQuery | DrillAcross:
    """Lower one three-band query to a :class:`BandQuery`, or a declared
    constellation (``drill_across``) to a :class:`DrillAcross`.

    *model* is the declared model of the data; it supplies the node
    identities that SIMPLE and TRAIL repetition, the traces of a temporal
    mode over a Repeat and the order ≺ of a strategy need (§2.6, §3.2, §3.3).
    Without it those constructs are refused.
    """
    ids = None if model is None else NodeIdentities(model)
    with _context(ids=ids):
        return _lower_query(q)


def _lower_query(q: "DGMQuery") -> BandQuery | DrillAcross:
    from sqldim.core.query.dgm._query_helpers import _validate_bands

    _validate_bands(q)  # same construction errors as DGMQuery.to_sql()
    if q._temporal_context is not None:
        raise UnsupportedConstruct(
            "TemporalContext is not lowered by this IR version; DGMQuery.to_sql() "
            "ignores it, so no plan is produced rather than one that drops it"
        )
    if q._branches:
        return _lower_drill(q)
    return _lower_band(q)


def _lower_band(q: "DGMQuery") -> BandQuery:
    table = check_relation_name(q._anchor_table)
    anchor_alias = q._anchor_alias or table
    scope = _Scope()
    scope.add(anchor_alias, table)
    anchor = Scan(table, anchor_alias, ReadRole.ANCHOR)
    joins = _path_joins(q, scope) + _dim_joins(q, anchor_alias, scope)
    with _context(relations=dict(scope.relations)):
        return _band(q, anchor, joins, scope.frozen())


def _band(q: "DGMQuery", anchor: Scan, joins: list, s: frozenset[str]) -> BandQuery:
    from sqldim.core.query.dgm._query_helpers import rendered_aggregates

    return BandQuery(
        anchor=anchor,
        joins=tuple(joins),
        where=_opt_pred(q._where_pred, s),
        group_by=tuple(check_fragment(g, s) for g in q._group_by_cols),
        aggregates=_named(rendered_aggregates(q), s),
        having=_opt_pred(q._having_pred, s),
        windows=_named(q._window_exprs, s),
        qualify=_opt_pred(q._qualify_pred, s),
    )


def _lower_drill(q: "DGMQuery") -> DrillAcross:
    """§5.2.2 steps 1–3; DA1–DA3 are checked as ``DGMQuery.to_sql()`` does.

    The branches are put in the byte order of their ``dag_hash`` and the
    ``b<i>`` references of step 3 renumbered to match, so the identity of
    the node does not depend on the order the branches were declared in.
    """
    from sqldim.core.query.dgm._drill_across import drill_plan
    from sqldim.core.query.dgm.planner._ir_identity import dag_hash

    plan = drill_plan(q)
    bands = [_lower_band(b) for b in plan.branches]
    order = sorted(range(len(bands)), key=lambda i: dag_hash(bands[i]))
    rename = {f"b{old}": f"b{new}" for new, old in enumerate(order)}
    refs = frozenset(rename)
    select = tuple(
        (
            check_identifier(name, "output name"),
            rename_aliases(check_fragment(expr, refs), rename),
        )
        for name, expr in plan.outer
    )
    for name in plan.key_names:
        check_identifier(name, "output name")
    return DrillAcross(
        branches=tuple(bands[i] for i in order),
        key_names=plan.key_names,
        select=select,
        having=_opt_pred(plan.having, frozenset({"da"})),
    )


# ---------------------------------------------------------------------------
# QuestionAlgebra → PlanIR
# ---------------------------------------------------------------------------

_SET_BODIES = {
    ComposeOp.UNION: "SELECT * FROM {l}\nUNION\nSELECT * FROM {r}",
    ComposeOp.INTERSECT: "SELECT * FROM {l}\nINTERSECT\nSELECT * FROM {r}",
    ComposeOp.EXCEPT: "SELECT * FROM {l}\nEXCEPT\nSELECT * FROM {r}",
    ComposeOp.WITH: "SELECT * FROM {r}",
}
_JOIN_PREFIX = "SELECT l.*, r.*\nFROM {l} l\nJOIN {r} r ON "


def _join_on(sql: str, left: str, right: str) -> Fragment:
    prefix = _JOIN_PREFIX.format(l=left, r=right)
    if not sql.startswith(prefix):
        raise UnsupportedConstruct("JOIN CTE body was not produced by compose()")
    return check_fragment(sql[len(prefix) :], frozenset({"l", "r"}))


class _AlgebraLowering:
    def __init__(self, alg: "QuestionAlgebra") -> None:
        self.alg = alg
        self.order: list[Cte] = []
        self.by_name: dict[str, PlanNode] = {}
        self.known: dict[PlanNode, PlanNode] = {}  # sharing rule

    def node(self, name: str) -> PlanNode:
        if name not in self.by_name:
            self.by_name[name] = self._share(name, self._build(name))
        return self.by_name[name]

    def _share(self, name: str, n: PlanNode) -> PlanNode:
        if n not in self.known:
            self.known[n] = n
            self.order.append(Cte(check_identifier(name, "CTE name"), n))
        return self.known[n]

    def _build(self, name: str) -> PlanNode:
        if name not in self.alg._ctes:
            raise UnsupportedConstruct(f"CTE {name!r} is not registered")
        cq = self.alg._ctes[name]
        if name not in self.alg._composition:
            if cq.query is None:
                raise UnsupportedConstruct(
                    f"CTE {name!r} is raw SQL; its read set cannot be determined"
                )
            return _lower_query(cq.query)
        left, op, right = self.alg._composition[name]
        sql = cq._sql or ""
        on = None
        if op is ComposeOp.JOIN:
            on = _join_on(sql, left, right)
        elif sql != _SET_BODIES[op].format(l=left, r=right):
            raise UnsupportedConstruct(
                f"CTE {name!r} body was not produced by compose()"
            )
        return Compose(op, self.node(left), self.node(right), on)


def lower_algebra(
    alg: "QuestionAlgebra",
    final: str,
    pre_compute: tuple = (),
    model: "DGMModel | None" = None,
) -> PlanIR:
    """Lower the CTEs *final* depends on; unreferenced CTEs are not emitted.

    *model* is as for :func:`lower_query`.
    """
    lowering = _AlgebraLowering(alg)
    ids = None if model is None else NodeIdentities(model)
    with _context(ids=ids):
        root = lowering.node(final)
    return PlanIR(root=root, ctes=tuple(lowering.order), pre_compute=pre_compute)
