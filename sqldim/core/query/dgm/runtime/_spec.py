"""Question specs: the serialisable form of a plan IR (§11.10 ``QueryCandidate.spec``).

A *spec* is plain JSON data — dicts, lists, strings, numbers, booleans and
``None`` — that describes one question completely: its plan IR (§6.2,
#148) and its resolved parameters π (§8.14).  It is what a candidate carries
before any DAG exists, what the checkpointed state can hold, and what the
question store keeps under a ``question_key``.

Decoding re-admits every name and SQL fragment through the same checks the
planner's lowering applies (:func:`check_identifier`,
:func:`check_relation_name`, :func:`check_fragment`), so a spec cannot smuggle
in a relation read that is not a declared :class:`Scan`: ``deps(plan)`` of a
decoded spec is as exact as that of a lowered query.  Unknown keys, unknown
tags and missing fields are errors, never ignored.

Format ``dgm-question/1``::

    {"format": "dgm-question/1",
     "nodes":  [<node>, ...],          # topological order; children by index
     "root":   <index>,
     "ctes":   [[<name>, <index>], ...],
     "params": {<name>: <scalar or list>, ...}}
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from typing import Any

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.planner._canon import canonical_hash
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
)

__all__ = [
    "SPEC_FORMAT",
    "InvalidSpec",
    "QuestionSpec",
    "encode_spec",
    "decode_spec",
    "spec_hash",
    "decode_pred",
    "encode_pred",
    "band_coverage",
]

SPEC_FORMAT = "dgm-question/1"

_OPS = frozenset(
    {"=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "ILIKE",
     "NOT ILIKE", "IS", "IS NOT"}
)  # fmt: skip


class InvalidSpec(UnsupportedConstruct):
    """A spec that is malformed or describes a construct the IR rejects."""


class QuestionSpec:
    """A decoded spec: the plan IR and its parameters."""

    __slots__ = ("ir", "params")

    def __init__(self, ir: PlanIR, params: Mapping[str, object]) -> None:
        self.ir = ir
        self.params = dict(params)


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def _scan_out(s: Scan) -> dict:
    return {"relation": s.relation, "alias": s.alias, "role": s.role.value}


def _no_repeat(item: object) -> InvalidSpec:
    return InvalidSpec(
        f"{type(item).__name__} (Repeat, §3.2) has no form in spec format "
        f"{SPEC_FORMAT}; a question with Repeat cannot be carried as a spec yet"
    )


def _join_out(j: Join | RepeatJoin) -> dict:
    if not isinstance(j, Join):
        raise _no_repeat(j)
    v = j.version
    ver = None if v is None else {"kind": v.kind.value, "as_of": v.as_of}
    return {"scan": _scan_out(j.scan), "on": j.on.sql, "version": ver}


def _hop_out(h: PathHop | PathRepeat) -> dict:
    if not isinstance(h, PathHop):
        raise _no_repeat(h)
    if h.key or h.weighted is not None:
        raise InvalidSpec(
            f"node identities of a hop have no form in spec format {SPEC_FORMAT}"
        )
    return {"kind": h.kind, "label": h.label, "scan": _scan_out(h.scan), "on": h.on.sql}


def _pred_out(p: Pred | None) -> dict | None:
    if p is None:
        return None
    if isinstance(p, Compare):
        return {"cmp": [p.lhs.sql, p.op, p.rhs.value]}
    if isinstance(p, RawCondition):
        return {"raw": p.fragment.sql}
    if isinstance(p, (Conj, Disj)):
        tag = "and" if isinstance(p, Conj) else "or"
        return {tag: [_pred_out(s) for s in p.preds]}
    if isinstance(p, Neg):
        return {"not": _pred_out(p.pred)}
    return _path_out(p)


def _path_out(p: PathExists) -> dict:
    if p.strategy != "ALL" or p.mode is not None:
        raise InvalidSpec(
            f"a PathPred with strategy {p.strategy} or a temporal mode over a "
            f"Repeat has no form in spec format {SPEC_FORMAT}"
        )
    return {
        "path": {
            "quantifier": p.quantifier.value,
            "strategy": p.strategy,
            "hops": [_hop_out(h) for h in p.hops],
            "filter": _pred_out(p.sub_filter),
        }
    }


def _band_out(q: BandQuery) -> dict:
    return {
        "type": "band",
        "anchor": _scan_out(q.anchor),
        "joins": [_join_out(j) for j in q.joins],
        "where": _pred_out(q.where),
        "group_by": [g.sql for g in q.group_by],
        "aggregates": [[n, e.sql] for n, e in q.aggregates],
        "having": _pred_out(q.having),
        "windows": [[n, e.sql] for n, e in q.windows],
        "qualify": _pred_out(q.qualify),
    }


def _drill_out(d: DrillAcross) -> dict:
    return {
        "type": "drill_across",
        "branches": [_band_out(b) for b in d.branches],
        "keys": list(d.key_names),
        "select": [[n, e.sql] for n, e in d.select],
        "having": _pred_out(d.having),
    }


class _Encoder:
    def __init__(self) -> None:
        self.nodes: list[dict] = []
        self.index: dict[PlanNode, int] = {}

    def node(self, n: PlanNode) -> int:
        if n in self.index:
            return self.index[n]
        if isinstance(n, BandQuery):
            out = _band_out(n)
        elif isinstance(n, DrillAcross):
            out = _drill_out(n)
        else:
            left, right = self.node(n.left), self.node(n.right)
            on = None if n.on is None else n.on.sql
            out = {"type": "compose", "op": n.op.value, "left": left, "right": right}
            out["on"] = on
        self.index[n] = len(self.nodes)
        self.nodes.append(out)
        return self.index[n]


def encode_spec(ir: PlanIR, params: Mapping[str, object] | None = None) -> dict:
    """The spec of *ir* with parameters *params*."""
    if ir.pre_compute:
        raise InvalidSpec(
            f"PreComputation steps have no form in spec format {SPEC_FORMAT}; "
            "a plan with pre_compute cannot be carried as a spec yet"
        )
    enc = _Encoder()
    ctes = [[c.name, enc.node(c.node)] for c in ir.ctes]
    root = enc.node(ir.root)
    items = dict(params or {})
    for k in items:
        if not isinstance(k, str):
            raise InvalidSpec(f"parameter names must be strings, got {k!r}")
    return {
        "format": SPEC_FORMAT,
        "nodes": enc.nodes,
        "root": root,
        "ctes": ctes,
        "params": {k: items[k] for k in sorted(items)},
    }


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _obj(value: object, keys: frozenset[str], what: str) -> dict:
    if not isinstance(value, dict):
        raise InvalidSpec(f"{what} must be an object, got {type(value).__name__}")
    if set(value) != keys:
        raise InvalidSpec(
            f"{what} has keys {sorted(value)}, expected exactly {sorted(keys)}"
        )
    return value


def _list(value: object, what: str) -> list:
    if not isinstance(value, list):
        raise InvalidSpec(f"{what} must be a list, got {type(value).__name__}")
    return value


def _str(value: object, what: str) -> str:
    if not isinstance(value, str):
        raise InvalidSpec(f"{what} must be a string, got {type(value).__name__}")
    return value


def _enum(enum: Any, value: object, what: str) -> Any:
    try:
        return enum(_str(value, what))
    except ValueError:
        raise InvalidSpec(f"{what} {value!r} is not one of {[e.value for e in enum]}")


_SCAN_KEYS = frozenset({"relation", "alias", "role"})
_JOIN_KEYS = frozenset({"scan", "on", "version"})
_HOP_KEYS = frozenset({"kind", "label", "scan", "on"})
_PATH_KEYS = frozenset({"quantifier", "strategy", "hops", "filter"})
_BAND_KEYS = frozenset(
    {"type", "anchor", "joins", "where", "group_by", "aggregates", "having",
     "windows", "qualify"}
)  # fmt: skip
_COMPOSE_KEYS = frozenset({"type", "op", "left", "right", "on"})
_DRILL_KEYS = frozenset({"type", "branches", "keys", "select", "having"})
_TOP_KEYS = frozenset({"format", "nodes", "root", "ctes", "params"})


def _scan_in(value: object, role: ReadRole | None = None) -> Scan:
    d = _obj(value, _SCAN_KEYS, "scan")
    r = _enum(ReadRole, d["role"], "read role")
    if role is not None and r is not role:
        raise InvalidSpec(
            f"a scan in this position has role {role.value}, not {r.value}"
        )
    return Scan(
        check_relation_name(d["relation"]), check_identifier(d["alias"], "alias"), r
    )


def _version_in(value: object) -> VersionResolution | None:
    if value is None:
        return None
    d = _obj(value, frozenset({"kind", "as_of"}), "version")
    kind = _enum(VersionKind, d["kind"], "version kind")
    as_of = d["as_of"]
    if (kind is VersionKind.IS_CURRENT) != (as_of is None):
        raise InvalidSpec("IS_CURRENT has no as_of; VALID_RANGE requires one")
    return VersionResolution(kind, None if as_of is None else _str(as_of, "as_of"))


class _Scope:
    """Aliases bound so far in one band query (each bound once)."""

    def __init__(self) -> None:
        self.aliases: set[str] = set()

    def bind(self, alias: str) -> frozenset[str]:
        if alias in self.aliases:
            raise InvalidSpec(f"alias {alias!r} is bound twice")
        self.aliases.add(alias)
        return frozenset(self.aliases)


def _literal_in(value: object) -> Literal:
    if isinstance(value, float) and not math.isfinite(value):
        raise InvalidSpec(f"comparison value {value!r} is not finite")
    if value is None or isinstance(value, (bool, int, float, str)):
        return Literal(value)
    raise InvalidSpec(f"comparison value {value!r} has no literal form")


def _compare_in(value: object, scope: frozenset[str]) -> Compare:
    parts = _list(value, "cmp")
    if len(parts) != 3:
        raise InvalidSpec("cmp is [lhs, op, value]")
    lhs, op, rhs = parts
    if op not in _OPS:
        raise InvalidSpec(f"comparison operator {op!r} is not supported")
    if rhs is None and op not in ("IS", "IS NOT"):
        raise InvalidSpec(f"cmp [{lhs!r}, {op!r}, null]: NULL is not a literal (§4.1)")
    return Compare(check_fragment(lhs, scope), op, _literal_in(rhs))


def _hops_in(value: object, scope: frozenset[str]) -> tuple[PathHop, ...]:
    visible = set(scope)
    hops = []
    for raw in _list(value, "hops"):
        d = _obj(raw, _HOP_KEYS, "hop")
        scan = _scan_in(d["scan"], ReadRole.EXISTS_PATH)
        if scan.alias in visible:
            raise InvalidSpec(f"path alias {scan.alias!r} shadows an alias in scope")
        visible.add(scan.alias)
        on = check_fragment(d["on"], frozenset(visible))
        hops.append(
            PathHop(_str(d["kind"], "hop kind"), _str(d["label"], "label"), scan, on)
        )
    if not hops:
        raise InvalidSpec("a path needs at least one hop")
    return tuple(hops)


def _path_in(value: object, scope: frozenset[str]) -> PathExists:
    d = _obj(value, _PATH_KEYS, "path")
    if d["strategy"] != "ALL":
        raise InvalidSpec("only the ALL path strategy is executable in this IR version")
    hops = _hops_in(d["hops"], scope)
    inner = scope | {h.scan.alias for h in hops}
    sub = _pred_in(d["filter"], inner)
    return PathExists(
        _enum(Quantifier, d["quantifier"], "quantifier"), "ALL", hops, sub
    )


def _preds_in(value: object, scope: frozenset[str], what: str) -> tuple[Pred, ...]:
    items = _list(value, what)
    if not items:
        raise InvalidSpec(f"an empty {what} has no SQL form")
    return tuple(_pred_in(v, scope) for v in items)


_PRED_DECODERS: dict[str, Callable[[object, frozenset[str]], Pred]] = {
    "cmp": _compare_in,
    "raw": lambda v, s: RawCondition(check_fragment(v, s)),
    "and": lambda v, s: Conj(_preds_in(v, s, "and")),
    "or": lambda v, s: Disj(_preds_in(v, s, "or")),
    "not": lambda v, s: Neg(_pred_in(v, s)),
    "path": _path_in,
}


def _pred_in(value: object, scope: frozenset[str]) -> Pred:
    if not isinstance(value, dict) or len(value) != 1:
        raise InvalidSpec(f"a predicate is a one-key object, got {value!r}")
    ((tag, body),) = value.items()
    if tag not in _PRED_DECODERS:
        raise InvalidSpec(f"unknown predicate tag {tag!r}")
    return _PRED_DECODERS[tag](body, scope)


def _opt_pred_in(value: object, scope: frozenset[str]) -> Pred | None:
    return None if value is None else _pred_in(value, scope)


def _named_in(value: object, scope: frozenset[str], what: str) -> tuple:
    out = []
    for pair in _list(value, what):
        if not isinstance(pair, list) or len(pair) != 2:
            raise InvalidSpec(f"{what} entries are [name, expression]")
        out.append(
            (check_identifier(pair[0], "output name"), check_fragment(pair[1], scope))
        )
    return tuple(out)


def _joins_in(value: object, scope: _Scope) -> tuple[Join, ...]:
    joins = []
    for raw in _list(value, "joins"):
        d = _obj(raw, _JOIN_KEYS, "join")
        scan = _scan_in(d["scan"])
        if scan.role not in (ReadRole.PATH_JOIN, ReadRole.VERSIONED_JOIN):
            raise InvalidSpec(f"a join scan cannot have role {scan.role.value}")
        visible = scope.bind(scan.alias)
        joins.append(
            Join(scan, check_fragment(d["on"], visible), _version_in(d["version"]))
        )
    return tuple(joins)


def _band_in(d: dict) -> BandQuery:
    _obj(d, _BAND_KEYS, "band node")
    anchor = _scan_in(d["anchor"], ReadRole.ANCHOR)
    scope = _Scope()
    scope.bind(anchor.alias)
    joins = _joins_in(d["joins"], scope)
    s = frozenset(scope.aliases)
    return BandQuery(
        anchor=anchor,
        joins=joins,
        where=_opt_pred_in(d["where"], s),
        group_by=tuple(check_fragment(g, s) for g in _list(d["group_by"], "group_by")),
        aggregates=_named_in(d["aggregates"], s, "aggregates"),
        having=_opt_pred_in(d["having"], s),
        windows=_named_in(d["windows"], s, "windows"),
        qualify=_opt_pred_in(d["qualify"], s),
    )


def _drill_in(d: dict) -> DrillAcross:
    _obj(d, _DRILL_KEYS, "drill_across node")
    branches = []
    for raw in _list(d["branches"], "branches"):
        if not isinstance(raw, dict) or raw.get("type") != "band":
            raise InvalidSpec("a drill-across branch is a band node")
        branches.append(_band_in(raw))
    refs = frozenset(f"b{i}" for i in range(len(branches)))
    keys = tuple(
        check_identifier(_str(k, "key name"), "output name")
        for k in _list(d["keys"], "keys")
    )
    return DrillAcross(
        branches=tuple(branches),
        key_names=keys,
        select=_named_in(d["select"], refs, "select"),
        having=_opt_pred_in(d["having"], frozenset({"da"})),
    )


def _child(value: object, built: list[PlanNode]) -> PlanNode:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidSpec(f"a child reference is a node index, got {value!r}")
    if not 0 <= value < len(built):
        raise InvalidSpec(f"node index {value} does not refer to an earlier node")
    return built[value]


def _compose_in(d: dict, built: list[PlanNode]) -> Compose:
    _obj(d, _COMPOSE_KEYS, "compose node")
    op = _enum(ComposeOp, d["op"], "compose op")
    on: Fragment | None = None
    if op is ComposeOp.JOIN:
        on = check_fragment(d["on"], frozenset({"l", "r"}))
    elif d["on"] is not None:
        raise InvalidSpec(f"only JOIN carries an on clause, not {op.value}")
    return Compose(op, _child(d["left"], built), _child(d["right"], built), on)


def _node_in(value: object, built: list[PlanNode]) -> PlanNode:
    if not isinstance(value, dict):
        raise InvalidSpec("a node is an object")
    kind = value.get("type")
    if kind == "band":
        return _band_in(value)
    if kind == "compose":
        return _compose_in(value, built)
    if kind == "drill_across":
        return _drill_in(value)
    raise InvalidSpec(f"unknown node type {kind!r}")


def _params_in(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise InvalidSpec("params must be an object")
    for k in value:
        check_identifier(k, "parameter name")
    return dict(value)


def decode_spec(spec: object) -> QuestionSpec:
    """Decode and check *spec*; raise :class:`InvalidSpec` (or an IR error)."""
    d = _obj(spec, _TOP_KEYS, "spec")
    if d["format"] != SPEC_FORMAT:
        raise InvalidSpec(f"spec format {d['format']!r} is not {SPEC_FORMAT!r}")
    built: list[PlanNode] = []
    for raw in _list(d["nodes"], "nodes"):
        built.append(_node_in(raw, built))
    root = _child(d["root"], built)
    ctes = []
    for pair in _list(d["ctes"], "ctes"):
        if not isinstance(pair, list) or len(pair) != 2:
            raise InvalidSpec("ctes entries are [name, node index]")
        ctes.append(Cte(check_identifier(pair[0], "CTE name"), _child(pair[1], built)))
    return QuestionSpec(PlanIR(root=root, ctes=tuple(ctes)), _params_in(d["params"]))


def decode_pred(value: object, scope: frozenset[str]) -> Pred:
    """Decode one predicate of a spec; names must start with an alias of *scope*."""
    return _pred_in(value, scope)


def encode_pred(p: Pred) -> dict:
    out = _pred_out(p)
    assert out is not None
    return out


def band_coverage(ir: PlanIR) -> list[str]:
    """The bands the leaves of *ir* use (§5.2): B1 always, B2/B3 when present."""
    bands = {"B1"}
    stack: list[PlanNode] = [ir.root]
    while stack:
        n = stack.pop()
        if isinstance(n, Compose):
            stack += [n.left, n.right]
            continue
        if isinstance(n, DrillAcross):
            bands.add("B2")  # §5.2.2 aggregates per branch
            continue
        if n.group_by or n.aggregates or n.having is not None:
            bands.add("B2")
        if n.windows or n.qualify is not None:
            bands.add("B3")
    return sorted(bands)


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        for k in value:
            if not isinstance(k, str):
                raise InvalidSpec(f"object keys must be strings, got {k!r}")
        return ("{}", tuple((k, _freeze(value[k])) for k in sorted(value)))
    if isinstance(value, list):
        return ("[]", tuple(_freeze(v) for v in value))
    return value


def spec_hash(spec: Mapping[str, object]) -> str:
    """Hash of the canonical serialisation of *spec* (a ``CandidateId``)."""
    return canonical_hash("dgm-candidate", _freeze(dict(spec)))
