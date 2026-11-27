"""SQL emission from the physical plan IR (DuckDB dialect).

Emission is a pure function of the IR: it never consults the source query.
For a single :class:`BandQuery` the text equals ``DGMQuery.to_sql()`` of the
query it was lowered from, except that string literals are always escaped.

A Repeat (§3.2) is emitted only where its SQL is exact without node or edge
identities, which a hop does not declare: a PathJoin over a one-hop body in
``WALK`` mode with ``1 ≤ min ≤ max ≤ MAX_UNROLL`` and no ``depth_alias``,
unrolled into one join chain per iteration count (Rule 5).  ``SIMPLE`` and
``TRAIL`` need the identities to exclude revisits, and a Repeat inside a
PathPred needs per-iteration evaluation positions (§4.1); those raise
:class:`UnsupportedConstruct`.  Their reads are still in the IR, so
``deps`` of the lowered plan is defined.
"""

from __future__ import annotations

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Compose,
    Conj,
    Disj,
    Join,
    Literal,
    Neg,
    PathExists,
    PathRepeat,
    PlanIR,
    PlanNode,
    Pred,
    Quantifier,
    RawCondition,
    RepeatJoin,
    RepeatMode,
    UnsupportedConstruct,
    VersionKind,
    rename_aliases,
    sql_string,
)

__all__ = ["MAX_UNROLL", "emit_sql", "emit_node", "emit_pred"]

#: Largest ``max`` of a Repeat the emitter unrolls.  A larger bound is an
#: :class:`UnsupportedConstruct`, never a truncated traversal (§3.2).
MAX_UNROLL: int = 8


def _literal_sql(lit: Literal) -> str:
    v = lit.value
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return sql_string(v)
    return str(v)


def _compare_sql(p: Compare) -> str:
    if p.op in ("IS", "IS NOT") and p.rhs.value is None:
        return f"{p.lhs.sql} {p.op} NULL"
    return f"{p.lhs.sql} {p.op} {_literal_sql(p.rhs)}"


def _paren(parts: list[str], op: str) -> str:
    if len(parts) == 1:
        return parts[0]
    return "(" + f" {op} ".join(parts) + ")"


def _path_sql(p: PathExists) -> str:
    if any(isinstance(h, PathRepeat) for h in p.hops):
        raise UnsupportedConstruct(
            "a Repeat inside a PathPred binds its body aliases per iteration "
            "(§3.2), which needs the temporal modes this IR version does not emit"
        )
    hops = [h for h in p.hops if not isinstance(h, PathRepeat)]
    first, rest = hops[0], hops[1:]
    sql = f"SELECT 1 FROM {first.scan.relation} {first.scan.alias}"
    for h in rest:
        sql += f" JOIN {h.scan.relation} {h.scan.alias} ON {h.on.sql}"
    sub = emit_pred(p.sub_filter)
    if p.quantifier is Quantifier.FORALL:
        return f"NOT EXISTS ({sql} WHERE NOT ({sub}) AND {first.on.sql})"
    return f"EXISTS ({sql} WHERE {sub} AND {first.on.sql})"


def emit_pred(p: Pred) -> str:
    """Render a predicate of the IR."""
    if isinstance(p, Compare):
        return _compare_sql(p)
    if isinstance(p, RawCondition):
        return p.fragment.sql
    if isinstance(p, Conj):
        return _paren([emit_pred(s) for s in p.preds], "AND")
    if isinstance(p, Disj):
        return _paren([emit_pred(s) for s in p.preds], "OR")
    if isinstance(p, Neg):
        return f"NOT ({emit_pred(p.pred)})"
    if isinstance(p, PathExists):
        return _path_sql(p)
    raise UnsupportedConstruct(f"no SQL for predicate node {type(p).__name__}")


def repeat_iteration_alias(end: str, i: int) -> str:
    """Alias of iteration *i* of a Repeat unrolled to end at alias *end*."""
    return f"{end}__r{i}"


def _check_unrollable(r: PathRepeat) -> None:
    if r.mode is not RepeatMode.WALK:
        raise UnsupportedConstruct(
            f"Repeat mode {r.mode.value} excludes revisited node or edge "
            "instances, which needs identities a hop does not declare; only "
            "WALK is emitted in this IR version"
        )
    if len(r.body) != 1:
        raise UnsupportedConstruct("only a one-hop Repeat body is emitted")
    if r.min < 1 or r.depth_alias is not None:
        raise UnsupportedConstruct(
            "Repeat with min = 0 or a depth_alias is not emitted in this IR version"
        )
    if r.max is None or r.max > MAX_UNROLL:
        raise UnsupportedConstruct(
            f"Repeat max {r.max} is above MAX_UNROLL={MAX_UNROLL}; it is not "
            "unrolled, and never truncated"
        )
    if r.start_relation != r.body[0].scan.relation:
        raise UnsupportedConstruct(
            f"Repeat body over {r.body[0].scan.relation!r} does not end at the "
            f"relation it starts from ({r.start_relation!r}; R1)"
        )


def _repeat_branch(r: PathRepeat, k: int) -> str:
    hop = r.body[0]
    rel, end = hop.scan.relation, r.end
    names = [repeat_iteration_alias(end, i) for i in range(1, k)] + [end]
    sql = f"SELECT {end}.* FROM {rel} {names[0]}"
    for prev, cur in zip(names, names[1:]):
        on = rename_aliases(hop.on, {r.start: prev, end: cur})
        sql += f" JOIN {rel} {cur} ON {on.sql}"
    first = rename_aliases(hop.on, {end: names[0]})
    return sql + f" WHERE {first.sql}"


def _repeat_join_sql(j: RepeatJoin) -> str:
    r = j.repeat
    _check_unrollable(r)
    assert r.max is not None
    body = "\nUNION ALL\n".join(_repeat_branch(r, k) for k in range(r.min, r.max + 1))
    return f"LEFT JOIN LATERAL (\n{body}\n) {r.end} ON TRUE"


def _join_sql(j: Join | RepeatJoin) -> str:
    if isinstance(j, RepeatJoin):
        return _repeat_join_sql(j)
    return f"LEFT JOIN {j.scan.relation} {j.scan.alias} ON {j.on.sql}{_version_sql(j)}"


def _version_sql(j: Join) -> str:
    v, a = j.version, j.scan.alias
    if v is None:
        return ""
    if v.kind is VersionKind.IS_CURRENT:
        return f" AND {a}.is_current = TRUE"
    assert v.as_of is not None
    t = sql_string(v.as_of)
    if v.kind is VersionKind.VALID_RANGE_DATE:
        return (
            f" AND {a}.valid_from::DATE <= {t}::DATE"
            f" AND ({a}.valid_to IS NULL OR {a}.valid_to::DATE > {t}::DATE)"
        )
    return (
        f" AND {a}.valid_from <= {t} AND ({a}.valid_to IS NULL OR {a}.valid_to > {t})"
    )


def _select_list(q: BandQuery) -> list[str]:
    if q.group_by or q.aggregates:
        parts = [g.sql for g in q.group_by]
        parts += [f"{e.sql} AS {n}" for n, e in q.aggregates]
    else:
        parts = ["*"]
    parts += [f"{e.sql} AS {n}" for n, e in q.windows]
    return parts


def _band_sql(q: BandQuery) -> str:
    lines = [
        "SELECT " + ", ".join(_select_list(q)),
        f"FROM {q.anchor.relation} {q.anchor.alias}",
    ]
    lines += [_join_sql(j) for j in q.joins]
    if q.where is not None:
        lines.append("WHERE " + emit_pred(q.where))
    if q.group_by:
        lines.append("GROUP BY " + ", ".join(g.sql for g in q.group_by))
    if q.having is not None:
        lines.append("HAVING " + emit_pred(q.having))
    if q.qualify is not None:
        lines.append("QUALIFY " + emit_pred(q.qualify))
    return "\n".join(lines)


_SET_OPS = {
    ComposeOp.UNION: "UNION ALL",
    ComposeOp.INTERSECT: "INTERSECT",
    ComposeOp.EXCEPT: "EXCEPT",
}


def _child_name(child: PlanNode, names: dict[PlanNode, str]) -> str:
    try:
        return names[child]
    except KeyError:
        raise UnsupportedConstruct(
            "a composed node references a child that is not an earlier CTE"
        ) from None


def _compose_sql(c: Compose, names: dict[PlanNode, str]) -> str:
    left, right = _child_name(c.left, names), _child_name(c.right, names)
    if c.op in _SET_OPS:
        return f"SELECT * FROM {left}\n{_SET_OPS[c.op]}\nSELECT * FROM {right}"
    if c.op is ComposeOp.WITH:
        return f"SELECT * FROM {right}"
    if c.op is ComposeOp.JOIN and c.on is not None:
        return f"SELECT l.*, r.*\nFROM {left} l\nJOIN {right} r ON {c.on.sql}"
    raise UnsupportedConstruct(f"no SQL for composition {c.op!r} (on={c.on!r})")


def emit_node(node: PlanNode, names: dict[PlanNode, str] | None = None) -> str:
    """Render the body of one plan node; *names* maps earlier nodes to CTE names."""
    if isinstance(node, BandQuery):
        return _band_sql(node)
    return _compose_sql(node, names or {})


def emit_sql(ir: PlanIR) -> str:
    """Render the whole plan as one DuckDB statement."""
    if not ir.ctes:
        if not isinstance(ir.root, BandQuery):
            raise UnsupportedConstruct("a composed root needs its CTEs")
        return _band_sql(ir.root)
    if ir.ctes[-1].node is not ir.root:
        raise UnsupportedConstruct("the root of a WITH plan must be its last CTE")
    names: dict[PlanNode, str] = {}
    defs: list[str] = []
    for cte in ir.ctes:
        defs.append(f"{cte.name} AS (\n{emit_node(cte.node, names)}\n)")
        names.setdefault(cte.node, cte.name)
    return "WITH\n" + ",\n".join(defs) + f"\nSELECT * FROM {ir.ctes[-1].name}"
