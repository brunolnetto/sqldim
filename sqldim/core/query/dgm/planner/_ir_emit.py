"""SQL emission from the physical plan IR (DuckDB dialect).

Emission is a pure function of the IR: it never consults the source query.
For a single :class:`BandQuery` the text equals ``DGMQuery.to_sql()`` of the
query it was lowered from, except that string literals are always escaped.
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
    PlanIR,
    PlanNode,
    Pred,
    Quantifier,
    RawCondition,
    UnsupportedConstruct,
    VersionKind,
    sql_string,
)

__all__ = ["emit_sql", "emit_node", "emit_pred"]


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
    first, rest = p.hops[0], p.hops[1:]
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
    lines += [
        f"LEFT JOIN {j.scan.relation} {j.scan.alias} ON {j.on.sql}{_version_sql(j)}"
        for j in q.joins
    ]
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
