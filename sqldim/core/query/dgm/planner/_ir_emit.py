"""SQL emission from the physical plan IR (DuckDB dialect).

Emission is a pure function of the IR: it never consults the source query.
For a single :class:`BandQuery` the text equals ``DGMQuery.to_sql()`` of the
query it was lowered from, except that string literals are always escaped.

A Repeat (§3.2) in ``WALK`` mode over a one-hop body with
``1 ≤ min ≤ max ≤ MAX_UNROLL`` and no ``depth_alias`` is unrolled into one
join chain per iteration count (Rule 5).  Any other one-hop Repeat is
emitted as a ``WITH RECURSIVE`` over node identities — the entity keys a
declared model gives the hop (``PathHop.key``): SIMPLE excludes a revisited
node, TRAIL a revisited edge instance (the pair of node identities it
joins), and an iteration beyond ``MAX_REPEAT_DEPTH`` is reported as an
error, never cut silently.  Without the keys it raises
:class:`UnsupportedConstruct`.  The same keys give the order ≺ of a
strategy other than ALL (§3.3) and the maximal traces of a temporal mode
over one Repeat (§4.1, ADR 0001), formed from the instances the strategy
selects.  Any other temporal mode never reaches the emitter: lowering
rewrites it into a mode-free form, and a past mode into its mirror over
reversed hops (``preds/_positions.py``).  Every read is still in the IR, so ``deps``
of the lowered plan is defined.
"""

from __future__ import annotations

from sqldim.core.query.dgm._dim_joins import valid_at
from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.planner._ir import (
    BandQuery,
    Compare,
    Compose,
    Conj,
    Disj,
    DrillAcross,
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
    RepeatJoin,
    RepeatMode,
    UnsupportedConstruct,
    VersionKind,
    rename_aliases,
    sql_string,
)

__all__ = [
    "MAX_REPEAT_DEPTH",
    "MAX_UNROLL",
    "emit_sql",
    "emit_node",
    "emit_pred",
    "unroll_refusal",
]

#: Largest ``max`` of a Repeat the emitter unrolls.  A larger bound is an
#: :class:`UnsupportedConstruct`, never a truncated traversal (§3.2).
MAX_UNROLL: int = 8

#: Ceiling on the iteration count of a recursively emitted Repeat (§3.2).  An
#: instance that could be extended beyond it makes the query fail with an
#: error that names it: a truncated traversal is never returned as complete.
MAX_REPEAT_DEPTH: int = 256


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
    if p.mode is not None:
        return _trace_sql(p)
    if any(isinstance(h, PathRepeat) for h in p.hops):
        raise UnsupportedConstruct(
            "a Repeat inside a PathPred binds its body aliases per iteration "
            "(§3.2): a trace has several evaluation positions, which this IR "
            "version does not emit"
        )
    hops = [h for h in p.hops if not isinstance(h, PathRepeat)]
    if p.strategy != "ALL":
        return _strategy_sql(p, hops)
    first = hops[0]
    sql = "SELECT 1 " + _path_from(hops)
    sub = emit_pred(p.sub_filter)
    if p.quantifier is Quantifier.FORALL:
        # §4.1 FORALL elimination: NotTrue — an UNKNOWN position counts
        # against — and the REQUIRE_WITNESS guard of ADR 0001 (the default;
        # PathPred has no on_empty field, so VACUOUS is never requested).
        return (
            f"(NOT EXISTS ({sql} WHERE ({sub}) IS NOT TRUE AND {first.on.sql})"
            f" AND EXISTS ({sql} WHERE {first.on.sql}))"
        )
    return f"EXISTS ({sql} WHERE {sub} AND {first.on.sql})"


def _path_from(hops: list[PathHop]) -> str:
    first, rest = hops[0], hops[1:]
    sql = f"FROM {first.scan.relation} {first.scan.alias}"
    for h in rest:
        sql += f" JOIN {h.scan.relation} {h.scan.alias} ON {h.on.sql}"
    return sql


def _strategy_keys(p: PathExists, hops: list[PathHop]) -> str:
    """ORDER BY of ≺ (§3.3) over instances of one length.

    ``key(π)`` is the sequence of edge instances ``(label, source key,
    target key)``.  Along a pattern without a Repeat every instance has the
    same length and labels, and consecutive edges share a node, so the order
    is the lexicographic order of the keys of the nodes the hops reach.
    """
    for h in hops:
        if not h.key:
            raise UnsupportedConstruct(
                f"strategy {p.strategy} orders instances by ≺ (§3.3), which "
                f"breaks ties by entity keys; those of {h.scan.relation!r} are "
                "not declared: plan the query with DGMPlanner(model=…) whose "
                "NodeTable maps that relation without a version_key (§2.6)"
            )
        if p.strategy == "MIN_WEIGHT" and h.weighted is not False:
            raise UnsupportedConstruct(
                f"MIN_WEIGHT needs weight(π) (§3.3): hop {h.label!r} is "
                + (
                    "a weighted bridge, whose weight is a property of the edge "
                    "instance that a hop over a NodeTable does not read"
                    if h.weighted
                    else "a bridge whose weight no declared edge type states"
                )
            )
    return ", ".join(f"{h.scan.alias}.{c}" for h in hops for c in h.key)


def _strategy_sql(p: PathExists, hops: list[PathHop]) -> str:
    """EXISTS / FORALL over the instances a strategy selects (§3.3).

    With every weight 1, MIN_WEIGHT orders by (len, key) as SHORTEST does.
    """
    order = _strategy_keys(p, hops)
    limit = p.k if p.strategy == "K_SHORTEST" else 1
    sub = emit_pred(p.sub_filter)
    ranked = (
        f"SELECT ({sub}) IS TRUE AS __dgm_ok, ROW_NUMBER() OVER (ORDER BY "
        f"{order}) AS __dgm_rn {_path_from(hops)} WHERE {hops[0].on.sql}"
    )
    sel = f"SELECT 1 FROM ({ranked}) __dgm_i WHERE __dgm_i.__dgm_rn <= {limit}"
    if p.quantifier is Quantifier.FORALL:
        # NotTrue: a selected instance where sub_filter is not TRUE counts
        # against; REQUIRE_WITNESS: at least one instance is selected
        return f"(NOT EXISTS ({sel} AND NOT __dgm_i.__dgm_ok) AND EXISTS ({sel}))"
    return f"EXISTS ({sel} AND __dgm_i.__dgm_ok)"


def _trace_check(p: PathExists) -> PathRepeat:
    if len(p.hops) != 1 or not isinstance(p.hops[0], PathRepeat):
        raise UnsupportedConstruct(
            "a temporal mode over several positions is emitted for one Repeat "
            "over a one-hop body only (§3.2)"
        )
    r = p.hops[0]
    _recursive_check(r)
    return r


_PER_TRACE = {
    "EVENTUALLY": "list_contains({phi}, TRUE)",
    "GLOBALLY": "NOT list_contains({phi}, FALSE)",
    # strong UNTIL: ψ at some position j, φ at every position before j
    "UNTIL": (
        "COALESCE(list_position({psi}, TRUE) > 0 AND NOT list_contains("
        "list_slice({phi}, 1, list_position({psi}, TRUE) - 1), FALSE), FALSE)"
    ),
}


def _trace_sql(p: PathExists) -> str:
    """A temporal mode over the maximal traces of one Repeat (§4.1, ADR 0001).

    Each iteration's end node is a position, and with ``min = 0`` the start
    node is the first (§3.2).  A row of the recursion is an instance; its
    trace is maximal when it has ``max`` iterations or no admitted next
    iteration — a terminal, since an edge instance is the pair of node
    identities it joins.  EXISTS is ``bool_or`` of the per-trace value, FORALL
    ``bool_and``; an empty ``T(s)`` is FALSE (REQUIRE_WITNESS).
    """
    r = _trace_check(p)
    hop = r.body[0]
    rel, y, key = hop.scan.relation, hop.scan.alias, hop.key
    phi = f"({emit_pred(p.sub_filter)}) IS TRUE"
    psi = "FALSE" if p.until is None else f"({emit_pred(p.until)}) IS TRUE"
    if r.min == 0:
        # position 0: the start node, read under the body alias
        at = f"(SELECT {{}} FROM {rel} {y} WHERE {_key(y, key)} = {_key(r.start, key)})"
        base = [f"[{at.format(phi)}]", f"[{at.format(psi)}]"]
    else:
        base = ["[TRUE][1:0]", "[TRUE][1:0]"]
    cols = [
        ("__dgm_phi", base[0], f"list_append({_W}.__dgm_phi, {phi})"),
        ("__dgm_psi", base[1], f"list_append({_W}.__dgm_psi, {psi})"),
    ]
    bounded = "" if r.max is None else f"{_W}.__dgm_k >= {r.max} OR "
    on = rename_aliases(hop.on, {r.start: _P})
    extend = (
        f"SELECT 1 FROM {rel} {_P} JOIN {rel} {y} ON {on.sql} WHERE "
        f"{_key(_P, key)} = {_W}.__dgm_end AND {_admits(r, y)}"
    )
    agg = "bool_and" if p.quantifier is Quantifier.FORALL else "bool_or"
    if p.strategy != "ALL":
        return _selected_trace_sql(p, r, cols, agg)
    per_trace = _PER_TRACE[p.mode or ""].format(
        phi=f"{_W}.__dgm_phi", psi=f"{_W}.__dgm_psi"
    )
    return (
        f"COALESCE((\n{_recursive(r, cols)}\nSELECT {agg}({per_trace}) FROM {_W} "
        f"WHERE {_W}.__dgm_k >= {r.min} AND len({_W}.__dgm_phi) > 0 AND "
        f"({bounded}NOT EXISTS ({extend}))\n), FALSE)"
    )


def _selected_trace_sql(
    p: PathExists, r: PathRepeat, cols: list[tuple[str, str, str]], agg: str
) -> str:
    """A temporal mode over the instances a strategy selects (§3.3, §4.1).

    The strategy first selects instances of the Repeat by ≺ — fewest
    iterations, then the sequence of node identities, which orders the edge
    instances of a one-label body as ``key(π)`` does.  The traces are those
    of the selected instances, and a trace is maximal when no other selected
    instance extends it.  With every weight 1 MIN_WEIGHT is SHORTEST.  The
    recursive CTE is read once: DuckDB fails to bind a second reference to it
    inside a correlated subquery, so the selected instances are compared in
    a list (``__dgm_all``).
    """
    if p.strategy == "MIN_WEIGHT" and r.body[0].weighted is not False:
        raise UnsupportedConstruct(
            f"MIN_WEIGHT needs weight(π) (§3.3): hop {r.body[0].label!r} is not "
            "declared an unweighted bridge"
        )
    limit = p.k if p.strategy == "K_SHORTEST" else 1
    a = "__dgm_a"
    per_trace = _PER_TRACE[p.mode or ""].format(
        phi=f"{a}.__dgm_phi", psi=f"{a}.__dgm_psi"
    )
    ranked = (
        f"SELECT *, ROW_NUMBER() OVER (ORDER BY {_W}.__dgm_k, {_W}.__dgm_seg) AS "
        f"__dgm_rn FROM {_W} WHERE {_W}.__dgm_k >= {r.min}"
    )
    selected = (
        f"SELECT *, list(__dgm_r.__dgm_seg) OVER () AS __dgm_all FROM ({ranked}) "
        f"__dgm_r WHERE __dgm_r.__dgm_rn <= {limit}"
    )
    seg = f"{a}.__dgm_seg"
    extends = f"lambda x: len(x) > len({seg}) AND list_slice(x, 1, len({seg})) = {seg}"
    return (
        f"COALESCE((\n{_recursive(r, cols)}\nSELECT {agg}({per_trace}) FROM "
        f"({selected}) {a} WHERE len({a}.__dgm_phi) > 0 AND "
        f"len(list_filter({a}.__dgm_all, {extends})) = 0\n), FALSE)"
    )


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


def unroll_refusal(r: PathRepeat) -> str | None:
    """Why *r* is not unrolled into join chains (Rule 5), or ``None``."""
    if r.mode is not RepeatMode.WALK:
        return (
            f"Repeat mode {r.mode.value} excludes revisited node or edge "
            "instances, which needs node identities"
        )
    if len(r.body) != 1:
        return "only a one-hop Repeat body is emitted"
    if r.min < 1 or r.depth_alias is not None:
        return "Repeat with min = 0 or a depth_alias is not unrolled"
    if r.max is None or r.max > MAX_UNROLL:
        return (
            f"Repeat max {r.max} is above MAX_UNROLL={MAX_UNROLL}; it is not "
            "unrolled, and never truncated"
        )
    if r.start_relation != r.body[0].scan.relation:
        return _r1(r)
    return None


def _r1(r: PathRepeat) -> str:
    return (
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


# ---------------------------------------------------------------------------
# Recursive Repeat (§3.2) over node identities
# ---------------------------------------------------------------------------

_W, _P = "__dgm_w", "__dgm_p"  # the recursive CTE; the previous node


def _key(alias: str, key: tuple[str, ...]) -> str:
    """The entity key of the node bound to *alias* (§2.6)."""
    if len(key) == 1:
        return f"{alias}.{key[0]}"
    return "ROW(" + ", ".join(f"{alias}.{c}" for c in key) + ")"


def _edge(src: str, dst: str, key: tuple[str, ...]) -> str:
    """An edge instance of the one-hop body: its two node identities."""
    return f"ROW({_key(src, key)}, {_key(dst, key)})"


def _recursive_check(r: PathRepeat) -> None:
    """What the recursive form of *r* needs; raise when it is missing."""
    if len(r.body) != 1:
        raise UnsupportedConstruct("only a one-hop Repeat body is emitted")
    if r.depth_alias is not None:
        raise UnsupportedConstruct(
            "a Repeat depth_alias is not emitted: §3.2 binds it to the "
            "iteration count, and the fragments of a query qualify names only "
            "with aliases bound to rows"
        )
    if r.start_relation != r.body[0].scan.relation:
        raise UnsupportedConstruct(_r1(r))
    if not r.body[0].key:
        raise UnsupportedConstruct(
            f"{unroll_refusal(r)}: the entity keys of "
            f"{r.body[0].scan.relation!r} are not declared; plan the query "
            "with DGMPlanner(model=…) whose NodeTable maps that relation "
            "without a version_key (§2.6)"
        )


def _admits(r: PathRepeat, y: str) -> str:
    """RepeatMode (§3.2): the step to *y* revisits nothing it may not."""
    key = r.body[0].key
    if r.mode is RepeatMode.SIMPLE:
        return f"NOT list_contains({_W}.__dgm_seg, {_key(y, key)})"
    if r.mode is RepeatMode.TRAIL:
        return f"NOT list_contains({_W}.__dgm_es, {_edge(_P, y, key)})"
    return "TRUE"


def _step_guard(r: PathRepeat, y: str) -> str:
    """Admitted, within max, and below MAX_REPEAT_DEPTH — or reported."""
    bound = "" if r.max is None else f" WHEN {_W}.__dgm_k >= {r.max} THEN FALSE"
    return (
        f"CASE WHEN NOT ({_admits(r, y)}) THEN FALSE{bound} "
        f"WHEN {_W}.__dgm_k >= {MAX_REPEAT_DEPTH} THEN error("
        f"'Repeat traversal truncated at MAX_REPEAT_DEPTH={MAX_REPEAT_DEPTH} "
        "(§3.2)') ELSE TRUE END"
    )


def _step_from(r: PathRepeat) -> str:
    """The relations of one more iteration from the end of a row of ``_W``."""
    hop = r.body[0]
    rel, y, key = hop.scan.relation, hop.scan.alias, hop.key
    on = rename_aliases(hop.on, {r.start: _P})
    return f"{rel} {_P} ON {_key(_P, key)} = {_W}.__dgm_end JOIN {rel} {y} ON {on.sql}"


def _recursive(r: PathRepeat, cols: list[tuple[str, str, str]]) -> str:
    """``WITH RECURSIVE`` over the instances of *r* from its start node.

    *cols* are ``(name, base, step)`` of the extra columns: the value in
    the zero-iteration row and the expression of one more iteration.
    """
    hop = r.body[0]
    s, y, key = r.start, hop.scan.alias, hop.key
    names = ["__dgm_k", "__dgm_end", "__dgm_seg", "__dgm_es", *(c[0] for c in cols)]
    base = [
        "0",
        _key(s, key),
        f"[{_key(s, key)}]",
        f"[{_edge(s, s, key)}][1:0]",
        *(c[1] for c in cols),
    ]
    step = [
        f"{_W}.__dgm_k + 1",
        _key(y, key),
        f"list_append({_W}.__dgm_seg, {_key(y, key)})",
        f"list_append({_W}.__dgm_es, {_edge(_P, y, key)})",
        *(c[2] for c in cols),
    ]
    step_sql = (
        f"SELECT {', '.join(step)}\nFROM {_W} JOIN {_step_from(r)}\n"
        f"WHERE {_step_guard(r, y)}"
    )
    return (
        f"WITH RECURSIVE {_W}({', '.join(names)}) AS (\n"
        f"SELECT {', '.join(base)}\nUNION ALL\n{step_sql}\n)"
    )


def _recursive_join_sql(r: PathRepeat) -> str:
    """A PathJoin over *r* under ALL: one row per instance, its end node."""
    _recursive_check(r)
    hop = r.body[0]
    rel, y = hop.scan.relation, hop.scan.alias
    body = (
        f"{_recursive(r, [])}\nSELECT {y}.* FROM {_W} JOIN {rel} {y} ON "
        f"{_key(y, hop.key)} = {_W}.__dgm_end WHERE {_W}.__dgm_k >= {r.min}"
    )
    return f"LEFT JOIN LATERAL (\n{body}\n) {y} ON TRUE"


def _repeat_join_sql(j: RepeatJoin) -> str:
    r = j.repeat
    if unroll_refusal(r) is not None:
        return _recursive_join_sql(r)
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
    return " AND " + valid_at(a, sql_string(v.as_of))


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
    ComposeOp.UNION: "UNION",
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


def _drill_sql(d: DrillAcross) -> str:
    """§5.2.2 steps 1–3, as ``DGMQuery.to_sql()`` renders a constellation."""
    from sqldim.core.query.dgm._drill_across import aligned_sql

    branches = [(_band_sql(b), [n for n, _ in b.aggregates]) for b in d.branches]
    having = None if d.having is None else emit_pred(d.having)
    select = [(n, e.sql) for n, e in d.select]
    return aligned_sql(branches, d.key_names, select, having)


def _leaf_sql(node: PlanNode) -> str | None:
    if isinstance(node, BandQuery):
        return _band_sql(node)
    if isinstance(node, DrillAcross):
        return _drill_sql(node)
    return None


def emit_node(node: PlanNode, names: dict[PlanNode, str] | None = None) -> str:
    """Render the body of one plan node; *names* maps earlier nodes to CTE names."""
    leaf = _leaf_sql(node)
    if leaf is not None:
        return leaf
    assert isinstance(node, Compose)
    return _compose_sql(node, names or {})


def emit_sql(ir: PlanIR) -> str:
    """Render the whole plan as one DuckDB statement."""
    if not ir.ctes:
        leaf = _leaf_sql(ir.root)
        if leaf is None:
            raise UnsupportedConstruct("a composed root needs its CTEs")
        return leaf
    if ir.ctes[-1].node is not ir.root:
        raise UnsupportedConstruct("the root of a WITH plan must be its last CTE")
    names: dict[PlanNode, str] = {}
    defs: list[str] = []
    for cte in ir.ctes:
        defs.append(f"{cte.name} AS (\n{emit_node(cte.node, names)}\n)")
        names.setdefault(cte.node, cte.name)
    return "WITH\n" + ",\n".join(defs) + f"\nSELECT * FROM {ir.ctes[-1].name}"
