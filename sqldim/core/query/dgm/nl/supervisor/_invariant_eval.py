"""DGM Supervisor — evaluators of H1–H3, PARTITION and M1–M7 (§2.1, §2.6).

Each evaluator examines every element of its scope through the
:class:`~._invariant_core.Evaluator` and records violations; ``checked``
counts the elements examined (§12.3):

* schema-level invariants (PARTITION, M1, M4) examine types;
* H2 and H3 examine property bags — one per NodeTable row (one per version
  of a versioned entity) and one per edge instance;
* M2, M3, M6 and M7 examine the declaration of each type in scope and then
  the rows the data clause reads (see each function).

H1 and M5 hold by construction of the instance graph as a view (§2.6): ``E_I``
of an edge type is *defined* as the pairs its mapping produces, subject to M5,
and each pair is typed by the endpoints of that edge type.  No instance could
violate them, so they are reported UNSUPPORTED rather than ``holds = true``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqldim.core.query.dgm.nl.supervisor._invariant_core import (
    EdgeSql,
    Evaluator,
    NotEvaluable,
    col,
    conforms,
    describe,
    ident,
    lit,
    plain,
)
from sqldim.core.query.dgm.nl.supervisor._invariant_model import (
    Derived,
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    NodeKind,
    NodeTable,
    NodeType,
    PropType,
    Reference,
    RelationTable,
)
from sqldim.core.query.dgm.nl.supervisor._types import ToolErrorKind

__all__ = ["EVALUATORS", "SCHEMA_ONLY"]

Check = Callable[[Evaluator], None]

SCHEMA_ONLY = frozenset({"PARTITION", "M1", "M4"})
"""Invariants decided from the declared model alone; they read no data."""


def _by_construction(name: str, reason: str) -> Check:
    def run(ev: Evaluator) -> None:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            f"{name} holds by construction of the instance graph (§2.6) and has "
            "no instance that could violate it",
            invariant=name,
            reason=reason,
        )

    return run


# ---------------------------------------------------------------------------
# PARTITION, M1, M4 — schema level
# ---------------------------------------------------------------------------


def partition(ev: Evaluator) -> None:
    """§2.1: τ_N, τ_E partition N_S, E_S; V_S ⊆ D_S×F_S×L; B_S ⊆ N_S×N_S×L."""
    kinds = {n.name: n.kind for n in ev.model.node_types}
    ev.examine(len(kinds))  # τ_N is total and single-valued by declaration
    for edge in ev.model.edge_types:
        ev.examine()
        problems = [
            f"{end} {name!r} is not a node type"
            for end, name in (("source", edge.source), ("target", edge.target))
            if name not in kinds
        ]
        if edge.kind is EdgeKind.VERB and not problems:
            if kinds[edge.source] is not NodeKind.DIM:
                problems.append("a verb edge type must start at a dimension type")
            if kinds[edge.target] is not NodeKind.FACT:
                problems.append("a verb edge type must end at a fact type")
        if problems:
            ev.violate(
                {"edge": edge.key, "kind": edge.kind.value, "problems": problems}
            )


def m1(ev: Evaluator) -> None:
    """M1: M_N and M_E are total."""
    for node in ev.model.node_types:
        ev.examine()
        if node.mapping is None:
            ev.violate({"node_type": node.name, "problem": "no NodeTable"})
    for edge in ev.model.edge_types:
        ev.examine()
        if edge.mapping is None:
            ev.violate({"edge": edge.key, "problem": "no EdgeMapping"})


def _needs_relation(edge: EdgeType) -> list[str]:
    needs = [
        ("declares properties", bool(edge.properties)),
        ("declares a weight", edge.weighted),
        ("declares a validity window", edge.validity),
        ("is N:N", edge.many_to_many),
    ]
    return [reason for reason, hit in needs if hit]


def m4(ev: Evaluator) -> None:
    """M4: such an edge type is mapped by RelationTable or Derived."""
    for edge in ev.model.edge_types:
        ev.examine()
        needs = _needs_relation(edge)
        if needs and not isinstance(edge.mapping, (RelationTable, Derived)):
            mapping = type(edge.mapping).__name__ if edge.mapping else None
            ev.violate({"edge": edge.key, "mapping": mapping, "because": needs})


# ---------------------------------------------------------------------------
# H2, H3 — property bags
# ---------------------------------------------------------------------------


def _keys(alias: str, table: NodeTable) -> str:
    return ", ".join(col(alias, k) for k in table.entity_key)


def _edge_keys(es: EdgeSql) -> str:
    cols = [f"x.s_{i}" for i in range(es.n_source)]
    cols += [f"x.t_{i}" for i in range(es.n_target)]
    return ", ".join(cols)


def _edge_instance(es: EdgeSql, row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "source": plain(list(row[: es.n_source])),
        "target": plain(list(row[es.n_source : es.n_source + es.n_target])),
    }


def h2(ev: Evaluator) -> None:
    """H2: dom P(x) ⊆ dom K_S(h(x)); dom P(x) is the mapped property keys."""
    for node, table in ev.mapped_nodes():
        base = f"FROM {ident(table.relation)} x"
        n = ev.count(base)
        ev.examine(n)
        extra = sorted(set(table.props) - set(node.properties))
        if extra and n:
            rows = ev.sample(_keys("x", table), base)
            ev.violate_many(
                n,
                [
                    {
                        "node_type": node.name,
                        "instance": plain(list(r)),
                        "undeclared": extra,
                    }
                    for r in rows
                ],
            )
    for edge in ev.model.edge_types:
        es = ev.edge_sql(edge)
        base = f"FROM ({es.sql}) x"
        n = ev.count(base)
        ev.examine(n)
        extra = sorted(set(es.props) - set(edge.properties))
        if extra and n:
            rows = ev.sample(_edge_keys(es), base)
            ev.violate_many(
                n,
                [
                    {"edge": edge.key, **_edge_instance(es, r), "undeclared": extra}
                    for r in rows
                ],
            )


def _value_check(expr: str, column_type: str, declared: PropType) -> str | None:
    """SQL true iff the value *expr* does not inhabit *declared* (H3)."""
    if not conforms(column_type, declared):
        return f"{expr} IS NOT NULL"
    if declared.name == "Enum":
        values = ", ".join(lit(v) for v in declared.values)
        return f"{expr} IS NOT NULL AND CAST({expr} AS VARCHAR) NOT IN ({values})"
    return None


def _bag_checks(
    ev: Evaluator,
    relation: str,
    owner: str,
    props: dict[str, tuple[str, str]],
    declared: dict[str, PropType],
) -> list[tuple[str, str, str, str]]:
    """(key, value expression, violation predicate, column type) per checked key.

    *props* maps a property key to (column in *relation*, expression in the
    evaluated row).  Keys outside ``K_S`` are H2's concern.
    """
    checks = []
    for key, (column, expr) in sorted(props.items()):
        if key not in declared:
            continue
        ctype = ev.column_type(relation, column, owner)
        predicate = _value_check(expr, ctype, declared[key])
        if predicate is not None:
            checks.append((key, expr, predicate, ctype))
    return checks


def _check_bags(
    ev: Evaluator,
    base: str,
    keys: tuple[str, int],
    checks: list[tuple[str, str, str, str]],
    declared: dict[str, PropType],
    instance: Callable[[tuple[Any, ...]], dict[str, Any]],
) -> None:
    ev.examine(ev.count(base))
    if not checks:
        return
    bad = " OR ".join(f"({c[2]})" for c in checks)
    count = ev.count(f"{base} WHERE {bad}")
    if not count:
        return
    flags = ", ".join(f"({c[2]}), {c[1]}" for c in checks)
    witnesses = []
    select, width = keys
    for row in ev.sample(f"{select}, {flags}", f"{base} WHERE {bad}"):
        values = row[width:]
        offending = {
            key: {
                "value": plain(values[2 * i + 1]),
                "declared": describe(declared[key]),
                "column_type": ctype,
            }
            for i, (key, _, _, ctype) in enumerate(checks)
            if values[2 * i]
        }
        witnesses.append({**instance(row[:width]), "values": offending})
    ev.violate_many(count, witnesses)


def h3(ev: Evaluator) -> None:
    """H3: P(x)(k) ∈ K_S(h(x))(k) ∪ {NULL} for every declared key."""
    for node, table in ev.mapped_nodes():
        props = {k: (c, col("x", c)) for k, c in table.props.items()}
        owner = f"node type {node.name!r}"
        checks = _bag_checks(ev, table.relation, owner, props, dict(node.properties))
        _check_bags(
            ev,
            f"FROM {ident(table.relation)} x",
            (_keys("x", table), len(table.entity_key)),
            checks,
            dict(node.properties),
            lambda r, name=node.name: {"node_type": name, "instance": plain(list(r))},
        )
    for edge in ev.model.edge_types:
        es = ev.edge_sql(edge)
        checks = []
        if isinstance(edge.mapping, RelationTable):
            mapping = edge.mapping
            props = {k: (mapping.props[k], f"x.p_{i}") for i, k in enumerate(es.props)}
            checks = _bag_checks(
                ev,
                mapping.relation,
                f"edge type {edge.key}",
                props,
                dict(edge.properties),
            )
        _check_bags(
            ev,
            f"FROM ({es.sql}) x",
            (_edge_keys(es), es.n_source + es.n_target),
            checks,
            dict(edge.properties),
            lambda r, e=edge, s=es: {"edge": e.key, **_edge_instance(s, r)},
        )


# ---------------------------------------------------------------------------
# M2 — version columns and intervals
# ---------------------------------------------------------------------------


def _m2_declaration(node: NodeType, table: NodeTable) -> str | None:
    declared = [table.version_key, table.effective_from, table.effective_to]
    if node.versioned and None in declared:
        return "an SCD2/SCD6 NodeTable must declare version_key, effective_from and effective_to"
    if not node.versioned and any(c is not None for c in declared):
        return "only an SCD2/SCD6 NodeTable declares version_key, effective_from or effective_to"
    return None


def _m2_intervals(ev: Evaluator, node: NodeType, table: NodeTable) -> None:
    """Every version row: effective_from set, from < to, no overlap per entity."""
    ef, et = table.effective_from or "", table.effective_to or ""
    same = " AND ".join(f"{col('b', k)} = {col('a', k)}" for k in table.entity_key)
    overlap = (
        f"EXISTS (SELECT 1 FROM v b WHERE b.__dgm_rn <> a.__dgm_rn AND {same} "
        f"AND {col('b', ef)} IS NOT NULL "
        f"AND ({col('b', et)} IS NULL OR {col('b', ef)} < {col('b', et)}) "
        f"AND ({col('b', et)} IS NULL OR {col('a', ef)} < {col('b', et)}) "
        f"AND ({col('a', et)} IS NULL OR {col('b', ef)} < {col('a', et)}))"
    )
    null_from = f"{col('a', ef)} IS NULL"
    inverted = f"({col('a', et)} IS NOT NULL AND {col('a', et)} <= {col('a', ef)})"
    base = f"FROM {ident(table.relation)} a"
    # Materialised so that both references see the same row numbers.
    with_v = (
        "WITH v AS MATERIALIZED (SELECT *, row_number() OVER () AS __dgm_rn "
        f"FROM {ident(table.relation)}) "
    )
    ev.examine(ev.count(base))
    where = f"WHERE {null_from} OR {inverted} OR {overlap}"
    count = int(ev.scalar(f"{with_v}SELECT COUNT(*) FROM v a {where}"))
    if not count:
        return
    reason = (
        f"CASE WHEN {null_from} THEN 'effective_from is NULL' "
        f"WHEN {inverted} THEN 'effective_to <= effective_from' "
        "ELSE 'overlaps another version of the same entity_key' END"
    )
    select = (
        f"{_keys('a', table)}, {col('a', table.version_key or '')}, "
        f"{col('a', ef)}, {col('a', et)}, {reason}"
    )
    rows = (
        ev.rows(f"{with_v}SELECT {select} FROM v a {where} LIMIT {ev.room}")
        if ev.room
        else []
    )
    width = len(table.entity_key)
    ev.violate_many(
        count,
        [
            {
                "node_type": node.name,
                "entity_key": plain(list(r[:width])),
                "version_key": plain(r[width]),
                "effective_from": plain(r[width + 1]),
                "effective_to": plain(r[width + 2]),
                "problem": r[width + 3],
            }
            for r in rows
        ],
    )


def m2(ev: Evaluator) -> None:
    """M2: version columns iff SCD2/SCD6; half-open, pairwise disjoint intervals.

    ``checked`` = node types with a NodeTable + version rows of versioned ones.
    """
    for node in ev.model.node_types:
        table = node.mapping
        if table is None:
            continue  # no NodeTable to examine: M1's finding
        ev.examine()
        problem = _m2_declaration(node, table)
        if problem is not None:
            ev.violate({"node_type": node.name, "problem": problem})
        elif node.versioned:
            _m2_intervals(ev, node, table)


# ---------------------------------------------------------------------------
# M3, M6 — references
# ---------------------------------------------------------------------------


def _ambiguous_versions(table: NodeTable) -> str:
    """Version-key values of *table* that belong to more than one entity."""
    vk = col("v", table.version_key or "")
    entity = ", ".join(col("v", k) for k in table.entity_key)
    return (
        f"SELECT {vk} AS vk FROM {ident(table.relation)} v WHERE {vk} IS NOT NULL "
        f"GROUP BY {vk} HAVING COUNT(DISTINCT ({entity})) > 1"
    )


def _fk_tables(
    ev: Evaluator, edge: EdgeType, fk: ForeignKey
) -> tuple[NodeTable, NodeTable]:
    """(holder, referenced) NodeTables of a ForeignKey edge type."""
    owner = f"edge type {edge.key}"
    src = ev.node_table(edge.source, owner)
    tgt = ev.node_table(edge.target, owner)
    return (tgt, src) if fk.holder is Holder.TARGET else (src, tgt)


def _m3_multiplicity(
    ev: Evaluator,
    edge: EdgeType,
    fk: ForeignKey,
    holder: NodeTable,
    referenced: NodeTable,
) -> None:
    """Holder rows whose VERSION_KEY reference resolves to several entities."""
    if referenced.version_key is None or len(fk.columns) != 1:
        raise NotEvaluable(
            ToolErrorKind.UNSUPPORTED,
            f"edge type {edge.key}: VERSION_KEY reference to a NodeTable "
            "without a version_key cannot be resolved (M6)",
            edge=edge.key,
        )
    ref = col("h", fk.columns[0])
    where = (
        f"FROM {ident(holder.relation)} h "
        f"WHERE {ref} IN ({_ambiguous_versions(referenced)})"
    )
    count = ev.count(where)
    if not count:
        return
    width = len(holder.entity_key)
    ev.violate_many(
        count,
        [
            {
                "edge": edge.key,
                "holder_row": plain(list(r[:width])),
                "reference": plain(r[width]),
                "problem": "the reference yields more than one edge instance",
            }
            for r in ev.sample(f"{_keys('h', holder)}, {ref}", where)
        ],
    )


def m3(ev: Evaluator) -> None:
    """M3: a ForeignKey row yields at most one edge instance; no window, no props.

    ``checked`` = ForeignKey edge types (their declaration) + holder rows.
    """
    for edge in ev.model.edge_types:
        fk = edge.mapping
        if not isinstance(fk, ForeignKey):
            continue
        ev.examine()
        carried = [r for r in _needs_relation(edge) if r != "is N:N"]
        if carried:
            ev.violate(
                {
                    "edge": edge.key,
                    "problem": "a ForeignKey edge type " + ", ".join(carried),
                }
            )
        holder, referenced = _fk_tables(ev, edge, fk)
        ev.examine(ev.count(f"FROM {ident(holder.relation)} h"))
        # An ENTITY_KEY value is one node instance (§2.6): at most one edge.
        if fk.references is Reference.VERSION_KEY:
            _m3_multiplicity(ev, edge, fk, holder, referenced)


def _version_key_targets(edge: EdgeType) -> list[str]:
    """Node types *edge* references by VERSION_KEY."""
    mapping = edge.mapping
    if isinstance(mapping, ForeignKey) and mapping.references is Reference.VERSION_KEY:
        return [edge.source if mapping.holder is Holder.TARGET else edge.target]
    if (
        isinstance(mapping, RelationTable)
        and mapping.references is Reference.VERSION_KEY
    ):
        return sorted({edge.source, edge.target})
    return []


def _m6_versions(ev: Evaluator, name: str, table: NodeTable) -> None:
    vk = table.version_key or ""
    distinct = f"FROM (SELECT DISTINCT {col('v', vk)} FROM {ident(table.relation)} v WHERE {col('v', vk)} IS NOT NULL) d"
    ev.examine(ev.count(distinct))
    amb = f"FROM ({_ambiguous_versions(table)}) a"
    count = ev.count(amb)
    if count:
        ev.violate_many(
            count,
            [
                {
                    "node_type": name,
                    "version_key": plain(r[0]),
                    "problem": "the version surrogate belongs to more than one entity",
                }
                for r in ev.sample("a.vk", amb)
            ],
        )


def m6(ev: Evaluator) -> None:
    """M6: a VERSION_KEY reference stores the surrogate of one entity's version.

    ``checked`` = (edge type, referenced type) pairs + distinct version keys of
    each referenced type.
    """
    seen: set[str] = set()
    for edge in ev.model.edge_types:
        for name in _version_key_targets(edge):
            ev.examine()
            node = ev.model.node(name)
            table = node.mapping if node else None
            if table is None or table.version_key is None:
                ev.violate(
                    {
                        "edge": edge.key,
                        "referenced": name,
                        "problem": "the referenced type declares no version_key",
                    }
                )
            elif name not in seen:
                seen.add(name)
                _m6_versions(ev, name, table)


# ---------------------------------------------------------------------------
# M7 — event_time of facts that reference versioned types
# ---------------------------------------------------------------------------


def _references_versioned(ev: Evaluator, fact: str, edge: EdgeType) -> bool:
    """*fact* holds an ENTITY_KEY reference to a versioned type through *edge*."""
    if fact not in (edge.source, edge.target):
        return False
    other = ev.model.node(edge.target if edge.source == fact else edge.source)
    if other is None or not other.versioned:
        return False
    mapping = edge.mapping
    if isinstance(mapping, ForeignKey):
        holder = edge.source if mapping.holder is Holder.SOURCE else edge.target
        return holder == fact and mapping.references is Reference.ENTITY_KEY
    if isinstance(mapping, RelationTable):
        return mapping.references is Reference.ENTITY_KEY
    return isinstance(mapping, Derived)


def _m7_fact(ev: Evaluator, node: NodeType, refs: list[str]) -> None:
    table = ev.node_table(node.name, f"fact type {node.name!r}")
    if table.event_time is None:
        ev.violate(
            {
                "fact_type": node.name,
                "references": refs,
                "problem": "declares no event_time",
            }
        )
        return
    ctype = ev.column_type(table.relation, table.event_time, f"fact type {node.name!r}")
    if not (conforms(ctype, PropType("Timestamp")) or ctype.upper() == "DATE"):
        ev.violate(
            {
                "fact_type": node.name,
                "event_time": table.event_time,
                "column_type": ctype,
                "problem": "event_time is not a Timestamp or Date column",
            }
        )
        return
    base = f"FROM {ident(table.relation)} x"
    ev.examine(ev.count(base))
    where = f"{base} WHERE {col('x', table.event_time)} IS NULL"
    count = ev.count(where)
    if count:
        ev.violate_many(
            count,
            [
                {
                    "fact_type": node.name,
                    "instance": plain(list(r)),
                    "problem": "event_time is NULL",
                }
                for r in ev.sample(_keys("x", table), where)
            ],
        )


def m7(ev: Evaluator) -> None:
    """M7: such a fact type declares a non-NULL Timestamp/Date event_time.

    ``checked`` = fact types in scope + their rows (when event_time is typed).
    """
    for node in ev.model.node_types:
        if node.kind is not NodeKind.FACT:
            continue
        refs = [
            e.key
            for e in ev.model.edge_types
            if _references_versioned(ev, node.name, e)
        ]
        if refs:
            ev.examine()
            _m7_fact(ev, node, refs)


EVALUATORS: dict[str, Check] = {
    "H1": _by_construction(
        "H1",
        "E_I of an edge type is the pairs its mapping yields between its endpoints",
    ),
    "H2": h2,
    "H3": h3,
    "PARTITION": partition,
    "M1": m1,
    "M2": m2,
    "M3": m3,
    "M4": m4,
    "M5": _by_construction(
        "M5", "a reference that matches no row is not an edge instance by definition"
    ),
    "M6": m6,
    "M7": m7,
}
