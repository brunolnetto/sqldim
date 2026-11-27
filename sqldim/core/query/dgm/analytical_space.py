"""Declared schema graph, anchor sets and vocabulary decomposition (§2.1, §3.4.6, §8.16).

Everything here is computed from the schema graph ``G_S = (N_S, E_S, τ_N, τ_E,
K_S, Σ)`` alone and reads no instance data (§8.11, *Schema determinacy*):

* the schema graph is the one the dataset declares in its
  :class:`~sqldim.core.query.dgm.model.DGMModel` (``DGMSchema`` is that type);
  :func:`decompose` and the anchor-set functions read ``N_S``, ``E_S``,
  ``K_S`` and the kinds of ``Σ`` they need, and raise
  :class:`~sqldim.core.query.dgm.model.AnnotationNotDeclared` for a kind the
  model does not state;
* :func:`validate_anchor_set` / :func:`admissible_anchor_sets` — AS1–AS3 of
  §3.4.6 with connection taken in ``U(G_S)``;
* :func:`decompose` — the six vocabularies of §8.16.1 with their members.

Nothing is inferred: a schema that is not declared is not guessed from tables,
and an annotation that is not declared is not assumed.
"""

from __future__ import annotations

import dataclasses
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from itertools import combinations, combinations_with_replacement
from typing import Any

from sqldim.core.query.dgm.annotations import (
    FactlessFact,
    MedallionLayer,
    MedallionLayerKind,
    SchemaAnnotation,
    annotation_kind,
)
from sqldim.core.query.dgm.model import DGMModel, ModelDeclarationError, NodeType

__all__ = [
    "AGG_FNS",
    "COMPARISON_OPS",
    "JOIN_KINDS",
    "PREDICATE_TERMS",
    "TEMPORAL_MODES",
    "VOCABULARIES",
    "Anchor",
    "DGMSchema",
    "SchemaDeclarationError",
    "admissible_anchor_sets",
    "decompose",
    "parse_anchor",
    "validate_anchor_set",
]

# Formalism-derived vocabularies (§4.1, §4.2, §3.1): fixed by the specification.
AGG_FNS: tuple[str, ...] = ("SUM", "COUNT", "AVG", "MIN", "MAX", "PROD")
COMPARISON_OPS: tuple[str, ...] = ("=", "≠", "<", "≤", ">", "≥")
JOIN_KINDS: tuple[str, ...] = ("VerbHop", "VerbHop⁻¹", "BridgeHop")
TEMPORAL_MODES: tuple[str, ...] = (
    "EVENTUALLY",
    "GLOBALLY",
    "NEXT",
    "UNTIL",
    "SINCE",
    "ONCE",
    "PREVIOUSLY",
)
PREDICATE_TERMS: tuple[str, ...] = ("PathPred", "SignaturePred") + tuple(
    f"TemporalMode.{m}" for m in TEMPORAL_MODES
)

VOCABULARIES: tuple[str, ...] = ("V_N", "V_E", "V_P", "V_Pred", "V_Op", "V_Par")
"""The six vocabularies of §8.16.1, in table order."""

SIGMA_KEY = "Σ"
"""Key of the declared annotations in :func:`decompose` output."""


DGMSchema = DGMModel
"""The declared schema graph is the declared model (§2.6); kept as a name."""

SchemaDeclarationError = ModelDeclarationError
"""A declaration rejected at construction; kept as a name."""


# ---------------------------------------------------------------------------
# Connection in U(G_S) and anchor sets (§3.4.6)
# ---------------------------------------------------------------------------


def _undirected_adjacency(schema: DGMSchema) -> dict[str, set[str]]:
    adj: dict[str, set[str]] = {n: set() for n in schema.node_names}
    for e in schema.edge_types:
        adj[e.source].add(e.target)
        adj[e.target].add(e.source)
    return adj


def _distances_from(adj: Mapping[str, set[str]], start: str) -> dict[str, int]:
    dist = {start: 0}
    queue = deque([start])
    while queue:
        cur = queue.popleft()
        for nxt in adj[cur]:
            if nxt not in dist:
                dist[nxt] = dist[cur] + 1
                queue.append(nxt)
    return dist


def _shortest_closed_walk(schema: DGMSchema, name: str) -> int | None:
    """Length of the shortest walk of ≥ 1 hop from *name* back to *name*."""
    if any(e.source == e.target == name for e in schema.edge_types):
        return 1
    if any(name in (e.source, e.target) for e in schema.edge_types):
        return 2
    return None


class _Connectivity:
    """Pairwise walk lengths in ``U(G_S)``, computed once per schema."""

    def __init__(self, schema: DGMSchema) -> None:
        adj = _undirected_adjacency(schema)
        self._dist = {n: _distances_from(adj, n) for n in adj}
        self._loop = {n: _shortest_closed_walk(schema, n) for n in adj}

    def joined(self, a: str, b: str, h: int) -> bool:
        """AS1 for one pair: a walk of at most *h* hops between two anchors.

        Two anchors of one type are distinct occurrences, so their walk has at
        least one hop.
        """
        if a == b:
            loop = self._loop[a]
            return loop is not None and loop <= h
        d = self._dist[a].get(b)
        return d is not None and d <= h


@dataclass(frozen=True, order=True)
class Anchor:
    """``(type, role)`` of one anchor; ``role`` only when the type repeats."""

    type: str
    role: str | None = None

    def __str__(self) -> str:
        return self.type if self.role is None else f"{self.type}@{self.role}"


def parse_anchor(text: str) -> Anchor:
    """``"Type"`` or ``"Type@role"``."""
    type_, sep, role = text.partition("@")
    return Anchor(type_, role if sep else None)


def _role_violations(schema: DGMSchema, anchors: list[Anchor]) -> list[str]:
    problems: list[str] = []
    by_type: dict[str, list[Anchor]] = {}
    for a in anchors:
        by_type.setdefault(a.type, []).append(a)
    for type_, group in sorted(by_type.items()):
        if len(group) < 2:
            continue
        roles = [a.role for a in group]
        allowed = schema.roles_of(type_)
        if None in roles or len(set(roles)) != len(roles):
            problems.append(f"AS2: anchors of type {type_} need distinct roles")
        elif not set(roles) <= allowed:
            problems.append(
                f"AS2: {type_} roles must be among {sorted(allowed) or 'none'}"
            )
    return problems


def _connection_violations(
    conn: _Connectivity, anchors: list[Anchor], h: int
) -> list[str]:
    return [
        f"AS1: {a} and {b} are not joined by a walk of at most {h} hops"
        for a, b in combinations(anchors, 2)
        if not conn.joined(a.type, b.type, h)
    ]


def validate_anchor_set(
    schema: DGMSchema,
    anchors: Iterable[str],
    *,
    max_hops: int,
    max_anchors: int,
    _conn: _Connectivity | None = None,
) -> list[str]:
    """Violations of k ≥ 2 and AS1–AS3 for *anchors*; empty iff valid."""
    parsed = [parse_anchor(a) for a in anchors]
    unknown = sorted({a.type for a in parsed} - set(schema.node_names))
    if unknown:
        return [f"not a node type of the schema: {', '.join(unknown)}"]
    problems: list[str] = []
    if len(parsed) < 2:
        problems.append("an anchor set has at least two anchors")
    if len(set(parsed)) != len(parsed):
        problems.append("an anchor is listed twice")
    if len(parsed) > max_anchors:
        problems.append(f"AS3: {len(parsed)} anchors exceed MAX_ANCHORS={max_anchors}")
    problems.extend(_role_violations(schema, parsed))
    conn = _conn or _Connectivity(schema)
    problems.extend(_connection_violations(conn, parsed, max_hops))
    return problems


def _occurrences(schema: DGMSchema, type_: str, count: int) -> list[list[Anchor]]:
    """Every way *type_* can occur *count* times in a canonical anchor set."""
    if count == 1:
        return [[Anchor(type_)]]
    roles = sorted(schema.roles_of(type_))
    return [[Anchor(type_, r) for r in combo] for combo in combinations(roles, count)]


def _expand(schema: DGMSchema, types: tuple[str, ...]) -> Iterator[list[Anchor]]:
    counts: dict[str, int] = {}
    for t in types:
        counts[t] = counts.get(t, 0) + 1
    choices = [_occurrences(schema, t, c) for t, c in sorted(counts.items())]
    yield from _product(choices)


def _product(choices: list[list[list[Anchor]]]) -> Iterator[list[Anchor]]:
    if not choices:
        yield []
        return
    for head in choices[0]:
        for tail in _product(choices[1:]):
            yield head + tail


def admissible_anchor_sets(
    schema: DGMSchema, *, max_hops: int, max_anchors: int
) -> list[list[str]]:
    """Every valid anchor set with 2…*max_anchors* anchors (AS1–AS3).

    Roles appear only on a type that occurs more than once, since a role is
    needed only to tell such anchors apart (§3.4.6).  Ordered by size, then
    lexicographically.
    """
    conn = _Connectivity(schema)
    out: list[list[str]] = []
    for k in range(2, max_anchors + 1):
        for types in combinations_with_replacement(schema.node_names, k):
            for anchors in _expand(schema, types):
                if not _connection_violations(conn, anchors, max_hops):
                    out.append(sorted(str(a) for a in anchors))
    return sorted(out, key=lambda s: (len(s), s))


# ---------------------------------------------------------------------------
# Vocabulary decomposition (§8.16.1)
# ---------------------------------------------------------------------------


def _render_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, (set, frozenset, list, tuple)):
        return "{" + ", ".join(sorted(_render_value(v) for v in value)) + "}"
    return str(value)


def render_annotation(ann: SchemaAnnotation) -> str:
    """``Kind(field=value, …)`` with sets sorted, for a stable listing."""
    if not dataclasses.is_dataclass(ann):
        return annotation_kind(ann)
    args = ", ".join(
        f"{f.name}={_render_value(getattr(ann, f.name))}"
        for f in dataclasses.fields(ann)
        if getattr(ann, f.name) is not None
    )
    return f"{annotation_kind(ann)}({args})"


def _agg_domain(schema: DGMSchema, node: NodeType) -> tuple[str, ...]:
    """AggFn values left by Σ for aggregates over *node* (§8.15 projection)."""
    factless = any(a.fact == node.name for a in schema.annotations(FactlessFact))
    return ("COUNT",) if factless else AGG_FNS


def _bronze(schema: DGMSchema) -> set[str]:
    return {
        a.node
        for a in schema.annotations(MedallionLayer)
        if a.layer is MedallionLayerKind.BRONZE
    }


def _set(values: Iterable[str]) -> str:
    return "{" + ", ".join(values) + "}"


def _fixed_parameters(has_edges: bool) -> list[str]:
    temporal = [
        "TEMPORAL.as_of: Timestamp",
        "TEMPORAL.node_resolution: {LAX, STRICT}",
        "TEMPORAL.edge_resolution: {LAX, STRICT}",
    ]
    window = [
        "WINDOW.partition_by: subset of Band 2 output attributes",
        "WINDOW.order_by: attribute × {ASC, DESC}",
        "WINDOW.frame: {RANGE, ROWS} × bounds",
        "WINDOW.qualify_threshold: ℝ",
    ]
    if not has_edges:
        return temporal + window
    paths = [
        "TEMPORAL.hop_ordering: point | Allen",
        "STRATEGY.strategy: {ALL, K_SHORTEST(k), MIN_WEIGHT, SHORTEST}",
        "STRATEGY.repeat_min: ℕ",
        "STRATEGY.repeat_max: ℕ ∪ {UNBOUNDED}",
        "STRATEGY.repeat_mode: {SIMPLE, TRAIL, WALK}",
        "STRATEGY.k: ℕ",
        "ALGORITHM.page_rank_damping: (0, 1)",
        "ALGORITHM.page_rank_iterations: ℕ",
        "ALGORITHM.k_shortest_k: ℕ",
        "ALGORITHM.signature_entropy_max_depth: ℕ",
        "ALGORITHM.community_label_variant: "
        "{CONNECTED_COMPONENTS, LABEL_PROPAGATION, LOUVAIN, TARJAN_SCC}",
    ]
    return temporal + paths + window


def _schema_parameters(schema: DGMSchema) -> list[str]:
    out: list[str] = []
    for node in sorted(schema.node_types, key=lambda n: n.name):
        out.extend(
            f"FILTER.threshold[{node.name}.{key}]: {type_}"
            for key, type_ in sorted(node.properties.items())
        )
        out.append(
            f"AGGREGATION.agg_fn[{node.name}]: {_set(_agg_domain(schema, node))}"
        )
    excluded = sorted(_bronze(schema))
    group_by = "AGGREGATION.group_by: 𝒫(V_P)"
    if excluded:
        group_by += f" without properties of BRONZE types {_set(excluded)}"
    out += [group_by, "AGGREGATION.temporal_window: TemporalWindow"]
    return out


def decompose(schema: DGMSchema) -> dict[str, list[str]]:
    """The six vocabularies of §8.16.1 over the whole schema, plus Σ.

    Schema-derived: ``V_N`` (node types), ``V_E`` (edge types), ``V_P``
    (declared node properties ``T.key``).  Formalism-derived: ``V_Pred``,
    ``V_Op`` (``AggFn``, ``ComparisonOp`` and the hop kinds of §3.1) and
    ``V_Par`` (parameter families of §8.15 as ``FAMILY.slot: domain``, with the
    projections of Σ applied).
    """
    return {
        "V_N": schema.node_names,
        "V_E": sorted(e.term for e in schema.edge_types),
        "V_P": sorted(
            f"{n.name}.{key}" for n in schema.node_types for key in n.properties
        ),
        "V_Pred": list(PREDICATE_TERMS),
        "V_Op": list(AGG_FNS + COMPARISON_OPS + JOIN_KINDS),
        "V_Par": sorted(
            _fixed_parameters(bool(schema.edge_types)) + _schema_parameters(schema)
        ),
        SIGMA_KEY: sorted(render_annotation(a) for a in schema.sigma),
    }
