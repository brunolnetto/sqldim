"""The scope ``G_𝒜`` of one anchor set and the §4.1 types (§3.4.6, §8.11).

:func:`build_space` checks an anchor set (AS1–AS3, canonical roles), computes
``N_𝒜``, ``E_𝒜`` and ``Σ|_𝒜`` within the hop bound and refuses — with
:class:`LatticeUnsupported` — what the enumeration cannot decide from the
declaration: a property whose type is not a §4.1 ``Type``, or a SUM governed
by AD2 whose period the declaration does not state.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from sqldim.core.query.dgm.analytical_space import (
    COMPARISON_OPS,
    Anchor,
    DGMSchema,
    _distances_from,
    _undirected_adjacency,
    parse_anchor,
    render_annotation,
    validate_anchor_set,
)
from sqldim.core.query.dgm.annotations import (
    BridgeSemantics,
    BridgeSemanticsKind,
    Conformed,
    FactlessFact,
    Grain,
    GrainKind,
    MedallionLayer,
    SchemaAnnotation,
)
from sqldim.core.query.dgm.model import EdgeKind, EdgeType, NodeKind, PropType
from sqldim.core.query.dgm.planner._canon import canonical_hash

__all__ = [
    "Hop",
    "InvalidAnchors",
    "LatticeUnsupported",
    "NUMERIC",
    "ORDERED",
    "Space",
    "build_space",
    "compatible",
    "ops_for",
    "parse_type",
    "schema_version",
]

Hop = tuple[str, str, str, str]
"""``(source, label, target, dir)``: an edge type of ``E_𝒜`` traversed
forwards (``"+"``) or backwards (``"-"``)."""

ORDERED = frozenset({"Int", "Real", "String", "Date", "Timestamp", "Duration"})
NUMERIC = frozenset({"Int", "Real"})
_DECLARED = {
    "String": "String",
    "Integer": "Int",
    "Real": "Real",
    "Boolean": "Bool",
    "Date": "Date",
    "Timestamp": "Timestamp",
}
"""§2.1 property types and the §4.1 ``Type`` each one is."""


def _needed_kinds(schema: DGMSchema, h: int, nodes, edges, props) -> list[type]:
    """Annotation kinds whose declaration decides something in this ``G_𝒜``.

    MedallionLayer filters GroupBy keys; FactlessFact restricts aggregates over
    a fact; Grain decides SUM over a fact with a numeric property (AD1–AD4);
    BridgeSemantics decides time order on a bridge (§2.3); Conformed decides
    fact branches, which need a dimension between two facts (two hops).
    """
    kinds = {
        type_.value: [n for n in nodes if schema.node(n).kind is type_]
        for type_ in (NodeKind.DIM, NodeKind.FACT)
    }
    facts = kinds["fact"]
    needed: list[type] = [MedallionLayer]
    if facts:
        needed.append(FactlessFact)
    if any(t in NUMERIC for f in facts for _, t in props.get(f, ())):
        needed.append(Grain)
    if any(e.kind is EdgeKind.BRIDGE for e in edges):
        needed.append(BridgeSemantics)
    if facts and kinds["dim"] and h >= 2:
        needed.append(Conformed)
    return needed


_TIME_ORDERED = frozenset(
    {
        BridgeSemanticsKind.CAUSAL,
        BridgeSemanticsKind.TEMPORAL,
        BridgeSemanticsKind.SUPERSESSION,
    }
)


class LatticeUnsupported(Exception):
    """The declaration does not let the enumeration decide membership."""


class InvalidAnchors(ValueError):
    """The anchors are not an anchor set (AS1–AS3) in canonical form."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def parse_type(declared: PropType) -> str | None:
    """The §4.1 ``Type`` of a declared property type, or ``None``.

    An ``Enum`` is identified by its declared values, so two properties with
    the same values have the same type.
    """
    if declared.name == "Enum":
        return str(declared)
    return _DECLARED.get(declared.name)


def ops_for(type_: str) -> tuple[str, ...]:
    """Comparison operators a type admits (§4.1 *Compare*)."""
    return COMPARISON_OPS if type_ in ORDERED else ("=", "≠")


def compatible(a: str, b: str) -> str | None:
    """The common type of two operands after Int → Real widening."""
    if a == b:
        return a
    return "Real" if {a, b} == NUMERIC else None


def _render(value: Any) -> Any:
    """A canonically encodable rendering of a declaration value."""
    if isinstance(value, Enum):
        return str(value.value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__name__,
            tuple(
                (f.name, _render(getattr(value, f.name)))
                for f in dataclasses.fields(value)
            ),
        )
    if isinstance(value, Mapping):
        return tuple(sorted((str(k), _render(v)) for k, v in value.items()))
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(_render(v) for v in value))
    if isinstance(value, (tuple, list)):
        return tuple(_render(v) for v in value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


def schema_version(schema: DGMSchema) -> str:
    """``schema_version`` of §8.14: the hash of the declared ``G_S`` (with
    ``K_S`` and ``M``) and ``Σ`` with its declared kinds."""
    nodes = tuple(_render(n) for n in sorted(schema.node_types, key=lambda n: n.name))
    edges = tuple(
        sorted(
            (_render(e) for e in schema.edge_types),
            key=lambda r: repr(r),
        )
    )
    sigma = tuple(sorted(render_annotation(a) for a in schema.sigma))
    kinds = tuple(sorted(schema.sigma_kinds))
    return canonical_hash("dgm-model", nodes, edges, sigma, kinds)


@dataclass(frozen=True)
class Space:
    """``G_𝒜`` with the data the enumeration reads (no instance data)."""

    schema: DGMSchema
    anchors: tuple[Anchor, ...]
    h: int
    nodes: frozenset[str]
    edges: tuple[EdgeType, ...]
    sigma: tuple[SchemaAnnotation, ...]
    props: Mapping[str, tuple[tuple[str, str], ...]]
    hops: Mapping[str, tuple[Hop, ...]]
    dist: Mapping[str, Mapping[str, int]]
    version: str
    cache: dict = field(default_factory=dict, compare=False, repr=False)

    @property
    def anchor_names(self) -> tuple[str, ...]:
        return tuple(str(a) for a in self.anchors)

    def kind(self, type_: str) -> str:
        node = self.schema.node(type_)
        assert node is not None
        return node.kind.value

    def prop_type(self, type_: str, key: str) -> str | None:
        return dict(self.props.get(type_, ())).get(key)

    def factless(self, type_: str) -> bool:
        return any(isinstance(a, FactlessFact) and a.fact == type_ for a in self.sigma)

    def bronze(self, type_: str) -> bool:
        return any(
            isinstance(a, MedallionLayer) and a.node == type_ and a.is_bronze
            for a in self.sigma
        )

    def conformed_for(self, dim: str, facts: set[str]) -> bool:
        """``Conformed(dim, Fs) ∈ Σ|_𝒜`` with ``facts ⊆ Fs``."""
        return any(
            isinstance(a, Conformed) and a.dim == dim and facts <= a.fact_types
            for a in self.sigma
        )

    def time_ordered(self, hop: Hop) -> bool:
        """A hop along a CAUSAL, TEMPORAL or SUPERSESSION bridge, forwards (§2.3)."""
        source, label, target, direction = hop
        edge = next(e for e in self.edges if (e.source, e.label, e.target) == hop[:3])
        return (
            direction == "+"
            and edge.kind.value == "bridge"
            and any(
                isinstance(a, BridgeSemantics)
                and a.bridge == label
                and a.sem in _TIME_ORDERED
                for a in self.sigma
            )
        )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def _canonical_anchors(schema: DGMSchema, anchors: Sequence[str], h: int, k: int):
    problems = validate_anchor_set(schema, anchors, max_hops=h, max_anchors=k)
    parsed = [parse_anchor(a) for a in anchors]
    counts: dict[str, int] = {}
    for a in parsed:
        counts[a.type] = counts.get(a.type, 0) + 1
    problems += [
        f"a role is written only on a type that occurs more than once: {a}"
        for a in parsed
        if a.role is not None and counts.get(a.type, 0) == 1
    ]
    if problems:
        raise InvalidAnchors(problems)
    return tuple(sorted(parsed, key=lambda a: (a.type, a.role or "")))


def _on_walk(d: Mapping[str, Mapping[str, int]], anchors, x: str, y: str, h: int):
    """Some walk ``Aᵢ → … x … y … → Aⱼ`` of ≤ h hops between distinct anchors."""
    types = [a.type for a in anchors]
    return any(
        d[a].get(x, h + 1) + d[y].get(b, h + 1) <= h
        for i, a in enumerate(types)
        for j, b in enumerate(types)
        if i != j
    )


def _scope_types(schema: DGMSchema, anchors, h: int, dist) -> frozenset[str]:
    nodes = {a.type for a in anchors}
    nodes |= {t for t in schema.node_names if _on_walk(dist, anchors, t, t, h)}
    return frozenset(nodes)


def _scope_edges(schema: DGMSchema, anchors, h: int, dist) -> tuple[EdgeType, ...]:
    def on(e: EdgeType) -> bool:
        return _on_walk(dist, anchors, e.source, e.target, h - 1) or _on_walk(
            dist, anchors, e.target, e.source, h - 1
        )

    return tuple(
        sorted(
            (e for e in schema.edge_types if on(e)),
            key=lambda e: (e.source, e.label, e.target),
        )
    )


def _named_types(ann: SchemaAnnotation) -> set[str] | None:
    if isinstance(ann, Conformed):
        return {ann.dim, *ann.fact_types}
    if isinstance(ann, (Grain, FactlessFact)):
        return {ann.fact}
    if isinstance(ann, MedallionLayer):
        return {ann.node}
    return None


def _restrict_sigma(schema: DGMSchema, nodes, edges) -> tuple[SchemaAnnotation, ...]:
    labels = {e.label for e in edges if e.kind.value == "bridge"}
    out: list[SchemaAnnotation] = []
    for ann in schema.sigma:
        if isinstance(ann, BridgeSemantics):
            keep = ann.bridge in labels
        else:
            named = _named_types(ann)
            keep = named is not None and named <= nodes
        if keep:
            out.append(ann)
    return tuple(out)


def _hops(nodes: frozenset[str], edges) -> dict[str, tuple[Hop, ...]]:
    out: dict[str, list[Hop]] = {t: [] for t in nodes}
    for e in edges:
        out[e.source].append((e.source, e.label, e.target, "+"))
        out[e.target].append((e.source, e.label, e.target, "-"))
    return {t: tuple(sorted(hs)) for t, hs in out.items()}


def _typed_props(schema: DGMSchema, nodes: frozenset[str]):
    props: dict[str, tuple[tuple[str, str], ...]] = {}
    for name in sorted(nodes):
        node = schema.node(name)
        assert node is not None
        typed = []
        for key, declared in sorted(node.properties.items()):
            t = parse_type(declared)
            if t is None:
                raise LatticeUnsupported(
                    f"property {name}.{key} declares type {declared}, which is "
                    "not a §4.1 Type; its atoms cannot be typed"
                )
            typed.append((key, t))
        props[name] = tuple(typed)
    return props


def _check_additivity(schema: DGMSchema, sigma, props) -> None:
    for ann in sigma:
        if not (isinstance(ann, Grain) and ann.grain is GrainKind.PERIOD):
            continue
        if any(t in NUMERIC for _, t in props.get(ann.fact, ())):
            raise LatticeUnsupported(
                f"Grain({ann.fact}, PERIOD): AD2 decides a SUM by the period of "
                "the fact, which the declared schema does not state"
            )


def build_space(
    schema: DGMSchema, anchors: Sequence[str], *, h: int, max_anchors: int
) -> Space:
    """``G_𝒜`` for *anchors* within *h* hops; raises on an invalid anchor set."""
    canonical = _canonical_anchors(schema, anchors, h, max_anchors)
    adj = _undirected_adjacency(schema)
    dist = {n: _distances_from(adj, n) for n in adj}
    nodes = _scope_types(schema, canonical, h, dist)
    edges = _scope_edges(schema, canonical, h, dist)
    sigma = _restrict_sigma(schema, nodes, edges)
    props = _typed_props(schema, nodes)
    for kind in _needed_kinds(schema, h, nodes, edges, props):
        schema.annotations(kind)  # AnnotationNotDeclared if the kind is unstated
    _check_additivity(schema, sigma, props)
    return Space(
        schema=schema,
        anchors=canonical,
        h=h,
        nodes=nodes,
        edges=edges,
        sigma=sigma,
        props=props,
        hops=_hops(nodes, edges),
        dist=dist,
        version=schema_version(schema),
    )
