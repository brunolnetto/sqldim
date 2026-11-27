"""The scope ``G_𝒜`` of one anchor set and the §4.1 types (§3.4.6, §8.11).

:func:`build_space` checks an anchor set (AS1–AS3, canonical roles), computes
``N_𝒜``, ``E_𝒜`` and ``Σ|_𝒜`` within the hop bound and refuses — with
:class:`LatticeUnsupported` — what the enumeration cannot decide from the
declaration: a property whose type is not a §4.1 ``Type``, or a SUM governed
by AD2 whose period the declaration does not state.

It also resolves what each numeric property may be aggregated by (§2.4): a
measure of a fact type by its ``Additivity`` or the default of its ``Grain``
(AD1–AD4), a ``Degenerate`` attribute by COUNT only, and a numeric attribute
of a dimension type by COUNT, MIN, MAX and AVG over ENTITIES, never SUM.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping, Sequence
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
from sqldim.core.query.dgm.analytical_space import AGG_FNS
from sqldim.core.query.dgm.annotations import (
    Additivity,
    AdditivityKind,
    BridgeSemantics,
    BridgeSemanticsKind,
    Conformed,
    Degenerate,
    FactlessFact,
    Grain,
    GrainKind,
    MedallionLayer,
    SchemaAnnotation,
)
from sqldim.core.query.dgm.model import (
    EdgeKind,
    EdgeType,
    NodeKind,
    PropType,
    sigma_refs,
)
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
    a fact; Grain, Additivity and Degenerate decide the aggregates over a fact
    with a numeric property (AD1–AD4).
    BridgeSemantics and Conformed are read where they decide something — a
    bridge hop of a pattern, a dimension between two facts of a Join — and
    raise there (:meth:`Space.time_ordered`, :meth:`Space.conformed_for`).
    """
    facts = _of_kind(schema, nodes, NodeKind.FACT)
    rules = (
        (MedallionLayer, True),
        (FactlessFact, bool(facts)),
        (Grain, _numeric(facts, props)),
        (Additivity, _numeric(facts, props)),
        (Degenerate, _numeric(facts, props)),
    )
    return [kind for kind, needed in rules if needed]


def _of_kind(schema: DGMSchema, nodes: Iterable[str], kind: NodeKind) -> list[str]:
    return [n for n in nodes if getattr(schema.node(n), "kind", None) is kind]


def _numeric(facts: Iterable[str], props: Mapping[str, Any]) -> bool:
    return any(t in NUMERIC for f in facts for _, t in props.get(f, ()))


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
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Enum):
        return str(value.value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = dataclasses.fields(value)
        return (
            type(value).__name__,
            tuple((f.name, _render(getattr(value, f.name))) for f in fields),
        )
    return _render_collection(value)


def _render_collection(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(k), _render(v)) for k, v in value.items()))
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(_render(v) for v in value))
    if isinstance(value, (tuple, list)):
        return tuple(_render(v) for v in value)
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
    measures: Mapping[tuple[str, str], str] = field(default_factory=dict)
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

    def agg_fns(
        self, type_: str, key: str, *, per_tuple: bool = False
    ) -> frozenset[str] | None:
        """AggFns admitted over the numeric property ``type_.key``.

        ``None`` for a non-numeric property (only §4.2 typing applies).  A
        numeric attribute of a dimension type is not a measure: it admits
        COUNT, MIN, MAX and AVG over ENTITIES, never SUM or PROD (§2.4).  A
        window (*per_tuple*) reads each tuple of C, not each instance once,
        so there it admits COUNT only.
        """
        if self.prop_type(type_, key) not in NUMERIC:
            return None
        if self.kind(type_) != NodeKind.FACT.value:
            return _COUNT_ONLY if per_tuple else _DIM_ATTRIBUTE
        return _BY_KIND[self.measures[(type_, key)]]

    def factless(self, type_: str) -> bool:
        return any(isinstance(a, FactlessFact) and a.fact == type_ for a in self.sigma)

    def bronze(self, type_: str) -> bool:
        return any(
            isinstance(a, MedallionLayer) and a.node == type_ and a.is_bronze
            for a in self.sigma
        )

    def conformed_for(self, dim: str, facts: set[str]) -> bool:
        """``Conformed(dim, Fs) ∈ Σ|_𝒜`` with ``facts ⊆ Fs``.

        Raises ``AnnotationNotDeclared`` if the model does not state Conformed.
        """
        self.schema.annotations(Conformed)
        return any(
            isinstance(a, Conformed) and a.dim == dim and facts <= a.fact_types
            for a in self.sigma
        )

    def time_ordered(self, hop: Hop) -> bool:
        """A hop along a CAUSAL, TEMPORAL or SUPERSESSION bridge, forwards (§2.3)."""
        source, label, target, direction = hop
        edge = next(e for e in self.edges if (e.source, e.label, e.target) == hop[:3])
        if direction != "+" or edge.kind is not EdgeKind.BRIDGE:
            return False
        self.schema.annotations(BridgeSemantics)  # AnnotationNotDeclared if unstated
        return any(
            isinstance(a, BridgeSemantics)
            and a.bridge == label
            and a.sem in _TIME_ORDERED
            for a in self.sigma
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


def _restrict_sigma(schema: DGMSchema, nodes, edges) -> tuple[SchemaAnnotation, ...]:
    """``Σ|_𝒜``: every annotation whose type arguments all lie in ``G_𝒜`` (§3.4.6).

    A node type argument must be in ``N_𝒜`` and a bridge edge type argument
    must label a bridge edge of ``E_𝒜``, whatever the annotation kind.
    """
    labels = {e.label for e in edges if e.kind.value == "bridge"}
    out: list[SchemaAnnotation] = []
    for ann in schema.sigma:
        refs = sigma_refs(ann)
        if refs is not None and all(
            (name in labels) if kind == "bridge" else (name in nodes)
            for kind, name in refs
        ):
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


DEGENERATE = "DEGENERATE"
"""Resolution of a ``Degenerate`` attribute: not a measure (§2.4)."""

_COUNT_ONLY = frozenset({"COUNT"})
_DIM_ATTRIBUTE = frozenset({"COUNT", "MIN", "MAX", "AVG"})
"""§2.4: a numeric dimension attribute, aggregated over ENTITIES (each
instance once); a SUM over it is rejected."""
_BY_KIND: dict[str, frozenset[str]] = {
    AdditivityKind.ADDITIVE.value: frozenset(AGG_FNS),  # AD1, AD4
    # AD3 rejects SUM (PROD with it); AVG only with a warning: not enumerated.
    AdditivityKind.NON_ADDITIVE.value: frozenset({"COUNT", "MIN", "MAX"}),
    DEGENERATE: _COUNT_ONLY,
}
"""AggFns the enumeration admits over a numeric property, by its resolution."""

_GRAIN_DEFAULT = {
    GrainKind.EVENT: AdditivityKind.ADDITIVE,
    GrainKind.ACCUMULATING: AdditivityKind.ADDITIVE,
    GrainKind.PERIOD: AdditivityKind.SEMI_ADDITIVE,
}
"""§2.4: the Additivity a measure takes from its grain when it declares none."""


def _measures(sigma, props, facts: Iterable[str]) -> dict[tuple[str, str], str]:
    """Resolve every numeric property of a fact of ``G_𝒜`` (§2.4).

    Raises :class:`LatticeUnsupported` for a SEMI_ADDITIVE measure: AD2
    decides its SUM by the period or the entity of each axis, which the
    enumeration does not decide.
    """
    grain = {a.fact: a.grain for a in sigma if isinstance(a, Grain)}
    declared = _declared_measures(sigma)
    return {
        (fact, key): _resolve(grain, declared, fact, key)
        for fact in facts
        for key, t in props.get(fact, ())
        if t in NUMERIC
    }


def _declared_measures(sigma) -> dict[tuple[str, str], Any]:
    """``(fact, key)`` → its AdditivityKind, or DEGENERATE."""
    out: dict[tuple[str, str], Any] = {}
    for a in sigma:
        if isinstance(a, Additivity):
            out[(a.fact, a.key)] = a.kind
        elif isinstance(a, Degenerate):
            out[(a.fact or "", a.key)] = DEGENERATE
    return out


def _resolve(grain, declared, fact: str, key: str) -> str:
    """The resolution of one numeric fact property; SEMI_ADDITIVE raises."""
    kind = declared.get((fact, key))
    if kind is None:
        kind = _default(grain, fact, key)
    if kind is AdditivityKind.SEMI_ADDITIVE:
        how = (
            f"Grain({fact}, PERIOD)"
            if (fact, key) not in declared
            else f"Additivity({fact}, {key}, SEMI_ADDITIVE)"
        )
        raise LatticeUnsupported(
            f"{how}: {fact}.{key} is SEMI_ADDITIVE and AD2 decides its "
            "SUM by the period or entity of each axis, which the "
            "enumeration does not decide"
        )
    return kind if isinstance(kind, str) else kind.value


def _default(grain: Mapping[str, GrainKind], fact: str, key: str) -> AdditivityKind:
    if fact not in grain:
        raise LatticeUnsupported(
            f"{fact}.{key} is a measure of a fact with no Grain: its Additivity "
            "has no default (§2.4)"
        )
    return _GRAIN_DEFAULT[grain[fact]]


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
    facts = _of_kind(schema, nodes, NodeKind.FACT)
    # a measure is resolved by its fact's declaration, whatever else Σ|_𝒜 keeps
    measures = _measures(schema.sigma, props, facts) if _numeric(facts, props) else {}
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
        measures=measures,
    )
