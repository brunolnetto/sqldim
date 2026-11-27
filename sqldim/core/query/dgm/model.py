"""The declared DGM model of a dataset: ``G_S``, ``K_S``, ``M`` and ``Σ`` (§2.1, §2.4, §2.6).

A DGM model is never inferred from table or column names.  A dataset declares
it: the handle opened through ``ToolContext.data`` implements
:class:`DeclaresDGMModel`, whose ``dgm_model()`` returns a :class:`DGMModel` or
raises :class:`ModelNotDeclared`.  Every tool that reads a model — the
invariants of ``dgm_validate_invariant``, the vocabularies and anchor sets of
``dgm_map_analytical_space`` — reads this one declaration.

The classes mirror the grammar of §2.6 (``NodeTable``, ``ForeignKey``,
``RelationTable``, ``Derived``) and the schema graph of §2.1.

Two checks are kept apart:

* **Well-formedness**, at construction (:class:`ModelDeclarationError`): every
  name is declared once, every edge type and every argument of Σ names a type
  of the kind §2.4 requires, every value type is one of ``K_S``.
* **Invariants** of §2.1 and §2.6 (PARTITION, H1–H3, M1–M7, PA1–PA4) are claims
  about a well-formed model and its data; ``dgm_validate_invariant`` evaluates
  them, so construction does not reject a model that violates one.

``Σ`` is declared per annotation kind.  ``sigma_kinds`` names the kinds for
which ``sigma`` is complete: for such a kind, a type without that annotation
does not have it.  A kind outside ``sigma_kinds`` is undeclared, and reading it
raises :class:`AnnotationNotDeclared` — its absence is never read as "none".
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, TypeVar, runtime_checkable

from sqldim.core.query.dgm.annotations import (
    ANNOTATION_KINDS,
    BridgeSemantics,
    Conformed,
    DerivedFact,
    FactlessFact,
    Grain,
    Hierarchy,
    MedallionLayer,
    PipelineArtifact,
    ProjectsFrom,
    RolePlaying,
    SCDKind,
    SCDType,
    SchemaAnnotation,
    WeightConstraint,
    annotation_kind,
)

__all__ = [
    "VERSIONED_SCD",
    "AnnotationNotDeclared",
    "DGMModel",
    "DeclaresDGMModel",
    "Derived",
    "EdgeKind",
    "EdgeMapping",
    "EdgeType",
    "ForeignKey",
    "Holder",
    "ModelDeclarationError",
    "ModelNotDeclared",
    "NodeKind",
    "NodeTable",
    "NodeType",
    "PropType",
    "Reference",
    "RelationTable",
]


class ModelDeclarationError(ValueError):
    """A declared model is ill-formed and cannot be constructed."""


class ModelNotDeclared(LookupError):
    """The dataset declares no DGM model."""


class AnnotationNotDeclared(LookupError):
    """A tool read an annotation kind the declaration does not state."""

    def __init__(self, kind: str) -> None:
        super().__init__(
            f"the declared model does not state Σ for {kind}: its absence is "
            "not read as 'none'"
        )
        self.kind = kind


class NodeKind(str, Enum):
    """``τ_N`` (§2.1)."""

    DIM = "dim"
    FACT = "fact"


class EdgeKind(str, Enum):
    """``τ_E`` (§2.1)."""

    VERB = "verb"
    BRIDGE = "bridge"


class Holder(str, Enum):
    """Which endpoint's NodeTable stores a ForeignKey column (§2.6)."""

    SOURCE = "SOURCE"
    TARGET = "TARGET"


class Reference(str, Enum):
    """What a stored reference matches in the referenced NodeTable (§2.6)."""

    ENTITY_KEY = "ENTITY_KEY"
    VERSION_KEY = "VERSION_KEY"


VERSIONED_SCD = frozenset({SCDKind.SCD2, SCDKind.SCD6})
"""SCD types whose NodeTable holds one row per version (§2.6, M2)."""

_BASE_TYPES = frozenset({"String", "Integer", "Real", "Boolean", "Date", "Timestamp"})


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise ModelDeclarationError(message)


def _columns(owner: str, what: str, cols: Iterable[str]) -> None:
    cols = tuple(cols)
    _require(bool(cols) and all(cols), f"{owner}: {what} needs non-empty columns")


@dataclass(frozen=True)
class PropType:
    """A value type of ``K_S`` (§2.1): a base type or ``Enum(values)``."""

    name: str
    values: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.name == "Enum":
            _require(bool(self.values), "Enum needs at least one value")
        else:
            _require(self.name in _BASE_TYPES, f"unknown property type {self.name!r}")
            _require(not self.values, f"{self.name} takes no values")

    @classmethod
    def enum(cls, *values: str) -> "PropType":
        return cls("Enum", tuple(values))

    def __str__(self) -> str:
        if self.name == "Enum":
            return f"Enum({', '.join(self.values)})"
        return self.name


# ---------------------------------------------------------------------------
# M (§2.6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeTable:
    """``NodeMapping`` of §2.6; ``props`` maps a property key to a column."""

    relation: str
    entity_key: tuple[str, ...]
    props: Mapping[str, str] = field(default_factory=dict)
    version_key: str | None = None
    effective_from: str | None = None
    effective_to: str | None = None
    event_time: str | None = None

    def __post_init__(self) -> None:
        _require(
            bool(self.relation) and bool(self.entity_key),
            "a NodeTable needs a relation and an entity_key",
        )
        _columns(self.relation, "entity_key", self.entity_key)
        _require(all(self.props.values()), f"{self.relation}: a prop needs a column")

    def columns(self) -> tuple[str, ...]:
        """Every column the mapping names, in declaration order."""
        optional = (
            self.version_key,
            self.effective_from,
            self.effective_to,
            self.event_time,
        )
        named = (*self.entity_key, *self.props.values(), *optional)
        return tuple(dict.fromkeys(c for c in named if c is not None))


@dataclass(frozen=True)
class ForeignKey:
    holder: Holder
    columns: tuple[str, ...]
    references: Reference

    def __post_init__(self) -> None:
        _columns("ForeignKey", "columns", self.columns)


@dataclass(frozen=True)
class RelationTable:
    relation: str
    source: tuple[str, ...]
    target: tuple[str, ...]
    references: Reference
    props: Mapping[str, str] = field(default_factory=dict)
    valid_from: str | None = None
    valid_to: str | None = None

    def __post_init__(self) -> None:
        _require(bool(self.relation), "a RelationTable needs a relation")
        _columns(self.relation, "source", self.source)
        _columns(self.relation, "target", self.target)


@dataclass(frozen=True)
class Derived:
    """A read-only view; its source and target columns hold entity keys."""

    view: str
    source: tuple[str, ...]
    target: tuple[str, ...]
    valid_from: str | None = None
    valid_to: str | None = None

    def __post_init__(self) -> None:
        _require(bool(self.view), "a Derived mapping needs a view")
        _columns(self.view, "source", self.source)
        _columns(self.view, "target", self.target)


EdgeMapping = ForeignKey | RelationTable | Derived


# ---------------------------------------------------------------------------
# G_S with K_S and M (§2.1)
# ---------------------------------------------------------------------------


def _signature(owner: str, properties: Mapping[str, PropType]) -> None:
    for key, type_ in properties.items():
        _require(bool(key), f"{owner}: property keys must be non-empty")
        _require(
            isinstance(type_, PropType), f"{owner}.{key}: value type is not a PropType"
        )


def _coerce(enum: type[Enum], value: Any, what: str) -> Any:
    try:
        return enum(value)
    except ValueError:
        raise ModelDeclarationError(f"{what} must be one of {[m.value for m in enum]}")


@dataclass(frozen=True)
class NodeType:
    """A node type of ``N_S`` with ``τ_N``, ``K_S`` and ``M_N``."""

    name: str
    kind: NodeKind
    properties: Mapping[str, PropType] = field(default_factory=dict)
    mapping: NodeTable | None = None

    def __post_init__(self) -> None:
        _require(
            bool(self.name) and "@" not in self.name,
            f"invalid node type name {self.name!r}",
        )
        object.__setattr__(
            self, "kind", _coerce(NodeKind, self.kind, f"{self.name}: τ_N")
        )
        _signature(self.name, self.properties)
        _require(
            self.mapping is None or isinstance(self.mapping, NodeTable),
            f"{self.name}: M_N must be a NodeTable",
        )


@dataclass(frozen=True)
class EdgeType:
    """An edge type ``(source, target, label)`` of ``E_S`` with its mapping.

    ``weighted``, ``validity`` and ``many_to_many`` are the declarations M4
    reads; ``properties`` is ``K_S(e)``.
    """

    source: str
    target: str
    label: str
    kind: EdgeKind
    properties: Mapping[str, PropType] = field(default_factory=dict)
    weighted: bool = False
    validity: bool = False
    many_to_many: bool = False
    mapping: EdgeMapping | None = None

    def __post_init__(self) -> None:
        _require(bool(self.label), f"{self.term}: label must be non-empty")
        object.__setattr__(
            self, "kind", _coerce(EdgeKind, self.kind, f"{self.term}: τ_E")
        )
        _signature(self.term, self.properties)
        _require(
            self.mapping is None
            or isinstance(self.mapping, (ForeignKey, RelationTable, Derived)),
            f"{self.term}: M_E must be a ForeignKey, RelationTable or Derived",
        )

    @property
    def key(self) -> str:
        return f"({self.source}, {self.target}, {self.label!r})"

    @property
    def term(self) -> str:
        """The edge type as a §8.16.1 term: ``(source, label, target)``."""
        return f"({self.source}, {self.label}, {self.target})"


# ---------------------------------------------------------------------------
# Σ: the argument kinds of §2.4
# ---------------------------------------------------------------------------

_Ref = tuple[str, str]  # (required kind: dim | fact | node | bridge, name)

_SIGMA_REFS: dict[type, Callable[[Any], list[_Ref]]] = {
    Conformed: lambda a: [("dim", a.dim)] + [("fact", f) for f in a.fact_types],
    Grain: lambda a: [("fact", a.fact)],
    SCDType: lambda a: [("dim", a.dim)],
    RolePlaying: lambda a: [("dim", a.dim)],
    ProjectsFrom: lambda a: [("dim", a.dim_mini), ("dim", a.dim_full)],
    FactlessFact: lambda a: [("fact", a.fact)],
    DerivedFact: lambda a: [("fact", a.fact)] + [("fact", s) for s in a.sources],
    WeightConstraint: lambda a: [("bridge", a.bridge)],
    BridgeSemantics: lambda a: [("bridge", a.bridge)],
    Hierarchy: lambda a: [("dim", a.root)],
    PipelineArtifact: lambda a: (
        [("fact", a.fact)] + ([("fact", a.transitions)] if a.transitions else [])
    ),
    MedallionLayer: lambda a: [("node", a.node)],
}
"""The type arguments of each annotation and the kind §2.4 requires of them."""

_UNIQUE_PER: dict[type, Callable[[Any], str]] = {
    SCDType: lambda a: a.dim,
    MedallionLayer: lambda a: a.node,
    FactlessFact: lambda a: a.fact,
    PipelineArtifact: lambda a: a.fact,
}
"""Annotations a type carries at most once."""

_A = TypeVar("_A", bound=SchemaAnnotation)


@dataclass(frozen=True)
class DGMModel:
    """A declared DGM model: ``G_S``, ``K_S``, ``M`` and ``Σ``, checked at construction.

    ``sigma_kinds`` names the annotation kinds ``sigma`` is complete for; see
    the module docstring.
    """

    node_types: tuple[NodeType, ...]
    edge_types: tuple[EdgeType, ...] = ()
    sigma: tuple[SchemaAnnotation, ...] = ()
    sigma_kinds: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_types", tuple(self.node_types))
        object.__setattr__(self, "edge_types", tuple(self.edge_types))
        object.__setattr__(self, "sigma", tuple(self.sigma))
        object.__setattr__(self, "sigma_kinds", frozenset(self.sigma_kinds))
        self._check_types()
        self._check_sigma()

    # -- construction ----------------------------------------------------------

    def _check_types(self) -> None:
        _require(bool(self.node_types), "N_S is empty: a model has at least one type")
        names = [n.name for n in self.node_types]
        twice = sorted({n for n in names if names.count(n) > 1})
        _require(not twice, f"node type declared twice: {', '.join(twice)}")
        seen: set[tuple[str, str, str]] = set()
        for e in self.edge_types:
            _require(
                e.source in names and e.target in names,
                f"{e.term}: endpoint is not a node type",
            )
            triple = (e.source, e.target, e.label)
            _require(triple not in seen, f"{e.term}: edge type declared twice")
            seen.add(triple)

    def _check_sigma(self) -> None:
        unknown = sorted(self.sigma_kinds - ANNOTATION_KINDS)
        _require(not unknown, f"Σ: unknown annotation kinds {unknown}")
        once: set[tuple[type, str]] = set()
        for ann in self.sigma:
            _require(isinstance(ann, SchemaAnnotation), "Σ holds only annotations")
            kind = annotation_kind(ann)
            _require(
                kind in self.sigma_kinds,
                f"Σ: {kind} is listed but not in sigma_kinds",
            )
            for required, name in _SIGMA_REFS.get(type(ann), lambda _: [])(ann):
                _require(
                    self._is(required, name),
                    f"Σ: {kind} names {name!r}, which is not a {required} type",
                )
            unique = _UNIQUE_PER.get(type(ann))
            if unique is not None:
                tag = (type(ann), unique(ann))
                _require(tag not in once, f"Σ: {kind} declared twice for {tag[1]!r}")
                once.add(tag)

    def _is(self, required: str, name: str) -> bool:
        if required == "bridge":
            return any(
                e.kind is EdgeKind.BRIDGE and e.label == name for e in self.edge_types
            )
        node = self.node(name)
        return node is not None and required in ("node", node.kind.value)

    # -- G_S -------------------------------------------------------------------

    @property
    def node_names(self) -> list[str]:
        return sorted(n.name for n in self.node_types)

    def node(self, name: str) -> NodeType | None:
        return next((n for n in self.node_types if n.name == name), None)

    def edge(self, source: str, target: str, label: str) -> EdgeType | None:
        return next(
            (
                e
                for e in self.edge_types
                if (e.source, e.target, e.label) == (source, target, label)
            ),
            None,
        )

    def partition_problems(self, edge: EdgeType) -> list[str]:
        """Why *edge* breaks the §2.1 structural invariants (PARTITION)."""
        ends = (("source", edge.source), ("target", edge.target))
        problems = [
            f"{end} {n!r} is not a node type" for end, n in ends if not self.node(n)
        ]
        if problems or edge.kind is not EdgeKind.VERB:
            return problems
        if self.node(edge.source).kind is not NodeKind.DIM:  # type: ignore[union-attr]
            problems.append("a verb edge type must start at a dimension type")
        if self.node(edge.target).kind is not NodeKind.FACT:  # type: ignore[union-attr]
            problems.append("a verb edge type must end at a fact type")
        return problems

    # -- Σ ---------------------------------------------------------------------

    def annotations(self, kind: type[_A]) -> tuple[_A, ...]:
        """Every annotation of *kind*; :class:`AnnotationNotDeclared` if unstated."""
        name = kind.__name__
        if name not in self.sigma_kinds:
            raise AnnotationNotDeclared(name)
        return tuple(a for a in self.sigma if type(a) is kind)

    def scd_of(self, name: str) -> SCDKind | None:
        """The SCDType of *name*, ``None`` if it has none (SCDType declared)."""
        return next((a.scd for a in self.annotations(SCDType) if a.dim == name), None)

    def versioned(self, name: str) -> bool:
        """*name* is SCD2 or SCD6, so its NodeTable has one row per version."""
        return self.scd_of(name) in VERSIONED_SCD

    def roles_of(self, name: str) -> frozenset[str]:
        """Roles that tell apart two anchors of type *name* (AS2, §3.4.6).

        A role is declared by ``RolePlaying`` or is the label of an edge type
        reaching *name*.
        """
        declared = {
            r for a in self.annotations(RolePlaying) if a.dim == name
            for r in a.roles
        }  # fmt: skip
        labels = {e.label for e in self.edge_types if name in (e.source, e.target)}
        return frozenset(declared | labels)


@runtime_checkable
class DeclaresDGMModel(Protocol):
    """A dataset handle that declares its DGM model.

    ``dgm_model()`` returns the :class:`DGMModel` or raises
    :class:`ModelNotDeclared` when the dataset declares none.
    """

    def dgm_model(self) -> DGMModel: ...
