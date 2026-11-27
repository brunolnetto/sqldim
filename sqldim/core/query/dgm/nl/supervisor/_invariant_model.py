"""DGM Supervisor — the declared model a dataset is validated against.

``dgm_validate_invariant`` checks the invariants of §2.1, §2.4 and §2.6.  They
are stated over the schema graph ``G_S`` (node and edge types, their kinds and
property signature ``K_S``), the relational mapping ``M`` and the annotations
``Σ`` — schema metadata held with the mapping, never stored in the data
relations (§2.6, *Mapping table*).  A DuckDB catalog does not carry it, so it
is never inferred from table or column names: the dataset handle opened
through ``ToolContext.data`` declares it by implementing
:class:`DeclaresDGMModel`.  A handle that does not is a dataset this tool
cannot validate (``UNSUPPORTED``).

The classes mirror the grammar of §2.6 (``NodeTable``, ``ForeignKey``,
``RelationTable``, ``Derived``) and the parts of §2.1/§2.4 the invariants read.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, runtime_checkable

from sqldim.core.query.dgm.annotations import SCDKind

__all__ = [
    "ARTIFACT_STATES",
    "DELTA",
    "DGMModel",
    "DeclaresDGMModel",
    "Derived",
    "EdgeKind",
    "EdgeMapping",
    "EdgeType",
    "ForeignKey",
    "Holder",
    "NodeKind",
    "NodeTable",
    "NodeType",
    "PipelineArtifactRef",
    "PropType",
    "Reference",
    "RelationTable",
    "VERSIONED_SCD",
]


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


@dataclass(frozen=True)
class PropType:
    """A value type of ``K_S`` (§2.1): a base type or ``Enum(values)``."""

    name: str
    values: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.name == "Enum":
            if not self.values:
                raise ValueError("Enum needs at least one value")
        elif self.name not in _BASE_TYPES:
            raise ValueError(f"unknown property type {self.name!r}")
        elif self.values:
            raise ValueError(f"{self.name} takes no values")

    @classmethod
    def enum(cls, *values: str) -> "PropType":
        return cls("Enum", tuple(values))


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
        if not self.relation or not self.entity_key:
            raise ValueError("a NodeTable needs a relation and an entity_key")


@dataclass(frozen=True)
class ForeignKey:
    holder: Holder
    columns: tuple[str, ...]
    references: Reference


@dataclass(frozen=True)
class RelationTable:
    relation: str
    source: tuple[str, ...]
    target: tuple[str, ...]
    references: Reference
    props: Mapping[str, str] = field(default_factory=dict)
    valid_from: str | None = None
    valid_to: str | None = None


@dataclass(frozen=True)
class Derived:
    """A read-only view; its source and target columns hold entity keys."""

    view: str
    source: tuple[str, ...]
    target: tuple[str, ...]
    valid_from: str | None = None
    valid_to: str | None = None


EdgeMapping = ForeignKey | RelationTable | Derived


@dataclass(frozen=True)
class NodeType:
    """A node type of ``N_S`` with ``τ_N``, ``K_S``, ``M_N`` and its SCDType."""

    name: str
    kind: NodeKind
    properties: Mapping[str, PropType] = field(default_factory=dict)
    mapping: NodeTable | None = None
    scd: SCDKind | None = None

    @property
    def versioned(self) -> bool:
        return self.scd in VERSIONED_SCD


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

    @property
    def key(self) -> str:
        return f"({self.source}, {self.target}, {self.label!r})"


@dataclass(frozen=True)
class PipelineArtifactRef:
    """``PipelineArtifact(f, …, transitions)`` of Σ (§2.4): the two fact types.

    The bridge edge types it implies are looked up in the model as
    ``(transitions, artifact, 'transition_of')`` and
    ``(transitions, transitions, 'followed_by')``.
    """

    artifact: str
    transitions: str


ARTIFACT_STATES = ("Missing", "In-flight", "Complete", "Stale", "Failed")

DELTA: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("Missing", "scheduled", "In-flight"),
        ("In-flight", "succeeded", "Complete"),
        ("In-flight", "failed", "Failed"),
        ("In-flight", "failed", "Stale"),
        ("Complete", "expired", "Stale"),
        ("Stale", "rescheduled", "In-flight"),
        ("Failed", "retried", "In-flight"),
    }
)
"""The admissible ``(from_state, kind, to_state)`` triples δ of §2.4."""


@dataclass(frozen=True)
class DGMModel:
    """``G_S`` with ``M`` and the parts of Σ the invariants read."""

    node_types: tuple[NodeType, ...]
    edge_types: tuple[EdgeType, ...] = ()
    pipeline_artifacts: tuple[PipelineArtifactRef, ...] = ()

    def __post_init__(self) -> None:
        names = [n.name for n in self.node_types]
        if len(set(names)) != len(names):
            raise ValueError("node type names must be unique")
        keys = [(e.source, e.target, e.label) for e in self.edge_types]
        if len(set(keys)) != len(keys):
            raise ValueError("edge types must be unique")

    def node(self, name: str) -> NodeType | None:
        for n in self.node_types:
            if n.name == name:
                return n
        return None

    def edge(self, source: str, target: str, label: str) -> EdgeType | None:
        for e in self.edge_types:
            if (e.source, e.target, e.label) == (source, target, label):
                return e
        return None


@runtime_checkable
class DeclaresDGMModel(Protocol):
    """A dataset handle that declares its DGM model."""

    def dgm_model(self) -> DGMModel: ...
