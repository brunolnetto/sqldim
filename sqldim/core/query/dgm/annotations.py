"""DGM schema annotation layer Σ (DGM v0.17 §2.4).

Provides the 12 SchemaAnnotation dataclass types, their enum types, the RAGGED
sentinel, the AnnotationSigma collection, and the annotation_kind() dispatch
helper.

Every annotation is an operational decision object: its properties are directly
consulted by the planner (Rules 1a-1d, 10), recommender, and exporter without
requiring string-matching on type names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator, Sequence, Type, TypeVar

__all__ = [
    # Enum types
    "GrainKind",
    "SCDKind",
    "WeightConstraintKind",
    "BridgeSemanticsKind",
    "WriteModeKind",
    "PipelineStateKind",
    # RAGGED sentinel
    "RAGGED",
    # Abstract base
    "SchemaAnnotation",
    # 12 annotation types
    "Conformed",
    "Grain",
    "SCDType",
    "Degenerate",
    "RolePlaying",
    "ProjectsFrom",
    "FactlessFact",
    "DerivedFact",
    "WeightConstraint",
    "BridgeSemantics",
    "Hierarchy",
    "PipelineArtifact",
    # Helpers
    "annotation_kind",
    "AnnotationSigma",
]

# ---------------------------------------------------------------------------
# Enum types
# ---------------------------------------------------------------------------


class GrainKind(Enum):
    """Grain classifications for fact tables (spec §2.4 Grain)."""
    EVENT = "EVENT"
    PERIOD = "PERIOD"
    ACCUMULATING = "ACCUMULATING"


class SCDKind(Enum):
    """SCD type variants (spec §2.4 SCDType)."""
    SCD1 = "SCD1"
    SCD2 = "SCD2"
    SCD3 = "SCD3"
    SCD6 = "SCD6"


class WeightConstraintKind(Enum):
    """Weight constraint policy for bridge tables (spec §2.4 WeightConstraint)."""
    ALLOCATIVE = "ALLOCATIVE"
    UNCONSTRAINED = "UNCONSTRAINED"


class BridgeSemanticsKind(Enum):
    """Bridge-edge semantic category (spec §2.4 BridgeSemantics)."""
    CAUSAL = "CAUSAL"
    TEMPORAL = "TEMPORAL"
    SUPERSESSION = "SUPERSESSION"
    STRUCTURAL = "STRUCTURAL"


class WriteModeKind(Enum):
    """Write mode for pipeline-managed fact tables (spec §2.4 PipelineArtifact)."""
    BACKFILL = "BACKFILL"
    REFRESH = "REFRESH"
    ADAPTIVE = "ADAPTIVE"


class PipelineStateKind(Enum):
    """Five-state lifecycle for a PipelineArtifact fact (spec §2.4)."""
    MISSING = "MISSING"
    IN_FLIGHT = "IN_FLIGHT"
    COMPLETE = "COMPLETE"
    STALE = "STALE"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# RAGGED sentinel
# ---------------------------------------------------------------------------


class _Ragged:
    """Singleton sentinel for Hierarchy(depth=RAGGED)."""

    _instance: "_Ragged | None" = None

    def __new__(cls) -> "_Ragged":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "RAGGED"


RAGGED = _Ragged()


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class SchemaAnnotation:
    """Base class for all Σ annotation types.

    All 11 annotation dataclasses inherit from this base so that
    ``isinstance(ann, SchemaAnnotation)`` works uniformly.
    """


# ---------------------------------------------------------------------------
# 11 annotation dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=True)
class Conformed(SchemaAnnotation):
    """d is reachable via verb edges from each fact type in fact_types.

    Enables constellation paths (spec §2.4).
    """
    dim: str
    fact_types: frozenset[str] = field(default_factory=frozenset, compare=True)

    def __init__(self, dim: str, fact_types: set[str] | frozenset[str]) -> None:
        object.__setattr__(self, "dim", dim)
        object.__setattr__(self, "fact_types", frozenset(fact_types))


@dataclass(frozen=True, eq=True)
class Grain(SchemaAnnotation):
    """Declares the measurement grain for a fact table.

    Rule 1b consequences:
    - PERIOD   → SUM is invalid; LAST is suggested.
    - ACCUMULATING → cross-row agg warns; NULL → COALESCE.
    - EVENT    → fully additive; no restrictions.
    """
    fact: str
    grain: GrainKind

    @property
    def sum_invalid(self) -> bool:
        """True when SUM aggregation must be rejected at construction (PERIOD only)."""
        return self.grain is GrainKind.PERIOD


@dataclass(frozen=True, eq=True)
class SCDType(SchemaAnnotation):
    """SCD versioning policy for a dimension table.

    Rule 1c consequences:
    - SCD1 → strip temporal resolution (strip_temporal=True).
    - SCD2 → standard effective_from/to join.
    - SCD3 → PropRef(d.current_value) / PropRef(d.previous_value).
    - SCD6 → lateral join; SCD2 for versioned_attrs; direct for overwrite_attrs.
    """
    dim: str
    scd: SCDKind
    versioned_attrs: frozenset[str] | None = None
    overwrite_attrs: frozenset[str] | None = None
    prev_attrs: frozenset[str] | None = None

    def __init__(
        self,
        dim: str,
        scd: SCDKind,
        versioned_attrs: set[str] | frozenset[str] | None = None,
        overwrite_attrs: set[str] | frozenset[str] | None = None,
        prev_attrs: set[str] | frozenset[str] | None = None,
    ) -> None:
        object.__setattr__(self, "dim", dim)
        object.__setattr__(self, "scd", scd)
        object.__setattr__(
            self, "versioned_attrs",
            frozenset(versioned_attrs) if versioned_attrs is not None else None,
        )
        object.__setattr__(
            self, "overwrite_attrs",
            frozenset(overwrite_attrs) if overwrite_attrs is not None else None,
        )
        object.__setattr__(
            self, "prev_attrs",
            frozenset(prev_attrs) if prev_attrs is not None else None,
        )

    @property
    def strip_temporal(self) -> bool:
        """True for SCD1 — planner strips TemporalJoin predicate entirely."""
        return self.scd is SCDKind.SCD1


@dataclass(frozen=True, eq=True)
class Degenerate(SchemaAnnotation):
    """Declares a dimension as degenerate (stored on the fact, no dimension table).

    GroupBy planning excludes degenerate dims unless explicitly requested.
    """
    dim: str


@dataclass(eq=True)
class RolePlaying(SchemaAnnotation):
    """Declares a role-playing dimension and its aliases."""
    dim: str
    roles: list[str]


@dataclass(frozen=True, eq=True)
class ProjectsFrom(SchemaAnnotation):
    """Declares that dim_mini is a mini-dimension projected from dim_full."""
    dim_mini: str
    dim_full: str


@dataclass(frozen=True, eq=True)
class FactlessFact(SchemaAnnotation):
    """Declares a factless fact table (no measures — records occurrence only).

    Rule 1b: SUM/AVG/MIN/MAX must be rejected at construction.
    """
    fact: str

    @property
    def measures_invalid(self) -> bool:
        """True — all measure aggregations are invalid for factless facts."""
        return True


@dataclass(eq=True)
class DerivedFact(SchemaAnnotation):
    """Declares a fact as derived from one or more source facts via an expression."""
    fact: str
    sources: list[str]
    expr: str


@dataclass(frozen=True, eq=True)
class WeightConstraint(SchemaAnnotation):
    """Declares the weight constraint policy for a bridge table."""
    bridge: str
    constraint: WeightConstraintKind

    @property
    def is_allocative(self) -> bool:
        """True when weights must sum to 1.0 across all rows for a given key."""
        return self.constraint is WeightConstraintKind.ALLOCATIVE


@dataclass(frozen=True, eq=True)
class BridgeSemantics(SchemaAnnotation):
    """Declares the semantic category of a bridge-edge type.

    Rule 1a: CAUSAL → DAG BFS (drop cycle guard); reverse topological order on G^T.
    """
    bridge: str
    sem: BridgeSemanticsKind

    @property
    def is_dag(self) -> bool:
        """True for CAUSAL bridges — guarantees DAG topology; cycle guard dropped."""
        return self.sem is BridgeSemanticsKind.CAUSAL


@dataclass(frozen=True, eq=True)
class Hierarchy(SchemaAnnotation):
    """Declares the hierarchy structure rooted at a dimension node.

    depth is an integer for fixed-depth hierarchies or the RAGGED sentinel for
    variable-depth (self-referencing) hierarchies.
    """
    root: str
    depth: int | _Ragged


@dataclass(frozen=True, eq=True)
class PipelineArtifact(SchemaAnnotation):
    """Composite annotation for pipeline-managed accumulating fact tables (spec §2.4).

    Implies ``Grain(fact, ACCUMULATING)``.  Declares the five-state lifecycle
    (Missing → In-flight → Complete/Failed; Complete → Stale → In-flight) and
    the BridgeSemantics on each transition edge.  ``write_mode`` drives Rule 10.

    Parameters
    ----------
    fact:
        Name of the accumulating fact table.
    pipeline_id:
        Unique pipeline identifier (key into the pipeline registry).
    ttl:
        Artifact TTL in seconds.  Complete → Stale transition fires when
        ``now - completed_at > ttl``.
    backfill_horizon:
        Maximum look-back in days.  Missing/Failed windows older than this
        are not backfilled.
    write_mode:
        BACKFILL → always APPEND; REFRESH → always MERGE;
        ADAPTIVE → inferred from P(f).state at planning time (Rule 10).
    """
    fact: str
    pipeline_id: str
    ttl: int          # seconds
    backfill_horizon: int  # days
    write_mode: WriteModeKind

    @property
    def effective_grain(self) -> GrainKind:
        """PipelineArtifact always implies ACCUMULATING grain."""
        return GrainKind.ACCUMULATING

    def transition_semantics(
        self,
        from_state: PipelineStateKind,
        to_state: PipelineStateKind,
        *,
        is_refresh: bool = False,
    ) -> BridgeSemanticsKind:
        """Return the implied BridgeSemanticsKind for a state transition.

        In-flight → Failed during a REFRESH run (or ADAPTIVE with prior
        Complete) uses SUPERSESSION so the prior Complete artifact is
        preserved and the state regresses to Stale rather than Failed.
        All other successful/failed transitions from In-flight are CAUSAL.
        Time-driven transitions (TTL expiry, scheduling) are TEMPORAL.
        """
        if (
            from_state is PipelineStateKind.IN_FLIGHT
            and to_state is PipelineStateKind.FAILED
            and is_refresh
        ):
            return BridgeSemanticsKind.SUPERSESSION
        if from_state is PipelineStateKind.IN_FLIGHT:
            return BridgeSemanticsKind.CAUSAL
        return BridgeSemanticsKind.TEMPORAL


# ---------------------------------------------------------------------------
# annotation_kind() dispatch helper
# ---------------------------------------------------------------------------

_KIND_MAP: dict[type, str] = {
    Conformed: "Conformed",
    Grain: "Grain",
    SCDType: "SCDType",
    Degenerate: "Degenerate",
    RolePlaying: "RolePlaying",
    ProjectsFrom: "ProjectsFrom",
    FactlessFact: "FactlessFact",
    DerivedFact: "DerivedFact",
    WeightConstraint: "WeightConstraint",
    BridgeSemantics: "BridgeSemantics",
    Hierarchy: "Hierarchy",
    PipelineArtifact: "PipelineArtifact",
}

_AT = TypeVar("_AT", bound=SchemaAnnotation)


def annotation_kind(ann: SchemaAnnotation) -> str:
    """Return the canonical kind name for an annotation (e.g. ``"Grain"``)."""
    kind = _KIND_MAP.get(type(ann))
    if kind is None:
        return type(ann).__name__
    return kind


# ---------------------------------------------------------------------------
# AnnotationSigma — Σ collection
# ---------------------------------------------------------------------------


class AnnotationSigma:
    """Ordered collection of SchemaAnnotation objects — the Σ set.

    Provides O(1) typed-lookup helpers used by the planner and recommender.
    """

    def __init__(self, annotations: Sequence[SchemaAnnotation]) -> None:
        self._anns: list[SchemaAnnotation] = list(annotations)

    # -- Mutation ------------------------------------------------------------

    def add(self, ann: SchemaAnnotation) -> None:
        """Append *ann*; raises ValueError on exact duplicate."""
        if ann in self._anns:
            raise ValueError(f"duplicate annotation: {ann!r}")
        self._anns.append(ann)

    # -- Collection protocol -------------------------------------------------

    def __iter__(self) -> Iterator[SchemaAnnotation]:
        return iter(self._anns)

    def __len__(self) -> int:
        return len(self._anns)

    def filter(self, ann_type: Type[_AT]) -> list[_AT]:
        """Return all annotations of exactly *ann_type*."""
        return [a for a in self._anns if type(a) is ann_type]

    # -- Typed lookup helpers ------------------------------------------------

    def grain_of(self, fact: str) -> GrainKind | None:
        """Return the GrainKind for *fact*, or None if not annotated."""
        for a in self._anns:
            if isinstance(a, Grain) and a.fact == fact:
                return a.grain
        return None

    def scd_of(self, dim: str) -> SCDKind | None:
        """Return the SCDKind for *dim*, or None if not annotated."""
        for a in self._anns:
            if isinstance(a, SCDType) and a.dim == dim:
                return a.scd
        return None

    def is_factless(self, fact: str) -> bool:
        """Return True if *fact* has a FactlessFact annotation."""
        return any(isinstance(a, FactlessFact) and a.fact == fact for a in self._anns)

    def bridge_sem_of(self, bridge: str) -> BridgeSemanticsKind | None:
        """Return the BridgeSemanticsKind for *bridge*, or None."""
        for a in self._anns:
            if isinstance(a, BridgeSemantics) and a.bridge == bridge:
                return a.sem
        return None

    def is_conformed(self, dim: str, fact_type: str) -> bool:
        """Return True if dim is Conformed with *fact_type* in its fact_types."""
        for a in self._anns:
            if isinstance(a, Conformed) and a.dim == dim:
                return fact_type in a.fact_types
        return False

    def hierarchy_depth_of(self, root: str) -> int | _Ragged | None:
        """Return the hierarchy depth for *root*, or None."""
        for a in self._anns:
            if isinstance(a, Hierarchy) and a.root == root:
                return a.depth
        return None

    def pipeline_artifact_of(self, fact: str) -> "PipelineArtifact | None":
        """Return the PipelineArtifact annotation for *fact*, or None."""
        for a in self._anns:
            if isinstance(a, PipelineArtifact) and a.fact == fact:
                return a
        return None
