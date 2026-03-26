"""DGM Natural Language Interface — types and enumerations (§11).

Enums used across the NL interface:
  BandKind, TemporalIntent, CompositionOp, AmbiguityKind,
  SchemaEvolutionKind, SchemaEvolutionAction.

EntityRegistry and PathResolutionResult live here too.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

__all__ = [
    "BandKind",
    "TemporalIntent",
    "CompositionOp",
    "AmbiguityKind",
    "SchemaEvolutionKind",
    "SchemaEvolutionAction",
    "EntityRegistry",
    "PathResolutionResult",
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class BandKind(Enum):
    """Three-band structure of a DGM query (§5.2)."""

    B1 = "B1"
    B2 = "B2"
    B3 = "B3"


class TemporalIntent(Enum):
    """Temporal mode or property inferred from a natural-language phrase (§11.4)."""

    EVENTUALLY = "EVENTUALLY"
    GLOBALLY = "GLOBALLY"
    NEXT = "NEXT"
    UNTIL = "UNTIL"
    SINCE = "SINCE"
    ONCE = "ONCE"
    PREVIOUSLY = "PREVIOUSLY"
    SAFETY = "SAFETY"
    LIVENESS = "LIVENESS"
    RESPONSE = "RESPONSE"
    PERSISTENCE = "PERSISTENCE"
    RECURRENCE = "RECURRENCE"


class CompositionOp(Enum):
    """Q_algebra composition operations inferred from NL patterns (§11.4)."""

    UNION = "UNION"
    INTERSECT = "INTERSECT"
    JOIN = "JOIN"
    CHAIN = "CHAIN"


class AmbiguityKind(Enum):
    """Types of ambiguity resolvable by the NL interface (§11.6)."""

    MEASURE = "MEASURE"
    PATH = "PATH"
    TEMPORAL = "TEMPORAL"
    GRAIN = "GRAIN"
    COMPOSITIONAL = "COMPOSITIONAL"


class SchemaEvolutionKind(Enum):
    """Q_delta event types that drive interface updates (§11.7)."""

    ADDED_NODES = "ADDED_NODES"
    REMOVED_NODES = "REMOVED_NODES"
    ADDED_EDGES = "ADDED_EDGES"
    REMOVED_EDGES = "REMOVED_EDGES"
    ROLE_DRIFT = "ROLE_DRIFT"
    CHANGED_PROPERTY = "CHANGED_PROPERTY"


class SchemaEvolutionAction(Enum):
    """Actions the NL interface takes in response to schema evolution (§11.7)."""

    SUGGEST_NEW_QUESTIONS = "SUGGEST_NEW_QUESTIONS"
    INVALIDATE_SAVED_QUESTIONS = "INVALIDATE_SAVED_QUESTIONS"
    REVALIDATE_SAVED_QUESTIONS = "REVALIDATE_SAVED_QUESTIONS"
    REFRESH_ENTITY_REGISTRY = "REFRESH_ENTITY_REGISTRY"
    NOTIFY_SEMANTIC_CHANGE = "NOTIFY_SEMANTIC_CHANGE"


# ---------------------------------------------------------------------------
# EntityRegistry
# ---------------------------------------------------------------------------


class EntityRegistry:
    """Grounding registry that maps NL terms to DGM formal objects (§11.3).

    Built once from ``G`` and updated via ``Q_delta`` when the graph changes.
    """

    def __init__(self) -> None:
        self.prop_terms: dict[str, str] = {}
        self.node_terms: dict[str, str] = {}
        self.verb_terms: dict[str, str] = {}
        self.temporal_terms: dict[str, str] = {}
        self.agg_terms: dict[str, str] = {}

    def register_prop(self, nl_term: str, propref: str) -> None:
        self.prop_terms[nl_term] = propref

    def register_node(self, nl_term: str, alias: str) -> None:
        self.node_terms[nl_term] = alias

    def register_verb(self, nl_term: str, verb_label: str) -> None:
        self.verb_terms[nl_term] = verb_label

    def register_agg(self, nl_term: str, agg_fn: str) -> None:
        self.agg_terms[nl_term] = agg_fn

    def propref_vocabulary(self) -> set[str]:
        """Closed set of valid PropRef values (§11.8 hallucination prevention)."""
        return set(self.prop_terms.values())

    def resolve_prop(self, nl_term: str) -> str | None:
        return self.prop_terms.get(nl_term)

    def resolve_node(self, nl_term: str) -> str | None:
        return self.node_terms.get(nl_term)


# ---------------------------------------------------------------------------
# PathResolutionResult
# ---------------------------------------------------------------------------


@dataclass
class PathResolutionResult:
    """Result of resolving a natural-language path reference (§11.3).

    Parameters
    ----------
    resolved:
        True when a unique path was found (DISTINCT_SIGNATURES == 1 or
        LLM confidence above threshold).
    path:
        The resolved path string, or None when ambiguous.
    candidates:
        Top-k candidate paths when the result is ambiguous.
    """

    resolved: bool
    path: str | None
    candidates: list[str] = field(default_factory=list)
