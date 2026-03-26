"""DGM NL Interface — intent classification and grounding functions (§11.3–11.8).

Pure functions:
  validate_propref()             — hallucination prevention (§11.8)
  resolve_band()                 — intent signal → BandKind (§11.3)
  classify_temporal_intent()     — NL phrase → TemporalIntent (§11.4)
  classify_compositional_intent()— NL phrase → CompositionOp (§11.4)
  apply_schema_evolution()       — SchemaEvolutionKind → actions (§11.7)
"""

from __future__ import annotations

from sqldim.core.query.dgm.nl._types import (
    BandKind,
    CompositionOp,
    EntityRegistry,
    SchemaEvolutionAction,
    SchemaEvolutionKind,
    TemporalIntent,
)

__all__ = [
    "validate_propref",
    "resolve_band",
    "classify_temporal_intent",
    "classify_compositional_intent",
    "apply_schema_evolution",
]

# ---------------------------------------------------------------------------
# Band-intent keyword maps (§11.3)
# ---------------------------------------------------------------------------

_B2_SIGNALS = ("total", "sum", "average", "count", "avg")
_B3_SIGNALS = ("top ", "rank", "highest", "lowest", "ntile", "percentile")
_B1_TEMPORAL = ("has always", "never", "eventually", "once", "previously")
_B1_FILTER = ("where", "which", "filter", "that ")

# ---------------------------------------------------------------------------
# Temporal-intent phrase map (§11.4)
# ---------------------------------------------------------------------------

_TEMPORAL_MAP: list[tuple[str, TemporalIntent]] = [
    ("has always been", TemporalIntent.GLOBALLY),
    ("always eventually", TemporalIntent.LIVENESS),
    ("whenever ", TemporalIntent.RESPONSE),
    ("never violated", TemporalIntent.SAFETY),
    ("never ", TemporalIntent.SAFETY),
    ("eventually", TemporalIntent.EVENTUALLY),
    ("at the next step", TemporalIntent.NEXT),
    ("until ", TemporalIntent.UNTIL),
    ("since ", TemporalIntent.SINCE),
    ("was once", TemporalIntent.ONCE),
    ("once ", TemporalIntent.ONCE),
    ("previous value", TemporalIntent.PREVIOUSLY),
    ("previously", TemporalIntent.PREVIOUSLY),
    ("safety", TemporalIntent.SAFETY),
    ("liveness", TemporalIntent.LIVENESS),
]

# ---------------------------------------------------------------------------
# Compositional-intent phrase map (§11.4)
# ---------------------------------------------------------------------------

_COMP_MAP: list[tuple[str, CompositionOp]] = [
    ("then ", CompositionOp.CHAIN),
    ("first ", CompositionOp.CHAIN),
    ("compared to", CompositionOp.JOIN),
    ("alongside", CompositionOp.JOIN),
    ("or ", CompositionOp.UNION),
    ("and also", CompositionOp.INTERSECT),
    (" and ", CompositionOp.INTERSECT),
]

# ---------------------------------------------------------------------------
# Schema-evolution action map (§11.7)
# ---------------------------------------------------------------------------

_EVOLUTION_ACTIONS: dict[SchemaEvolutionKind, list[SchemaEvolutionAction]] = {
    SchemaEvolutionKind.ADDED_NODES: [
        SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS,
    ],
    SchemaEvolutionKind.REMOVED_NODES: [
        SchemaEvolutionAction.INVALIDATE_SAVED_QUESTIONS,
        SchemaEvolutionAction.NOTIFY_SEMANTIC_CHANGE,
    ],
    SchemaEvolutionKind.ADDED_EDGES: [
        SchemaEvolutionAction.SUGGEST_NEW_QUESTIONS,
    ],
    SchemaEvolutionKind.REMOVED_EDGES: [
        SchemaEvolutionAction.INVALIDATE_SAVED_QUESTIONS,
    ],
    SchemaEvolutionKind.ROLE_DRIFT: [
        SchemaEvolutionAction.REVALIDATE_SAVED_QUESTIONS,
        SchemaEvolutionAction.NOTIFY_SEMANTIC_CHANGE,
    ],
    SchemaEvolutionKind.CHANGED_PROPERTY: [
        SchemaEvolutionAction.REFRESH_ENTITY_REGISTRY,
        SchemaEvolutionAction.REVALIDATE_SAVED_QUESTIONS,
    ],
}


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def validate_propref(propref: str, registry: EntityRegistry) -> bool:
    """Return True iff *propref* is in the closed vocabulary (§11.8)."""
    return propref in registry.propref_vocabulary()


def resolve_band(phrase: str) -> BandKind:
    """Map a natural-language phrase to the most likely query band (§11.3).

    Returns B1 when no recognisable signal is found (safe default).
    """
    lower = phrase.lower()
    if any(sig in lower for sig in _B3_SIGNALS):
        return BandKind.B3
    if any(sig in lower for sig in _B2_SIGNALS):
        return BandKind.B2
    return BandKind.B1


def classify_temporal_intent(phrase: str) -> TemporalIntent | None:
    """Map a natural-language temporal phrase to a TemporalIntent (§11.4).

    Returns None when no temporal pattern is recognised.
    """
    lower = phrase.lower()
    for pattern, intent in _TEMPORAL_MAP:
        if pattern in lower:
            return intent
    return None


def classify_compositional_intent(phrase: str) -> CompositionOp | None:
    """Map a natural-language phrase to a Q_algebra operation (§11.4).

    Returns None when no compositional pattern is recognised.
    """
    lower = phrase.lower()
    for pattern, op in _COMP_MAP:
        if pattern in lower:
            return op
    return None


def apply_schema_evolution(kind: SchemaEvolutionKind) -> list[SchemaEvolutionAction]:
    """Return the actions triggered by a schema evolution event (§11.7)."""
    return list(_EVOLUTION_ACTIONS.get(kind, []))
