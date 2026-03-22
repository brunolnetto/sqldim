"""DGM Recommender support types — SuggestionKind, Suggestion, Stage1/2Result (DGM §7)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.bdd import DGMPredicateBDD
    from sqldim.core.query.dgm.graph import GraphStatistics
    from sqldim.core.query.dgm.annotations import AnnotationSigma

__all__ = [
    "SuggestionKind",
    "Suggestion",
    "Stage1Result",
    "Stage2Result",
    "ENTROPY_THRESHOLD",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: SIGNATURE_ENTROPY routing threshold.  Values above this threshold trigger
#: Stage 1 free-endpoint characterisation before fixing the target.
ENTROPY_THRESHOLD: float = 0.5


# ---------------------------------------------------------------------------
# SuggestionKind
# ---------------------------------------------------------------------------


class SuggestionKind(Enum):
    """Categorises the type of analytic suggestion produced by the recommender."""
    # Band 1 — Context
    SCALAR_PRED = "SCALAR_PRED"
    PATH_PRED = "PATH_PRED"
    TRIM_JOIN = "TRIM_JOIN"
    TEMPORAL_PIVOT = "TEMPORAL_PIVOT"
    # Band 2 — Aggregation
    GROUP_BY = "GROUP_BY"
    AGG = "AGG"
    HAVING = "HAVING"
    # Band 3 — Ranking
    COMMUNITY_PARTITION = "COMMUNITY_PARTITION"
    K_SHORTEST = "K_SHORTEST"
    # Cross-band temporal
    Q_DELTA = "Q_DELTA"
    # Trail-expr
    TRAIL_PIVOT = "TRAIL_PIVOT"
    SIGNATURE_RESTRICT = "SIGNATURE_RESTRICT"
    # Suppression marker (Band)
    SUPPRESS = "SUPPRESS"


# ---------------------------------------------------------------------------
# Suggestion
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class Suggestion:
    """A single analytic suggestion produced by the recommender.

    Parameters
    ----------
    kind:
        Category of suggestion.
    band:
        Query band where the suggestion applies (``"B1"``, ``"B2"``, ``"B3"``,
        ``"temporal"``, ``"trail"``, or ``"suppress"``).
    text:
        Formulaic (not natural language) description of the suggestion.
    priority:
        Integer 0–100; higher = more important.  Defaults to 50.
    """

    kind: SuggestionKind
    band: str
    text: str
    priority: int = 50

    def __repr__(self) -> str:
        return (
            f"Suggestion(kind={self.kind.name}, band={self.band!r},"
            f" priority={self.priority}, text={self.text!r})"
        )


# ---------------------------------------------------------------------------
# Stage1Result / Stage2Result
# ---------------------------------------------------------------------------


@dataclass(eq=True)
class Stage1Result:
    """Result of Stage 1 free-endpoint characterisation (§7.2).

    Parameters
    ----------
    anchor:
        Anchor node alias.
    outgoing_sig_count:
        Count of distinct outgoing path signatures from *anchor*.
    dominant_signature:
        Most frequent outgoing label sequence.
    diversity:
        SIGNATURE_DIVERSITY scalar in [0, 1].
    """

    anchor: str
    outgoing_sig_count: int
    dominant_signature: list[str]
    diversity: float

    @property
    def recommend_stage2(self) -> bool:
        """True when diversity is low enough to proceed to Bound→Bound deep-dive."""
        return self.diversity < ENTROPY_THRESHOLD


@dataclass(eq=True)
class Stage2Result:
    """Result of Stage 2 fixed-endpoint deep-dive (§7.2).

    Parameters
    ----------
    source:
        Source anchor alias.
    target:
        Target anchor alias.
    distinct_sigs:
        Count of distinct label sequences connecting source to target.
    dominant_signature:
        Most frequent label sequence on that connection.
    density:
        DENSITY of the REACHABLE_BETWEEN subgraph (None if not computed).
    """

    source: str
    target: str
    distinct_sigs: int
    dominant_signature: list[str]
    density: float | None = None
