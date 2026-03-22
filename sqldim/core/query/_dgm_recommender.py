"""DGM Recommender architecture (DGM v0.16 §7).

Three-layer architecture:
  Layer 1 — Schema atom generation (bounded set of candidate suggestions)
  Layer 2 — BDD feasibility filter (O(n × BDD_size))
  Layer 3 — Data scoring (returns ranked Suggestion list)

Annotation-driven rules (§7.4) and TrailExpr-driven rules (§7.5) populate
Layer 1.  The BDD feasibility filter in Layer 2 gates candidates using the
DGMPredicateBDD.is_satisfiable() test.  Layer 3 priority ordering is by
annotation specificity and entropy signal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query._dgm_bdd import DGMPredicateBDD
    from sqldim.core.query._dgm_graph import GraphStatistics
    from sqldim.core.query._dgm_annotations import AnnotationSigma

__all__ = [
    "SuggestionKind",
    "Suggestion",
    "Stage1Result",
    "Stage2Result",
    "DGMRecommender",
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


# ---------------------------------------------------------------------------
# DGMRecommender
# ---------------------------------------------------------------------------


class DGMRecommender:
    """Three-layer DGM recommender (§7.1).

    Parameters
    ----------
    sigma:
        The schema annotation set Σ.
    statistics:
        Optional graph statistics for data-driven rules (§7.6).
    """

    def __init__(
        self,
        sigma: "AnnotationSigma",
        statistics: "GraphStatistics | None" = None,
    ) -> None:
        self.sigma = sigma
        self.statistics = statistics

    # -- Routing signal (§7.2) -----------------------------------------------

    def route(self, entropy: float) -> str:
        """Return ``"stage1"`` if entropy > THRESHOLD; ``"stage2"`` otherwise."""
        if entropy > ENTROPY_THRESHOLD:
            return "stage1"
        return "stage2"

    # -- BDD feasibility filter (Layer 2) ------------------------------------

    def bdd_feasible(self, bdd: "DGMPredicateBDD", uid: int) -> bool:
        """Return True if the predicate is satisfiable (not identically False)."""
        return bdd.is_satisfiable(uid)

    # -- Two-stage trail exploration (§7.2) ----------------------------------

    def stage1_characterise(
        self,
        anchor: str,
        outgoing_sig_count: int,
        dominant_signature: list[str],
        diversity: float,
    ) -> Stage1Result:
        """Build a Stage1Result from measured trail statistics."""
        return Stage1Result(
            anchor=anchor,
            outgoing_sig_count=outgoing_sig_count,
            dominant_signature=dominant_signature,
            diversity=diversity,
        )

    def stage2_deep_dive(
        self,
        source: str,
        target: str,
        distinct_sigs: int,
        dominant_signature: list[str],
        density: float | None = None,
    ) -> Stage2Result:
        """Build a Stage2Result from measured fixed-endpoint statistics."""
        return Stage2Result(
            source=source,
            target=target,
            distinct_sigs=distinct_sigs,
            dominant_signature=dominant_signature,
            density=density,
        )

    # -- Annotation-driven rules (§7.4) -------------------------------------

    def run_annotation_rules(self) -> list[Suggestion]:
        """Apply §7.4 annotation-driven rules; return a flat suggestion list."""
        suggestions: list[Suggestion] = []
        for ann in self.sigma:
            self._apply_annotation_rule(ann, suggestions)
        return suggestions

    def _apply_annotation_rule(self, ann: object, out: list[Suggestion]) -> None:
        from sqldim.core.query._dgm_annotations import (
            Degenerate, Conformed, Grain, SCDType, FactlessFact,
            DerivedFact, WeightConstraint, BridgeSemantics,
            Hierarchy, RolePlaying,
        )

        _dispatch = {
            Degenerate: self._ann_degenerate,
            Conformed: self._ann_conformed,
            Grain: self._ann_grain,
            SCDType: self._ann_scd_type,
            FactlessFact: self._ann_factless_fact,
            DerivedFact: self._ann_derived_fact,
            WeightConstraint: self._ann_weight_constraint,
            BridgeSemantics: self._ann_bridge_semantics,
            Hierarchy: self._ann_hierarchy,
            RolePlaying: self._ann_role_playing,
        }
        handler = _dispatch.get(type(ann))
        if handler is not None:
            handler(ann, out)

    def _ann_degenerate(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.SUPPRESS, band="suppress",
            text=f"Suppress GroupBy({ann.dim}); Suppress PathPred from {ann.dim}",  # type: ignore[attr-defined]
            priority=70,
        ))
        out.append(Suggestion(
            kind=SuggestionKind.SCALAR_PRED, band="B1",
            text=f"Add ScalarPred({ann.dim}.key) instead of GroupBy",  # type: ignore[attr-defined]
            priority=60,
        ))

    def _ann_conformed(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.PATH_PRED, band="B1",
            text=(
                f"Constellation path via Conformed({ann.dim},"  # type: ignore[attr-defined]
                f" {sorted(ann.fact_types)})"  # type: ignore[attr-defined]
            ),
            priority=75,
        ))

    def _ann_grain(self, ann: object, out: list[Suggestion]) -> None:
        from sqldim.core.query._dgm_annotations import GrainKind

        if ann.grain is GrainKind.PERIOD:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.SUPPRESS, band="suppress",
                text=f"Suppress SUM aggregation — Grain(PERIOD) on {ann.fact}",  # type: ignore[attr-defined]
                priority=80,
            ))
            out.append(Suggestion(
                kind=SuggestionKind.AGG, band="B2",
                text=f"Use LAST instead of SUM on Grain(PERIOD) {ann.fact}",  # type: ignore[attr-defined]
                priority=65,
            ))
            out.append(Suggestion(
                kind=SuggestionKind.Q_DELTA, band="temporal",
                text=f"Consider Q_delta for period-over-period diff on {ann.fact}",  # type: ignore[attr-defined]
                priority=60,
            ))
        elif ann.grain is GrainKind.ACCUMULATING:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.TEMPORAL_PIVOT, band="temporal",
                text=f"Warn: cross-row agg on ACCUMULATING {ann.fact}; add stage predicates",  # type: ignore[attr-defined]
                priority=55,
            ))

    def _ann_scd_type(self, ann: object, out: list[Suggestion]) -> None:
        from sqldim.core.query._dgm_annotations import SCDKind

        if ann.scd is SCDKind.SCD3:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.SCALAR_PRED, band="B1",
                text=f"Add PropRef({ann.dim}.previous_value) — SCD3 comparison available",  # type: ignore[attr-defined]
                priority=60,
            ))
        elif ann.scd is SCDKind.SCD1:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.SUPPRESS, band="suppress",
                text=f"Suppress TemporalJoin — SCDType(SCD1) on {ann.dim}: strip temporal",  # type: ignore[attr-defined]
                priority=70,
            ))

    def _ann_factless_fact(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.AGG, band="B2",
            text=f"Use COUNT/EXISTS on FactlessFact({ann.fact}); SUM/AVG invalid",  # type: ignore[attr-defined]
            priority=80,
        ))

    def _ann_derived_fact(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.PATH_PRED, band="B1",
            text=f"Drill-down to DerivedFact({ann.fact}) sources: {ann.sources}",  # type: ignore[attr-defined]
            priority=55,
        ))

    def _ann_weight_constraint(self, ann: object, out: list[Suggestion]) -> None:
        if ann.is_allocative:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.PATH_PRED, band="B1",
                text=(
                    f"Use weighted PathAgg on {ann.bridge}"  # type: ignore[attr-defined]
                    " — WeightConstraint(ALLOCATIVE)"
                ),
                priority=65,
            ))

    def _ann_bridge_semantics(self, ann: object, out: list[Suggestion]) -> None:
        from sqldim.core.query._dgm_annotations import BridgeSemanticsKind

        if ann.sem is BridgeSemanticsKind.CAUSAL:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.PATH_PRED, band="B1",
                text=(
                    f"BETWEENNESS on G_AB; TARJAN_SCC partition"
                    f" — BridgeSemantics(CAUSAL) on {ann.bridge}"  # type: ignore[attr-defined]
                ),
                priority=70,
            ))
        elif ann.sem is BridgeSemanticsKind.SUPERSESSION:  # type: ignore[attr-defined]
            out.append(Suggestion(
                kind=SuggestionKind.SUPPRESS, band="suppress",
                text=f"Use negation-aware aggregation on SUPERSESSION bridge {ann.bridge}",  # type: ignore[attr-defined]
                priority=65,
            ))

    def _ann_hierarchy(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.GROUP_BY, band="B2",
            text=(
                f"Drill-down/roll-up via Hierarchy(root={ann.root},"  # type: ignore[attr-defined]
                f" depth={ann.depth!r})"  # type: ignore[attr-defined]
            ),
            priority=60,
        ))

    def _ann_role_playing(self, ann: object, out: list[Suggestion]) -> None:
        out.append(Suggestion(
            kind=SuggestionKind.GROUP_BY, band="B2",
            text=(
                f"Cross-role comparisons via RolePlaying({ann.dim},"  # type: ignore[attr-defined]
                f" roles={ann.roles})"  # type: ignore[attr-defined]
            ),
            priority=55,
        ))

    # -- TrailExpr-driven rules (§7.5) ---------------------------------------

    def run_trail_rules(
        self,
        anchor: str,
        outgoing_sig_count: int,
        diversity: float,
        entropy: float,
    ) -> list[Suggestion]:
        """Apply §7.5 TrailExpr-driven rules; return suggestions."""
        suggestions: list[Suggestion] = []
        high_outgoing = outgoing_sig_count > 4
        high_diversity = diversity > ENTROPY_THRESHOLD
        high_entropy = entropy > ENTROPY_THRESHOLD

        if high_outgoing:
            suggestions.append(Suggestion(
                kind=SuggestionKind.SIGNATURE_RESTRICT,
                band="B1",
                text=(
                    f"OUTGOING_SIGNATURES({anchor}) high ({outgoing_sig_count}):"
                    " isolate via SignaturePred(EXACT, dominant_signature)"
                ),
                priority=75,
            ))
            suggestions.append(Suggestion(
                kind=SuggestionKind.TRIM_JOIN,
                band="B1",
                text=f"TrimJoin(REACHABLE_FROM({anchor!r})) to forward cone",
                priority=70,
            ))
        if not high_diversity:
            suggestions.append(Suggestion(
                kind=SuggestionKind.PATH_PRED,
                band="B1",
                text=(
                    f"Low SIGNATURE_DIVERSITY on {anchor}:"
                    " proceed directly to Bound→Bound deep-dive"
                ),
                priority=80,
            ))
        if high_entropy:
            suggestions.append(Suggestion(
                kind=SuggestionKind.TRAIL_PIVOT,
                band="trail",
                text=(
                    "High SIGNATURE_ENTROPY: use GLOBAL_DOMINANT_SIGNATURE"
                    " as first characterisation step"
                ),
                priority=75,
            ))
        if not high_entropy:
            suggestions.append(Suggestion(
                kind=SuggestionKind.SIGNATURE_RESTRICT,
                band="B1",
                text="Low SIGNATURE_ENTROPY: restrict scope to dominant signature",
                priority=65,
            ))
        return suggestions
