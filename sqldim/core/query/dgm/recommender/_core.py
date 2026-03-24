"""DGM Recommender — DGMRecommender class (DGM v0.16 §7).

Three-layer architecture: schema atom generation → BDD feasibility filter →
data scoring.  Support types live in :mod:`sqldim.core.query.dgm.recommender_types`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqldim.core.query.dgm.recommender._types import (  # noqa: F401
    ENTROPY_THRESHOLD, SuggestionKind, Suggestion, Stage1Result, Stage2Result,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.bdd import DGMPredicateBDD
    from sqldim.core.query.dgm.graph import GraphStatistics
    from sqldim.core.query.dgm.annotations import AnnotationSigma

__all__ = [
    "SuggestionKind", "Suggestion", "Stage1Result", "Stage2Result",
    "DGMRecommender", "ENTROPY_THRESHOLD",
]


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
        from sqldim.core.query.dgm.annotations import (
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
        from sqldim.core.query.dgm.annotations import GrainKind

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
        from sqldim.core.query.dgm.annotations import SCDKind

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
        from sqldim.core.query.dgm.annotations import BridgeSemanticsKind

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

    # -- Compositional correlation suggestions (§7.2) --------------------

    @staticmethod
    def _group_leaf_ctes_by_anchor(algebra: "object") -> "dict[str, list[str]]":
        """Return {anchor_table: [cte_name, …]} for all DGMQuery-backed leaf CTEs."""
        anchor_to_names: dict[str, list[str]] = {}
        for name, cq in algebra._ctes.items():
            if cq.query is None:
                continue
            anchor = getattr(cq.query, "_anchor_table", None)
            if anchor is None:
                continue
            anchor_to_names.setdefault(anchor, []).append(name)
        return anchor_to_names

    def suggest_correlations(
        self,
        algebra: "object",
    ) -> list[Suggestion]:
        """Suggest cross-question JOIN compositions for an algebra (§7.2).

        For every unordered pair of leaf CTEs (backed by a DGMQuery with a
        non-None ``_anchor_table``) that share the same anchor table, a
        ``SuggestionKind.CORRELATE`` suggestion is returned proposing a
        ``ComposeOp.JOIN`` composition.

        Parameters
        ----------
        algebra:
            A :class:`~sqldim.core.query.dgm.algebra.QuestionAlgebra`.

        Returns
        -------
        list[Suggestion]
            One ``CORRELATE`` suggestion per shareable pair, in insertion order.
            Empty when no sharing opportunities exist.

        Complexity
        ----------
        O(|leaf_CTEs|) grouping pass + O(pairs_per_group) pair generation.
        Worst case O(n²) when all n CTEs share one anchor; typical O(n).
        """
        anchor_to_names = self._group_leaf_ctes_by_anchor(algebra)
        suggestions: list[Suggestion] = []
        for anchor, names in anchor_to_names.items():
            if len(names) < 2:
                continue
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    left, right = names[i], names[j]
                    suggestions.append(Suggestion(
                        kind=SuggestionKind.CORRELATE,
                        band="algebra",
                        text=(
                            f"CORRELATE({left!r}, {right!r}) via shared anchor"
                            f" {anchor!r}: consider ComposeOp.JOIN on the"
                            f" natural key of {anchor!r}"
                        ),
                        priority=60,
                    ))
        return suggestions

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
