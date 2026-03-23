"""Phase 3 — Compositional CORRELATE suggestion (DGM §7.2, fourth suggestion type).

RED → GREEN test suite.

``SuggestionKind.CORRELATE`` identifies cross-question JOIN opportunities:
when two leaf CTEs in a QuestionAlgebra share the same anchor table, the
recommender surfaces a ``CORRELATE`` suggestion proposing a JOIN composition.

``DGMRecommender.suggest_correlations(algebra)`` → ``list[Suggestion]``
  Returns one ``CORRELATE`` suggestion per unordered pair of leaf CTEs that
  share an anchor table, O(|leaf_CTEs|²) worst-case but O(|leaf_CTEs|) for
  the common grouping pass.  Band is ``"algebra"`` (cross-CTE scope).
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.algebra import QuestionAlgebra, ComposeOp
from sqldim.core.query.dgm.annotations import AnnotationSigma
from sqldim.core.query.dgm.core import DGMQuery
from sqldim.core.query.dgm.preds import ScalarPred
from sqldim.core.query.dgm.recommender import SuggestionKind, Suggestion, DGMRecommender
from sqldim.core.query.dgm.refs import PropRef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sigma() -> AnnotationSigma:
    return AnnotationSigma([])


def _rec() -> DGMRecommender:
    return DGMRecommender(_sigma())


def _q(anchor: str, alias: str = "f") -> DGMQuery:
    return DGMQuery().anchor(anchor, alias)


def _q_where(anchor: str, alias: str, col: str, val: object) -> DGMQuery:
    return (
        DGMQuery()
        .anchor(anchor, alias)
        .where(ScalarPred(PropRef(alias, col), "=", val))
    )


# ---------------------------------------------------------------------------
# SuggestionKind.CORRELATE — enum member
# ---------------------------------------------------------------------------


class TestSuggestionKindCorrelate:
    def test_correlate_member_exists(self):
        """SuggestionKind.CORRELATE must be defined."""
        assert SuggestionKind.CORRELATE

    def test_correlate_value_is_string(self):
        """CORRELATE value must be the string 'CORRELATE'."""
        assert SuggestionKind.CORRELATE.value == "CORRELATE"

    def test_correlate_is_enum_member(self):
        """CORRELATE must be a member of SuggestionKind."""
        assert SuggestionKind.CORRELATE in SuggestionKind

    def test_existing_kinds_preserved(self):
        """Adding CORRELATE must not remove existing SuggestionKind members."""
        for name in (
            "SCALAR_PRED", "PATH_PRED", "TRIM_JOIN", "TEMPORAL_PIVOT",
            "GROUP_BY", "AGG", "HAVING", "COMMUNITY_PARTITION",
            "K_SHORTEST", "Q_DELTA", "TRAIL_PIVOT", "SIGNATURE_RESTRICT",
            "SUPPRESS",
        ):
            assert hasattr(SuggestionKind, name), f"Missing: {name}"


# ---------------------------------------------------------------------------
# DGMRecommender.suggest_correlations — method signature
# ---------------------------------------------------------------------------


class TestSuggestCorrelationsSignature:
    def test_method_exists(self):
        """DGMRecommender.suggest_correlations must be callable."""
        assert callable(getattr(DGMRecommender, "suggest_correlations", None))

    def test_returns_list(self):
        """Returns a list (possibly empty)."""
        result = _rec().suggest_correlations(QuestionAlgebra())
        assert isinstance(result, list)

    def test_empty_algebra_returns_empty(self):
        """Empty algebra → no CORRELATE suggestions."""
        result = _rec().suggest_correlations(QuestionAlgebra())
        assert result == []

    def test_single_cte_returns_empty(self):
        """Single CTE → cannot correlate with anything."""
        qa = QuestionAlgebra()
        qa.add("q1", _q("dim_customer"))
        result = _rec().suggest_correlations(qa)
        assert result == []


# ---------------------------------------------------------------------------
# DGMRecommender.suggest_correlations — detection logic
# ---------------------------------------------------------------------------


class TestSuggestCorrelationsDetection:
    def test_two_ctes_same_anchor_detected(self):
        """Two leaf CTEs with the same anchor table → one CORRELATE suggestion."""
        qa = QuestionAlgebra()
        qa.add("q1", _q_where("dim_customer", "f", "region", "US"))
        qa.add("q2", _q_where("dim_customer", "g", "tier", "gold"))
        result = _rec().suggest_correlations(qa)
        assert len(result) == 1
        assert result[0].kind is SuggestionKind.CORRELATE

    def test_two_ctes_different_anchor_no_suggestion(self):
        """Two leaf CTEs with different anchor tables → no CORRELATE suggestion."""
        qa = QuestionAlgebra()
        qa.add("q1", _q("dim_customer"))
        qa.add("q2", _q("dim_product"))
        result = _rec().suggest_correlations(qa)
        assert result == []

    def test_three_ctes_two_share_anchor(self):
        """Three CTEs — two share anchor → one CORRELATE suggestion (the sharing pair)."""
        qa = QuestionAlgebra()
        qa.add("q1", _q("orders"))
        qa.add("q2", _q("orders"))
        qa.add("q3", _q("products"))
        result = _rec().suggest_correlations(qa)
        correlate = [s for s in result if s.kind is SuggestionKind.CORRELATE]
        assert len(correlate) == 1

    def test_three_ctes_all_share_anchor(self):
        """Three CTEs all sharing anchor → 3 CORRELATE suggestions (C(3,2)=3 pairs)."""
        qa = QuestionAlgebra()
        qa.add("q1", _q("fact_sales"))
        qa.add("q2", _q("fact_sales"))
        qa.add("q3", _q("fact_sales"))
        result = _rec().suggest_correlations(qa)
        correlate = [s for s in result if s.kind is SuggestionKind.CORRELATE]
        assert len(correlate) == 3  # pairs: (q1,q2), (q1,q3), (q2,q3)

    def test_composed_ctes_skipped(self):
        """Composed (raw-SQL) CTEs have no anchor table → not correlation candidates."""
        qa = QuestionAlgebra()
        qa.add("q1", _q("dim_t"))
        qa.add("q2", _q("dim_t"))
        qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        result = _rec().suggest_correlations(qa)
        # Only the two leaf CTEs with same anchor produce a suggestion.
        correlate = [s for s in result if s.kind is SuggestionKind.CORRELATE]
        assert len(correlate) == 1

    def test_no_anchor_cte_skipped(self):
        """A DGMQuery with no anchor set is not a correlation candidate."""
        # DGMQuery() with no .anchor() call has _anchor_table = None
        qa = QuestionAlgebra()
        qa.add("q1", DGMQuery())   # no anchor
        qa.add("q2", _q("dim_t"))
        result = _rec().suggest_correlations(qa)
        assert result == []


# ---------------------------------------------------------------------------
# DGMRecommender.suggest_correlations — suggestion shape
# ---------------------------------------------------------------------------


class TestSuggestCorrelationsSuggestionShape:
    def _get_suggestion(self) -> Suggestion:
        qa = QuestionAlgebra()
        qa.add("alpha", _q("dim_customer"))
        qa.add("beta", _q("dim_customer"))
        return _rec().suggest_correlations(qa)[0]

    def test_kind_is_correlate(self):
        s = self._get_suggestion()
        assert s.kind is SuggestionKind.CORRELATE

    def test_band_is_algebra(self):
        """CORRELATE suggestions have band='algebra' (cross-CTE scope)."""
        s = self._get_suggestion()
        assert s.band == "algebra"

    def test_text_mentions_both_cte_names(self):
        """Suggestion text must reference both CTE names."""
        s = self._get_suggestion()
        assert "alpha" in s.text
        assert "beta" in s.text

    def test_text_mentions_anchor(self):
        """Suggestion text must reference the shared anchor table."""
        s = self._get_suggestion()
        assert "dim_customer" in s.text

    def test_priority_is_positive(self):
        s = self._get_suggestion()
        assert s.priority > 0

    def test_suggestion_is_suggestion_instance(self):
        s = self._get_suggestion()
        assert isinstance(s, Suggestion)
