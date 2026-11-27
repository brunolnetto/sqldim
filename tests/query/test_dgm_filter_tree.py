"""RED tests for Phase 3a — filter tree predicate type enforcement.

Covers:
- SignaturePred rejected in B2 (Having) and B3 (Qualify)
- SignatureRef rejected in B2 and B3 ScalarPred
- PathPred TemporalMode SQL template dispatch
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm import DGMQuery
from sqldim.core.query.dgm.preds import (
    ScalarPred,
    AND,
    NOT,
    SignaturePred,
    SequenceMatch,
    PathPred,
    VerbHop,
    BridgeHop,
)
from sqldim.core.query.dgm.refs import PropRef, AggRef, WinRef, SignatureRef
from sqldim.core.query.dgm.temporal import (
    EVENTUALLY,
    GLOBALLY,
    NEXT,
    ONCE,
    PREVIOUSLY,
    UntilMode,
    SinceMode,
)
from sqldim.exceptions import SemanticError


# ---------------------------------------------------------------------------
# Band enforcement: SignaturePred must not appear in B2 / B3
# ---------------------------------------------------------------------------


class TestSignaturePredBandEnforcement:
    """SignaturePred is B₁-only (Where); rejected at construction in B2/B3."""

    def _base_having_query(self):
        """Minimal DGMQuery with B2 context."""
        return (
            DGMQuery()
            .anchor("fact_order", alias="f")
            .group_by("f.region")
            .agg(total="SUM(f.amount)")
        )

    def _base_qualify_query(self):
        """Minimal DGMQuery with B3 context."""
        return (
            DGMQuery()
            .anchor("fact_order", alias="f")
            .window(rn="ROW_NUMBER() OVER (ORDER BY f.amount DESC)")
        )

    def test_signature_pred_rejected_in_having(self):
        q = self._base_having_query()
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        with pytest.raises(SemanticError, match="SignaturePred"):
            q.having(sig)

    def test_signature_pred_rejected_in_qualifying(self):
        q = self._base_qualify_query()
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        with pytest.raises(SemanticError, match="SignaturePred"):
            q.qualify(sig)

    def test_signature_pred_in_and_rejected_in_having(self):
        q = self._base_having_query()
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        pred = AND(ScalarPred(AggRef("total"), ">", 100), sig)
        with pytest.raises(SemanticError, match="SignaturePred"):
            q.having(pred)

    def test_signature_pred_in_not_rejected_in_having(self):
        q = self._base_having_query()
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        with pytest.raises(SemanticError, match="SignaturePred"):
            q.having(NOT(sig))

    def test_signature_pred_in_and_rejected_in_qualify(self):
        q = self._base_qualify_query()
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        pred = AND(ScalarPred(WinRef("rn"), "<=", 5), sig)
        with pytest.raises(SemanticError, match="SignaturePred"):
            q.qualify(pred)

    def test_signature_pred_allowed_in_where(self):
        """SignaturePred is valid in Where (B₁)."""
        q = DGMQuery().anchor("fact_order", alias="f")
        sig = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        q.where(sig)  # must NOT raise
        assert q._where_pred is sig


# ---------------------------------------------------------------------------
# Band enforcement: SignatureRef must not appear in B2 / B3
# ---------------------------------------------------------------------------


class TestSignatureRefBandEnforcement:
    """SignatureRef is B₁-only; ScalarPred(SignatureRef) rejected in B2/B3."""

    def test_signature_ref_rejected_in_having_scalar_pred(self):
        q = (
            DGMQuery()
            .anchor("fact_order", alias="f")
            .group_by("f.region")
            .agg(total="SUM(f.amount)")
        )
        sig_ref = SignatureRef("p1")
        with pytest.raises(SemanticError, match="SignatureRef"):
            q.having(ScalarPred(sig_ref, "=", "placed"))

    def test_signature_ref_rejected_in_qualify_scalar_pred(self):
        q = (
            DGMQuery()
            .anchor("fact_order", alias="f")
            .window(rn="ROW_NUMBER() OVER (ORDER BY f.amount DESC)")
        )
        sig_ref = SignatureRef("p1")
        with pytest.raises(SemanticError, match="SignatureRef"):
            q.qualify(ScalarPred(sig_ref, "=", "placed"))


# ---------------------------------------------------------------------------
# PathPred.to_sql() is fail-closed for temporal modes (§4.1)
# ---------------------------------------------------------------------------


def _make_path_pred_with_mode(mode, hop_cls=None):
    """A minimal PathPred over one hop with *mode*."""
    hop_cls = hop_cls or VerbHop
    hop = hop_cls("c", "placed", "s", table="fact_order", on="c.id = s.cust_id")
    sub = ScalarPred(PropRef("s", "status"), "=", "ok")
    return PathPred("c", hop, sub, temporal_mode=mode)


class TestTemporalModeSQLTemplates:
    """No mode has an executable SQL template: to_sql() refuses rather than
    rendering the EXISTS template, which reads AF/AG/AX as EF (#173)."""

    @pytest.mark.parametrize(
        "mode",
        [
            EVENTUALLY,
            GLOBALLY,
            NEXT,
            UntilMode(ScalarPred(PropRef("s", "done"), "=", True)),
        ],
    )
    def test_future_modes_are_refused(self, mode):
        from sqldim.core.query.dgm.planner import UnsupportedConstruct

        pp = _make_path_pred_with_mode(mode)
        with pytest.raises(UnsupportedConstruct, match="temporal mode"):
            pp.to_sql()

    @pytest.mark.parametrize(
        "mode",
        [ONCE, PREVIOUSLY, SinceMode(ScalarPred(PropRef("s", "a"), "=", True))],
    )
    def test_past_modes_rejected_over_a_verb_hop(self, mode):
        """§4.1: past-time modes need a time-ordered path; a verb hop is not."""
        with pytest.raises(SemanticError, match="time-ordered"):
            _make_path_pred_with_mode(mode)

    def test_past_mode_over_bridge_is_refused_by_to_sql(self):
        from sqldim.core.query.dgm.planner import UnsupportedConstruct

        pp = _make_path_pred_with_mode(ONCE, BridgeHop)
        with pytest.raises(UnsupportedConstruct):
            pp.to_sql()

    def test_no_mode_produces_plain_exists(self):
        hop = VerbHop("c", "placed", "s", table="fact_order", on="c.id = s.cust_id")
        sub = ScalarPred(PropRef("s", "status"), "=", "ok")
        pp = PathPred("c", hop, sub)
        sql = pp.to_sql()
        assert sql.startswith("EXISTS")
