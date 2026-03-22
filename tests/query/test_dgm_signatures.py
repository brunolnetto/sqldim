"""RED tests for Phase 2 DGM signature/hop/property/context DSL extensions.

Covers:
- VerbHopInverse (kind="verb_inverse")
- PathPred with temporal_mode and promote
- TemporalProperty sugar classes (SAFETY, LIVENESS, RESPONSE, PERSISTENCE, RECURRENCE)
- SignaturePred + SequenceMatch
- SignatureRef
- REACHABLE_FROM / REACHABLE_TO TrimCriteria
- TemporalContext on DGMQuery
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.preds import (
    VerbHopInverse,
    _HopBase,
    PathPred,
    Quantifier,
    ALL as StratALL,
    SHORTEST,
    SAFETY,
    LIVENESS,
    RESPONSE,
    PERSISTENCE,
    RECURRENCE,
    SignaturePred,
    SequenceMatch,
)
from sqldim.core.query.dgm.refs import PropRef, SignatureRef
from sqldim.core.query.dgm.graph import (
    TrimCriterion,
    REACHABLE_FROM,
    REACHABLE_TO,
)
from sqldim.core.query.dgm.temporal import GLOBALLY, UntilMode
from sqldim.core.query.dgm import DGMQuery, TemporalContext


# ---------------------------------------------------------------------------
# VerbHopInverse
# ---------------------------------------------------------------------------


class TestVerbHopInverse:
    def test_is_hop_base(self):
        hop = VerbHopInverse("f", "placed", "c", table="fact_order", on="f.cust_fk = c.id")
        assert isinstance(hop, _HopBase)

    def test_kind(self):
        hop = VerbHopInverse("f", "placed", "c", table="fact_order", on="f.cust_fk = c.id")
        assert hop.kind == "verb_inverse"

    def test_stores_fields(self):
        hop = VerbHopInverse("f", "verb", "d", table="t", on="f.d_id = d.id")
        assert hop.from_alias == "f"
        assert hop.label == "verb"
        assert hop.to_alias == "d"

    def test_different_kind_from_verb_hop(self):
        from sqldim.core.query.dgm.preds import VerbHop
        inverse = VerbHopInverse("f", "placed", "c", table="t", on="x.y = y.x")
        forward = VerbHop("c", "placed", "f", table="t", on="x.y = y.x")
        assert inverse.kind != forward.kind


# ---------------------------------------------------------------------------
# PathPred with temporal_mode and promote
# ---------------------------------------------------------------------------


class TestPathPredExtended:
    def _make_basic_pred(self):
        return PathPred(
            anchor="c",
            path=None,  # minimal
            sub_filter=PropRef("c", "active"),
        )

    def test_temporal_mode_defaults_none(self):
        pp = self._make_basic_pred()
        assert pp.temporal_mode is None

    def test_promote_defaults_none(self):
        pp = self._make_basic_pred()
        assert pp.promote is None

    def test_temporal_mode_stored(self):
        pred = PropRef("c", "active")
        pp = PathPred(
            anchor="c",
            path=None,
            sub_filter=pred,
            temporal_mode=GLOBALLY,
        )
        assert pp.temporal_mode is GLOBALLY

    def test_promote_stored_true(self):
        pred = PropRef("c", "active")
        pp = PathPred(
            anchor="c",
            path=None,
            sub_filter=pred,
            strategy=SHORTEST(),
            promote=True,
        )
        assert pp.promote is True

    def test_promote_false_stored(self):
        pred = PropRef("c", "active")
        pp = PathPred(
            anchor="c",
            path=None,
            sub_filter=pred,
            strategy=SHORTEST(),
            promote=False,
        )
        assert pp.promote is False

    def test_until_mode_stored(self):
        inner = PropRef("f", "done")
        mode = UntilMode(PropRef("f", "done"))
        pp = PathPred(
            anchor="c",
            path=None,
            sub_filter=inner,
            temporal_mode=mode,
        )
        assert pp.temporal_mode is mode


# ---------------------------------------------------------------------------
# TemporalProperty sugar
# ---------------------------------------------------------------------------


class TestTemporalProperty:
    def _bad_pred(self):
        return PropRef("e", "error")

    def _good_pred(self):
        return PropRef("c", "satisfied")

    def test_safety_construction(self):
        sp = SAFETY(self._bad_pred())
        assert hasattr(sp, "to_sql")

    def test_safety_to_sql_negates(self):
        bad = PropRef("e", "error")
        sql = SAFETY(bad).to_sql()
        assert "e.error" in sql

    def test_liveness_to_sql_contains_good(self):
        good = PropRef("c", "satisfied")
        sql = LIVENESS(good).to_sql()
        assert "c.satisfied" in sql

    def test_response_stores_both(self):
        trig = PropRef("e", "trigger")
        resp = PropRef("e", "response")
        r = RESPONSE(trig, resp)
        assert r.trigger is trig
        assert r.response is resp

    def test_response_to_sql_contains_both(self):
        trig = PropRef("e", "trigger")
        resp = PropRef("e", "response")
        sql = RESPONSE(trig, resp).to_sql()
        assert "e.trigger" in sql
        assert "e.response" in sql

    def test_persistence_construction(self):
        good = PropRef("c", "active")
        p = PERSISTENCE(good)
        assert p.good is good

    def test_persistence_to_sql_contains_pred(self):
        good = PropRef("c", "active")
        sql = PERSISTENCE(good).to_sql()
        assert "c.active" in sql

    def test_recurrence_construction(self):
        good = PropRef("c", "visited")
        from sqldim.core.query.dgm.temporal import ROLLING
        win = ROLLING("7 DAYS")
        r = RECURRENCE(good, win)
        assert r.good is good
        assert r.window is win

    def test_recurrence_to_sql_string(self):
        good = PropRef("c", "visited")
        from sqldim.core.query.dgm.temporal import ROLLING
        win = ROLLING("7 DAYS")
        sql = RECURRENCE(good, win).to_sql()
        assert isinstance(sql, str)


# ---------------------------------------------------------------------------
# SignaturePred + SequenceMatch
# ---------------------------------------------------------------------------


class TestSignaturePred:
    def test_construction(self):
        sp = SignaturePred("path1", ["placed", "shipped"], SequenceMatch.EXACT)
        assert sp.path_alias == "path1"
        assert sp.sequence == ["placed", "shipped"]
        assert sp.match == SequenceMatch.EXACT

    def test_exact_match_value(self):
        assert SequenceMatch.EXACT.value == "EXACT"

    def test_prefix_match_value(self):
        assert SequenceMatch.PREFIX.value == "PREFIX"

    def test_contains_match_value(self):
        assert SequenceMatch.CONTAINS.value == "CONTAINS"

    def test_regex_match_value(self):
        assert SequenceMatch.REGEX.value == "REGEX"

    def test_to_sql_returns_string(self):
        sp = SignaturePred("p1", ["placed"], SequenceMatch.EXACT)
        assert isinstance(sp.to_sql(), str)

    def test_to_sql_contains_path_alias(self):
        sp = SignaturePred("path1", ["placed", "shipped"], SequenceMatch.PREFIX)
        sql = sp.to_sql()
        assert "path1" in sql

    def test_to_sql_contains_labels(self):
        sp = SignaturePred("p1", ["verb_a", "verb_b"], SequenceMatch.CONTAINS)
        sql = sp.to_sql()
        assert "verb_a" in sql or "verb_b" in sql

    def test_wildcard_in_sequence(self):
        sp = SignaturePred("p1", ["placed", None, "returned"], SequenceMatch.CONTAINS)
        assert sp.sequence[1] is None

    def test_to_sql_has_match_type(self):
        sp = SignaturePred("p1", ["a"], SequenceMatch.REGEX)
        sql = sp.to_sql()
        assert "REGEX" in sql or "regex" in sql.lower()


# ---------------------------------------------------------------------------
# SignatureRef
# ---------------------------------------------------------------------------


class TestSignatureRef:
    def test_to_sql(self):
        ref = SignatureRef("path1")
        assert ref.to_sql() == "path1.signature"

    def test_path_alias_stored(self):
        ref = SignatureRef("mypath")
        assert ref.path_alias == "mypath"

    def test_repr(self):
        ref = SignatureRef("p")
        assert "p" in repr(ref)


# ---------------------------------------------------------------------------
# REACHABLE_FROM / REACHABLE_TO
# ---------------------------------------------------------------------------


class TestReachableFromTo:
    def test_reachable_from_is_trim_criterion(self):
        rc = REACHABLE_FROM("c")
        assert isinstance(rc, TrimCriterion)

    def test_reachable_from_stores_source(self):
        rc = REACHABLE_FROM("customer_node")
        assert rc.source == "customer_node"

    def test_reachable_to_is_trim_criterion(self):
        rc = REACHABLE_TO("s")
        assert isinstance(rc, TrimCriterion)

    def test_reachable_to_stores_target(self):
        rc = REACHABLE_TO("sale_node")
        assert rc.target == "sale_node"

    def test_reachable_from_to_sql(self):
        sql = REACHABLE_FROM("c").to_sql()
        assert "c" in sql

    def test_reachable_to_to_sql(self):
        sql = REACHABLE_TO("s").to_sql()
        assert "s" in sql


# ---------------------------------------------------------------------------
# TemporalContext
# ---------------------------------------------------------------------------


class TestTemporalContext:
    def test_construction(self):
        ctx = TemporalContext("2024-06-30", "LAX", "STRICT")
        assert ctx.default_as_of == "2024-06-30"
        assert ctx.node_resolution == "LAX"
        assert ctx.edge_resolution == "STRICT"

    def test_default_resolutions_lax(self):
        ctx = TemporalContext("2024-01-01")
        assert ctx.node_resolution == "LAX"
        assert ctx.edge_resolution == "LAX"

    def test_dgmquery_temporal_context_method(self):
        q = DGMQuery()
        result = q.temporal_context("2024-01-01")
        assert result is q  # fluent

    def test_dgmquery_stores_temporal_context(self):
        q = DGMQuery()
        q.temporal_context("2024-01-01", node_resolution="STRICT")
        assert q._temporal_context is not None
        assert q._temporal_context.default_as_of == "2024-01-01"

    def test_dgmquery_temporal_context_defaults(self):
        q = DGMQuery()
        q.temporal_context("2024-06-30")
        assert q._temporal_context.node_resolution == "LAX"
        assert q._temporal_context.edge_resolution == "LAX"
