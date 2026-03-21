"""RED tests for Phase 2 DGM temporal DSL extensions.

Covers:
- TemporalMode hierarchy (EVENTUALLY, GLOBALLY, NEXT, ONCE, PREVIOUSLY, UNTIL, SINCE)
- TemporalOrdering hierarchy (point-based, Allen's 13 interval relations, LTL)
- TemporalWindow hierarchy (ROLLING, TRAILING, PERIOD, YTD, QTD, MTD)
- TemporalAgg expression type
- DeltaSpec variants and DeltaQuery
- TemporalContext on DGMQuery
"""

from __future__ import annotations

import pytest

from sqldim.core.query._dgm_temporal import (
    # TemporalMode
    TemporalMode,
    EVENTUALLY,
    GLOBALLY,
    NEXT,
    ONCE,
    PREVIOUSLY,
    UntilMode,
    SinceMode,
    # TemporalOrdering
    TemporalOrdering,
    BEFORE,
    AFTER,
    CONCURRENT,
    MEETS,
    MET_BY,
    OVERLAPS,
    OVERLAPPED_BY,
    STARTS,
    STARTED_BY,
    DURING,
    CONTAINS,
    FINISHES,
    FINISHED_BY,
    EQUALS,
    UntilOrdering,
    SinceOrdering,
    # TemporalWindow
    TemporalWindow,
    ROLLING,
    TRAILING,
    PERIOD,
    YTD,
    QTD,
    MTD,
    # TemporalAgg
    TemporalAgg,
    # DeltaSpec / DeltaQuery
    DeltaSpec,
    ADDED_NODES,
    REMOVED_NODES,
    ADDED_EDGES,
    REMOVED_EDGES,
    CHANGED_PROPERTY,
    ROLE_DRIFT,
    DeltaQuery,
)
from sqldim.core.query._dgm_refs import PropRef
from sqldim.core.query._dgm_preds import ScalarPred


# ---------------------------------------------------------------------------
# TemporalMode
# ---------------------------------------------------------------------------


class TestTemporalMode:
    def test_eventually_is_temporal_mode(self):
        assert issubclass(EVENTUALLY, TemporalMode)

    def test_globally_is_temporal_mode(self):
        assert issubclass(GLOBALLY, TemporalMode)

    def test_next_is_temporal_mode(self):
        assert issubclass(NEXT, TemporalMode)

    def test_once_is_temporal_mode(self):
        assert issubclass(ONCE, TemporalMode)

    def test_previously_is_temporal_mode(self):
        assert issubclass(PREVIOUSLY, TemporalMode)

    def test_until_mode_stores_pred(self):
        pred = ScalarPred(PropRef("e", "status"), "=", "active")
        m = UntilMode(pred)
        assert m.pred is pred

    def test_until_mode_is_temporal_mode(self):
        pred = ScalarPred(PropRef("e", "status"), "=", "active")
        m = UntilMode(pred)
        assert isinstance(m, TemporalMode)

    def test_since_mode_stores_pred(self):
        pred = ScalarPred(PropRef("e", "ok"), "IS NOT", None)
        m = SinceMode(pred)
        assert m.pred is pred

    def test_since_mode_is_temporal_mode(self):
        pred = ScalarPred(PropRef("e", "ok"), "IS NOT", None)
        assert isinstance(SinceMode(pred), TemporalMode)

    def test_singleton_modes_have_sql_operator(self):
        for cls in (EVENTUALLY, GLOBALLY, NEXT, ONCE, PREVIOUSLY):
            assert hasattr(cls, "sql_operator")

    def test_eventually_sql_operator(self):
        assert EVENTUALLY.sql_operator == "EVENTUALLY"

    def test_globally_sql_operator(self):
        assert GLOBALLY.sql_operator == "GLOBALLY"

    def test_next_sql_operator(self):
        assert NEXT.sql_operator == "NEXT"

    def test_once_sql_operator(self):
        assert ONCE.sql_operator == "ONCE"

    def test_previously_sql_operator(self):
        assert PREVIOUSLY.sql_operator == "PREVIOUSLY"


# ---------------------------------------------------------------------------
# TemporalOrdering — point-based
# ---------------------------------------------------------------------------


class TestPointOrdering:
    def test_before_is_ordering(self):
        o = BEFORE("e.ts", "f.ts")
        assert isinstance(o, TemporalOrdering)

    def test_before_to_sql(self):
        o = BEFORE("e.ts", "f.ts")
        assert o.to_sql() == "e.ts < f.ts"

    def test_after_to_sql(self):
        o = AFTER("e.ts", "f.ts")
        assert o.to_sql() == "e.ts > f.ts"

    def test_concurrent_no_tolerance_to_sql(self):
        o = CONCURRENT("e.ts", "f.ts")
        assert o.to_sql() == "e.ts = f.ts"

    def test_concurrent_with_tolerance_to_sql(self):
        o = CONCURRENT("e.ts", "f.ts", tolerance=5)
        sql = o.to_sql()
        assert "e.ts" in sql and "f.ts" in sql and "5" in sql

    def test_before_stores_fields(self):
        o = BEFORE("e.ts", "f.ts")
        assert o.left_ts == "e.ts"
        assert o.right_ts == "f.ts"


# ---------------------------------------------------------------------------
# TemporalOrdering — Allen's 13
# ---------------------------------------------------------------------------


_ALLEN_CLASSES = [
    MEETS, MET_BY, OVERLAPS, OVERLAPPED_BY,
    STARTS, STARTED_BY, DURING, CONTAINS,
    FINISHES, FINISHED_BY, EQUALS,
]


class TestAllenOrdering:
    def test_all_allen_are_temporal_ordering(self):
        l_iv = ("a.start", "a.end")
        r_iv = ("b.start", "b.end")
        for cls in _ALLEN_CLASSES:
            assert isinstance(cls(l_iv, r_iv), TemporalOrdering), cls.__name__

    def test_meets_to_sql(self):
        o = MEETS(("a.start", "a.end"), ("b.start", "b.end"))
        assert o.to_sql() == "a.end = b.start"

    def test_met_by_to_sql(self):
        o = MET_BY(("a.start", "a.end"), ("b.start", "b.end"))
        assert o.to_sql() == "b.end = a.start"

    def test_equals_to_sql(self):
        o = EQUALS(("a.start", "a.end"), ("b.start", "b.end"))
        assert o.to_sql() == "a.start = b.start AND a.end = b.end"

    def test_overlaps_to_sql(self):
        o = OVERLAPS(("a.start", "a.end"), ("b.start", "b.end"))
        sql = o.to_sql()
        assert "a.start < b.start" in sql
        assert "a.end > b.start" in sql
        assert "a.end < b.end" in sql

    def test_during_to_sql(self):
        o = DURING(("a.start", "a.end"), ("b.start", "b.end"))
        sql = o.to_sql()
        assert "a.start > b.start" in sql
        assert "a.end < b.end" in sql

    def test_starts_to_sql(self):
        o = STARTS(("a.start", "a.end"), ("b.start", "b.end"))
        sql = o.to_sql()
        assert "a.start = b.start" in sql
        assert "a.end < b.end" in sql

    def test_allen_stores_left_right(self):
        l_iv = ("a.start", "a.end")
        r_iv = ("b.start", "b.end")
        o = DURING(l_iv, r_iv)
        assert o.left == l_iv
        assert o.right == r_iv


# ---------------------------------------------------------------------------
# TemporalOrdering — LTL
# ---------------------------------------------------------------------------


class TestLTLOrdering:
    def test_until_ordering_stores_preds(self):
        hold = ScalarPred(PropRef("e", "ok"), "=", True)
        trig = ScalarPred(PropRef("e", "status"), "=", "done")
        o = UntilOrdering(hold, trig)
        assert o.hold_pred is hold
        assert o.trigger_pred is trig

    def test_until_ordering_is_temporal_ordering(self):
        hold = ScalarPred(PropRef("e", "ok"), "=", True)
        trig = ScalarPred(PropRef("e", "status"), "=", "done")
        assert isinstance(UntilOrdering(hold, trig), TemporalOrdering)

    def test_since_ordering_stores_preds(self):
        hold = ScalarPred(PropRef("e", "ok"), "=", True)
        trig = ScalarPred(PropRef("e", "status"), "=", "start")
        o = SinceOrdering(hold, trig)
        assert o.hold_pred is hold
        assert o.trigger_pred is trig

    def test_until_ordering_to_sql_contains_subpreds(self):
        hold = ScalarPred(PropRef("e", "ok"), "=", True)
        trig = ScalarPred(PropRef("e", "status"), "=", "done")
        sql = UntilOrdering(hold, trig).to_sql()
        assert "e.ok" in sql
        assert "e.status" in sql


# ---------------------------------------------------------------------------
# TemporalWindow
# ---------------------------------------------------------------------------


class TestTemporalWindow:
    def test_rolling_is_window(self):
        w = ROLLING("30 DAYS")
        assert isinstance(w, TemporalWindow)

    def test_rolling_to_sql(self):
        w = ROLLING("30 DAYS")
        assert "30 DAYS" in w.to_sql()

    def test_trailing_to_sql(self):
        w = TRAILING(7, "DAY")
        sql = w.to_sql()
        assert "7" in sql
        assert "DAY" in sql

    def test_period_to_sql(self):
        w = PERIOD("2024-01-01", "2024-12-31")
        sql = w.to_sql()
        assert "2024-01-01" in sql
        assert "2024-12-31" in sql

    def test_ytd_is_window(self):
        assert issubclass(YTD, TemporalWindow)

    def test_ytd_sql_operator(self):
        assert YTD.sql_operator == "YTD"

    def test_qtd_sql_operator(self):
        assert QTD.sql_operator == "QTD"

    def test_mtd_sql_operator(self):
        assert MTD.sql_operator == "MTD"

    def test_rolling_stores_duration(self):
        w = ROLLING("7 DAYS")
        assert w.duration == "7 DAYS"

    def test_trailing_stores_n_unit(self):
        w = TRAILING(3, "MONTH")
        assert w.n == 3
        assert w.unit == "MONTH"


# ---------------------------------------------------------------------------
# TemporalAgg
# ---------------------------------------------------------------------------


class TestTemporalAgg:
    def test_construction(self):
        ref = PropRef("s", "revenue")
        ts = PropRef("s", "event_ts")
        w = ROLLING("30 DAYS")
        ta = TemporalAgg("SUM", ref, ts, w)
        assert ta.fn == "SUM"
        assert ta.ref is ref
        assert ta.timestamp is ts
        assert ta.window is w

    def test_to_sql_contains_fn_and_ref(self):
        ref = PropRef("s", "revenue")
        ts = PropRef("s", "event_ts")
        ta = TemporalAgg("SUM", ref, ts, YTD)
        sql = ta.to_sql()
        assert "SUM" in sql
        assert "s.revenue" in sql

    def test_to_sql_contains_window_sql(self):
        ref = PropRef("s", "amt")
        ts = PropRef("s", "ts")
        ta = TemporalAgg("SUM", ref, ts, ROLLING("7 DAYS"))
        sql = ta.to_sql()
        assert "7 DAYS" in sql

    def test_to_sql_contains_timestamp_ref(self):
        ref = PropRef("s", "amt")
        ts = PropRef("s", "created_at")
        ta = TemporalAgg("AVG", ref, ts, MTD)
        sql = ta.to_sql()
        assert "s.created_at" in sql


# ---------------------------------------------------------------------------
# DeltaSpec / DeltaQuery
# ---------------------------------------------------------------------------


class TestDeltaSpec:
    def test_added_nodes_is_delta_spec(self):
        spec = ADDED_NODES("DimensionModel")
        assert isinstance(spec, DeltaSpec)

    def test_added_nodes_stores_type(self):
        spec = ADDED_NODES("Customer")
        assert spec.node_type == "Customer"

    def test_removed_nodes_is_delta_spec(self):
        spec = REMOVED_NODES("Order")
        assert isinstance(spec, DeltaSpec)

    def test_added_edges_stores_type(self):
        spec = ADDED_EDGES("verb")
        assert isinstance(spec, DeltaSpec)
        assert spec.edge_type == "verb"

    def test_removed_edges_is_delta_spec(self):
        assert isinstance(REMOVED_EDGES("bridge"), DeltaSpec)

    def test_changed_property_stores_fields(self):
        spec = CHANGED_PROPERTY("c", "region", ">")
        assert isinstance(spec, DeltaSpec)
        assert spec.alias == "c"
        assert spec.prop == "region"
        assert spec.comparator == ">"

    def test_role_drift_stores_fields(self):
        spec = ROLE_DRIFT("c", "standard", "vip")
        assert isinstance(spec, DeltaSpec)
        assert spec.alias == "c"
        assert spec.from_type == "standard"
        assert spec.to_type == "vip"


class TestDeltaQuery:
    def test_construction(self):
        spec = ADDED_NODES("Customer")
        dq = DeltaQuery("2024-01-01", "2024-06-30", spec)
        assert dq.t1 == "2024-01-01"
        assert dq.t2 == "2024-06-30"
        assert dq.spec is spec
        assert dq.filter is None

    def test_construction_with_filter(self):
        spec = CHANGED_PROPERTY("c", "region", "!=")
        filt = ScalarPred(PropRef("c", "active"), "=", True)
        dq = DeltaQuery("2024-01-01", "2024-06-30", spec, filter=filt)
        assert dq.filter is filt

    def test_to_sql_returns_string(self):
        spec = ADDED_NODES("Customer")
        dq = DeltaQuery("2024-01-01", "2024-06-30", spec)
        sql = dq.to_sql()
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_to_sql_mentions_timestamps(self):
        spec = REMOVED_EDGES("verb")
        dq = DeltaQuery("2024-01-01", "2024-06-30", spec)
        sql = dq.to_sql()
        assert "2024-01-01" in sql
        assert "2024-06-30" in sql
