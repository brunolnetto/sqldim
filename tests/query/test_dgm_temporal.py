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


from sqldim.core.query.dgm.temporal import (
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
    PRECEDES,
    PRECEDED_BY,
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
from sqldim.core.query.dgm.refs import PropRef
from sqldim.core.query.dgm.preds import ScalarPred
from sqldim.exceptions import SemanticError


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
    PRECEDES,
    PRECEDED_BY,
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
]


def _holds(rel: TemporalOrdering, ls, le, rs, re) -> bool:
    """Evaluate *rel* over l = [ls, le), r = [rs, re) on DuckDB."""
    import duckdb

    row = duckdb.execute(
        f"SELECT ({rel.to_sql()}) FROM (SELECT ?::INT AS ls, ?::INT AS le, "
        "?::INT AS rs, ?::INT AS re)",
        [ls, le, rs, re],
    ).fetchone()
    assert row is not None and row[0] is not None  # two-valued
    return bool(row[0])


class TestAllenOrdering:
    """§3.2: half-open intervals, NULL bounds unbounded, degenerate excluded."""

    L, R = ("ls", "le"), ("rs", "re")

    def test_all_allen_are_temporal_ordering(self):
        for cls in _ALLEN_CLASSES:
            assert isinstance(cls(self.L, self.R), TemporalOrdering), cls.__name__

    @pytest.mark.parametrize(
        ("cls", "bounds"),
        [
            (PRECEDES, (1, 2, 3, 4)),
            (PRECEDED_BY, (3, 4, 1, 2)),
            (MEETS, (1, 2, 2, 3)),
            (MET_BY, (2, 3, 1, 2)),
            (OVERLAPS, (1, 3, 2, 4)),
            (OVERLAPPED_BY, (2, 4, 1, 3)),
            (STARTS, (1, 2, 1, 3)),
            (STARTED_BY, (1, 3, 1, 2)),
            (DURING, (2, 3, 1, 4)),
            (CONTAINS, (1, 4, 2, 3)),
            (FINISHES, (2, 3, 1, 3)),
            (FINISHED_BY, (1, 3, 2, 3)),
            (EQUALS, (1, 2, 1, 2)),
        ],
    )
    def test_exactly_one_relation_holds(self, cls, bounds):
        held = [c for c in _ALLEN_CLASSES if _holds(c(self.L, self.R), *bounds)]
        assert held == [cls]

    def test_null_bounds_are_unbounded(self):
        assert _holds(DURING(self.L, self.R), 2, 3, None, None)
        assert _holds(EQUALS(self.L, self.R), None, None, None, None)
        assert _holds(STARTS(self.L, self.R), None, 2, None, 3)
        assert _holds(PRECEDES(self.L, self.R), None, 1, 2, None)
        assert not _holds(MEETS(self.L, self.R), 1, None, None, 2)

    def test_degenerate_interval_satisfies_no_relation(self):
        for bounds in ((2, 2, 2, 2), (3, 1, 0, 5)):
            assert not any(
                _holds(c(self.L, self.R), *bounds) for c in _ALLEN_CLASSES
            ), bounds

    def test_allen_stores_left_right(self):
        o = DURING(("a.start", "a.end"), ("b.start", "b.end"))
        assert o.left == ("a.start", "a.end")
        assert o.right == ("b.start", "b.end")


class TestOrderingTyping:
    """TO1–TO3 are checked at construction (§3.2)."""

    def test_to2_allen_rejects_time_points(self):
        with pytest.raises(SemanticError, match="TO2"):
            MEETS("s.occurred_on", "r.occurred_on")

    def test_to1_point_relation_rejects_interval(self):
        with pytest.raises(SemanticError, match="TO1"):
            BEFORE(("a.window_start", "a.window_end"), "s.occurred_on")

    def test_to3_negative_tolerance_rejected(self):
        with pytest.raises(SemanticError, match="TO3"):
            CONCURRENT("a.ts", "b.ts", tolerance=-1)


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

    def test_since_ordering_to_sql_contains_subpreds(self):
        hold = ScalarPred(PropRef("e", "active"), "=", True)
        trig = ScalarPred(PropRef("e", "start"), "=", "launch")
        sql = SinceOrdering(hold, trig).to_sql()
        assert "e.active" in sql
        assert "e.start" in sql


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

    def test_window_sql_with_string_window(self):
        """TemporalAgg._window_sql falls back to str() for unknown window types."""
        ref = PropRef("s", "val")
        ts = PropRef("s", "ts")
        # Pass a raw string as window (triggers the str(self.window) fallback)
        ta = TemporalAgg("SUM", ref, ts, "CUSTOM_WINDOW_EXPR")
        sql = ta._window_sql()
        assert "CUSTOM_WINDOW_EXPR" in sql

    def test_singleton_window_instance_to_sql(self):
        """_SingletonWindow instance to_sql() returns sql_operator (line 318)."""
        w = YTD()
        assert w.to_sql() == "YTD"

    def test_window_sql_with_singleton_instance(self):
        """TemporalAgg._window_sql handles a _SingletonWindow *instance* (line 405)."""
        ref = PropRef("s", "amt")
        ts = PropRef("s", "ts")
        ta = TemporalAgg("SUM", ref, ts, YTD())
        sql = ta._window_sql()
        assert sql == "YTD"


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

    def test_to_sql_with_filter_appends_where(self):
        """DeltaQuery with a filter should include WHERE clause in to_sql."""
        spec = CHANGED_PROPERTY("c", "region", "!=")
        filt = ScalarPred(PropRef("c", "active"), "=", True)
        dq = DeltaQuery("2024-01-01", "2024-06-30", spec, filter=filt)
        sql = dq.to_sql()
        assert "WHERE" in sql
        assert "c.active" in sql
