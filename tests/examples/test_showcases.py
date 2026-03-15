"""Tests for all feature and real-world showcases."""
import asyncio
import pytest


# ── Feature showcases (synchronous) ─────────────────────────────────────────

def test_scd_types_showcase():
    from sqldim.examples.features.scd_types.showcase import run_showcase
    run_showcase()


def test_composition_showcase():
    from sqldim.examples.features.composition.showcase import run_showcase
    run_showcase()


def test_fact_patterns_showcase():
    from sqldim.examples.features.fact_patterns.showcase import run_showcase
    run_showcase()


def test_graph_showcase():
    from sqldim.examples.features.graph.showcase import run_showcase
    run_showcase()


def test_integrations_showcase():
    from sqldim.examples.features.integrations.showcase import run_showcase
    run_showcase()


def test_prebuilt_dims_showcase():
    from sqldim.examples.features.prebuilt_dims.showcase import run_showcase
    run_showcase()


# ── Real-world showcases (asynchronous) ─────────────────────────────────────

@pytest.mark.asyncio
async def test_nba_analytics_showcase():
    from sqldim.examples.real_world.nba_analytics.showcase import run_showcase
    await run_showcase()


@pytest.mark.asyncio
async def test_saas_growth_showcase():
    from sqldim.examples.real_world.saas_growth.showcase import run_saas_showcase
    await run_saas_showcase()


@pytest.mark.asyncio
async def test_user_activity_showcase():
    from sqldim.examples.real_world.user_activity.showcase import run_activity_showcase
    await run_activity_showcase()


# ── Helper coverage — branches not exercised by full run_showcase paths ──────

def test_to_pandas_with_to_pandas_method():
    # Lines 55-56: branch where df has to_pandas() but is not a pd.DataFrame
    import pandas as pd
    from sqldim.examples.features.composition.showcase import _to_pandas

    class FakeDf:
        def to_pandas(self):
            return pd.DataFrame({"x": [1]})

    result = _to_pandas(FakeDf())
    assert isinstance(result, pd.DataFrame)


def test_close_scd2_rows_empty_to_close():
    # Line 72: early return when to_close is None
    import duckdb
    from datetime import date
    from sqldim.examples.features.composition.showcase import _close_scd2_rows

    class FakeResult:
        to_close = None

    con = duckdb.connect()
    _close_scd2_rows(FakeResult(), date.today(), con)  # must not raise


def test_print_paths_fewer_than_two_ranked():
    # Line 74: early return when ranked list has < 2 elements
    from sqldim.examples.features.graph.showcase import _print_paths
    _print_paths(None, None, {}, [])   # empty → returns immediately
    _print_paths(None, None, {}, [(1, "Alice")])  # single → also returns early
