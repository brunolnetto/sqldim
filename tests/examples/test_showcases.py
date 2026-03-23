"""Tests for all feature and real-world showcases."""
import pytest


# ── Feature showcases (synchronous) ─────────────────────────────────────────

def test_scd_types_showcase():
    from sqldim.application.examples.features.scd_types.showcase import run_showcase
    run_showcase()


def test_composition_showcase():
    from sqldim.application.examples.features.composition.showcase import run_showcase
    run_showcase()


def test_fact_patterns_showcase():
    from sqldim.application.examples.features.fact_patterns.showcase import run_showcase
    run_showcase()


def test_graph_showcase():
    from sqldim.application.examples.features.graph.showcase import run_showcase
    run_showcase()


def test_integrations_showcase():
    from sqldim.application.examples.features.integrations.showcase import run_showcase
    run_showcase()


def test_prebuilt_dims_showcase():
    from sqldim.application.examples.features.prebuilt_dims.showcase import run_showcase
    run_showcase()


def test_hierarchy_showcase():
    from sqldim.application.examples.features.hierarchy.showcase import run_showcase
    run_showcase()


def test_observability_showcase():
    from sqldim.application.examples.features.observability.showcase import run_showcase
    run_showcase()


def test_dgm_showcase():
    from sqldim.application.examples.features.dgm.showcase import run_all
    run_all()



@pytest.mark.asyncio
async def test_nba_analytics_showcase():
    from sqldim.application.examples.real_world.nba_analytics.showcase import run_showcase
    await run_showcase()


@pytest.mark.asyncio
async def test_saas_growth_showcase():
    from sqldim.application.examples.real_world.saas_growth.showcase import run_saas_showcase
    await run_saas_showcase()


@pytest.mark.asyncio
async def test_ecommerce_showcase():
    from sqldim.application.examples.real_world.ecommerce.showcase import run_ecommerce_showcase
    await run_ecommerce_showcase()


@pytest.mark.asyncio
async def test_fintech_showcase():
    from sqldim.application.examples.real_world.fintech.showcase import run_fintech_showcase
    await run_fintech_showcase()


@pytest.mark.asyncio
async def test_supply_chain_showcase():
    from sqldim.application.examples.real_world.supply_chain.showcase import run_supply_chain_showcase
    await run_supply_chain_showcase()


@pytest.mark.asyncio
async def test_user_activity_showcase():
    from sqldim.application.examples.real_world.user_activity.showcase import run_activity_showcase
    await run_activity_showcase()


# ── Helper coverage — branches not exercised by full run_showcase paths ──────

def test_to_pandas_with_to_pandas_method():
    # Lines 55-56: branch where df has to_pandas() but is not a pd.DataFrame
    import pandas as pd
    from sqldim.application.examples.features.composition.showcase import _to_pandas

    class FakeDf:
        def to_pandas(self):
            return pd.DataFrame({"x": [1]})

    result = _to_pandas(FakeDf())
    assert isinstance(result, pd.DataFrame)


def test_model_ddl_returns_create_statement():
    """Lines 234-235, 237-238: model_ddl compiles a SQLModel to DDL."""
    from sqldim.application.examples.utils import model_ddl
    from sqldim import DimensionModel, Field, SCD2Mixin

    class _TestDim(DimensionModel, SCD2Mixin, table=True):
        __natural_key__ = ["code"]
        id: int = Field(primary_key=True, surrogate_key=True)
        code: str

    ddl = model_ddl(_TestDim)
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "code" in ddl



def test_close_scd2_rows_empty_to_close():
    # Line 72: early return when to_close is None
    import duckdb
    from datetime import date
    from sqldim.application.examples.features.composition.showcase import _close_scd2_rows

    class FakeResult:
        to_close = None

    con = duckdb.connect()
    _close_scd2_rows(FakeResult(), date.today(), con)  # must not raise


def test_print_paths_fewer_than_two_ranked():
    # Line 74: early return when ranked list has < 2 elements
    from sqldim.application.examples.features.graph.showcase import _print_paths
    _print_paths(None, None, {}, [])   # empty → returns immediately
    _print_paths(None, None, {}, [(1, "Alice")])  # single → also returns early


def test_order_stage_all_milestones():
    """ecommerce showcase lines 46, 48, 50, 52: each non-'placed' stage branch."""
    from types import SimpleNamespace
    from sqldim.application.examples.real_world.ecommerce.showcase import _order_stage
    from datetime import datetime
    _dt = datetime(2024, 1, 1)
    placed  = SimpleNamespace(returned_at=None, delivered_at=None, shipped_at=None, paid_at=None)
    paid    = SimpleNamespace(returned_at=None, delivered_at=None, shipped_at=None, paid_at=_dt)
    shipped = SimpleNamespace(returned_at=None, delivered_at=None, shipped_at=_dt, paid_at=_dt)
    dlvrd   = SimpleNamespace(returned_at=None, delivered_at=_dt,  shipped_at=_dt, paid_at=_dt)
    retd    = SimpleNamespace(returned_at=_dt,  delivered_at=_dt,  shipped_at=_dt, paid_at=_dt)
    assert _order_stage(placed)  == "placed"
    assert _order_stage(paid)    == "paid"
    assert _order_stage(shipped) == "shipped"
    assert _order_stage(dlvrd)   == "delivered"
    assert _order_stage(retd)    == "returned"


def test_coerce_placed_at_invalid_string():
    """ecommerce showcase lines 129-130 (now _coerce_placed_at): ValueError fallback."""
    from datetime import datetime
    from sqldim.application.examples.real_world.ecommerce.showcase import _coerce_placed_at
    result = _coerce_placed_at("not-a-date")
    assert result == datetime(2024, 6, 1)


def test_coerce_placed_at_valid_iso():
    """_coerce_placed_at: valid ISO string is parsed correctly."""
    from datetime import datetime
    from sqldim.application.examples.real_world.ecommerce.showcase import _coerce_placed_at
    result = _coerce_placed_at("2024-03-15T12:00:00")
    assert result == datetime(2024, 3, 15, 12, 0, 0)


def test_coerce_placed_at_none_returns_default():
    """_coerce_placed_at: None returns the 2024-06-01 fallback."""
    from datetime import datetime
    from sqldim.application.examples.real_world.ecommerce.showcase import _coerce_placed_at
    assert _coerce_placed_at(None) == datetime(2024, 6, 1)


# ── Supply chain DGM v0.11 helper functions ────────────────────────────────

def test_scq_detect_shipment_cycles_returns_strings():
    from sqldim.application.examples.real_world.supply_chain.showcase import _scq_detect_shipment_cycles
    trim_sql, scc_sql = _scq_detect_shipment_cycles()
    assert isinstance(trim_sql, str) and len(trim_sql) > 0
    assert isinstance(scc_sql, str) and "tarjan_scc" in scc_sql


def test_scq_weighted_shipment_cost():
    from sqldim.application.examples.real_world.supply_chain.showcase import _scq_weighted_shipment_cost
    sql = _scq_weighted_shipment_cost()
    assert "sh.quantity" in sql
    assert "sh.unit_cost" in sql
    assert "*" in sql


def test_scq_hub_degree_out():
    from sqldim.application.examples.real_world.supply_chain.showcase import _scq_hub_degree_out
    sql = _scq_hub_degree_out()
    assert "OUT" in sql
    assert isinstance(sql, str)


def test_scq_regional_density():
    from sqldim.application.examples.real_world.supply_chain.showcase import _scq_regional_density
    sql = _scq_regional_density()
    assert "w.region" in sql
    assert "density" in sql.lower()
