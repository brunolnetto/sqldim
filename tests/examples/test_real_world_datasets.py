"""
Tests for sqldim/examples/datasets/{nba_analytics,saas_growth,user_activity}.py

Coverage targets
----------------
sqldim/examples/datasets/nba_analytics.py
sqldim/examples/datasets/saas_growth.py
sqldim/examples/datasets/user_activity.py
sqldim/examples/datasets/fintech.py
sqldim/examples/datasets/supply_chain.py
"""
from __future__ import annotations

import duckdb
import pytest

from sqldim.application.datasets import DatasetFactory
from sqldim.sources.batch.sql import SQLSource

from sqldim.application.datasets.domains.nba_analytics import PlayerSeasonsSource
from sqldim.application.datasets.domains.saas_growth import SaaSUsersSource
from sqldim.application.datasets.domains.user_activity import DevicesSource, EventsSource
from sqldim.application.datasets.domains.fintech import (
    AccountsSource as FintechAccountsSource,
    CounterpartiesSource,
    TransactionsSource,
)
from sqldim.application.datasets.domains.supply_chain import (
    SuppliersSource,
    WarehousesSource,
    SKUsSource,
    ReceiptsSource,
)


# ---------------------------------------------------------------------------
# PlayerSeasonsSource
# ---------------------------------------------------------------------------

class TestPlayerSeasonsSource:

    @pytest.fixture()
    def src(self):
        return PlayerSeasonsSource(n=10, seed=0)

    def test_registered_in_factory(self):
        assert "player_seasons" in DatasetFactory.available()

    def test_factory_create(self):
        src = DatasetFactory.create("player_seasons", n=5, seed=1)
        assert isinstance(src, PlayerSeasonsSource)  # type: ignore[misc]

    def test_initial_row_count(self, src):
        assert len(src.initial) == 10

    def test_snapshot_returns_sql_source(self, src):
        snap = src.snapshot()
        assert isinstance(snap, SQLSource)

    def test_snapshot_row_count(self, src):
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 10

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("player_name", "season", "pts", "reb", "ast"):
            assert col in df.columns

    def test_snapshot_has_advanced_stats(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("netrtg", "oreb_pct", "dreb_pct", "usg_pct", "ts_pct", "ast_pct"):
            assert col in df.columns

    def test_event_batch_returns_sql_source(self, src):
        batch = src.event_batch(1)
        assert isinstance(batch, SQLSource)

    def test_event_batch_advances_season(self, src):
        con = duckdb.connect()
        initial = con.execute(src.snapshot().as_sql(con)).fetchdf()
        events  = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        # Event batch should have incremented season by 1 for each row
        assert (events["season"] > initial["season"].min()).any()

    def test_provider_is_set(self, src):
        from sqldim.application.datasets.base import SourceProvider
        assert src.provider is not None
        assert isinstance(src.provider, SourceProvider)
        assert "NBA" in src.provider.name

    def test_provider_describe(self, src):
        desc = src.describe_provider()
        assert "NBA" in desc

    def test_setup_and_teardown(self, src):
        con = duckdb.connect()
        src.setup(con, "dim_player_seasons")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_player_seasons" in tables
        src.teardown(con, "dim_player_seasons")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_player_seasons" not in tables

    def test_pts_range(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert df["pts"].between(0, 50).all()

    def test_seed_reproducibility(self):
        a = PlayerSeasonsSource(n=5, seed=42)
        b = PlayerSeasonsSource(n=5, seed=42)
        assert a.initial[0]["player_name"] == b.initial[0]["player_name"]


# ---------------------------------------------------------------------------
# SaaSUsersSource
# ---------------------------------------------------------------------------

class TestSaaSUsersSource:

    @pytest.fixture()
    def src(self):
        return SaaSUsersSource(n=20, seed=7)

    def test_registered_in_factory(self):
        assert "saas_users" in DatasetFactory.available()

    def test_factory_create(self):
        src = DatasetFactory.create("saas_users", n=5, seed=2)
        assert isinstance(src, SaaSUsersSource)

    def test_initial_row_count(self, src):
        assert len(src.initial) == 20

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_row_count(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(df) == 20

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("user_id", "email", "plan_tier", "acq_source", "device"):
            assert col in df.columns

    def test_plan_tier_values(self, src):
        tiers = {row["plan_tier"] for row in src.initial}
        assert tiers <= {"free", "pro", "enterprise"}

    def test_event_batch_returns_sql_source(self, src):
        assert isinstance(src.event_batch(1), SQLSource)

    def test_event_batch_has_upgrades(self, src):
        """At least some free users should have upgraded to pro/enterprise."""
        # Use a large seed population so upgrades are statistically certain
        big = SaaSUsersSource(n=100, seed=99)
        upgraded = [
            row for row in big.events
            if row.get("plan_tier") in ("pro", "enterprise")
        ]
        assert len(upgraded) > 0

    def test_provider_is_set(self, src):
        from sqldim.application.datasets.base import SourceProvider
        assert isinstance(src.provider, SourceProvider)

    def test_setup_and_teardown(self, src):
        con = duckdb.connect()
        src.setup(con, "dim_saas_users")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_saas_users" in tables
        src.teardown(con, "dim_saas_users")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_saas_users" not in tables

    def test_acq_source_values(self, src):
        valid = {"organic", "ads", "referral", "content", "social"}
        actual = {row["acq_source"] for row in src.initial}
        assert actual <= valid


# ---------------------------------------------------------------------------
# DevicesSource
# ---------------------------------------------------------------------------

class TestDevicesSource:

    @pytest.fixture()
    def src(self):
        return DevicesSource(n=15, seed=3)

    def test_registered_in_factory(self):
        assert "devices" in DatasetFactory.available()

    def test_factory_create(self):
        src = DatasetFactory.create("devices", n=5, seed=0)
        assert isinstance(src, DevicesSource)

    def test_initial_row_count(self, src):
        assert len(src.initial) == 15

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_row_count(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(df) == 15

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("device_id", "browser_type", "os_type", "device_type"):
            assert col in df.columns

    def test_browser_type_values(self, src):
        valid = {"Chrome", "Firefox", "Safari", "Edge", "Brave"}
        actual = {row["browser_type"] for row in src.initial}
        assert actual <= valid

    def test_event_batch_returns_sql_source(self, src):
        assert isinstance(src.event_batch(1), SQLSource)

    def test_provider_is_set(self, src):
        from sqldim.application.datasets.base import SourceProvider
        assert isinstance(src.provider, SourceProvider)

    def test_setup_and_teardown(self, src):
        con = duckdb.connect()
        src.setup(con, "dim_devices")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_devices" in tables
        src.teardown(con, "dim_devices")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_devices" not in tables


# ---------------------------------------------------------------------------
# EventsSource
# ---------------------------------------------------------------------------

class TestEventsSource:

    @pytest.fixture()
    def src(self):
        return EventsSource(n=30, seed=5)

    def test_registered_in_factory(self):
        assert "events" in DatasetFactory.available()

    def test_factory_create(self):
        src = DatasetFactory.create("events", n=8, seed=1)
        assert isinstance(src, EventsSource)

    def test_initial_row_count(self, src):
        assert len(src.initial) == 30

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_row_count(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(df) == 30

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("event_id", "url", "user_id", "device_id", "event_time"):
            assert col in df.columns

    def test_url_values(self, src):
        valid_prefixes = {"/home", "/dashboard", "/pricing", "/docs",
                         "/blog", "/login", "/signup", "/settings"}
        actual = {row["url"] for row in src.initial}
        assert actual <= valid_prefixes

    def test_event_batch_returns_sql_source(self, src):
        assert isinstance(src.event_batch(1), SQLSource)

    def test_event_batch_has_new_urls(self, src):
        con = duckdb.connect()
        rows = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(rows) > 0
        assert "url" in rows.columns

    def test_provider_is_set(self, src):
        from sqldim.application.datasets.base import SourceProvider
        assert isinstance(src.provider, SourceProvider)

    def test_setup_and_teardown(self, src):
        con = duckdb.connect()
        src.setup(con, "fact_events")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "fact_events" in tables
        src.teardown(con, "fact_events")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "fact_events" not in tables


# ---------------------------------------------------------------------------
# CounterpartiesSource  (fintech)
# ---------------------------------------------------------------------------

class TestCounterpartiesSource:

    @pytest.fixture()
    def src(self):
        return CounterpartiesSource(n_entities=10, seed=0)

    def test_registered_in_factory(self):
        assert "fintech_counterparties" in DatasetFactory.available()

    def test_initial_row_count(self, src):
        assert len(src.initial) == 10

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("cp_id", "bic_code", "institution_name", "country_code", "is_sanctioned"):
            assert col in df.columns

    def test_event_batch_returns_sql_source(self, src):
        assert isinstance(src.event_batch(1), SQLSource)


# ---------------------------------------------------------------------------
# TransactionsSource  (fintech)
# ---------------------------------------------------------------------------

class TestTransactionsSource:

    @pytest.fixture()
    def src(self):
        return TransactionsSource(n_entities=20, seed=1)

    def test_registered_in_factory(self):
        assert "fintech_transactions" in DatasetFactory.available()

    def test_initial_row_count(self, src):
        assert len(src.initial) == 20

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("txn_id", "account_id", "counterparty_id", "amount_usd", "txn_type"):
            assert col in df.columns

    def test_event_batch_changes_amounts(self, src):
        con = duckdb.connect()
        initial = con.execute(src.snapshot().as_sql(con)).fetchdf()
        events = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(events) > 0
        assert set(events.columns) == set(initial.columns)


# ---------------------------------------------------------------------------
# SuppliersSource  (supply_chain)
# ---------------------------------------------------------------------------

class TestSuppliersSource:

    @pytest.fixture()
    def src(self):
        return SuppliersSource(n_entities=8, seed=10)

    def test_registered_in_factory(self):
        assert "supply_chain_suppliers" in DatasetFactory.available()

    def test_initial_row_count(self, src):
        assert len(src.initial) == 8

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("supplier_id", "supplier_code", "supplier_name",
                    "country_code", "reliability_score"):
            assert col in df.columns

    def test_event_batch_returns_sql_source(self, src):
        assert isinstance(src.event_batch(1), SQLSource)


# ---------------------------------------------------------------------------
# ReceiptsSource  (supply_chain)
# ---------------------------------------------------------------------------

class TestReceiptsSource:

    @pytest.fixture()
    def src(self):
        return ReceiptsSource(n_entities=15, seed=7)

    def test_registered_in_factory(self):
        assert "supply_chain_receipts" in DatasetFactory.available()

    def test_initial_row_count(self, src):
        assert len(src.initial) == 15

    def test_snapshot_returns_sql_source(self, src):
        assert isinstance(src.snapshot(), SQLSource)

    def test_snapshot_has_key_fields(self, src):
        con = duckdb.connect()
        df = con.execute(src.snapshot().as_sql(con)).fetchdf()
        for col in ("receipt_id", "warehouse_id", "supplier_id",
                    "sku_id", "receipt_date", "quantity_received"):
            assert col in df.columns
