"""
Tests for sqldim/application/datasets/domains/*/dataset.py  and
             sqldim/application/datasets/domains/*/events/__init__.py

Coverage targets
----------------
Each domain exposes a module-level ``Dataset`` instance and an ``events``
sub-package.  These are thin wiring modules; importing them and exercising
the Dataset lifecycle is sufficient to reach 100 % coverage.
"""

from __future__ import annotations

import duckdb


# ---------------------------------------------------------------------------
# domain dataset objects
# ---------------------------------------------------------------------------


class TestDevopsDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.devops.dataset import devops_dataset

        assert devops_dataset.table_names() == ["github_issues"]

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.devops.dataset import devops_dataset

        con = duckdb.connect()
        devops_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "github_issues" in tables
        devops_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "github_issues" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.devops.dataset import devops_dataset

        snaps = devops_dataset.snapshots()
        assert "github_issues" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.devops.dataset import devops_dataset

        assert "devops" in repr(devops_dataset)


class TestDataModelsDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.data_models.dataset import (
            data_models_dataset,
        )

        assert data_models_dataset.table_names() == ["dgm_showcase", "org_dim"]

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.data_models.dataset import (
            data_models_dataset,
        )

        con = duckdb.connect()
        data_models_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dgm_showcase_customer" in tables
        assert "dgm_showcase_product" in tables
        assert "dgm_showcase_sale" in tables
        assert "org_dim" in tables
        assert "org_dim_closure" in tables
        assert "sales_fact" in tables
        data_models_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert not any(t.startswith("dgm_showcase") for t in tables)
        assert "org_dim" not in tables

    def test_custom_prefix(self):
        # Verify DGMShowcaseSource honours any alias — not hardcoded to "dgm_showcase"
        from sqldim.application.datasets.dataset import Dataset
        from sqldim.application.datasets.domains.data_models.sources import (
            DGMShowcaseSource,
        )

        ds = Dataset("test_dgm", [(DGMShowcaseSource(), "custom_prefix")])
        con = duckdb.connect()
        ds.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "custom_prefix_customer" in tables
        ds.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert not any(t.startswith("custom_prefix") for t in tables)

    def test_snapshots_excluded_for_static_fixtures(self):
        # Both DGMShowcaseSource and OrgChartSource are static fixtures;
        # snapshot() raises NotImplementedError so Dataset.snapshots() returns {}
        from sqldim.application.datasets.domains.data_models.dataset import (
            data_models_dataset,
        )

        snaps = data_models_dataset.snapshots()
        assert snaps == {}

    def test_repr(self):
        from sqldim.application.datasets.domains.data_models.dataset import (
            data_models_dataset,
        )

        assert "data_models" in repr(data_models_dataset)


class TestEcommerceDataset:
    """Backward-compat class — exercises the new commerce_dataset OLTP tables."""

    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        oltp_tables = {"customers", "products", "stores", "orders"}
        assert oltp_tables.issubset(set(commerce_dataset.table_names()))

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        con = duckdb.connect()
        commerce_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("customers", "products", "stores", "orders"):
            assert t in tables
        commerce_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("customers", "products", "stores", "orders"):
            assert t not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        snaps = commerce_dataset.snapshots()
        assert "customers" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        assert "commerce" in repr(commerce_dataset)


class TestCommerceDataset:
    _ALL_TABLES = {
        "customers",
        "products",
        "stores",
        "orders",
        "dim_customer",
        "fct_daily_sales",
        "fct_cohort_retention",
        "suppliers",
        "warehouses",
        "skus",
        "receipts",
    }

    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        assert set(commerce_dataset.table_names()) == self._ALL_TABLES

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        con = duckdb.connect()
        commerce_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in self._ALL_TABLES:
            assert t in tables, f"{t!r} missing after setup"
        commerce_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in self._ALL_TABLES:
            assert t not in tables, f"{t!r} still present after teardown"

    def test_snapshots_include_oltp(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        snaps = commerce_dataset.snapshots()
        # OLTP sources expose snapshot(); must be present
        assert "customers" in snaps
        assert "orders" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        assert "commerce" in repr(commerce_dataset)


class TestGrowthDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        assert set(growth_dataset.table_names()) == {
            "employees",
            "accounts",
            "saas_users",
            "saas_sessions",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        con = duckdb.connect()
        growth_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "employees" in tables and "accounts" in tables
        growth_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "employees" not in tables and "accounts" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        snaps = growth_dataset.snapshots()
        assert "employees" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        assert "growth" in repr(growth_dataset)


class TestFintechDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

        assert set(fintech_dataset.table_names()) == {
            "accounts",
            "counterparties",
            "transactions",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

        con = duckdb.connect()
        fintech_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "accounts" in tables and "transactions" in tables
        fintech_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "accounts" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

        snaps = fintech_dataset.snapshots()
        assert "accounts" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

        assert "fintech" in repr(fintech_dataset)


class TestEntertainmentDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.entertainment.dataset import (
            entertainment_dataset,
        )

        assert set(entertainment_dataset.table_names()) == {
            "actors",
            "movies",
            "social_network",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.entertainment.dataset import (
            entertainment_dataset,
        )

        con = duckdb.connect()
        entertainment_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "movies" in tables
        assert "actors" in tables
        entertainment_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "movies" not in tables
        assert "actors" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.entertainment.dataset import (
            entertainment_dataset,
        )

        snaps = entertainment_dataset.snapshots()
        assert "movies" in snaps
        assert "actors" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.entertainment.dataset import (
            entertainment_dataset,
        )

        assert "entertainment" in repr(entertainment_dataset)


class TestNbaAnalyticsDataset:
    """Tests for the nba_analytics domain Dataset.

    Tests that use DuckDB setup/teardown use a small dataset (n=5 games)
    to avoid the ~1.7s overhead from generating 200 × 22 = 4 400 box-score
    rows each time.  Metadata tests (table_names, repr, snapshots) still
    use the module-level dataset which never calls setup().
    """

    @staticmethod
    def _small_dataset():
        """Return a minimal NBA dataset with 5 games (fast to set up)."""
        from sqldim.application.datasets.dataset import Dataset
        from sqldim.application.datasets.domains.nba_analytics.sources import (
            PlayerSeasonsSource,
            TeamsSource,
            GamesSource,
            GameDetailsSource,
        )

        _games = GamesSource(n=5, seed=42)
        return Dataset(
            "nba_analytics",
            [
                (TeamsSource(), "teams"),
                (PlayerSeasonsSource(n=10, seed=42), "player_seasons"),
                (_games, "games"),
                (GameDetailsSource(_games, seed=42), "game_details"),
            ],
        )

    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.nba_analytics.dataset import (
            nba_analytics_dataset,
        )

        assert set(nba_analytics_dataset.table_names()) == {
            "teams",
            "player_seasons",
            "games",
            "game_details",
        }

    def test_setup_teardown(self):
        ds = self._small_dataset()
        con = duckdb.connect()
        ds.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("teams", "player_seasons", "games", "game_details"):
            assert t in tables
        ds.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("teams", "player_seasons", "games", "game_details"):
            assert t not in tables

    def test_teams_auto_populated(self):
        """TeamsSource.setup() inserts data automatically — 30 rows expected."""
        ds = self._small_dataset()
        con = duckdb.connect()
        ds.setup(con)
        n = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        ds.teardown(con)
        assert n == 30

    def test_snapshots(self):
        from sqldim.application.datasets.domains.nba_analytics.dataset import (
            nba_analytics_dataset,
        )

        snaps = nba_analytics_dataset.snapshots()
        for t in ("teams", "player_seasons", "games", "game_details"):
            assert t in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.nba_analytics.dataset import (
            nba_analytics_dataset,
        )

        assert "nba_analytics" in repr(nba_analytics_dataset)

    def test_teams_property_returns_raw_tuples(self):
        from sqldim.application.datasets.domains.nba_analytics.sources import (
            TeamsSource,
        )

        src = TeamsSource()
        teams = src.teams
        assert isinstance(teams, list)
        assert len(teams) == 30  # 30 NBA teams


class TestSaasGrowthDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        assert "saas_users" in growth_dataset.table_names()
        assert "saas_sessions" in growth_dataset.table_names()

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        con = duckdb.connect()
        growth_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "saas_users" in tables
        growth_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "saas_users" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        snaps = growth_dataset.snapshots()
        assert "saas_users" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.growth.dataset import (
            growth_dataset,
        )

        assert "growth" in repr(growth_dataset)


class TestCommerceSupplyChainSources:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        assert {"suppliers", "warehouses", "skus", "receipts"}.issubset(
            set(commerce_dataset.table_names())
        )

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        con = duckdb.connect()
        commerce_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "suppliers" in tables and "receipts" in tables
        commerce_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "suppliers" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        snaps = commerce_dataset.snapshots()
        assert "suppliers" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.commerce.dataset import (
            commerce_dataset,
        )

        assert "commerce" in repr(commerce_dataset)


class TestTelemetryDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.telemetry.dataset import (
            telemetry_dataset,
        )

        assert set(telemetry_dataset.table_names()) == {
            "devices",
            "page_events",
            "pipeline_spans",
            "metric_samples",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.telemetry.dataset import (
            telemetry_dataset,
        )

        con = duckdb.connect()
        telemetry_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "devices" in tables and "page_events" in tables
        telemetry_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "devices" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.telemetry.dataset import (
            telemetry_dataset,
        )

        snaps = telemetry_dataset.snapshots()
        assert "devices" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.telemetry.dataset import (
            telemetry_dataset,
        )

        assert "telemetry" in repr(telemetry_dataset)


# ---------------------------------------------------------------------------
# domain events __init__.py  (stub + real exports)
# ---------------------------------------------------------------------------


class TestDomainEvents:
    """Importing each events package is sufficient to cover the module body."""

    def test_devops_events_empty(self):
        from sqldim.application.datasets.domains.devops import events

        assert events.__all__ == []

    def test_data_models_events_exports(self):
        from sqldim.application.datasets.domains.data_models import events

        assert "apply_segment_upgrade" in events.__all__
        assert "apply_revenue_surge" in events.__all__

    def test_commerce_events_exports(self):
        from sqldim.application.datasets.domains.commerce import events

        assert "CustomerBulkCancelEvent" in events.__all__
        assert "CustomerAddressChangedEvent" in events.__all__
        assert "ProductStockOutEvent" in events.__all__

    def test_growth_events_exports(self):
        from sqldim.application.datasets.domains.growth import events

        assert "UserPlanUpgradedEvent" in events.__all__
        assert "UserChurnedEvent" in events.__all__

    def test_fintech_events_empty(self):
        from sqldim.application.datasets.domains.fintech import events

        assert events.__all__ == []

    def test_entertainment_events_exports(self):
        from sqldim.application.datasets.domains.entertainment import events

        assert "apply_new_release" in events.__all__

    def test_nba_analytics_events_empty(self):
        from sqldim.application.datasets.domains.nba_analytics import events

        assert events.__all__ == []

    def test_commerce_events_supply_chain(self):
        from sqldim.application.datasets.domains.commerce.events.scenarios import (
            apply_supplier_disruption,
            apply_warehouse_closure,
            apply_cost_shock,
        )

        assert callable(apply_supplier_disruption)
        assert callable(apply_warehouse_closure)
        assert callable(apply_cost_shock)

    def test_telemetry_events_empty(self):
        from sqldim.application.datasets.domains.telemetry import events

        assert events.__all__ == []
