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


class TestDgmDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

        assert dgm_dataset.table_names() == ["dgm_showcase"]

    def test_setup_teardown(self):
        # DGMShowcaseSource now uses the Dataset alias as a prefix.
        # Dataset alias "dgm_showcase" → tables dgm_showcase_{customer,product,...}
        from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

        con = duckdb.connect()
        dgm_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dgm_showcase_customer" in tables
        assert "dgm_showcase_product" in tables
        assert "dgm_showcase_sale" in tables
        dgm_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert not any(t.startswith("dgm_showcase") for t in tables)

    def test_custom_prefix(self):
        # Verify the source truly honours any alias — not hardcoded to "dgm_showcase"
        from sqldim.application.datasets.dataset import Dataset
        from sqldim.application.datasets.domains.dgm.sources import DGMShowcaseSource

        ds = Dataset("test_dgm", [(DGMShowcaseSource(), "custom_prefix")])
        con = duckdb.connect()
        ds.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "custom_prefix_customer" in tables
        ds.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert not any(t.startswith("custom_prefix") for t in tables)

    def test_snapshots_excluded_for_static_fixture(self):
        # DGMShowcaseSource is a static fixture; snapshot() raises NotImplementedError
        # so Dataset.snapshots() silently excludes it and returns {}
        from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

        snaps = dgm_dataset.snapshots()
        assert snaps == {}

    def test_repr(self):
        from sqldim.application.datasets.domains.dgm.dataset import dgm_dataset

        assert "dgm" in repr(dgm_dataset)


class TestEcommerceDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.ecommerce.dataset import (
            ecommerce_dataset,
        )

        assert set(ecommerce_dataset.table_names()) == {
            "customers",
            "products",
            "stores",
            "orders",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.ecommerce.dataset import (
            ecommerce_dataset,
        )

        con = duckdb.connect()
        ecommerce_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("customers", "products", "stores", "orders"):
            assert t in tables
        ecommerce_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        for t in ("customers", "products", "stores", "orders"):
            assert t not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.ecommerce.dataset import (
            ecommerce_dataset,
        )

        snaps = ecommerce_dataset.snapshots()
        assert "customers" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.ecommerce.dataset import (
            ecommerce_dataset,
        )

        assert "ecommerce" in repr(ecommerce_dataset)


class TestEnterpriseDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.enterprise.dataset import (
            enterprise_dataset,
        )

        assert set(enterprise_dataset.table_names()) == {"employees", "accounts"}

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.enterprise.dataset import (
            enterprise_dataset,
        )

        con = duckdb.connect()
        enterprise_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "employees" in tables and "accounts" in tables
        enterprise_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "employees" not in tables and "accounts" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.enterprise.dataset import (
            enterprise_dataset,
        )

        snaps = enterprise_dataset.snapshots()
        assert "employees" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.enterprise.dataset import (
            enterprise_dataset,
        )

        assert "enterprise" in repr(enterprise_dataset)


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


class TestHierarchyDataset:
    def test_import_and_tables(self):
        # Alias is "org_dim" — the primary table OrgChartSource creates
        from sqldim.application.datasets.domains.hierarchy.dataset import (
            hierarchy_dataset,
        )

        assert hierarchy_dataset.table_names() == ["org_dim"]

    def test_setup_teardown(self):
        # OrgChartSource creates org_dim (primary), org_dim_closure, and sales_fact
        from sqldim.application.datasets.domains.hierarchy.dataset import (
            hierarchy_dataset,
        )

        con = duckdb.connect()
        hierarchy_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "org_dim" in tables
        assert "org_dim_closure" in tables
        assert "sales_fact" in tables
        hierarchy_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "org_dim" not in tables

    def test_snapshots_excluded_for_static_fixture(self):
        # OrgChartSource is a static fixture; snapshot() raises NotImplementedError
        from sqldim.application.datasets.domains.hierarchy.dataset import (
            hierarchy_dataset,
        )

        snaps = hierarchy_dataset.snapshots()
        assert snaps == {}

    def test_repr(self):
        from sqldim.application.datasets.domains.hierarchy.dataset import (
            hierarchy_dataset,
        )

        assert "hierarchy" in repr(hierarchy_dataset)


class TestMediaDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.media.dataset import media_dataset

        assert media_dataset.table_names() == ["movies"]

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.media.dataset import media_dataset

        con = duckdb.connect()
        media_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "movies" in tables
        media_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "movies" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.media.dataset import media_dataset

        snaps = media_dataset.snapshots()
        assert "movies" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.media.dataset import media_dataset

        assert "media" in repr(media_dataset)


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
        from sqldim.application.datasets.domains.saas_growth.dataset import (
            saas_growth_dataset,
        )

        assert set(saas_growth_dataset.table_names()) == {"saas_users", "saas_sessions"}

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.saas_growth.dataset import (
            saas_growth_dataset,
        )

        con = duckdb.connect()
        saas_growth_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "saas_users" in tables
        saas_growth_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "saas_users" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.saas_growth.dataset import (
            saas_growth_dataset,
        )

        snaps = saas_growth_dataset.snapshots()
        assert "saas_users" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.saas_growth.dataset import (
            saas_growth_dataset,
        )

        assert "saas_growth" in repr(saas_growth_dataset)


class TestSupplyChainDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.supply_chain.dataset import (
            supply_chain_dataset,
        )

        assert set(supply_chain_dataset.table_names()) == {
            "suppliers",
            "warehouses",
            "skus",
            "receipts",
        }

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.supply_chain.dataset import (
            supply_chain_dataset,
        )

        con = duckdb.connect()
        supply_chain_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "suppliers" in tables and "receipts" in tables
        supply_chain_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "suppliers" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.supply_chain.dataset import (
            supply_chain_dataset,
        )

        snaps = supply_chain_dataset.snapshots()
        assert "suppliers" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.supply_chain.dataset import (
            supply_chain_dataset,
        )

        assert "supply_chain" in repr(supply_chain_dataset)


class TestUserActivityDataset:
    def test_import_and_tables(self):
        from sqldim.application.datasets.domains.user_activity.dataset import (
            user_activity_dataset,
        )

        assert set(user_activity_dataset.table_names()) == {"devices", "page_events"}

    def test_setup_teardown(self):
        from sqldim.application.datasets.domains.user_activity.dataset import (
            user_activity_dataset,
        )

        con = duckdb.connect()
        user_activity_dataset.setup(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "devices" in tables and "page_events" in tables
        user_activity_dataset.teardown(con)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "devices" not in tables

    def test_snapshots(self):
        from sqldim.application.datasets.domains.user_activity.dataset import (
            user_activity_dataset,
        )

        snaps = user_activity_dataset.snapshots()
        assert "devices" in snaps

    def test_repr(self):
        from sqldim.application.datasets.domains.user_activity.dataset import (
            user_activity_dataset,
        )

        assert "user_activity" in repr(user_activity_dataset)


# ---------------------------------------------------------------------------
# domain events __init__.py  (stub + real exports)
# ---------------------------------------------------------------------------


class TestDomainEvents:
    """Importing each events package is sufficient to cover the module body."""

    def test_devops_events_empty(self):
        from sqldim.application.datasets.domains.devops import events

        assert events.__all__ == []

    def test_dgm_events_empty(self):
        from sqldim.application.datasets.domains.dgm import events

        assert events.__all__ == []

    def test_ecommerce_events_exports(self):
        from sqldim.application.datasets.domains.ecommerce import events

        assert "CustomerBulkCancelEvent" in events.__all__
        assert "CustomerAddressChangedEvent" in events.__all__
        assert "ProductStockOutEvent" in events.__all__

    def test_enterprise_events_empty(self):
        from sqldim.application.datasets.domains.enterprise import events

        assert events.__all__ == []

    def test_fintech_events_empty(self):
        from sqldim.application.datasets.domains.fintech import events

        assert events.__all__ == []

    def test_hierarchy_events_empty(self):
        from sqldim.application.datasets.domains.hierarchy import events

        assert events.__all__ == []

    def test_media_events_empty(self):
        from sqldim.application.datasets.domains.media import events

        assert events.__all__ == []

    def test_nba_analytics_events_empty(self):
        from sqldim.application.datasets.domains.nba_analytics import events

        assert events.__all__ == []

    def test_saas_growth_events_exports(self):
        from sqldim.application.datasets.domains.saas_growth import events

        assert "UserPlanUpgradedEvent" in events.__all__
        assert "UserChurnedEvent" in events.__all__

    def test_supply_chain_events_empty(self):
        from sqldim.application.datasets.domains.supply_chain import events

        assert events.__all__ == []

    def test_user_activity_events_empty(self):
        from sqldim.application.datasets.domains.user_activity import events

        assert events.__all__ == []
