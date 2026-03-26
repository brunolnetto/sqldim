"""tests/test_dlt_source.py

Tests for sqldim.sources.dlt_source — DltSource and _DatasetSource.

dlt is NOT installed in the test environment; resource-mode tests inject a
fake ``dlt`` module via ``sys.modules`` so that ``_require_dlt()`` succeeds.
Dataset-mode tests use a real temporary DuckDB file and require no mocking.
"""

import contextlib
import os
import sys
import tempfile
from unittest.mock import MagicMock

import duckdb
import pytest

from sqldim.sources.base import SourceAdapter


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_dlt(monkeypatch):
    """Inject a fake ``dlt`` module so DltSource can be used without dlt installed."""
    mod = MagicMock(name="dlt")
    monkeypatch.setitem(sys.modules, "dlt", mod)
    # Also make sure any stale cached import inside dlt_source is refreshed
    # by removing the source module from the cache so it re-imports cleanly.
    monkeypatch.delitem(sys.modules, "sqldim.sources.dlt_source", raising=False)
    return mod


@pytest.fixture()
def tmp_duckdb():
    """Yield (path, con) for a temporary DuckDB file with a staging schema."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        path = f.name
    os.unlink(path)  # DuckDB needs to create the file itself
    setup_con = duckdb.connect(path)
    setup_con.execute("CREATE SCHEMA staging")
    setup_con.execute("CREATE TABLE staging.accounts (id INTEGER, name VARCHAR)")
    setup_con.execute("INSERT INTO staging.accounts VALUES (1, 'Acme'), (2, 'Beta')")
    setup_con.close()
    yield path
    os.unlink(path)


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    def test_dlt_source_is_source_adapter(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "accounts"
        src = DltSource(resource)
        assert isinstance(src, SourceAdapter)

    def test_dataset_source_is_source_adapter(self):
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource("/tmp/pipe.duckdb", "accounts", "staging")
        assert isinstance(src, SourceAdapter)

    def test_dlt_source_exported_from_sources_package(self, fake_dlt):
        from sqldim.sources import DltSource  # noqa: F401  (import must not raise)

    def test_dataset_source_exported_from_sources_package(self):
        from sqldim.sources import _DatasetSource  # noqa: F401


# ---------------------------------------------------------------------------
# DltSource constructor
# ---------------------------------------------------------------------------


class TestDltSourceConstructor:
    def test_default_pipeline_and_dataset_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "accounts"
        src = DltSource(resource)
        assert src._pipeline_name == "sqldim_staging"
        assert src._dataset_name == "sqldim_staging"

    def test_custom_pipeline_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "products"
        src = DltSource(resource, pipeline_name="shopify")
        assert src._pipeline_name == "shopify"

    def test_dataset_defaults_to_pipeline_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "contacts"
        src = DltSource(resource, pipeline_name="salesforce")
        assert src._dataset_name == "salesforce"

    def test_explicit_dataset_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "products"
        src = DltSource(resource, pipeline_name="shopify", dataset_name="shopify_raw")
        assert src._dataset_name == "shopify_raw"

    def test_resource_stored(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "orders"
        src = DltSource(resource, pipeline_name="shop")
        assert src._resource is resource

    def test_requires_dlt_at_construction_time(self, monkeypatch):
        """Construction fails with a clear message when dlt is absent."""
        monkeypatch.setitem(sys.modules, "dlt", None)
        monkeypatch.delitem(sys.modules, "sqldim.sources.dlt_source", raising=False)
        from sqldim.sources.dlt_source import DltSource

        with pytest.raises(ImportError, match="pip install dlt"):
            DltSource(MagicMock())


# ---------------------------------------------------------------------------
# DltSource.as_sql() — resource mode
# ---------------------------------------------------------------------------


class TestDltSourceAsSql:
    def test_runs_pipeline_with_correct_args(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "accounts"
        mock_pipeline = MagicMock()
        fake_dlt.pipeline.return_value = mock_pipeline

        src = DltSource(resource, pipeline_name="crm", dataset_name="crm_staging")
        con = MagicMock()
        src.as_sql(con)

        fake_dlt.pipeline.assert_called_once_with(
            pipeline_name="crm",
            destination="duckdb",
            dataset_name="crm_staging",
            credentials=con,
        )
        mock_pipeline.run.assert_called_once_with(resource)

    def test_returns_qualified_table_reference(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "accounts"
        fake_dlt.pipeline.return_value = MagicMock()

        src = DltSource(resource, pipeline_name="crm", dataset_name="crm_staging")
        sql = src.as_sql(MagicMock())
        assert sql == "SELECT * FROM crm_staging.accounts"

    def test_default_dataset_in_sql(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "orders"
        fake_dlt.pipeline.return_value = MagicMock()

        src = DltSource(resource)  # dataset_name defaults to "sqldim_staging"
        sql = src.as_sql(MagicMock())
        assert sql == "SELECT * FROM sqldim_staging.orders"

    def test_falls_back_to_dunder_name_when_no_name_attr(self, fake_dlt):
        """Resource without a .name attribute falls back to __name__."""
        resource = MagicMock(spec=[])  # no .name
        resource.__name__ = "events"
        fake_dlt.pipeline.return_value = MagicMock()

        from sqldim.sources.dlt_source import DltSource

        src = DltSource(resource, pipeline_name="track")
        sql = src.as_sql(MagicMock())
        assert "events" in sql

    def test_passes_connection_as_credentials(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "contacts"
        fake_dlt.pipeline.return_value = MagicMock()

        src = DltSource(resource)
        con = MagicMock()
        src.as_sql(con)

        _, kwargs = fake_dlt.pipeline.call_args
        assert kwargs["credentials"] is con


# ---------------------------------------------------------------------------
# DltSource.from_dataset() — factory
# ---------------------------------------------------------------------------


class TestFromDataset:
    def test_returns_dataset_source(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource, _DatasetSource

        src = DltSource.from_dataset("/tmp/pipe.duckdb", "accounts", "staging")
        assert isinstance(src, _DatasetSource)

    def test_stores_path(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        src = DltSource.from_dataset("/tmp/pipe.duckdb", "accounts", "staging")
        assert src._path == "/tmp/pipe.duckdb"

    def test_stores_table(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        src = DltSource.from_dataset("/tmp/pipe.duckdb", "products", "shop_raw")
        assert src._table == "products"

    def test_stores_dataset_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        src = DltSource.from_dataset("/tmp/pipe.duckdb", "accounts", "my_ds")
        assert src._dataset_name == "my_ds"

    def test_default_dataset_name(self, fake_dlt):
        from sqldim.sources.dlt_source import DltSource

        src = DltSource.from_dataset("/tmp/pipe.duckdb", "products")
        assert src._dataset_name == "sqldim_staging"


# ---------------------------------------------------------------------------
# _DatasetSource.as_sql() — dataset mode
# ---------------------------------------------------------------------------


class TestDatasetSourceAsSql:
    def test_returns_qualified_select(self, tmp_duckdb):
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource(tmp_duckdb, "accounts", "staging")
        con = duckdb.connect()
        sql = src.as_sql(con)
        # SQL must reference the attached alias, the schema, and the table
        alias = os.path.splitext(os.path.basename(tmp_duckdb))[0]
        assert alias in sql
        assert "staging" in sql
        assert "accounts" in sql
        con.close()

    def test_sql_actually_returns_rows(self, tmp_duckdb):
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource(tmp_duckdb, "accounts", "staging")
        con = duckdb.connect()
        sql = src.as_sql(con)
        rows = con.execute(sql).fetchall()
        assert len(rows) == 2
        assert (1, "Acme") in rows
        assert (2, "Beta") in rows
        con.close()

    def test_attach_idempotent(self, tmp_duckdb):
        """Calling as_sql() twice must not raise on the second ATTACH."""
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource(tmp_duckdb, "accounts", "staging")
        con = duckdb.connect()
        src.as_sql(con)  # first call — ATTACHes
        src.as_sql(con)  # second call — already attached, should not raise
        con.close()

    def test_alias_derived_from_filename(self, tmp_duckdb):
        """Alias == filename without extension."""
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource(tmp_duckdb, "accounts", "staging")
        con = duckdb.connect()
        sql = src.as_sql(con)
        expected_alias = os.path.splitext(os.path.basename(tmp_duckdb))[0]
        assert sql.startswith(f"SELECT * FROM {expected_alias}.")
        con.close()

    def test_multiple_datasets_same_connection(self):
        """Two different DuckDB files can be attached to the same connection."""
        from sqldim.sources.dlt_source import _DatasetSource

        paths = []
        cons_to_close = []
        try:
            for i in range(2):
                with tempfile.NamedTemporaryFile(
                    suffix=".duckdb", delete=False, prefix=f"ds{i}_"
                ) as f:
                    path = f.name
                os.unlink(path)  # DuckDB must create the file itself
                paths.append(path)
                setup = duckdb.connect(path)
                setup.execute("CREATE SCHEMA staging")
                setup.execute("CREATE TABLE staging.t (n INTEGER)")
                setup.execute(f"INSERT INTO staging.t VALUES ({i})")
                setup.close()

            con = duckdb.connect()
            cons_to_close.append(con)
            srcs = [_DatasetSource(p, "t", "staging") for p in paths]
            sqls = [s.as_sql(con) for s in srcs]
            results = [con.execute(sql).fetchall()[0][0] for sql in sqls]
            assert set(results) == {0, 1}
            con.close()
        finally:
            for p in paths:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(p)


# ---------------------------------------------------------------------------
# coerce_source passthrough
# ---------------------------------------------------------------------------


class TestCoerceSourcePassthrough:
    def test_dlt_source_passed_through_unchanged(self, fake_dlt):
        from sqldim.sources import coerce_source
        from sqldim.sources.dlt_source import DltSource

        resource = MagicMock()
        resource.name = "accounts"
        src = DltSource(resource)
        assert coerce_source(src) is src

    def test_dataset_source_passed_through_unchanged(self):
        from sqldim.sources import coerce_source
        from sqldim.sources.dlt_source import _DatasetSource

        src = _DatasetSource("/tmp/pipe.duckdb", "accounts", "staging")
        assert coerce_source(src) is src
