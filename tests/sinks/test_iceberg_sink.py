"""Tests for IcebergSink — all external dependencies mocked."""
import sys
import types
import pytest
import duckdb
from unittest.mock import MagicMock, patch


# ── Mock pyiceberg and pyarrow before importing IcebergSink ─────────────────

def _make_arrow_table(data: dict):
    """Build a real pyarrow Table for testing."""
    import pyarrow as pa
    arrays = {k: pa.array(v) for k, v in data.items()}
    return pa.table(arrays)


def _mock_catalog(tables: dict):
    """
    Return a mock pyiceberg catalog whose load_table() returns a stable mock
    table backed by real pyarrow data.  The same mock is returned for the
    same table name across multiple calls so assertions work correctly.
    """
    catalog = MagicMock()
    _cache: dict = {}

    def _load_table(full_name):
        table_name = full_name.split(".")[-1]
        if table_name not in _cache:
            arrow_data = tables.get(table_name)
            mock_table = MagicMock()
            mock_table.location.return_value = f"/tmp/iceberg/{table_name}"
            if arrow_data is not None:
                mock_table.scan.return_value.to_arrow.return_value = arrow_data
                mock_table.scan.return_value.count.return_value = len(arrow_data)
            else:
                mock_table.scan.return_value.count.return_value = 0
            _cache[table_name] = mock_table
        return _cache[table_name]

    catalog.load_table.side_effect = _load_table
    return catalog


# ── Fixture ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def iceberg_sink():
    """IcebergSink with pyiceberg mocked via patch."""
    from sqldim.sinks.file.iceberg import IcebergSink
    sink = IcebergSink(
        catalog_name="test",
        namespace="ns",
        catalog_config={"uri": "http://localhost:8181"},
        table_location_base="/tmp/iceberg",
    )
    return sink


# ── __init__ ─────────────────────────────────────────────────────────────────

def test_init_stores_params():
    from sqldim.sinks.file.iceberg import IcebergSink
    sink = IcebergSink("cat", "ns", {"uri": "x"}, "/tmp/base")
    assert sink._catalog_name == "cat"
    assert sink._namespace == "ns"
    assert sink._catalog_config == {"uri": "x"}
    assert sink._location_base == "/tmp/base"


# ── _table_location ───────────────────────────────────────────────────────────

def test_table_location_with_base(iceberg_sink):
    loc = iceberg_sink._table_location("dim_customer")
    assert loc == "/tmp/iceberg/dim_customer"


def test_table_location_without_base():
    """Falls back to catalog metadata when no base path given."""
    from sqldim.sinks.file.iceberg import IcebergSink
    sink = IcebergSink("cat", "ns")
    mock_cat = MagicMock()
    mock_cat.load_table.return_value.location.return_value = "/catalog/loc"
    sink._catalog = mock_cat
    loc = sink._table_location("my_table")
    assert loc == "/catalog/loc"


# ── _load_catalog ─────────────────────────────────────────────────────────────

def test_load_catalog_raises_importerror_when_pyiceberg_missing(iceberg_sink):
    with patch.dict(sys.modules, {"pyiceberg.catalog": None}):
        with pytest.raises(ImportError, match="pyiceberg"):
            iceberg_sink._load_catalog()


def test_load_catalog_calls_load_catalog(iceberg_sink):
    mock_lc = MagicMock(return_value=MagicMock())
    fake_module = types.ModuleType("pyiceberg.catalog")
    fake_module.load_catalog = mock_lc
    with patch.dict(sys.modules, {"pyiceberg.catalog": fake_module}):
        iceberg_sink._load_catalog()
    mock_lc.assert_called_once_with("test", uri="http://localhost:8181")


# ── context manager ───────────────────────────────────────────────────────────

def test_context_manager_sets_catalog_and_con(iceberg_sink):
    mock_lc = MagicMock(return_value=MagicMock())
    fake_module = types.ModuleType("pyiceberg.catalog")
    fake_module.load_catalog = mock_lc
    with patch.dict(sys.modules, {"pyiceberg.catalog": fake_module}):
        with iceberg_sink as s:
            assert s._catalog is not None
            assert s._con is not None
    assert iceberg_sink._catalog is None
    assert iceberg_sink._con is None


# ── current_state_sql ─────────────────────────────────────────────────────────

def test_current_state_sql(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    sql = iceberg_sink.current_state_sql("dim_product")
    assert "iceberg_scan" in sql
    assert "/tmp/iceberg/dim_product" in sql


# ── write ─────────────────────────────────────────────────────────────────────

def test_write_appends_arrow_batch():
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({"id": [1], "name": ["Alice"]})
    catalog = _mock_catalog({"dim_t": arrow_data})

    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    sink._con = duckdb.connect()

    sink._con.execute(
        "CREATE TABLE dim_t AS SELECT 1 AS id, 'Alice' AS name"
    )

    count = sink.write(sink._con, "dim_t", "dim_t")
    assert count == 1
    mock_table = catalog.load_table("ns.dim_t")
    mock_table.append.assert_called()


def test_write_raises_on_missing_pyarrow(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    con = duckdb.connect()
    con.execute("CREATE TABLE v AS SELECT 1 AS x")
    with patch.dict(sys.modules, {"pyarrow": None}):
        with pytest.raises(ImportError, match="pyarrow"):
            iceberg_sink.write(con, "v", "v")


# ── close_versions ────────────────────────────────────────────────────────────

def test_close_versions_updates_is_current():
    import pyarrow as pa
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({
        "nk": ["NK1", "NK2"],
        "name": ["Alice", "Bob"],
        "is_current": [True, True],
        "valid_to": pa.array([None, None], type=pa.string()),
    })
    catalog = _mock_catalog({"dim_t": arrow_data})

    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW nk_view AS SELECT 'NK1' AS nk")

    count = sink.close_versions(con, "dim_t", "nk", "nk_view", "2024-06-01")
    assert count == 1
    mock_table = catalog.load_table("ns.dim_t")
    mock_table.overwrite.assert_called()


def test_close_versions_noop_when_empty():
    import pyarrow as pa
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({
        "nk": ["NK1"],
        "is_current": [True],
        "valid_to": pa.array([None], type=pa.string()),
    })
    catalog = _mock_catalog({"dim_t": arrow_data})
    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    con = duckdb.connect()
    # nk_view with no matching keys
    con.execute("CREATE OR REPLACE VIEW nk_view AS SELECT 'ZZZZZ' AS nk")

    count = sink.close_versions(con, "dim_t", "nk", "nk_view", "2024-06-01")
    assert count == 0


def test_close_versions_raises_on_missing_pyarrow(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW nk_view AS SELECT 1 AS nk")
    with patch.dict(sys.modules, {"pyarrow.compute": None}):
        with pytest.raises(ImportError, match="pyarrow"):
            iceberg_sink.close_versions(con, "dim_t", "nk", "nk_view", "2024-01-01")


# ── update_attributes ─────────────────────────────────────────────────────────

def test_update_attributes():
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({
        "nk": ["NK1", "NK2"],
        "name": ["Old", "Bob"],
        "score": [10, 20],
    })
    catalog = _mock_catalog({"dim_t": arrow_data})
    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW upd AS SELECT 'NK1' AS nk, 'New' AS name")

    count = sink.update_attributes(con, "dim_t", "nk", "upd", ["name"])
    assert count == 1
    # overwrite must have been called on the iceberg table
    assert catalog.load_table.called


def test_update_attributes_raises_on_missing_pyarrow(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW upd AS SELECT 1 AS nk, 'v' AS name")
    with patch.dict(sys.modules, {"pyarrow": None}):
        with pytest.raises(ImportError, match="pyarrow"):
            iceberg_sink.update_attributes(con, "dim_t", "nk", "upd", ["name"])


# ── rotate_attributes ─────────────────────────────────────────────────────────

def test_rotate_attributes():
    import pyarrow as pa
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({
        "nk": ["NK1"],
        "city": ["Boston"],
        "prev_city": pa.array([None], type=pa.string()),
    })
    catalog = _mock_catalog({"dim_addr": arrow_data})
    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW rot AS SELECT 'NK1' AS nk, 'NYC' AS city")

    count = sink.rotate_attributes(
        con, "dim_addr", "nk", "rot", [("city", "prev_city")]
    )
    assert count == 1
    mock_table = catalog.load_table("ns.dim_addr")
    mock_table.overwrite.assert_called()


def test_rotate_attributes_raises_on_missing_pyarrow(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW rot AS SELECT 1 AS nk, 'x' AS c")
    with patch.dict(sys.modules, {"pyarrow": None}):
        with pytest.raises(ImportError, match="pyarrow"):
            iceberg_sink.rotate_attributes(con, "dim_t", "nk", "rot", [("c", "pc")])


# ── update_milestones ─────────────────────────────────────────────────────────

def test_update_milestones():
    import pyarrow as pa
    from sqldim.sinks.file.iceberg import IcebergSink

    arrow_data = _make_arrow_table({
        "order_id": [1, 2],
        "shipped_at": pa.array([None, None], type=pa.string()),
    })
    catalog = _mock_catalog({"fact_acc": arrow_data})
    sink = IcebergSink("cat", "ns", table_location_base="/tmp")
    sink._catalog = catalog
    con = duckdb.connect()
    con.execute(
        "CREATE OR REPLACE VIEW ms AS SELECT 1 AS order_id, '2024-01-05' AS shipped_at"
    )

    count = sink.update_milestones(
        con, "fact_acc", "order_id", "ms", ["shipped_at"]
    )
    assert count == 1
    mock_table = catalog.load_table("ns.fact_acc")
    mock_table.overwrite.assert_called()


def test_update_milestones_raises_on_missing_pyarrow(iceberg_sink):
    iceberg_sink._catalog = MagicMock()
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW ms AS SELECT 1 AS order_id, 'x' AS shipped_at")
    with patch.dict(sys.modules, {"pyarrow": None}):
        with pytest.raises(ImportError, match="pyarrow"):
            iceberg_sink.update_milestones(
                con, "fact_acc", "order_id", "ms", ["shipped_at"]
            )


# ── __enter__ INSTALL fallback ────────────────────────────────────────────────

def test_enter_falls_back_to_install_when_load_fails():
    """If 'LOAD iceberg' raises, __enter__ retries with INSTALL+LOAD."""
    import types
    from sqldim.sinks.file.iceberg import IcebergSink

    mock_lc = MagicMock(return_value=MagicMock())
    fake_module = types.ModuleType("pyiceberg.catalog")
    fake_module.load_catalog = mock_lc

    sink = IcebergSink(
        catalog_name="test",
        namespace="ns",
        catalog_config={"uri": "http://localhost:8181"},
    )

    _load_count = [0]
    _original_execute = None

    class _PatchedCon:
        """Thin wrapper that makes the first LOAD iceberg call fail."""
        def __init__(self, real_con):
            self._real = real_con

        def execute(self, sql, *args, **kwargs):
            if "LOAD iceberg" in sql and _load_count[0] == 0:
                _load_count[0] += 1
                raise Exception("Extension not found — use INSTALL first")
            return self._real.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._real, name)

    original_connect = duckdb.connect

    def patched_connect(*args, **kwargs):
        real = original_connect(*args, **kwargs)
        return _PatchedCon(real)

    with patch.dict(sys.modules, {"pyiceberg.catalog": fake_module}):
        with patch("duckdb.connect", side_effect=patched_connect):
            with sink:
                assert sink._catalog is not None
    assert sink._catalog is None
