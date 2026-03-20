"""
Tests for LazySCDMetadataProcessor — metadata-bag SCD-2.

All tests use an in-memory DuckDB connection and InMemorySink so no real
PostgreSQL or Parquet storage is required.

Coverage targets
----------------
sqldim/processors/_lazy_metadata.py  →  ~100%
  • _safe()            — identifier validation
  • _as_subquery()     — SQL wrapping helper
  • __init__           — constructor, sorted meta_cols
  • process()          — full cycle: new / changed / unchanged
  • _register_source() — builds `incoming` VIEW
  • _register_current_hashes() — builds `current_hashes` TABLE
  • _classify()        — builds `classified` VIEW
  • _write_new()       — inserts brand-new rows
  • _write_changed()   — expires old + inserts with metadata_diff
"""
from __future__ import annotations

import json
import pytest
import duckdb

from sqldim.core.kimball.dimensions.scd.processors.lazy.metadata._lazy_metadata import (
    LazySCDMetadataProcessor,
    _safe,
    _as_subquery,
)
from sqldim.core.kimball.dimensions.scd.handler import SCDResult


# ---------------------------------------------------------------------------
# InMemorySink — supports write_named, current_state_sql, close_versions
# ---------------------------------------------------------------------------

class InMemorySink:
    """DuckDB in-memory sink for tests — all state lives in *con*."""

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write_named(self, con, view_name, table_name, columns, batch_size=100_000):
        cols = ", ".join(columns)
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if n == 0:
            return 0
        try:
            con.execute(
                f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM {view_name}"
            )
        except Exception:
            con.execute(
                f"CREATE TABLE {table_name} AS SELECT {cols} FROM {view_name}"
            )
        return n

    def close_versions(self, con, table_name, nk_cols, nk_view, valid_to):
        if isinstance(nk_cols, list):
            pk = nk_cols[0]
        else:
            pk = nk_cols
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE is_current = TRUE
               AND ({pk}) IN (SELECT {pk} FROM {nk_view})
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dim_table(con, table_name: str, nk_cols: list[str]) -> None:
    """Create a SCD-2 metadata-bag dimension table in *con*."""
    nk_defs = " ".join(f"{c} VARCHAR," for c in nk_cols)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {nk_defs}
            valid_from    TIMESTAMPTZ,
            valid_to      TIMESTAMPTZ,
            is_current    BOOLEAN,
            metadata      JSON,
            metadata_diff JSON,
            row_hash      VARCHAR
        )
    """)


def _make_proc(con, nk_cols, meta_cols) -> LazySCDMetadataProcessor:
    sink = InMemorySink()
    return LazySCDMetadataProcessor(
        natural_key=nk_cols,
        metadata_columns=meta_cols,
        sink=sink,
        con=con,
    )


def _register_source_view(con, view_name: str, rows: list[dict]) -> None:
    """Register *rows* as a DuckDB VIEW for use as the streaming source."""
    select_rows = " UNION ALL ".join(
        "SELECT " + ", ".join(
            f"'{v}' AS {k}" if isinstance(v, str) else f"{v} AS {k}"
            for k, v in row.items()
        )
        for row in rows
    )
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {select_rows}")


# ---------------------------------------------------------------------------
# _safe() — identifier validation
# ---------------------------------------------------------------------------

class TestSafeIdentifier:
    def test_valid_lowercase_name(self):
        assert _safe("cnpj_basico") == "cnpj_basico"

    def test_valid_uppercase_name(self):
        assert _safe("CompanyName") == "CompanyName"

    def test_valid_name_with_digits(self):
        assert _safe("col1") == "col1"

    def test_leading_underscore_allowed(self):
        assert _safe("_private") == "_private"

    def test_leading_digit_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _safe("1bad")

    def test_hyphen_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _safe("bad-name")

    def test_space_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _safe("bad name")

    def test_semicolon_raises_injection_guard(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _safe("name; DROP TABLE")

    def test_dot_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _safe("schema.table")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _safe("")


# ---------------------------------------------------------------------------
# _as_subquery() helper
# ---------------------------------------------------------------------------

class TestAsSubquery:
    def test_select_is_wrapped(self):
        result = _as_subquery("SELECT 1 AS x")
        assert result.startswith("(")
        assert result.endswith(")")

    def test_table_name_not_wrapped(self):
        result = _as_subquery("my_table")
        assert result == "my_table"

    def test_with_clause_not_wrapped(self):
        """_as_subquery only wraps SELECT queries; WITH clauses are returned as-is."""
        result = _as_subquery("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert not result.startswith("(")
        assert result == "WITH cte AS (SELECT 1) SELECT * FROM cte"


# ---------------------------------------------------------------------------
# __init__ — constructor behaviour
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_single_string_nk(self):
        con = duckdb.connect()
        sink = InMemorySink()
        proc = LazySCDMetadataProcessor(
            natural_key="cnpj_basico",
            metadata_columns=["razao_social", "capital_social"],
            sink=sink,
            con=con,
        )
        assert proc._nk_cols == ["cnpj_basico"]

    def test_list_nk(self):
        con = duckdb.connect()
        proc = _make_proc(con, ["id_a", "id_b"], ["col1"])
        assert proc._nk_cols == ["id_a", "id_b"]

    def test_meta_cols_sorted(self):
        con = duckdb.connect()
        proc = _make_proc(con, ["id"], ["z_col", "a_col", "m_col"])
        assert proc._meta_cols == sorted(["z_col", "a_col", "m_col"])

    def test_creates_own_connection_when_none(self):
        sink = InMemorySink()
        proc = LazySCDMetadataProcessor(
            natural_key="id",
            metadata_columns=["val"],
            sink=sink,
        )
        assert proc._con is not None

    def test_invalid_nk_col_raises(self):
        con = duckdb.connect()
        sink = InMemorySink()
        with pytest.raises(ValueError):
            LazySCDMetadataProcessor(
                natural_key="bad-col",
                metadata_columns=["val"],
                sink=sink,
                con=con,
            )

    def test_invalid_meta_col_raises(self):
        con = duckdb.connect()
        sink = InMemorySink()
        with pytest.raises(ValueError):
            LazySCDMetadataProcessor(
                natural_key="id",
                metadata_columns=["good_col", "bad col"],
                sink=sink,
                con=con,
            )


# ---------------------------------------------------------------------------
# process() — full cycle
# ---------------------------------------------------------------------------

class TestProcessAllNew:
    """All incoming rows are new (empty table)."""

    def test_single_new_row_inserted(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [{"emp_id": "E1", "dept": "RD", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("src", "dim_emp")
        assert result.inserted == 1
        assert result.versioned == 0
        assert result.unchanged == 0

    def test_multiple_new_rows_inserted(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [
            {"emp_id": "E1", "dept": "Eng", "grade": "L4"},
            {"emp_id": "E2", "dept": "Mkt", "grade": "L3"},
            {"emp_id": "E3", "dept": "Fin", "grade": "L5"},
        ])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("src", "dim_emp")
        assert result.inserted == 3
        assert result.versioned == 0

    def test_metadata_diff_is_null_for_new_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp")
        row = con.execute("SELECT metadata_diff FROM dim_emp WHERE emp_id = 'E1'").fetchone()
        assert row[0] is None

    def test_is_current_true_for_new_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp")
        row = con.execute("SELECT is_current FROM dim_emp WHERE emp_id = 'E1'").fetchone()
        assert row[0] is True

    def test_row_hash_populated(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp")
        row = con.execute("SELECT row_hash FROM dim_emp WHERE emp_id = 'E1'").fetchone()
        assert row[0] is not None
        assert len(row[0]) == 32  # md5 hex string

    def test_returns_scd_result(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src", [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("src", "dim_emp")
        assert isinstance(result, SCDResult)


class TestProcessChanged:
    """Rows whose metadata hash differs from the current version."""

    def _seed_initial(self, con, table_name, nk_cols, meta_cols, nk_vals, meta_vals):
        """Seed the table with one current row."""
        proc = _make_proc(con, nk_cols, meta_cols)
        rows = {**nk_vals, **meta_vals}
        _register_source_view(con, "initial_src", [rows])
        proc.process("initial_src", table_name)

    def test_changed_row_creates_new_version(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        self._seed_initial(con, "dim_emp", "emp_id", ["dept", "grade"],
                           {"emp_id": "E1"}, {"dept": "Eng", "grade": "L4"})

        _register_source_view(con, "update_src",
                              [{"emp_id": "E1", "dept": "Research", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("update_src", "dim_emp")
        assert result.versioned == 1
        assert result.inserted == 0

    def test_old_version_marked_not_current(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        self._seed_initial(con, "dim_emp", "emp_id", ["dept", "grade"],
                           {"emp_id": "E1"}, {"dept": "Eng", "grade": "L4"})

        _register_source_view(con, "update_src",
                              [{"emp_id": "E1", "dept": "Research", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("update_src", "dim_emp")

        rows = con.execute(
            "SELECT is_current FROM dim_emp WHERE emp_id='E1' ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] is False  # old version expired
        assert rows[1][0] is True   # new version current

    def test_metadata_diff_captures_old_metadata(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        self._seed_initial(con, "dim_emp", "emp_id", ["dept", "grade"],
                           {"emp_id": "E1"}, {"dept": "Eng", "grade": "L4"})

        _register_source_view(con, "update_src",
                              [{"emp_id": "E1", "dept": "Research", "grade": "L5"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("update_src", "dim_emp")

        new_row = con.execute(
            "SELECT metadata, metadata_diff FROM dim_emp WHERE emp_id='E1' AND is_current=TRUE"
        ).fetchone()
        assert new_row[1] is not None  # metadata_diff must be set
        old_meta = json.loads(new_row[1])
        assert old_meta.get("dept") == "Eng"

    def test_multiple_changed_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        for eid, dept in [("E1", "Eng"), ("E2", "Mkt"), ("E3", "Fin")]:
            _register_source_view(con, f"src_{eid}",
                                  [{"emp_id": eid, "dept": dept, "grade": "L4"}])
            proc = _make_proc(con, "emp_id", ["dept", "grade"])
            proc.process(f"src_{eid}", "dim_emp")

        # Change all three
        _register_source_view(con, "update_all", [
            {"emp_id": "E1", "dept": "X", "grade": "L4"},
            {"emp_id": "E2", "dept": "Y", "grade": "L4"},
            {"emp_id": "E3", "dept": "Z", "grade": "L4"},
        ])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("update_all", "dim_emp")
        assert result.versioned == 3


class TestProcessUnchanged:
    """Rows with matching hashes are counted as unchanged."""

    def test_unchanged_row_counted(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src",
                              [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp")
        result = proc.process("src", "dim_emp")
        assert result.unchanged == 1
        assert result.inserted == 0
        assert result.versioned == 0

    def test_unchanged_does_not_create_duplicate(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src",
                              [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp")
        proc.process("src", "dim_emp")
        count = con.execute(
            "SELECT count(*) FROM dim_emp WHERE emp_id='E1'"
        ).fetchone()[0]
        assert count == 1

    def test_mixed_new_changed_unchanged(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "initial", [
            {"emp_id": "E1", "dept": "Eng", "grade": "L4"},
            {"emp_id": "E2", "dept": "Mkt", "grade": "L3"},
        ])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("initial", "dim_emp")

        _register_source_view(con, "update", [
            {"emp_id": "E1", "dept": "Eng",       "grade": "L4"},   # unchanged
            {"emp_id": "E2", "dept": "Finance",    "grade": "L3"},   # changed
            {"emp_id": "E3", "dept": "Operations", "grade": "L2"},   # new
        ])
        result = proc.process("update", "dim_emp")
        assert result.unchanged == 1
        assert result.versioned == 1
        assert result.inserted == 1


class TestProcessNullNk:
    """Rows where NK is NULL must be silently dropped."""

    def test_null_nk_row_silently_dropped(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        con.execute("""
            CREATE OR REPLACE VIEW src_with_null AS
            SELECT NULL::VARCHAR AS emp_id, 'Eng' AS dept, 'L4' AS grade
            UNION ALL
            SELECT 'E1', 'Fin', 'L3'
        """)
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("src_with_null", "dim_emp")
        assert result.inserted == 1  # only E1, not the NULL row


class TestProcessInvalidTableName:
    def test_bad_table_name_raises(self):
        con = duckdb.connect()
        _register_source_view(con, "src",
                              [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        with pytest.raises(ValueError):
            proc.process("src", "bad-table!")


class TestProcessCompositeKey:
    """Tests with composite natural keys."""

    def test_composite_nk_new_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_order_line", ["order_id", "line_no"])
        _register_source_view(con, "src_ol", [
            {"order_id": "O1", "line_no": "1", "product": "Widget", "qty": "5"},
            {"order_id": "O1", "line_no": "2", "product": "Gadget", "qty": "3"},
        ])
        proc = _make_proc(con, ["order_id", "line_no"], ["product", "qty"])
        result = proc.process("src_ol", "dim_order_line")
        assert result.inserted == 2

    def test_composite_nk_changed_row_versioned(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_order_line", ["order_id", "line_no"])
        _register_source_view(con, "src_ol", [
            {"order_id": "O1", "line_no": "1", "product": "Widget", "qty": "5"},
        ])
        proc = _make_proc(con, ["order_id", "line_no"], ["product", "qty"])
        proc.process("src_ol", "dim_order_line")

        _register_source_view(con, "src_ol_upd", [
            {"order_id": "O1", "line_no": "1", "product": "Widget", "qty": "10"},
        ])
        proc2 = _make_proc(con, ["order_id", "line_no"], ["product", "qty"])
        result = proc2.process("src_ol_upd", "dim_order_line")
        assert result.versioned == 1

    def test_composite_nk_unchanged_row_counted(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_order_line", ["order_id", "line_no"])
        _register_source_view(con, "src_ol", [
            {"order_id": "O1", "line_no": "1", "product": "Widget", "qty": "5"},
        ])
        proc = _make_proc(con, ["order_id", "line_no"], ["product", "qty"])
        proc.process("src_ol", "dim_order_line")
        result = proc.process("src_ol", "dim_order_line")
        assert result.unchanged == 1


class TestProcessEmptyMetadataColumns:
    """Processor works when metadata_columns is empty."""

    def test_empty_metadata_columns_uses_empty_json(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_minimal", ["entity_id"])
        con.execute("""
            CREATE OR REPLACE VIEW src_min AS
            SELECT 'X1' AS entity_id
        """)
        proc = _make_proc(con, "entity_id", [])
        result = proc.process("src_min", "dim_minimal")
        assert result.inserted == 1

    def test_empty_metadata_unchanged_on_reprocess(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_minimal", ["entity_id"])
        con.execute("""
            CREATE OR REPLACE VIEW src_min AS
            SELECT 'X1' AS entity_id
        """)
        proc = _make_proc(con, "entity_id", [])
        proc.process("src_min", "dim_minimal")
        result = proc.process("src_min", "dim_minimal")
        assert result.unchanged == 1
        assert result.inserted == 0


class TestProcessNowParameter:
    """Explicit `now` parameter is used for valid_from."""

    def test_explicit_now_used_for_valid_from(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src",
                              [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        proc.process("src", "dim_emp", now="2023-06-15T12:00:00+00:00")
        row = con.execute("SELECT valid_from FROM dim_emp WHERE emp_id='E1'").fetchone()
        assert "2023-06-15" in str(row[0])

    def test_none_now_uses_current_time(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        _register_source_view(con, "src",
                              [{"emp_id": "E1", "dept": "Eng", "grade": "L4"}])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        result = proc.process("src", "dim_emp")  # now=None
        assert result.inserted == 1


class TestWriteNewCountZeroBypass:
    """_write_new returns 0 immediately when count=0."""

    def test_write_new_count_zero_returns_zero(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        assert proc._write_new("dim_emp", "2024-01-01T00:00:00+00:00", 0) == 0


class TestWriteChangedCountZeroBypass:
    """_write_changed returns 0 immediately when count=0."""

    def test_write_changed_count_zero_returns_zero(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_emp", ["emp_id"])
        con.execute(
            "CREATE OR REPLACE TABLE current_hashes (emp_id VARCHAR, _row_hash VARCHAR)"
        )
        proc = _make_proc(con, "emp_id", ["dept", "grade"])
        assert proc._write_changed("dim_emp", "2024-01-01T00:00:00+00:00", 0) == 0
