"""
Tests for LazySCDMetadataProcessor.process_stream() — streaming path.

Coverage targets
----------------
sqldim/processors/_lazy_metadata.py
  • _register_source_from_sql()          — lines 418-448 (including empty meta_cols branch)
  • _update_local_hashes_after_batch()   — lines 450-465 (INSERT + UPDATE branches)
  • process_stream()                     — lines 467-574

All tests use an in-memory DuckDB connection and a local StubStreamSource.
"""
from __future__ import annotations
import json
import duckdb

from sqldim.core.kimball.dimensions.scd.processors._lazy_metadata import LazySCDMetadataProcessor
from sqldim.sources.stream import StreamSourceAdapter, StreamResult


# ---------------------------------------------------------------------------
# StubStreamSource — yields pre-registered view/table names
# ---------------------------------------------------------------------------

class StubStreamSource:
    def __init__(self, views: list[str]):
        self._views = views
        self._idx = 0
        self.committed: list = []

    def stream(self, con, batch_size=1_000_000):
        for v in self._views:
            self._idx += 1
            yield f"SELECT * FROM {v}"

    def commit(self, offset) -> None:
        self.committed.append(offset)

    def checkpoint(self) -> int:
        return self._idx


# ---------------------------------------------------------------------------
# InMemorySink — same helper as test_lazy_metadata.py
# ---------------------------------------------------------------------------

class InMemorySink:
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


def _make_proc(con, nk_cols, meta_cols=None) -> LazySCDMetadataProcessor:
    sink = InMemorySink()
    return LazySCDMetadataProcessor(
        natural_key=nk_cols,
        metadata_columns=meta_cols or [],
        sink=sink,
        con=con,
    )


def _seed_source_table(con, view_name: str, rows: list[dict]) -> None:
    select_rows = " UNION ALL ".join(
        "SELECT " + ", ".join(
            f"'{v}' AS {k}" if isinstance(v, str) else f"{v} AS {k}"
            for k, v in row.items()
        )
        for row in rows
    )
    con.execute(f"CREATE OR REPLACE TABLE {view_name} AS {select_rows}")


# ---------------------------------------------------------------------------
# TestRegisterSourceFromSql
# ---------------------------------------------------------------------------

class TestRegisterSourceFromSql:

    def test_builds_incoming_table_with_hash(self):
        con = duckdb.connect()
        _seed_source_table(con, "src_tab", [
            {"entity_id": "E1", "name": "Alice", "score": "10"},
        ])
        proc = _make_proc(con, ["entity_id"], ["name", "score"])
        proc._register_source_from_sql("SELECT * FROM src_tab")
        cols = {r[0] for r in con.execute("DESCRIBE incoming").fetchall()}
        assert "entity_id" in cols
        assert "_row_hash" in cols

    def test_empty_meta_cols_uses_empty_json(self):
        """When _meta_cols is empty, metadata payload should be '{}'."""
        con = duckdb.connect()
        _seed_source_table(con, "src_nk_only", [
            {"entity_id": "E1"},
        ])
        proc = _make_proc(con, ["entity_id"], meta_cols=[])
        proc._register_source_from_sql("SELECT * FROM src_nk_only")
        row = con.execute("SELECT _metadata FROM incoming").fetchone()
        assert row is not None
        meta = json.loads(row[0])
        assert meta == {}

    def test_null_nk_rows_excluded(self):
        """Rows where the NK is NULL must be filtered out."""
        con = duckdb.connect()
        con.execute("""
            CREATE OR REPLACE TABLE src_nullnk AS
            SELECT 'E1' AS entity_id, 'Alice' AS name
            UNION ALL
            SELECT NULL AS entity_id, 'Bob' AS name
        """)
        proc = _make_proc(con, ["entity_id"], ["name"])
        proc._register_source_from_sql("SELECT * FROM src_nullnk")
        n = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
        assert n == 1


# ---------------------------------------------------------------------------
# TestUpdateLocalHashesAfterBatch
# ---------------------------------------------------------------------------

class TestUpdateLocalHashesAfterBatch:

    def test_insert_branch_adds_new_rows(self):
        con = duckdb.connect()
        # Manually create current_hashes and classified as process_stream would
        con.execute("""
            CREATE OR REPLACE TABLE current_hashes (
                entity_id VARCHAR, _row_hash VARCHAR
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE classified AS
            SELECT 'E1' AS entity_id, 'h1' AS _row_hash, 'new' AS _scd_class
        """)
        proc = _make_proc(con, ["entity_id"])
        proc._update_local_hashes_after_batch()
        rows = con.execute("SELECT * FROM current_hashes").fetchall()
        assert len(rows) == 1
        assert rows[0] == ("E1", "h1")

    def test_update_branch_refreshes_hash(self):
        con = duckdb.connect()
        con.execute("""
            CREATE OR REPLACE TABLE current_hashes (
                entity_id VARCHAR, _row_hash VARCHAR
            )
        """)
        con.execute("INSERT INTO current_hashes VALUES ('E2', 'old_hash')")
        con.execute("""
            CREATE OR REPLACE TABLE classified AS
            SELECT 'E2' AS entity_id, 'new_hash' AS _row_hash, 'changed' AS _scd_class
        """)
        proc = _make_proc(con, ["entity_id"])
        proc._update_local_hashes_after_batch()
        rows = con.execute("SELECT _row_hash FROM current_hashes WHERE entity_id='E2'").fetchall()
        assert rows[0][0] == "new_hash"


# ---------------------------------------------------------------------------
# TestProcessStream
# ---------------------------------------------------------------------------

class TestProcessStreamSingleBatch:

    def test_single_batch_all_new_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_ent", ["entity_id"])
        _seed_source_table(con, "src_b1", [
            {"entity_id": "E1", "name": "Alice"},
            {"entity_id": "E2", "name": "Bob"},
        ])
        proc = _make_proc(con, ["entity_id"], ["name"])
        result = proc.process_stream(StubStreamSource(["src_b1"]), "dim_ent")
        assert result.inserted == 2
        assert result.versioned == 0
        assert result.batches_processed == 1
        assert result.batches_failed == 0

    def test_single_batch_accumulates_all_result_fields(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_e2", ["entity_id"])
        _seed_source_table(con, "src_single", [
            {"entity_id": "X", "status": "active"},
        ])
        proc = _make_proc(con, ["entity_id"], ["status"])
        result = proc.process_stream(StubStreamSource(["src_single"]), "dim_e2")
        assert isinstance(result, StreamResult)
        assert result.batches_processed == 1


class TestProcessStreamMultiBatch:

    def test_second_batch_unchanged_rows(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_m1", ["entity_id"])
        # Same data in both batches → all unchanged in second
        _seed_source_table(con, "m1_b1", [{"entity_id": "E1", "v": "hello"}])
        _seed_source_table(con, "m1_b2", [{"entity_id": "E1", "v": "hello"}])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(StubStreamSource(["m1_b1", "m1_b2"]), "dim_m1")
        assert result.inserted == 1
        assert result.unchanged == 1
        assert result.batches_processed == 2

    def test_second_batch_changed_row_creates_new_version(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_m2", ["entity_id"])
        _seed_source_table(con, "m2_b1", [{"entity_id": "E1", "v": "old"}])
        _seed_source_table(con, "m2_b2", [{"entity_id": "E1", "v": "new"}])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(StubStreamSource(["m2_b1", "m2_b2"]), "dim_m2")
        assert result.inserted == 1
        assert result.versioned == 1
        # Two rows exist: original (expired) + new version
        total = con.execute("SELECT count(*) FROM dim_m2").fetchone()[0]
        assert total == 2

    def test_cross_batch_change_detection_via_local_hashes(self):
        """
        b1: insert E1 with v='A'
        b2: E1 changes to v='B' → versioned
        b3: E1 unchanged at v='B' → unchanged (proves _update_local_hashes worked)
        """
        con = duckdb.connect()
        _make_dim_table(con, "dim_cb", ["entity_id"])
        _seed_source_table(con, "cb_b1", [{"entity_id": "E1", "v": "A"}])
        _seed_source_table(con, "cb_b2", [{"entity_id": "E1", "v": "B"}])
        _seed_source_table(con, "cb_b3", [{"entity_id": "E1", "v": "B"}])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(StubStreamSource(["cb_b1", "cb_b2", "cb_b3"]), "dim_cb")
        assert result.inserted == 1
        assert result.versioned == 1
        assert result.unchanged == 1
        assert result.batches_processed == 3

    def test_max_batches_limits_processing(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_mb", ["entity_id"])
        _seed_source_table(con, "mb_b1", [{"entity_id": "A", "v": "1"}])
        _seed_source_table(con, "mb_b2", [{"entity_id": "B", "v": "2"}])
        _seed_source_table(con, "mb_b3", [{"entity_id": "C", "v": "3"}])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(
            StubStreamSource(["mb_b1", "mb_b2", "mb_b3"]), "dim_mb",
            max_batches=2,
        )
        assert result.batches_processed == 2

    def test_on_batch_callback_called_per_successful_batch(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_cb2", ["entity_id"])
        _seed_source_table(con, "cb2_b1", [{"entity_id": "A", "v": "x"}])
        _seed_source_table(con, "cb2_b2", [{"entity_id": "B", "v": "y"}])
        calls = []
        proc = _make_proc(con, ["entity_id"], ["v"])
        proc.process_stream(
            StubStreamSource(["cb2_b1", "cb2_b2"]), "dim_cb2",
            on_batch=lambda i, r: calls.append((i, r.inserted)),
        )
        assert len(calls) == 2
        assert calls[0] == (0, 1)
        assert calls[1] == (1, 1)

    def test_commit_called_after_successful_batch(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_commit", ["entity_id"])
        _seed_source_table(con, "comm_b1", [{"entity_id": "A", "v": "z"}])
        source = StubStreamSource(["comm_b1"])
        proc = _make_proc(con, ["entity_id"], ["v"])
        proc.process_stream(source, "dim_commit")
        assert len(source.committed) == 1


class TestProcessStreamFailurePaths:

    def test_failed_batch_increments_batches_failed(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_fail", ["entity_id"])
        # "nonexistent_view" doesn't exist → register_source_from_sql fails
        source = StubStreamSource(["nonexistent_view"])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(source, "dim_fail")
        assert result.batches_failed == 1
        assert result.batches_processed == 0

    def test_failed_batch_does_not_halt_later_batches(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_fail2", ["entity_id"])
        _seed_source_table(con, "fb_good1", [{"entity_id": "A", "v": "1"}])
        _seed_source_table(con, "fb_good2", [{"entity_id": "B", "v": "2"}])
        # Interleave a missing view between two good ones
        source = StubStreamSource(["fb_good1", "no_such_view", "fb_good2"])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(source, "dim_fail2")
        assert result.batches_failed == 1
        assert result.batches_processed == 2

    def test_commit_not_called_on_failed_batch(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_nocommit", ["entity_id"])
        source = StubStreamSource(["does_not_exist"])
        proc = _make_proc(con, ["entity_id"], ["v"])
        proc.process_stream(source, "dim_nocommit")
        assert len(source.committed) == 0


class TestProcessStreamNowParameter:

    def test_custom_now_string_used_in_valid_from(self):
        con = duckdb.connect()
        _make_dim_table(con, "dim_now", ["entity_id"])
        _seed_source_table(con, "now_b1", [{"entity_id": "A", "v": "1"}])
        proc = _make_proc(con, ["entity_id"], ["v"])
        result = proc.process_stream(
            StubStreamSource(["now_b1"]), "dim_now",
            now="2099-01-01T00:00:00+00:00",
        )
        assert result.inserted == 1
        row = con.execute(
            "SELECT valid_from AT TIME ZONE 'UTC' FROM dim_now WHERE entity_id='A'"
        ).fetchone()
        assert row[0].year == 2099


class TestProcessStreamEmptyMetaCols:

    def test_no_meta_cols_inserts_empty_json(self):
        """When metadata_columns=[] every row gets metadata='{}'."""
        con = duckdb.connect()
        _make_dim_table(con, "dim_nk", ["entity_id"])
        _seed_source_table(con, "nk_b1", [{"entity_id": "Z"}])
        proc = _make_proc(con, ["entity_id"], meta_cols=[])
        result = proc.process_stream(StubStreamSource(["nk_b1"]), "dim_nk")
        assert result.inserted == 1
        row = con.execute("SELECT metadata FROM dim_nk WHERE entity_id='Z'").fetchone()
        meta = json.loads(row[0])
        assert meta == {}

    def test_no_meta_cols_cross_batch_unchanged(self):
        """Two batches with same NK and no meta cols → second batch unchanged."""
        con = duckdb.connect()
        _make_dim_table(con, "dim_nk2", ["entity_id"])
        _seed_source_table(con, "nk2_b1", [{"entity_id": "Z1"}])
        _seed_source_table(con, "nk2_b2", [{"entity_id": "Z1"}])
        proc = _make_proc(con, ["entity_id"], meta_cols=[])
        result = proc.process_stream(StubStreamSource(["nk2_b1", "nk2_b2"]), "dim_nk2")
        assert result.inserted == 1
        assert result.unchanged == 1
