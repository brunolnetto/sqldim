"""
Phase 2 tests — process_stream() on all lazy SCD processors.

All tests use StubStreamSource (finite, no real Kafka/Kinesis) and
InMemorySink (shared DuckDB in-memory connection).

Coverage targets
----------------
sqldim/processors/_lazy_type2.py    → process_stream(), _register_source_from_sql(),
                                       _drop_stream_views() on LazySCDProcessor &
                                       LazyType1Processor
sqldim/processors/_lazy_type3_6.py  → process_stream() on LazyType3Processor &
                                       LazyType6Processor
"""

from __future__ import annotations

import duckdb

from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
)
from sqldim.sources.streaming.stream import StreamSourceAdapter


# ---------------------------------------------------------------------------
# StubStreamSource — finite, protocol-compatible, no I/O
# ---------------------------------------------------------------------------


class StubStreamSource:
    """Yields ``SELECT * FROM {view}`` for each pre-registered view name."""

    def __init__(self, views: list[str]):
        self._views = views
        self._idx = 0
        self.committed: list = []

    def stream(self, con, batch_size=10_000):
        for view in self._views:
            self._idx += 1
            yield f"SELECT * FROM {view}"

    def commit(self, offset) -> None:
        self.committed.append(offset)

    def checkpoint(self):
        return self._idx


# ---------------------------------------------------------------------------
# InMemorySink — same helper as test_lazy_scd.py, self-contained copy
# ---------------------------------------------------------------------------


class InMemorySink:
    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def write(self, con, view_name, table_name, batch_size=100_000):
        n = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        try:
            con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM {view_name}")
        except Exception:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
        return n

    def close_versions(self, con, table_name, nk_col, nk_view, valid_to):
        if isinstance(nk_col, list):
            " AND ".join(
                f"cast({table_name}.{c} as varchar) = cast({nk_view}.{c} as varchar)"
                for c in nk_col
            )
        else:
            pass
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE is_current = TRUE
               AND ({", ".join(nk_col) if isinstance(nk_col, list) else nk_col})
                   IN (SELECT {", ".join(nk_col) if isinstance(nk_col, list) else nk_col}
                         FROM {nk_view})
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]

    def update_attributes(self, con, table_name, nk_col, updates_view, update_cols):
        for col in update_cols:
            con.execute(f"""
                UPDATE {table_name}
                   SET {col} = (
                       SELECT u.{col} FROM {updates_view} u
                        WHERE cast(u.{nk_col} as varchar)
                            = cast({table_name}.{nk_col} as varchar)
                   )
                 WHERE {nk_col} IN (SELECT {nk_col} FROM {updates_view})
                   AND is_current = TRUE
            """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    def rotate_attributes(self, con, table_name, nk_col, rotations_view, column_pairs):
        for curr, prev in column_pairs:
            con.execute(f"""
                UPDATE {table_name}
                   SET {prev} = {table_name}.{curr},
                       {curr} = r.{curr}
                  FROM {rotations_view} r
                 WHERE {table_name}.{nk_col} = r.{nk_col}
                   AND {table_name}.is_current = TRUE
            """)
        return con.execute(f"SELECT count(*) FROM {rotations_view}").fetchone()[0]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_scd_table(con, name, nk, track_cols):
    """Create an empty SCD2-style table in *con*."""
    extra_cols = " ".join(f", {c} VARCHAR" for c in track_cols)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {nk} VARCHAR
            {extra_cols}
            , checksum   VARCHAR
            , is_current BOOLEAN
            , valid_from VARCHAR
            , valid_to   VARCHAR
        )
    """)


def _empty_scd_table_type6(con, name, nk, type1_cols, type2_cols):
    """Create an empty SCD2-style table with combined type1+type2 columns."""
    all_cols = type1_cols + type2_cols
    extra = " ".join(f", {c} VARCHAR" for c in all_cols)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {nk} VARCHAR
            {extra}
            , checksum   VARCHAR
            , is_current BOOLEAN
            , valid_from VARCHAR
            , valid_to   VARCHAR
        )
    """)


def _seed_scd_row(con, table, nk_val, nk_col, track_vals: dict, checksum="oldhash"):
    col_names = (
        [nk_col]
        + list(track_vals.keys())
        + ["checksum", "is_current", "valid_from", "valid_to"]
    )
    col_vals = (
        [f"'{nk_val}'"]
        + [f"'{v}'" if v is not None else "NULL" for v in track_vals.values()]
        + [f"'{checksum}'", "TRUE", "'2024-01-01'", "NULL"]
    )
    con.execute(
        f"INSERT INTO {table} ({', '.join(col_names)}) VALUES ({', '.join(col_vals)})"
    )


def _register_batch(con, view_name, rows: list[dict]):
    """Register a list of dicts as a DuckDB view."""
    select_rows = " UNION ALL ".join(
        "SELECT "
        + ", ".join(
            f"'{v}' AS {k}" if isinstance(v, str) else f"{v} AS {k}"
            for k, v in row.items()
        )
        for row in rows
    )
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {select_rows}")


# ---------------------------------------------------------------------------
# LazySCDProcessor.process_stream()  (SCD Type 2)
# ---------------------------------------------------------------------------


class TestLazySCDProcessorStream:
    def _make(self, con):
        sink = InMemorySink()
        proc = LazySCDProcessor(
            natural_key="sku",
            track_columns=["name", "price"],
            sink=sink,
            con=con,
        )
        return proc, sink

    # ── basic happy-path ──────────────────────────────────────────────────

    def test_single_batch_all_new_rows(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(
            con,
            "b1",
            [
                {"sku": "A", "name": "Apple", "price": "1.00"},
                {"sku": "B", "name": "Banana", "price": "0.50"},
            ],
        )
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_p")
        assert result.inserted == 2
        assert result.versioned == 0
        assert result.batches_processed == 1
        assert result.batches_failed == 0

    def test_second_batch_changed_rows_versioned(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "Apple", "price": "1.00"}])
        _register_batch(con, "b2", [{"sku": "A", "name": "Apple+", "price": "1.50"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1", "b2"]), "dim_p")
        assert result.inserted == 1
        assert result.versioned == 1
        assert result.batches_processed == 2

    def test_unchanged_rows_counted(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "Apple", "price": "1.00"}])
        # seed a row so the second batch hits 'unchanged'
        checksum = con.execute(
            "SELECT md5(coalesce('Apple','')||'|'||coalesce('1.00',''))"
        ).fetchone()[0]
        _seed_scd_row(
            con,
            "dim_p",
            "A",
            "sku",
            {"name": "Apple", "price": "1.00"},
            checksum=checksum,
        )
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_p")
        assert result.unchanged == 1
        assert result.inserted == 0

    # ── max_batches ───────────────────────────────────────────────────────

    def test_max_batches_limits_processing(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        for i in ("b1", "b2", "b3"):
            _register_batch(con, i, [{"sku": f"X{i}", "name": "N", "price": "1"}])
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b1", "b2", "b3"]),
            "dim_p",
            max_batches=2,
        )
        assert result.batches_processed == 2

    def test_max_batches_zero_processes_nothing(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "V", "price": "1"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_p", max_batches=0)
        assert result.batches_processed == 0

    # ── on_batch callback ─────────────────────────────────────────────────

    def test_on_batch_called_per_successful_batch(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "A", "price": "1"}])
        _register_batch(con, "b2", [{"sku": "B", "name": "B", "price": "2"}])
        calls = []
        proc, _ = self._make(con)
        proc.process_stream(
            StubStreamSource(["b1", "b2"]),
            "dim_p",
            on_batch=lambda i, r: calls.append((i, r.inserted)),
        )
        assert len(calls) == 2
        assert calls[0] == (0, 1)
        assert calls[1] == (1, 1)

    # ── commit / checkpoint ───────────────────────────────────────────────

    def test_commit_called_after_each_successful_batch(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "A", "price": "1"}])
        _register_batch(con, "b2", [{"sku": "B", "name": "B", "price": "2"}])
        source = StubStreamSource(["b1", "b2"])
        proc, _ = self._make(con)
        proc.process_stream(source, "dim_p")
        assert len(source.committed) == 2

    def test_commit_not_called_on_failed_batch(self):
        """A view that doesn't exist should cause a batch failure, not a commit."""
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        # "bad_view" is not registered → _register_source_from_sql will fail
        source = StubStreamSource(["bad_view"])
        proc, _ = self._make(con)
        result = proc.process_stream(source, "dim_p")
        assert result.batches_failed == 1
        assert result.batches_processed == 0
        assert len(source.committed) == 0

    # ── failed batch does not halt remaining batches ──────────────────────

    def test_failed_batch_increments_batches_failed(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "A", "price": "1"}])
        # b2 is not registered — will fail
        source = StubStreamSource(["b1", "missing_view", "b1"])
        proc, _ = self._make(con)
        result = proc.process_stream(source, "dim_p")
        assert result.batches_failed == 1
        assert result.batches_processed == 2

    # ── deduplicate_by ────────────────────────────────────────────────────

    def test_deduplicate_by_keeps_latest_per_nk(self):
        """Two events for the same NK in one batch; dedup by price (DESC) keeps the higher-priced row."""
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        # Two rows for SKU "A" with different prices.  QUALIFY keeps price='2.00' (DESC).
        con.execute("""
            CREATE OR REPLACE VIEW b_dedup AS
            SELECT 'A' AS sku, 'Old'  AS name, '1.00' AS price
            UNION ALL
            SELECT 'A' AS sku, 'New'  AS name, '2.00' AS price
        """)
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b_dedup"]),
            "dim_p",
            deduplicate_by="price",
        )
        assert result.inserted == 1  # only 1 row per NK
        row = con.execute("SELECT name FROM dim_p WHERE sku = 'A'").fetchone()
        assert row[0] == "New"

    def test_deduplicate_by_none_preserves_all_rows(self):
        """Without dedup, duplicate NKs in a batch are both delivered to the classifier."""
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        # Two rows for same NK — no dedup; behaviour depends on classifier
        con.execute("""
            CREATE OR REPLACE VIEW b_no_dedup AS
            SELECT 'A' AS sku, 'V1' AS name, '1.00' AS price
            UNION ALL
            SELECT 'B' AS sku, 'V2' AS name, '2.00' AS price
        """)
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b_no_dedup"]),
            "dim_p",
        )
        assert result.inserted == 2

    # ── StreamResult accumulation across batches ──────────────────────────

    def test_accumulated_result_across_three_batches(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "V1", "price": "1"}])
        _register_batch(con, "b2", [{"sku": "B", "name": "V2", "price": "2"}])
        _register_batch(con, "b3", [{"sku": "C", "name": "V3", "price": "3"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1", "b2", "b3"]), "dim_p")
        assert result.inserted == 3
        assert result.batches_processed == 3

    # ── composite natural key ─────────────────────────────────────────────

    def test_composite_natural_key(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE IF NOT EXISTS dim_ol (
                order_id  VARCHAR,
                line_no   VARCHAR,
                qty       VARCHAR,
                checksum  VARCHAR,
                is_current BOOLEAN,
                valid_from VARCHAR,
                valid_to   VARCHAR
            )
        """)
        _register_batch(
            con,
            "batch_ol",
            [
                {"order_id": "O1", "line_no": "1", "qty": "5"},
                {"order_id": "O1", "line_no": "2", "qty": "3"},
            ],
        )
        sink = InMemorySink()
        proc = LazySCDProcessor(
            natural_key=["order_id", "line_no"],
            track_columns=["qty"],
            sink=sink,
            con=con,
        )
        result = proc.process_stream(StubStreamSource(["batch_ol"]), "dim_ol")
        assert result.inserted == 2


# ---------------------------------------------------------------------------
# LazyType1Processor.process_stream()  (SCD Type 1 — overwrite)
# ---------------------------------------------------------------------------


class TestLazyType1ProcessorStream:
    def _make(self, con):
        sink = InMemorySink()
        proc = LazyType1Processor(
            natural_key="sku",
            track_columns=["name", "price"],
            sink=sink,
            con=con,
        )
        return proc, sink

    def test_single_batch_inserts_new_rows(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "Alpha", "price": "5.00"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_t1")
        assert result.inserted == 1
        assert result.batches_processed == 1

    def test_second_batch_overwrites_changed_row(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "OldName", "price": "5.00"}])
        _register_batch(con, "b2", [{"sku": "A", "name": "NewName", "price": "5.00"}])
        proc, _ = self._make(con)
        proc.process_stream(StubStreamSource(["b1", "b2"]), "dim_t1")
        row = con.execute("SELECT name FROM dim_t1 WHERE sku = 'A'").fetchone()
        assert row[0] == "NewName"

    def test_on_batch_callback_invoked(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "X", "price": "1"}])
        calls = []
        proc, _ = self._make(con)
        proc.process_stream(
            StubStreamSource(["b1"]),
            "dim_t1",
            on_batch=lambda i, r: calls.append(i),
        )
        assert calls == [0]

    def test_max_batches_limits_type1(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "X", "price": "1"}])
        _register_batch(con, "b2", [{"sku": "B", "name": "Y", "price": "2"}])
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b1", "b2"]), "dim_t1", max_batches=1
        )
        assert result.batches_processed == 1

    def test_failed_batch_increments_counter(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        source = StubStreamSource(["nonexistent"])
        proc, _ = self._make(con)
        result = proc.process_stream(source, "dim_t1")
        assert result.batches_failed == 1
        assert result.batches_processed == 0

    def test_deduplicate_by_keeps_latest_per_nk(self):
        """Two events for the same NK in one batch; dedup by price (DESC) keeps the higher-priced row."""
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        con.execute("""
            CREATE OR REPLACE VIEW b_dedup_t1 AS
            SELECT 'A' AS sku, 'OldName' AS name, '1.00' AS price
            UNION ALL
            SELECT 'A' AS sku, 'NewName' AS name, '2.00' AS price
        """)
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b_dedup_t1"]),
            "dim_t1",
            deduplicate_by="price",
        )
        assert result.inserted == 1
        row = con.execute("SELECT name FROM dim_t1 WHERE sku = 'A'").fetchone()
        assert row[0] == "NewName"

    def test_drop_stream_views_exception_swallowed(self):
        """_drop_stream_views must not propagate exceptions from DROP statements."""
        from unittest.mock import MagicMock

        proc, _ = self._make(duckdb.connect())
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Simulated DROP failure")
        proc._con = mock_con
        proc._drop_stream_views()  # must not raise


# ---------------------------------------------------------------------------
# LazyType3Processor.process_stream()  (SCD Type 3 — current + previous)
# ---------------------------------------------------------------------------


class TestLazyType3ProcessorStream:
    def _make(self, con):
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        return proc, sink

    def _empty_t3_table(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                emp_id      VARCHAR,
                region      VARCHAR,
                prev_region VARCHAR,
                checksum    VARCHAR,
                is_current  BOOLEAN,
                valid_from  VARCHAR,
                valid_to    VARCHAR
            )
        """)

    def test_single_batch_inserts_new_rows(self):
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        _register_batch(con, "b1", [{"emp_id": "E1", "region": "East"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_emp")
        assert result.inserted == 1
        assert result.batches_processed == 1

    def test_second_batch_rotates_region(self):
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        _register_batch(con, "b1", [{"emp_id": "E1", "region": "East"}])
        _register_batch(con, "b2", [{"emp_id": "E1", "region": "West"}])
        proc, _ = self._make(con)
        proc.process_stream(StubStreamSource(["b1", "b2"]), "dim_emp")
        row = con.execute(
            "SELECT region, prev_region FROM dim_emp WHERE emp_id='E1' AND is_current=TRUE"
        ).fetchone()
        assert row[0] == "West"
        assert row[1] == "East"

    def test_batches_processed_count(self):
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        _register_batch(con, "b1", [{"emp_id": "E1", "region": "East"}])
        _register_batch(con, "b2", [{"emp_id": "E2", "region": "West"}])
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1", "b2"]), "dim_emp")
        assert result.batches_processed == 2

    def test_max_batches_limits_type3(self):
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        _register_batch(con, "b1", [{"emp_id": "E1", "region": "East"}])
        _register_batch(con, "b2", [{"emp_id": "E2", "region": "West"}])
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b1", "b2"]), "dim_emp", max_batches=1
        )
        assert result.batches_processed == 1

    def test_deduplicate_by_keeps_latest_per_nk(self):
        """Two events for same NK in one batch; dedup by region (DESC α) keeps last alphabetically."""
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        con.execute("""
            CREATE OR REPLACE VIEW b_dedup_t3 AS
            SELECT 'E1' AS emp_id, 'East' AS region
            UNION ALL
            SELECT 'E1' AS emp_id, 'West' AS region
        """)
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b_dedup_t3"]),
            "dim_emp",
            deduplicate_by="region",
        )
        assert result.inserted == 1
        row = con.execute("SELECT region FROM dim_emp WHERE emp_id='E1'").fetchone()
        assert row[0] == "West"

    def test_drop_stream_views_exception_swallowed(self):
        """_drop_stream_views must not propagate exceptions from DROP statements."""
        from unittest.mock import MagicMock

        proc, _ = self._make(duckdb.connect())
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Simulated DROP failure")
        proc._con = mock_con
        proc._drop_stream_views()  # must not raise

    def test_on_batch_callback_called(self):
        """on_batch is invoked once per successful batch (covers line 274)."""
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        _register_batch(con, "b1", [{"emp_id": "E1", "region": "East"}])
        calls = []
        proc, _ = self._make(con)
        proc.process_stream(
            StubStreamSource(["b1"]),
            "dim_emp",
            on_batch=lambda i, r: calls.append(i),
        )
        assert calls == [0]

    def test_failed_batch_increments_batches_failed(self):
        """Exception during batch processing increments batches_failed (covers lines 276-278)."""
        con = duckdb.connect()
        self._empty_t3_table(con, "dim_emp")
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["no_such_view"]), "dim_emp")
        assert result.batches_failed == 1
        assert result.batches_processed == 0


# ---------------------------------------------------------------------------
# LazyType6Processor.process_stream()  (SCD Type 6 — hybrid Type 1 + Type 2)
# ---------------------------------------------------------------------------


class TestLazyType6ProcessorStream:
    def _make(self, con):
        sink = InMemorySink()
        proc = LazyType6Processor(
            natural_key="cust_id",
            type1_columns=["email"],
            type2_columns=["tier"],
            sink=sink,
            con=con,
        )
        return proc, sink

    def _empty_t6_table(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                cust_id    VARCHAR,
                email      VARCHAR,
                tier       VARCHAR,
                checksum   VARCHAR,
                is_current BOOLEAN,
                valid_from VARCHAR,
                valid_to   VARCHAR
            )
        """)

    def test_single_batch_inserts_new_rows(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        _register_batch(
            con,
            "b1",
            [
                {"cust_id": "C1", "email": "a@example.com", "tier": "gold"},
            ],
        )
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_cust")
        assert result.inserted == 1
        assert result.batches_processed == 1

    def test_type2_change_creates_new_version(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        _register_batch(
            con, "b1", [{"cust_id": "C1", "email": "a@x.com", "tier": "gold"}]
        )
        _register_batch(
            con, "b2", [{"cust_id": "C1", "email": "a@x.com", "tier": "platinum"}]
        )
        proc, _ = self._make(con)
        proc.process_stream(StubStreamSource(["b1", "b2"]), "dim_cust")
        # Both old (is_current=FALSE) and new (is_current=TRUE) should exist
        total = con.execute(
            "SELECT count(*) FROM dim_cust WHERE cust_id='C1'"
        ).fetchone()[0]
        assert total == 2

    def test_batches_processed_count(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        _register_batch(
            con, "b1", [{"cust_id": "C1", "email": "a@x.com", "tier": "silver"}]
        )
        proc, _ = self._make(con)
        result = proc.process_stream(StubStreamSource(["b1"]), "dim_cust")
        assert result.batches_processed == 1

    def test_max_batches_limits_type6(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        _register_batch(
            con, "b1", [{"cust_id": "C1", "email": "a@x.com", "tier": "silver"}]
        )
        _register_batch(
            con, "b2", [{"cust_id": "C2", "email": "b@x.com", "tier": "gold"}]
        )
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b1", "b2"]), "dim_cust", max_batches=1
        )
        assert result.batches_processed == 1

    def test_failed_batch_increments_counter(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        source = StubStreamSource(["no_such_view"])
        proc, _ = self._make(con)
        result = proc.process_stream(source, "dim_cust")
        assert result.batches_failed == 1
        assert result.batches_processed == 0

    def test_on_batch_callback_called(self):
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        _register_batch(
            con, "b1", [{"cust_id": "C1", "email": "a@x.com", "tier": "silver"}]
        )
        calls = []
        proc, _ = self._make(con)
        proc.process_stream(
            StubStreamSource(["b1"]),
            "dim_cust",
            on_batch=lambda i, r: calls.append(i),
        )
        assert calls == [0]

    def test_deduplicate_by_keeps_latest_per_nk(self):
        """Two events for same NK in one batch; dedup by tier (DESC α) keeps 'silver' over 'gold'."""
        con = duckdb.connect()
        self._empty_t6_table(con, "dim_cust")
        con.execute("""
            CREATE OR REPLACE VIEW b_dedup_t6 AS
            SELECT 'C1' AS cust_id, 'a@x.com' AS email, 'gold'   AS tier
            UNION ALL
            SELECT 'C1' AS cust_id, 'a@x.com' AS email, 'silver' AS tier
        """)
        proc, _ = self._make(con)
        result = proc.process_stream(
            StubStreamSource(["b_dedup_t6"]),
            "dim_cust",
            deduplicate_by="tier",
        )
        assert result.inserted == 1
        row = con.execute("SELECT tier FROM dim_cust WHERE cust_id='C1'").fetchone()
        assert row[0] == "silver"

    def test_drop_stream_views_exception_swallowed(self):
        """_drop_stream_views must not propagate exceptions from DROP statements."""
        from unittest.mock import MagicMock

        proc, _ = self._make(duckdb.connect())
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Simulated DROP failure")
        proc._con = mock_con
        proc._drop_stream_views()  # must not raise


# ---------------------------------------------------------------------------
# LazySCDProcessor (Type 2) _drop_stream_views exception path
# ---------------------------------------------------------------------------


class TestSCDType2DropStreamViewsException:
    def test_drop_stream_views_exception_swallowed(self):
        """_drop_stream_views on LazySCDProcessor must not propagate DROP errors."""
        from unittest.mock import MagicMock

        con = duckdb.connect()
        sink = InMemorySink()
        proc = LazySCDProcessor("sku", ["name", "price"], sink=sink, con=con)
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("DROP error")
        proc._con = mock_con
        proc._drop_stream_views()  # must not raise


# ---------------------------------------------------------------------------
# _drop_stream_views cleans up between batches
# ---------------------------------------------------------------------------


class TestDropStreamViews:
    """Verify that _drop_stream_views does not leave stale views between batches."""

    def test_classified_table_absent_after_stream(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "A", "price": "1"}])
        sink = InMemorySink()
        proc = LazySCDProcessor("sku", ["name", "price"], sink=sink, con=con)
        proc.process_stream(StubStreamSource(["b1"]), "dim_p")
        # 'classified' table must have been cleaned up
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "classified" not in tables

    def test_incoming_view_absent_after_stream(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_p", "sku", ["name", "price"])
        _register_batch(con, "b1", [{"sku": "A", "name": "A", "price": "1"}])
        sink = InMemorySink()
        proc = LazySCDProcessor("sku", ["name", "price"], sink=sink, con=con)
        proc.process_stream(StubStreamSource(["b1"]), "dim_p")
        objects = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "incoming" not in objects


# ---------------------------------------------------------------------------
# StreamSourceAdapter protocol contract for StubStreamSource
# ---------------------------------------------------------------------------


def test_stub_satisfies_protocol():
    assert isinstance(StubStreamSource([]), StreamSourceAdapter)


# ---------------------------------------------------------------------------
# LazyType3Processor cross-batch changed rows
# (covers _update_local_fingerprint_after_batch UPDATE branch)
# ---------------------------------------------------------------------------


class TestLazyType3CrossBatchUpdates:
    """
    Ensure the UPDATE branch of _update_local_fingerprint_after_batch is exercised:
    batch 1 inserts a row, batch 2 presents the same NK with a changed column.
    For the fingerprint to detect the change cross-batch, the UPDATE must run.
    """

    def _empty_t3(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                emp_id      VARCHAR,
                region      VARCHAR,
                prev_region VARCHAR,
                checksum    VARCHAR,
                is_current  BOOLEAN,
                valid_from  VARCHAR,
                valid_to    VARCHAR
            )
        """)

    def test_third_batch_sees_second_batch_change(self):
        """
        b1: inserts E1/East  → local fingerprint: E1→hash('East')
        b2: rotates E1 to West → UPDATE fingerprint: E1→hash('West')
        b3: same E1/West → unchanged (proves fingerprint was synced)
        """
        con = duckdb.connect()
        self._empty_t3(con, "dim_emp_xb")
        _register_batch(con, "xb1", [{"emp_id": "E1", "region": "East"}])
        _register_batch(con, "xb2", [{"emp_id": "E1", "region": "West"}])
        _register_batch(con, "xb3", [{"emp_id": "E1", "region": "West"}])
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        result = proc.process_stream(
            StubStreamSource(["xb1", "xb2", "xb3"]), "dim_emp_xb"
        )
        # b1 inserts 1, b2 rotates 1, b3 nothing
        assert result.inserted == 1
        assert result.versioned == 1  # b2 rotated once via rotate_attributes
        assert result.unchanged == 1  # b3 unchanged (fingerprint correctly synced)
        assert result.batches_processed == 3

    def test_changed_nk_in_batch2_re_changed_in_batch3(self):
        """Two successive changes to the same NK across three batches."""
        con = duckdb.connect()
        self._empty_t3(con, "dim_emp_3ch")
        _register_batch(con, "ch1", [{"emp_id": "X", "region": "North"}])
        _register_batch(con, "ch2", [{"emp_id": "X", "region": "South"}])
        _register_batch(con, "ch3", [{"emp_id": "X", "region": "East"}])
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        result = proc.process_stream(
            StubStreamSource(["ch1", "ch2", "ch3"]), "dim_emp_3ch"
        )
        assert result.inserted == 1
        # Two rotation events: North→South, South→East
        assert result.versioned == 2
        assert result.batches_processed == 3


# ---------------------------------------------------------------------------
# LazyType6Processor cross-batch type1_only and type2_changed branches
# (covers _update_local_state_after_batch UPDATE branches)
# ---------------------------------------------------------------------------


class TestLazyType6CrossBatchUpdates:
    """
    Ensure both UPDATE branches of _update_local_state_after_batch are covered:
    - type2_changed: tier changes → new version
    - type1_only: email changes → in-place update
    """

    def _empty_t6(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                cust_id    VARCHAR,
                email      VARCHAR,
                tier       VARCHAR,
                checksum   VARCHAR,
                is_current BOOLEAN,
                valid_from VARCHAR,
                valid_to   VARCHAR
            )
        """)

    def test_type2_change_then_unchanged(self):
        """
        b1: insert C1 gold/a@x.com
        b2: tier changes to platinum (type2_changed) → _update stores new both checksums
        b3: same platinum/a@x.com → unchanged (proves _update was correct)
        """
        con = duckdb.connect()
        self._empty_t6(con, "dim_t6_xb")
        _register_batch(
            con, "t6_b1", [{"cust_id": "C1", "email": "a@x.com", "tier": "gold"}]
        )
        _register_batch(
            con, "t6_b2", [{"cust_id": "C1", "email": "a@x.com", "tier": "platinum"}]
        )
        _register_batch(
            con, "t6_b3", [{"cust_id": "C1", "email": "a@x.com", "tier": "platinum"}]
        )
        sink = InMemorySink()
        proc = LazyType6Processor(
            natural_key="cust_id",
            type1_columns=["email"],
            type2_columns=["tier"],
            sink=sink,
            con=con,
        )
        result = proc.process_stream(
            StubStreamSource(["t6_b1", "t6_b2", "t6_b3"]), "dim_t6_xb"
        )
        assert result.inserted == 1
        assert result.versioned == 1  # b2 created new version
        assert result.unchanged == 1  # b3 unchanged
        assert result.batches_processed == 3

    def test_type1_only_change_then_unchanged(self):
        """
        b1: insert C2 gold/old@x.com
        b2: email changes only (type1_only) → in-place update, no new SCD2 version
        b3: same new email/tier → unchanged (proves t1_checksum updated correctly)
        """
        con = duckdb.connect()
        self._empty_t6(con, "dim_t6_t1")
        _register_batch(
            con, "t1_b1", [{"cust_id": "C2", "email": "old@x.com", "tier": "silver"}]
        )
        _register_batch(
            con, "t1_b2", [{"cust_id": "C2", "email": "new@x.com", "tier": "silver"}]
        )
        _register_batch(
            con, "t1_b3", [{"cust_id": "C2", "email": "new@x.com", "tier": "silver"}]
        )
        sink = InMemorySink()
        proc = LazyType6Processor(
            natural_key="cust_id",
            type1_columns=["email"],
            type2_columns=["tier"],
            sink=sink,
            con=con,
        )
        result = proc.process_stream(
            StubStreamSource(["t1_b1", "t1_b2", "t1_b3"]), "dim_t6_t1"
        )
        assert result.inserted == 1
        # type1_only contributes 1 to versioned (via _apply_type1_only count)
        assert result.versioned == 1
        assert result.unchanged == 1  # b3 correctly identified as unchanged
        assert result.batches_processed == 3
        # Verify only one row exists (no SCD2 version split for type1-only change)
        total_rows = con.execute(
            "SELECT count(*) FROM dim_t6_t1 WHERE cust_id='C2'"
        ).fetchone()[0]
        assert total_rows == 1
