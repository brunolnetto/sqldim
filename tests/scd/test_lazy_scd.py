"""
Tests for the lazy SCD processors and LazySKResolver in sqldim.core.processors.

Coverage targets
----------------
sqldim/narwhals/scd_engine.py   44% → ~75%
sqldim/narwhals/sk_resolver.py  63% → ~85%
"""
from __future__ import annotations
import duckdb

from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
    LazySCDProcessor,
    LazyType1Processor,
    LazyType3Processor,
    LazyType6Processor,
)
from sqldim.core.kimball.dimensions.scd.processors.sk_resolver import LazySKResolver


# ---------------------------------------------------------------------------
# Shared in-memory sink  (same helper as test_lazy_loaders.py, duplicated to
# keep tests self-contained)
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
        con.execute(f"""
            UPDATE {table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
               AND is_current = TRUE
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

def _empty_scd_table(con: duckdb.DuckDBPyConnection, name: str,
                     nk: str, track_cols: list[str]) -> None:
    """Create an empty SCD2-style table in *con*."""
    cols = ", ".join([f"{nk} VARCHAR"] +
                     [f"{c} VARCHAR" for c in track_cols] +
                     ["checksum VARCHAR", "is_current BOOLEAN",
                      "valid_from VARCHAR", "valid_to VARCHAR"])
    con.execute(f"CREATE TABLE IF NOT EXISTS {name} ({cols})")


def _seed_scd_row(con, table, nk_val, nk_col, track_vals: dict,
                  checksum="oldhash") -> None:
    col_names = [nk_col] + list(track_vals.keys()) + [
        "checksum", "is_current", "valid_from", "valid_to"
    ]
    col_vals = (
        [f"'{nk_val}'"] +
        [f"'{v}'" if v is not None else "NULL" for v in track_vals.values()] +
        [f"'{checksum}'", "TRUE", "'2024-01-01'", "NULL"]
    )
    con.execute(
        f"INSERT INTO {table} ({', '.join(col_names)}) "
        f"VALUES ({', '.join(col_vals)})"
    )


# ---------------------------------------------------------------------------
# LazySCDProcessor  (SCD Type 2)
# ---------------------------------------------------------------------------

class TestLazySCDProcessor:
    def _make_processor(self, con):
        sink = InMemorySink()
        return LazySCDProcessor(
            natural_key="sku",
            track_columns=["name", "price"],
            sink=sink,
            con=con,
        ), sink

    def test_new_rows_inserted(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_product", "sku", ["name", "price"])
        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("incoming_src", "dim_product")
        assert result.inserted == 1

    def test_unchanged_rows_counted(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_product", "sku", ["name", "price"])
        # seed a row with checksum that matches incoming
        con.execute("""
            CREATE OR REPLACE VIEW check_src AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price
        """)
        # compute what the checksum would be
        checksum = con.execute(
            "SELECT md5(coalesce(cast('Widget' as varchar),'')"
            " || '|' || coalesce(cast('9.99' as varchar),''))"
        ).fetchone()[0]
        _seed_scd_row(con, "dim_product", "SKU1", "sku",
                      {"name": "Widget", "price": "9.99"}, checksum=checksum)

        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("incoming_src", "dim_product")
        assert result.unchanged == 1
        assert result.inserted == 0

    def test_changed_rows_versioned(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_product", "sku", ["name", "price"])
        _seed_scd_row(con, "dim_product", "SKU1", "sku",
                      {"name": "OldWidget", "price": "9.99"}, checksum="xxx")

        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU1' AS sku, 'NewWidget' AS name, '9.99' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("incoming_src", "dim_product")
        assert result.versioned >= 1
        # old row should now be closed
        old_row = con.execute(
            "SELECT is_current FROM dim_product WHERE checksum = 'xxx'"
        ).fetchone()
        assert old_row[0] is False

    def test_new_row_has_scd_metadata(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_product", "sku", ["name", "price"])
        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU2' AS sku, 'Gadget' AS name, '19.99' AS price
        """)
        proc, _ = self._make_processor(con)
        proc.process("incoming_src", "dim_product")
        row = con.execute(
            "SELECT is_current, valid_from, valid_to, checksum "
            "FROM dim_product WHERE sku = 'SKU2'"
        ).fetchone()
        assert row[0] is True
        assert row[1] is not None
        assert row[2] is None
        assert row[3] is not None


# ---------------------------------------------------------------------------
# LazyType1Processor  (SCD Type 1 — overwrite)
# ---------------------------------------------------------------------------

class TestLazyType1Processor:
    def _make_processor(self, con):
        sink = InMemorySink()
        return LazyType1Processor(
            natural_key="sku",
            track_columns=["name", "price"],
            sink=sink,
            con=con,
        ), sink

    def test_new_rows_inserted(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'A' AS sku, 'Alpha' AS name, '5.00' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t1")
        assert result.inserted == 1

    def test_changed_rows_updated_in_place(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        _seed_scd_row(con, "dim_t1", "A", "sku",
                      {"name": "OldName", "price": "5.00"}, checksum="old")

        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'A' AS sku, 'NewName' AS name, '5.00' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t1")
        assert result.versioned >= 1
        # row should still be current (Type 1 = overwrite, no new version)
        row = con.execute(
            "SELECT name, is_current FROM dim_t1 WHERE sku = 'A'"
        ).fetchone()
        assert row[0] == "NewName"
        assert row[1] is True

    def test_unchanged_rows_counted(self):
        con = duckdb.connect()
        _empty_scd_table(con, "dim_t1", "sku", ["name", "price"])
        # Build the checksum that matches 'Beta'|'7.00' (sorted track cols: name, price)
        checksum = con.execute(
            "SELECT md5(coalesce('Beta','')||'|'||coalesce('7.00',''))"
        ).fetchone()[0]
        _seed_scd_row(con, "dim_t1", "B", "sku",
                      {"name": "Beta", "price": "7.00"}, checksum=checksum)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'B' AS sku, 'Beta' AS name, '7.00' AS price
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t1")
        assert result.unchanged == 1


# ---------------------------------------------------------------------------
# LazyType3Processor  (SCD Type 3 — current + previous)
# ---------------------------------------------------------------------------

class TestLazyType3Processor:
    def _make_scd3_table(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                emp_id    VARCHAR,
                region    VARCHAR,
                prev_region VARCHAR,
                checksum  VARCHAR,
                is_current BOOLEAN,
                valid_from VARCHAR,
                valid_to   VARCHAR
            )
        """)

    def test_new_rows_inserted_with_null_prev(self):
        con = duckdb.connect()
        self._make_scd3_table(con, "dim_scd3")
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'E1' AS emp_id, 'East' AS region
        """)
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        result = proc.process("src", "dim_scd3")
        assert result.inserted == 1
        row = con.execute(
            "SELECT prev_region FROM dim_scd3 WHERE emp_id = 'E1'"
        ).fetchone()
        assert row[0] is None

    def test_changed_row_rotates_to_previous(self):
        con = duckdb.connect()
        self._make_scd3_table(con, "dim_scd3")
        # Build checksum for 'East'
        checksum = con.execute("SELECT md5('East')").fetchone()[0]
        con.execute("""
            INSERT INTO dim_scd3
            VALUES ('E1', 'East', NULL, '""" + checksum + """', TRUE, '2024-01-01', NULL)
        """)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'E1' AS emp_id, 'West' AS region
        """)
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        result = proc.process("src", "dim_scd3")
        assert result.versioned >= 1
        row = con.execute(
            "SELECT region, prev_region FROM dim_scd3 WHERE emp_id = 'E1'"
        ).fetchone()
        assert row[0] == "West"
        assert row[1] == "East"

    def test_unchanged_counted(self):
        con = duckdb.connect()
        self._make_scd3_table(con, "dim_scd3")
        checksum = con.execute("SELECT md5('North')").fetchone()[0]
        con.execute(f"""
            INSERT INTO dim_scd3
            VALUES ('E2', 'North', NULL, '{checksum}', TRUE, '2024-01-01', NULL)
        """)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'E2' AS emp_id, 'North' AS region
        """)
        sink = InMemorySink()
        proc = LazyType3Processor(
            natural_key="emp_id",
            column_pairs=[("region", "prev_region")],
            sink=sink,
            con=con,
        )
        result = proc.process("src", "dim_scd3")
        assert result.unchanged == 1


# ---------------------------------------------------------------------------
# LazyType6Processor  (hybrid Type 1 + Type 2)
# ---------------------------------------------------------------------------

class TestLazyType6Processor:
    def _make_scd6_table(self, con, name):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                product_id  VARCHAR,
                category    VARCHAR,
                name        VARCHAR,
                checksum    VARCHAR,
                is_current  BOOLEAN,
                valid_from  VARCHAR,
                valid_to    VARCHAR
            )
        """)

    def _make_processor(self, con):
        sink = InMemorySink()
        return LazyType6Processor(
            natural_key="product_id",
            type1_columns=["category"],
            type2_columns=["name"],
            sink=sink,
            con=con,
        ), sink

    def test_new_rows_inserted(self):
        con = duckdb.connect()
        self._make_scd6_table(con, "dim_t6")
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'P1' AS product_id, 'Electronics' AS category, 'Phone' AS name
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t6")
        assert result.inserted == 1

    def test_type2_change_creates_new_version(self):
        con = duckdb.connect()
        self._make_scd6_table(con, "dim_t6")
        # Seed row — type2 hash = md5('Phone')
        t2_hash = con.execute("SELECT md5('Phone')").fetchone()[0]
        con.execute("SELECT md5('Electronics')").fetchone()[0]
        con.execute(f"""
            INSERT INTO dim_t6
            VALUES ('P1', 'Electronics', 'Phone', '{t2_hash}', TRUE, '2024-01-01', NULL)
        """)
        # Incoming: name changed (type2), category same (type1)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'P1' AS product_id, 'Electronics' AS category, 'Tablet' AS name
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t6")
        assert result.versioned >= 1
        # old row should be closed
        old = con.execute(
            "SELECT is_current FROM dim_t6 WHERE name = 'Phone'"
        ).fetchone()
        assert old[0] is False

    def test_type1_only_change_updates_in_place(self):
        con = duckdb.connect()
        self._make_scd6_table(con, "dim_t6")
        # Build type2 hash for 'Laptop' (unchanged); type1 hash for 'Electronics'
        t2_hash = con.execute("SELECT md5('Laptop')").fetchone()[0]
        con.execute(f"""
            INSERT INTO dim_t6
            VALUES ('P2', 'Electronics', 'Laptop', '{t2_hash}', TRUE, '2024-01-01', NULL)
        """)
        # Incoming: category changed (type1), name unchanged (type2)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'P2' AS product_id, 'Computers' AS category, 'Laptop' AS name
        """)
        proc, _ = self._make_processor(con)
        proc.process("src", "dim_t6")
        # Type1-only change → no new version, just update
        row = con.execute(
            "SELECT category, is_current FROM dim_t6 WHERE product_id = 'P2'"
        ).fetchone()
        assert row[0] == "Computers"
        assert row[1] is True

    def test_unchanged_counted(self):
        con = duckdb.connect()
        self._make_scd6_table(con, "dim_t6")
        # Build both hashes matching incoming
        t2_hash = con.execute("SELECT md5('Camera')").fetchone()[0]
        con.execute("SELECT md5('Optics')").fetchone()[0]
        con.execute(f"""
            INSERT INTO dim_t6
            VALUES ('P3', 'Optics', 'Camera', '{t2_hash}', TRUE, '2024-01-01', NULL)
        """)
        con.execute("""
            CREATE OR REPLACE VIEW src AS
            SELECT 'P3' AS product_id, 'Optics' AS category, 'Camera' AS name
        """)
        proc, _ = self._make_processor(con)
        result = proc.process("src", "dim_t6")
        assert result.unchanged == 1


# ---------------------------------------------------------------------------
# LazySKResolver
# ---------------------------------------------------------------------------

class TestLazySKResolver:
    def _setup(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create dim_customer table with id surrogate key."""
        con.execute("""
            CREATE TABLE dim_customer (
                customer_code VARCHAR,
                id            INTEGER,
                name          VARCHAR,
                is_current    BOOLEAN
            )
        """)
        con.execute("""
            INSERT INTO dim_customer VALUES
            ('C001', 1, 'Alice', TRUE),
            ('C002', 2, 'Bob',   TRUE)
        """)
        con.execute("""
            CREATE OR REPLACE VIEW pending_facts AS
            SELECT 'C001' AS customer_code, 100.0 AS amount
            UNION ALL
            SELECT 'C002', 200.0
            UNION ALL
            SELECT 'C999', 50.0   -- no matching dim member
        """)

    def test_resolve_creates_output_view(self):
        con = duckdb.connect()
        self._setup(con)
        sink = InMemorySink()
        resolver = LazySKResolver(sink, con=con)
        out = resolver.resolve(
            fact_view="pending_facts",
            dim_table_name="dim_customer",
            natural_key_col="customer_code",
            surrogate_key_col="customer_sk",
            output_view="facts_resolved",
        )
        assert out == "facts_resolved"
        count = con.execute("SELECT count(*) FROM facts_resolved").fetchone()[0]
        assert count == 3

    def test_resolve_maps_natural_key_to_surrogate(self):
        con = duckdb.connect()
        self._setup(con)
        sink = InMemorySink()
        resolver = LazySKResolver(sink, con=con)
        resolver.resolve(
            fact_view="pending_facts",
            dim_table_name="dim_customer",
            natural_key_col="customer_code",
            surrogate_key_col="customer_sk",
            output_view="facts_resolved",
        )
        row = con.execute(
            "SELECT customer_sk FROM facts_resolved WHERE customer_code = 'C001'"
        ).fetchone()
        assert row[0] == 1

    def test_resolve_unmatched_key_is_null(self):
        con = duckdb.connect()
        self._setup(con)
        sink = InMemorySink()
        resolver = LazySKResolver(sink, con=con)
        resolver.resolve(
            fact_view="pending_facts",
            dim_table_name="dim_customer",
            natural_key_col="customer_code",
            surrogate_key_col="customer_sk",
            output_view="facts_resolved",
        )
        row = con.execute(
            "SELECT customer_sk FROM facts_resolved WHERE customer_code = 'C999'"
        ).fetchone()
        assert row[0] is None

    def test_resolve_all_chains_multiple_fks(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE dim_product (
                product_code VARCHAR,
                id           INTEGER,
                is_current   BOOLEAN
            )
        """)
        con.execute("INSERT INTO dim_product VALUES ('PRD1', 10, TRUE)")
        con.execute("""
            CREATE TABLE dim_customer (
                customer_code VARCHAR,
                id            INTEGER,
                is_current    BOOLEAN
            )
        """)
        con.execute("INSERT INTO dim_customer VALUES ('C001', 1, TRUE)")
        con.execute("""
            CREATE OR REPLACE VIEW raw_facts AS
            SELECT 'C001' AS customer_code, 'PRD1' AS product_code, 500.0 AS revenue
        """)
        sink = InMemorySink()
        resolver = LazySKResolver(sink, con=con)
        out = resolver.resolve_all(
            fact_view="raw_facts",
            key_map={
                "customer_id": ("dim_customer", "customer_code", "customer_id"),
                "product_id":  ("dim_product",  "product_code",  "product_id"),
            },
            output_view="final_facts",
        )
        assert out == "final_facts"
        row = con.execute(
            "SELECT customer_id, product_id FROM final_facts"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 10

    def test_resolve_all_uses_default_output_view(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE dim_customer (
                customer_code VARCHAR, id INTEGER, is_current BOOLEAN
            )
        """)
        con.execute("INSERT INTO dim_customer VALUES ('X', 99, TRUE)")
        con.execute("""
            CREATE OR REPLACE VIEW raw_facts AS SELECT 'X' AS customer_code, 1.0 AS val
        """)
        sink = InMemorySink()
        resolver = LazySKResolver(sink, con=con)
        out = resolver.resolve_all(
            fact_view="raw_facts",
            key_map={
                "customer_id": ("dim_customer", "customer_code", "customer_id"),
            },
        )
        # default output view name
        assert "fact" in out or "sk" in out or out == "fact_with_all_sk"


# ---------------------------------------------------------------------------
# LazySCDProcessor — is_inferred and reconnect branches
# ---------------------------------------------------------------------------

class TestLazySCDInferredPaths:
    """Coverage for lines 152, 157, 212, 244-250 in _lazy_type2.py."""

    def _make_proc(self, con, emitter=None):
        sink = InMemorySink()
        return LazySCDProcessor(
            natural_key="sku",
            track_columns=["name", "price"],
            sink=sink,
            con=con,
            lineage_emitter=emitter,
        )

    def test_classify_detects_reconnect_when_current_checksums_has_is_inferred(self):
        """Lines 152, 157: has_inferred=True path when current_checksums.is_inferred exists."""
        con = duckdb.connect()
        # Manually set up incoming with checksum
        con.execute("""
            CREATE OR REPLACE VIEW incoming AS
            SELECT 'SKU1' AS sku, 'Widget' AS name, '9.99' AS price,
                   'newhash' AS _checksum
        """)
        # Manually set up current_checksums WITH is_inferred column
        con.execute("""
            CREATE OR REPLACE TABLE current_checksums (
                sku VARCHAR, _checksum VARCHAR, is_inferred BOOLEAN
            )
        """)
        con.execute("INSERT INTO current_checksums VALUES ('SKU1', 'oldhash', TRUE)")

        proc = self._make_proc(con)
        proc._classify()

        result = con.execute(
            "SELECT _scd_class FROM classified WHERE sku = 'SKU1'"
        ).fetchone()
        assert result is not None
        assert result[0] == "reconnect"

    def test_classify_changed_when_is_inferred_false(self):
        """Lines 152, 157: existing row has is_inferred=FALSE → classifies as changed."""
        con = duckdb.connect()
        con.execute("""
            CREATE OR REPLACE VIEW incoming AS
            SELECT 'SKU2' AS sku, 'Gadget' AS name, '5.00' AS price,
                   'newhash2' AS _checksum
        """)
        con.execute("""
            CREATE OR REPLACE TABLE current_checksums (
                sku VARCHAR, _checksum VARCHAR, is_inferred BOOLEAN
            )
        """)
        con.execute("INSERT INTO current_checksums VALUES ('SKU2', 'oldhash2', FALSE)")

        proc = self._make_proc(con)
        proc._classify()

        result = con.execute(
            "SELECT _scd_class FROM classified WHERE sku = 'SKU2'"
        ).fetchone()
        assert result[0] == "changed"

    def test_write_changed_with_is_inferred_in_target_table(self):
        """Line 212: target table has is_inferred column → view includes is_inferred."""
        con = duckdb.connect()
        # Seed target table WITH is_inferred column
        con.execute("""
            CREATE TABLE dim_prod_inferred (
                sku VARCHAR, name VARCHAR, price VARCHAR,
                checksum VARCHAR, is_current BOOLEAN,
                valid_from VARCHAR, valid_to VARCHAR,
                is_inferred BOOLEAN
            )
        """)
        con.execute("""
            INSERT INTO dim_prod_inferred VALUES
                ('SKU3', 'OldName', '1.00', 'oldhash3', TRUE, '2024-01-01', NULL, FALSE)
        """)
        # Set up incoming
        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU3' AS sku, 'NewName' AS name, '1.00' AS price
        """)
        proc = self._make_proc(con)
        result = proc.process("incoming_src", "dim_prod_inferred")
        # Changed row should have been versioned
        assert result.versioned >= 1

    def test_reconnect_emits_lineage_event(self):
        """Lines 244-250: lineage emitter called for reconnect rows."""
        emitted = []

        def emitter(event):
            emitted.append(event)

        con = duckdb.connect()
        # Create target table WITH is_inferred
        con.execute("""
            CREATE TABLE dim_prod_reconnect (
                sku VARCHAR, name VARCHAR, price VARCHAR,
                checksum VARCHAR, is_current BOOLEAN,
                valid_from VARCHAR, valid_to VARCHAR,
                is_inferred BOOLEAN
            )
        """)
        con.execute("""
            INSERT INTO dim_prod_reconnect VALUES
                ('SKU4', 'OldName', '2.00', 'oldhash4', TRUE, '2024-01-01', NULL, TRUE)
        """)
        # Manually set up current_checksums with is_inferred to trigger reconnect
        con.execute("""
            CREATE OR REPLACE VIEW incoming_src AS
            SELECT 'SKU4' AS sku, 'NewName' AS name, '2.00' AS price
        """)
        # We need to intercept _register_current_checksums to add is_inferred
        proc = self._make_proc(con, emitter=emitter)

        # Manually build current_checksums with is_inferred
        con.execute("""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *, md5('NewName' || '|' || '2.00') AS _checksum
            FROM incoming_src
        """)
        con.execute("""
            CREATE OR REPLACE TABLE current_checksums (
                sku VARCHAR, _checksum VARCHAR, is_inferred BOOLEAN
            )
        """)
        con.execute("INSERT INTO current_checksums VALUES ('SKU4', 'oldhash4', TRUE)")
        # Classify to get 'reconnect' rows
        proc._classify()

        # Now call _write_changed to trigger lineage
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        proc._write_changed("dim_prod_reconnect", now)

        assert len(emitted) >= 1
        from sqldim.lineage.events import InferredMemberEventType
        assert emitted[0].event_type == InferredMemberEventType.RECONNECTED
