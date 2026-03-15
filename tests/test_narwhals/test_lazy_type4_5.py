"""
Tests for LazyType4Processor and LazyType5Processor.

Coverage targets
----------------
sqldim/narwhals/scd_engine.py  — LazyType4Processor, LazyType5Processor
sqldim/sinks/duckdb.py        — upsert()
"""
from __future__ import annotations
import duckdb
import pytest

from sqldim.processors.scd_engine import LazyType4Processor, LazyType5Processor


# ---------------------------------------------------------------------------
# InMemorySink — mirrors test_lazy_scd.py but adds upsert()
# ---------------------------------------------------------------------------

class InMemorySink:
    """Minimal sink that operates directly on an in-memory DuckDB connection."""

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

    def upsert(self, con, view_name, table_name, conflict_cols, returning_col,
               output_view):
        """Insert new combinations; register output_view mapping (id, *cols)."""
        cols_str = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join  = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)

        con.execute(f"""
            INSERT INTO {table_name} ({returning_col}, {cols_str})
            SELECT
                (SELECT COALESCE(MAX({returning_col}), 0) FROM {table_name})
                    + row_number() OVER () AS {returning_col},
                {', '.join(f'src.{c}' for c in conflict_cols)}
            FROM (
                SELECT DISTINCT {', '.join(f'src.{c}' for c in conflict_cols)}
                FROM {view_name} src
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t WHERE {inner_join}
                )
            ) src
        """)

        con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT t.{returning_col},
                   {', '.join(f't.{c}' for c in conflict_cols)}
            FROM {table_name} t
            INNER JOIN (SELECT DISTINCT {cols_str} FROM {view_name}) v
                ON {view_join}
        """)

        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_con():
    return duckdb.connect()


def _create_mini_dim(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Empty mini-dimension table with id PK and two profile columns."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            id          INTEGER,
            age_band    VARCHAR,
            income_band VARCHAR
        )
    """)


def _create_base_dim(con: duckdb.DuckDBPyConnection, name: str,
                     extra_cols: list[str] | None = None) -> None:
    """Empty base dimension table with SCD2 structure."""
    extras = ""
    if extra_cols:
        extras = ", " + ", ".join(f"{c} INTEGER" for c in extra_cols)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            customer_id    VARCHAR,
            name           VARCHAR,
            profile_sk     INTEGER{extras},
            checksum       VARCHAR,
            valid_from     VARCHAR,
            valid_to       VARCHAR,
            is_current     BOOLEAN
        )
    """)


def _seed_incoming(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> str:
    """Create 'src_table' in *con*, return its name for use as a source."""
    vals = ", ".join(
        f"('{r['customer_id']}', '{r['name']}', '{r['age_band']}', '{r['income_band']}')"
        for r in rows
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE src_table AS
        SELECT col0 AS customer_id,
               col1 AS name,
               col2 AS age_band,
               col3 AS income_band
        FROM (VALUES {vals})
    """)
    return "src_table"


# ---------------------------------------------------------------------------
# TestLazyType4Processor
# ---------------------------------------------------------------------------

class TestLazyType4Processor:

    def _make_processor(self, con, base_dim="dim_customer",
                        mini_dim="dim_profile", extra_base_cols=None):
        sink = InMemorySink()
        _create_mini_dim(con, mini_dim)
        _create_base_dim(con, base_dim, extra_base_cols)
        return LazyType4Processor(
            natural_key="customer_id",
            base_columns=["name"],
            mini_dim_columns=["age_band", "income_band"],
            base_dim_table=base_dim,
            mini_dim_table=mini_dim,
            mini_dim_fk_col="profile_sk",
            mini_dim_id_col="id",
            sink=sink,
            con=con,
        )

    def test_new_rows_inserted(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        result = proc.process(src_sql)
        assert result["inserted"] == 1
        assert result["versioned"] == 0
        assert result["unchanged"] == 0
        rows = con.execute("SELECT * FROM dim_customer").fetchdf()
        assert len(rows) == 1
        assert rows["customer_id"][0] == "C1"

    def test_mini_dim_rows_populated(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
            {"customer_id": "C2", "name": "Bob",   "age_band": "25-34", "income_band": "mid"},
        ])
        result = proc.process(src_sql)
        assert result["mini_dim_rows"] == 2
        mini = con.execute("SELECT * FROM dim_profile").fetchdf()
        assert len(mini) == 2

    def test_mini_dim_deduplicates_same_profile(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
            {"customer_id": "C2", "name": "Bob",   "age_band": "18-24", "income_band": "low"},
        ])
        result = proc.process(src_sql)
        assert result["mini_dim_rows"] == 1
        mini = con.execute("SELECT * FROM dim_profile").fetchdf()
        assert len(mini) == 1

    def test_profile_sk_attached_to_base_row(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src_sql)
        row = con.execute("SELECT profile_sk FROM dim_customer").fetchone()
        assert row[0] is not None and row[0] >= 1

    def test_changed_profile_triggers_new_version(self):
        con = _make_con()
        proc = self._make_processor(con)

        # Initial load
        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        r1 = proc.process(src1)
        assert r1["inserted"] == 1

        # Profile changes → new SCD2 version
        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "25-34", "income_band": "mid"},
        ])
        r2 = proc.process(src2)
        assert r2["versioned"] == 1
        assert r2["inserted"] == 0

        rows = con.execute("SELECT * FROM dim_customer ORDER BY valid_from").fetchdf()
        assert len(rows) == 2     # one historical, one current
        assert rows["is_current"].tolist() == [False, True]

    def test_unchanged_row_stays_unchanged(self):
        con = _make_con()
        proc = self._make_processor(con)

        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src1)

        # Same data → unchanged
        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        r2 = proc.process(src2)
        assert r2["unchanged"] == 1
        assert r2["inserted"] == 0
        assert r2["versioned"] == 0
        rows = con.execute("SELECT * FROM dim_customer").fetchdf()
        assert len(rows) == 1

    def test_mini_dim_not_duplicated_on_second_run(self):
        con = _make_con()
        proc = self._make_processor(con)

        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src1)

        # Same profile combination — mini dim should still have only 1 row
        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice updated", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src2)
        mini_count = con.execute("SELECT count(*) FROM dim_profile").fetchone()[0]
        assert mini_count == 1

    def test_process_returns_dict_keys(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        result = proc.process(src_sql)
        assert set(result.keys()) == {"mini_dim_rows", "inserted", "versioned", "unchanged"}

    def test_multiple_customers_different_profiles(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
            {"customer_id": "C2", "name": "Bob",   "age_band": "25-34", "income_band": "mid"},
            {"customer_id": "C3", "name": "Carol", "age_band": "35-44", "income_band": "high"},
        ])
        result = proc.process(src_sql)
        assert result["inserted"] == 3
        assert result["mini_dim_rows"] == 3


# ---------------------------------------------------------------------------
# TestLazyType5Processor
# ---------------------------------------------------------------------------

class TestLazyType5Processor:

    def _create_base_with_current_fk(self, con, name="dim_cust5"):
        """Base dim with both profile_sk (SCD2 tracked) and current_profile_sk (T1)."""
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                customer_id        VARCHAR,
                name               VARCHAR,
                profile_sk         INTEGER,
                current_profile_sk INTEGER,
                checksum           VARCHAR,
                valid_from         VARCHAR,
                valid_to           VARCHAR,
                is_current         BOOLEAN
            )
        """)

    def _make_processor(self, con):
        sink = InMemorySink()
        _create_mini_dim(con, "dim_profile5")
        self._create_base_with_current_fk(con)
        return LazyType5Processor(
            natural_key="customer_id",
            base_columns=["name"],
            mini_dim_columns=["age_band", "income_band"],
            base_dim_table="dim_cust5",
            mini_dim_table="dim_profile5",
            mini_dim_fk_col="profile_sk",
            mini_dim_id_col="id",
            current_profile_fk_col="current_profile_sk",
            sink=sink,
            con=con,
        )

    def test_new_row_has_current_profile_sk(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src_sql)
        row = con.execute(
            "SELECT profile_sk, current_profile_sk FROM dim_cust5"
        ).fetchone()
        # current_profile_sk should be set (Type1 update runs after insert)
        assert row[1] is not None

    def test_current_profile_sk_updated_when_profile_changes(self):
        con = _make_con()
        proc = self._make_processor(con)

        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src1)
        sk1 = con.execute(
            "SELECT current_profile_sk FROM dim_cust5 WHERE is_current = TRUE"
        ).fetchone()[0]

        # Change profile
        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "25-34", "income_band": "mid"},
        ])
        proc.process(src2)
        sk2 = con.execute(
            "SELECT current_profile_sk FROM dim_cust5 WHERE is_current = TRUE"
        ).fetchone()[0]

        assert sk2 != sk1, "current_profile_sk should be updated after profile change"

    def test_current_profile_sk_set_even_when_unchanged(self):
        con = _make_con()
        proc = self._make_processor(con)

        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src1)

        # Re-process identical data
        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        r2 = proc.process(src2)

        assert r2["unchanged"] == 1
        fk = con.execute(
            "SELECT current_profile_sk FROM dim_cust5 WHERE is_current = TRUE"
        ).fetchone()[0]
        assert fk is not None

    def test_inherits_type4_result_keys(self):
        con = _make_con()
        proc = self._make_processor(con)
        src_sql = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        result = proc.process(src_sql)
        assert set(result.keys()) == {"mini_dim_rows", "inserted", "versioned", "unchanged"}

    def test_is_instance_of_type4(self):
        con = _make_con()
        sink = InMemorySink()
        _create_mini_dim(con, "dim_profile_x")
        _create_base_dim(con, "dim_base_x", ["current_profile_sk"])
        proc = LazyType5Processor(
            natural_key="customer_id",
            base_columns=["name"],
            mini_dim_columns=["age_band", "income_band"],
            base_dim_table="dim_base_x",
            mini_dim_table="dim_profile_x",
            mini_dim_fk_col="profile_sk",
            current_profile_fk_col="current_profile_sk",
            sink=sink,
            con=con,
        )
        assert isinstance(proc, LazyType4Processor)
        assert proc.current_profile_fk_col == "current_profile_sk"

    def test_base_dim_has_two_versions_after_profile_change(self):
        con = _make_con()
        proc = self._make_processor(con)

        src1 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "18-24", "income_band": "low"},
        ])
        proc.process(src1)

        con.execute("DROP TABLE src_table")
        src2 = _seed_incoming(con, [
            {"customer_id": "C1", "name": "Alice", "age_band": "35-44", "income_band": "high"},
        ])
        proc.process(src2)

        rows = con.execute("SELECT * FROM dim_cust5 ORDER BY valid_from").fetchdf()
        assert len(rows) == 2
        assert rows["is_current"].tolist() == [False, True]


# ---------------------------------------------------------------------------
# DuckDBSink.upsert() via actual file sink
# ---------------------------------------------------------------------------

class TestDuckDBSinkUpsert:

    def test_upsert_inserts_new_combinations(self, tmp_path):
        from sqldim.sinks import DuckDBSink

        db = str(tmp_path / "upsert_test.duckdb")
        setup_con = duckdb.connect(db)
        setup_con.execute("""
            CREATE TABLE dim_profile (
                id INTEGER, age_band VARCHAR, income_band VARCHAR
            )
        """)
        setup_con.close()

        with DuckDBSink(db) as sink:
            sink._con.execute("""
                CREATE OR REPLACE VIEW dist_profiles AS
                SELECT * FROM (VALUES
                    ('18-24', 'low'),
                    ('25-34', 'mid')
                ) t(age_band, income_band)
            """)
            n = sink.upsert(
                sink._con,
                view_name="dist_profiles",
                table_name="dim_profile",
                conflict_cols=["age_band", "income_band"],
                returning_col="id",
                output_view="mini_dim_sks",
            )
        assert n == 2

    def test_upsert_does_not_duplicate_existing(self, tmp_path):
        from sqldim.sinks import DuckDBSink

        db = str(tmp_path / "upsert_test2.duckdb")
        setup_con = duckdb.connect(db)
        setup_con.execute("""
            CREATE TABLE dim_profile (
                id INTEGER, age_band VARCHAR, income_band VARCHAR
            )
        """)
        setup_con.execute("INSERT INTO dim_profile VALUES (1, '18-24', 'low')")
        setup_con.close()

        with DuckDBSink(db) as sink:
            sink._con.execute("""
                CREATE OR REPLACE VIEW dist_profiles AS
                SELECT * FROM (VALUES ('18-24', 'low')) t(age_band, income_band)
            """)
            n = sink.upsert(
                sink._con,
                view_name="dist_profiles",
                table_name="dim_profile",
                conflict_cols=["age_band", "income_band"],
                returning_col="id",
                output_view="mini_dim_sks_v2",
            )
        # Should return 1 row from the view (existing), not insert a duplicate
        assert n == 1
        # Verify no duplicate in the actual table
        count = duckdb.connect(db).execute("SELECT count(*) FROM dim_profile").fetchone()[0]
        assert count == 1
