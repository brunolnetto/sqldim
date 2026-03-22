"""
Tests for sqldim/sinks/postgresql.py — PostgreSQLSink.

Uses pytest-postgresql to spin up a real PostgreSQL 16 process via pg_ctl.
DuckDB's postgres extension (no psycopg2 in the hot path) writes through to PG.
"""
from __future__ import annotations
import duckdb
from pytest_postgresql import factories

from sqldim.sinks.sql.postgresql import PostgreSQLSink

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

PG_EXE  = "/usr/lib/postgresql/16/bin/pg_ctl"
PG_PORT = 15437  # unique port — avoid collisions with other fixtures

postgresql_proc = factories.postgresql_proc(
    executable=PG_EXE,
    port=PG_PORT,
)
postgresql = factories.postgresql("postgresql_proc")


def _dsn(pg_proc, pg_conn) -> str:
    """Build a libpq-style DSN from the pytest-postgresql fixtures."""
    return (
        f"host={pg_proc.host} port={pg_proc.port} "
        f"user={pg_proc.user} dbname={pg_conn.info.dbname}"
    )


def _duckdb_con(dsn: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with the postgres extension loaded + PG attached."""
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS sqldim_pg (TYPE postgres, READ_WRITE)")
    return con


def _create_scd_table(pg_conn, table_name: str) -> None:
    """Create a minimal SCD-2 table in PostgreSQL."""
    with pg_conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                player_id   TEXT    NOT NULL,
                player_name TEXT,
                rating      INT,
                is_current  BOOLEAN NOT NULL DEFAULT TRUE,
                valid_from  DATE    NOT NULL DEFAULT CURRENT_DATE,
                valid_to    DATE
            )
        """)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Unit-level tests (no live DB needed)
# ---------------------------------------------------------------------------


class TestPostgreSQLSinkUnit:
    def test_current_state_sql_contains_postgres_scan(self):
        sink = PostgreSQLSink("host=127.0.0.1 port=5432 user=postgres dbname=tests")
        sql = sink.current_state_sql("dim_player")
        assert "postgres_scan" in sql
        assert "dim_player" in sql

    def test_current_state_sql_contains_dsn(self):
        dsn = "host=127.0.0.1 port=5432 user=postgres dbname=tests"
        sink = PostgreSQLSink(dsn)
        sql = sink.current_state_sql("my_table")
        assert dsn in sql

    def test_current_state_sql_custom_schema(self):
        sink = PostgreSQLSink("host=127.0.0.1 port=5432 user=postgres dbname=tests", schema="analytics")
        sql = sink.current_state_sql("players")
        assert "analytics" in sql

    def test_init_defaults(self):
        sink = PostgreSQLSink("host=127.0.0.1 port=5432 user=postgres dbname=tests")
        assert sink._schema == "public"
        assert sink._alias  == "sqldim_pg"
        assert sink._con    is None


# ---------------------------------------------------------------------------
# Integration tests — require a live PostgreSQL process
# ---------------------------------------------------------------------------


class TestPostgreSQLSinkWrite:
    def test_write_inserts_rows(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        _create_scd_table(postgresql, "dim_player")

        con = _duckdb_con(dsn)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW src_players AS
                SELECT 'p1' AS player_id, 'Alice' AS player_name, 90 AS rating,
                       TRUE AS is_current, CURRENT_DATE AS valid_from, NULL::DATE AS valid_to
                UNION ALL
                SELECT 'p2', 'Bob', 85, TRUE, CURRENT_DATE, NULL::DATE
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.write(con, "src_players", "dim_player")
        finally:
            con.close()

        assert count == 2

        # Verify rows landed in PostgreSQL
        with postgresql.cursor() as cur:
            cur.execute("SELECT count(*) FROM dim_player")
            assert cur.fetchone()[0] == 2

    def test_write_returns_source_count(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        _create_scd_table(postgresql, "dim_player2")

        con = _duckdb_con(dsn)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW single_player AS
                SELECT 'px' AS player_id, 'Carol' AS player_name, 75 AS rating,
                       TRUE AS is_current, CURRENT_DATE AS valid_from, NULL::DATE AS valid_to
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.write(con, "single_player", "dim_player2")
        finally:
            con.close()

        assert count == 1


class TestPostgreSQLSinkCloseVersions:
    def test_close_versions_marks_is_current_false(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        _create_scd_table(postgresql, "dim_cv")

        # Seed two current rows
        with postgresql.cursor() as cur:
            cur.execute("""
                INSERT INTO dim_cv (player_id, player_name, rating, is_current, valid_from)
                VALUES ('a', 'Alpha', 10, TRUE, '2023-01-01'),
                       ('b', 'Beta',  20, TRUE, '2023-01-01')
            """)
        postgresql.commit()

        con = _duckdb_con(dsn)
        try:
            # Close player 'a'
            con.execute("""
                CREATE OR REPLACE VIEW to_close AS
                SELECT 'a' AS player_id
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.close_versions(con, "dim_cv", "player_id", "to_close", "2024-12-31")
        finally:
            con.close()

        assert count == 1

        with postgresql.cursor() as cur:
            cur.execute("SELECT is_current, valid_to FROM dim_cv WHERE player_id = 'a'")
            row = cur.fetchone()
        assert row[0] is False
        assert str(row[1]) == "2024-12-31"


class TestPostgreSQLSinkUpdateAttributes:
    def test_update_attributes_modifies_current_row(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        _create_scd_table(postgresql, "dim_ua")

        with postgresql.cursor() as cur:
            cur.execute("""
                INSERT INTO dim_ua (player_id, player_name, rating, is_current, valid_from)
                VALUES ('x', 'Xena', 50, TRUE, '2023-01-01')
            """)
        postgresql.commit()

        con = _duckdb_con(dsn)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW attr_updates AS
                SELECT 'x' AS player_id, 99 AS rating
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.update_attributes(con, "dim_ua", "player_id", "attr_updates", ["rating"])
        finally:
            con.close()

        assert count == 1

        with postgresql.cursor() as cur:
            cur.execute("SELECT rating FROM dim_ua WHERE player_id = 'x'")
            assert cur.fetchone()[0] == 99


class TestPostgreSQLSinkRotateAttributes:
    def _scd_table_with_prev(self, pg_conn, table_name: str) -> None:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    player_id   TEXT    NOT NULL,
                    rating      INT,
                    prev_rating INT,
                    is_current  BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from  DATE    NOT NULL DEFAULT CURRENT_DATE,
                    valid_to    DATE
                )
            """)
        pg_conn.commit()

    def test_rotate_attributes_shifts_column(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        self._scd_table_with_prev(postgresql, "dim_ra")

        with postgresql.cursor() as cur:
            cur.execute("""
                INSERT INTO dim_ra (player_id, rating, prev_rating, is_current, valid_from)
                VALUES ('y', 80, 70, TRUE, '2023-01-01')
            """)
        postgresql.commit()

        con = _duckdb_con(dsn)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW rot_data AS
                SELECT 'y' AS player_id, 90 AS rating
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.rotate_attributes(
                con, "dim_ra", "player_id", "rot_data",
                column_pairs=[("rating", "prev_rating")],
            )
        finally:
            con.close()

        assert count == 1

        with postgresql.cursor() as cur:
            cur.execute("SELECT rating, prev_rating FROM dim_ra WHERE player_id = 'y'")
            row = cur.fetchone()
        assert row[0] == 90
        assert row[1] == 80


class TestPostgreSQLSinkUpdateMilestones:
    def _milestone_table(self, pg_conn, table_name: str) -> None:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    player_id   TEXT NOT NULL,
                    first_game  DATE,
                    last_game   DATE,
                    is_current  BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from  DATE    NOT NULL DEFAULT CURRENT_DATE,
                    valid_to    DATE
                )
            """)
        pg_conn.commit()

    def test_update_milestones_fills_null_milestone(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        self._milestone_table(postgresql, "dim_ms")

        with postgresql.cursor() as cur:
            cur.execute("""
                INSERT INTO dim_ms (player_id, first_game, last_game, is_current, valid_from)
                VALUES ('z', NULL, NULL, TRUE, '2023-01-01')
            """)
        postgresql.commit()

        con = _duckdb_con(dsn)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW ms_updates AS
                SELECT 'z' AS player_id,
                       DATE '2020-01-15' AS first_game,
                       DATE '2024-06-30' AS last_game
            """)
            sink = PostgreSQLSink(dsn)
            count = sink.update_milestones(
                con, "dim_ms", "player_id", "ms_updates",
                milestone_cols=["first_game", "last_game"],
            )
        finally:
            con.close()

        assert count == 1

        with postgresql.cursor() as cur:
            cur.execute("SELECT first_game, last_game FROM dim_ms WHERE player_id = 'z'")
            row = cur.fetchone()
        assert str(row[0]) == "2020-01-15"
        assert str(row[1]) == "2024-06-30"


class TestPostgreSQLSinkContextManager:
    def test_context_manager_attaches_and_detaches(self, postgresql_proc, postgresql):
        dsn = _dsn(postgresql_proc, postgresql)
        sink = PostgreSQLSink(dsn)

        assert sink._con is None

        with sink:
            # Inside __enter__: _con should be open and postgres extension loaded
            assert sink._con is not None
            # Verify the postgres alias is attached by querying the PG DB list
            attached = sink._con.execute(
                "SELECT database_name FROM duckdb_databases() WHERE database_name = 'sqldim_pg'"
            ).fetchall()
            assert len(attached) == 1

        # After __exit__: alias is detached; _con is still referenced but closed
