"""
sqldim/sinks/postgresql.py

PostgreSQL sink backed by DuckDB's native postgres extension.
Every read and write is expressed as DuckDB SQL — no psycopg2 in the hot path.
"""

import duckdb

from sqldim.sinks._connection import make_connection


class PostgreSQLSink:
    """
    Sink backed by PostgreSQL.

    Uses DuckDB's postgres extension so every read and write
    is expressed as DuckDB SQL. No psycopg2 in the hot path.
    """

    def __init__(self, dsn: str, schema: str = "public"):
        self._dsn = dsn
        self._schema = schema
        self._alias = "sqldim_pg"
        self._con: duckdb.DuckDBPyConnection | None = None
        self._hash_cache: dict[str, str] = {}

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def prefetch_hashes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_cols: list[str],
        hash_col: str = "checksum",
        where: str = "is_current = TRUE",
    ) -> int:
        """Pull a slim (NK + hash) fingerprint from PostgreSQL into a local DuckDB TABLE.

        Call this once per run before any ``process()`` or ``process_stream()``
        call.  After calling this, ``current_state_sql()`` returns a reference
        to the local TABLE so all downstream joins read from DuckDB, not
        PostgreSQL.

        Parameters
        ----------
        con        : Shared DuckDB connection (must have the postgres extension loaded).
        table_name : Remote PostgreSQL table to fingerprint.
        nk_cols    : Natural-key column names to include in the fingerprint.
        hash_col   : Hash column name (``"checksum"`` for SCD processors,
                     ``"row_hash"`` for :class:`LazySCDMetadataProcessor`).
        where      : SQL filter applied to the remote table (default: ``is_current = TRUE``).

        Returns the number of rows materialised.
        """
        nk_select = ", ".join(nk_cols)
        local = f"_sqldim_hashes_{table_name}"
        remote_sql = (
            f"{self._alias}.{self._schema}.{table_name}"
            if self._con is not None
            else f"postgres_scan('{self._dsn}', '{self._schema}', '{table_name}')"
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE {local} AS
            SELECT {nk_select}, {hash_col}
            FROM {remote_sql}
            WHERE {where}
        """)
        self._hash_cache[table_name] = local
        return con.execute(f"SELECT count(*) FROM {local}").fetchone()[0]

    def current_state_sql(self, table_name: str) -> str:
        # Prefer a locally-cached slim fingerprint TABLE when available.
        if table_name in self._hash_cache:
            return f"SELECT * FROM {self._hash_cache[table_name]}"
        # When a shared DuckDB connection is attached, use the alias form so
        # all operations go through the same session (consistent snapshot).
        # Without an attached connection, fall back to the standalone
        # postgres_scan() table function.
        if self._con is not None:
            return f"{self._alias}.{self._schema}.{table_name}"
        return f"postgres_scan('{self._dsn}', '{self._schema}', '{table_name}')"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
    ) -> int:
        """Insert all rows from *view_name* into the PostgreSQL table.

        Uses DuckDB's ``pg_use_binary_copy`` streaming protocol — DuckDB reads
        the source in its internal 2 048-row vector batches and forwards them
        to PostgreSQL via binary COPY without ever materialising the full
        result set in Python memory.  All intermediate tables (``incoming``,
        ``current_hashes``, ``classified``) are already spill-eligible DuckDB
        TABLEs, so memory pressure is bounded regardless of row count.
        """
        import logging
        import time

        _log = logging.getLogger(__name__)

        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0
        target = f"{self._alias}.{self._schema}.{table_name}"
        _log.info(f"[sqldim] {table_name}: inserting {total:,} rows …")
        t0 = time.perf_counter()
        con.execute(f"INSERT INTO {target} SELECT * FROM {view_name}")
        _log.info(
            f"[sqldim] {table_name}: {total:,} rows written "
            f"in {time.perf_counter() - t0:.1f}s"
        )
        return total

    def write_named(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        columns: list[str],
        batch_size: int = 500_000,
    ) -> int:
        """Insert only the listed *columns* from *view_name* into the PostgreSQL table.

        Uses DuckDB's ``pg_use_binary_copy`` streaming protocol — DuckDB reads
        the source in its internal 2 048-row vector batches and forwards them
        to PostgreSQL via binary COPY without ever materialising the full
        result set in Python memory.  All intermediate tables (``incoming``,
        ``current_hashes``, ``classified``) are already spill-eligible DuckDB
        TABLEs, so memory pressure is bounded regardless of row count.

        Required when the target table has auto-generated columns (e.g.
        ``sk BIGSERIAL``) that must not appear in the ``INSERT`` list.
        All names in *columns* must have been validated by ``_safe()``.

        *batch_size* is accepted for API compatibility but is not used — the
        source is a VIEW over a materialised TABLE, so LIMIT/OFFSET chunking
        would re-scan the VIEW from offset 0 on every chunk (O(n) per chunk).
        Binary COPY streams the full result in one transaction at negligible
        memory cost.
        """
        import logging
        import time

        _log = logging.getLogger(__name__)

        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0
        cols = ", ".join(columns)
        target = f"{self._alias}.{self._schema}.{table_name}"
        _log.info(f"[sqldim] {table_name}: inserting {total:,} rows …")
        t0 = time.perf_counter()
        con.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {view_name}")
        _log.info(
            f"[sqldim] {table_name}: {total:,} rows written "
            f"in {time.perf_counter() - t0:.1f}s"
        )
        return total

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str | list[str],
        nk_view: str,
        valid_to: str,
    ) -> int:
        """Expire rows in the PostgreSQL table whose natural key appears in *nk_view*.

        Supports both single-column and composite natural keys.
        Uses DuckDB's ``UPDATE … FROM`` syntax which the postgresql extension
        translates to an efficient server-side ``UPDATE … FROM`` in PostgreSQL.
        """
        if isinstance(nk_col, list):
            join_cond = " AND ".join(f"t.{c} = n.{c}" for c in nk_col)
        else:
            join_cond = f"t.{nk_col} = n.{nk_col}"
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name} t
               SET is_current = FALSE,
                   valid_to   = '{valid_to}'
              FROM {nk_view} n
             WHERE {join_cond}
               AND t.is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {nk_view}").fetchone()[0]

    # ── SinkAdapter extended ──────────────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Apply SCD-1 in-place attribute updates via a DuckDB UPDATE … FROM clause."""
        set_clause = ",\n               ".join(f"{c} = u.{c}" for c in update_cols)
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name} t
               SET {set_clause}
              FROM {updates_view} u
             WHERE t.{nk_col} = u.{nk_col}
               AND t.is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Apply SCD-3 column rotation via a DuckDB ``UPDATE … FROM`` against PostgreSQL.

        Shifts each ``(current_col, previous_col)`` pair in a single SQL
        statement, atomically rotating current values to the previous column.
        """
        set_clause = ",\n               ".join(
            f"{prev} = t.{curr},\n               {curr} = r.{curr}"
            for curr, prev in column_pairs
        )
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name} t
               SET {set_clause}
              FROM {rotations_view} r
             WHERE t.{nk_col} = r.{nk_col}
               AND t.is_current = TRUE
        """)
        return con.execute(f"SELECT count(*) FROM {rotations_view}").fetchone()[0]

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """Patch milestone columns in the PostgreSQL table via ``UPDATE … FROM``.

        Uses ``COALESCE`` so only NULL milestone columns are filled from the
        incoming batch; already-completed milestones remain unchanged.
        """
        set_clause = ",\n               ".join(
            f"{c} = COALESCE(u.{c}, t.{c})" for c in milestone_cols
        )
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name} t
               SET {set_clause}
              FROM {updates_view} u
             WHERE t.{match_col} = u.{match_col}
        """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "PostgreSQLSink":
        self._con = make_connection()
        self._con.execute("INSTALL postgres; LOAD postgres;")
        self._con.execute("SET pg_use_binary_copy = true")
        self._con.execute(f"ATTACH '{self._dsn}' AS {self._alias} (TYPE postgres)")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.execute(f"DETACH {self._alias}")
            self._con.close()
