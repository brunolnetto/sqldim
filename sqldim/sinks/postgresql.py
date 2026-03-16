"""
sqldim/sinks/postgresql.py

PostgreSQL sink backed by DuckDB's native postgres extension.
Every read and write is expressed as DuckDB SQL — no psycopg2 in the hot path.
"""

import duckdb


class PostgreSQLSink:
    """
    Sink backed by PostgreSQL.

    Uses DuckDB's postgres extension so every read and write
    is expressed as DuckDB SQL. No psycopg2 in the hot path.
    """

    def __init__(self, dsn: str, schema: str = "public"):
        self._dsn    = dsn
        self._schema = schema
        self._alias  = "sqldim_pg"
        self._con: duckdb.DuckDBPyConnection | None = None

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
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
        batch_size: int = 100_000,
    ) -> int:
        """Stream all rows from *view_name* into the PostgreSQL table.

        Materialises *view_name* into a local DuckDB temp table first, then
        inserts into PostgreSQL in *batch_size*-row chunks.  This two-step
        approach prevents a single enormous ``INSERT … SELECT`` from exhausting
        DuckDB's memory budget (DuckDB can spill the temp table via
        ``temp_directory``; a streaming INSERT cannot be spilled the same way).
        The total row count is logged before and the elapsed time after.
        """
        import logging, time
        _log = logging.getLogger(__name__)

        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0

        target  = f"{self._alias}.{self._schema}.{table_name}"
        tmp     = f"_sqldim_write_{table_name}"
        _log.info(f"[sqldim] {table_name}: materialising {total:,} rows …")
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS SELECT * FROM {view_name}")
        try:
            _log.info(
                f"[sqldim] {table_name}: inserting {total:,} rows "
                f"in batches of {batch_size:,} …"
            )
            offset = 0
            while offset < total:
                con.execute(
                    f"INSERT INTO {target} "
                    f"SELECT * FROM {tmp} LIMIT {batch_size} OFFSET {offset}"
                )
                offset += batch_size
        finally:
            con.execute(f"DROP TABLE IF EXISTS {tmp}")
        _log.info(
            f"[sqldim] {table_name}: {total:,} rows written "
            f"in {time.perf_counter()-t0:.1f}s"
        )
        return total

    def write_named(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        columns: list[str],
        batch_size: int = 100_000,
    ) -> int:
        """Insert only the listed *columns* from *view_name* into the PostgreSQL table.

        Materialises *view_name* into a local DuckDB temp table first, then
        inserts into PostgreSQL in *batch_size*-row chunks.  Batching prevents
        a single enormous ``INSERT … SELECT`` from exhausting DuckDB's memory
        budget for large dimension tables (e.g. ``estabelecimento``, 50M rows).
        DuckDB can spill the temp table via ``temp_directory`` when necessary.
        Required when the target table has auto-generated columns (e.g.
        ``sk BIGSERIAL``) that must not appear in the ``INSERT`` list.
        All names in *columns* must have been validated by ``_safe()``.
        """
        import logging, time
        _log = logging.getLogger(__name__)

        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0

        cols   = ", ".join(columns)
        target = f"{self._alias}.{self._schema}.{table_name}"
        tmp    = f"_sqldim_write_named_{table_name}"
        _log.info(f"[sqldim] {table_name}: materialising {total:,} rows …")
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS SELECT {cols} FROM {view_name}")
        try:
            _log.info(
                f"[sqldim] {table_name}: inserting {total:,} rows "
                f"in batches of {batch_size:,} …"
            )
            offset = 0
            while offset < total:
                con.execute(
                    f"INSERT INTO {target} ({cols}) "
                    f"SELECT {cols} FROM {tmp} LIMIT {batch_size} OFFSET {offset}"
                )
                offset += batch_size
        finally:
            con.execute(f"DROP TABLE IF EXISTS {tmp}")
        _log.info(
            f"[sqldim] {table_name}: {total:,} rows written "
            f"in {time.perf_counter()-t0:.1f}s"
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
        set_clause = ",\n               ".join(
            f"{c} = u.{c}" for c in update_cols
        )
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
        self._con = duckdb.connect()
        self._con.execute("INSTALL postgres; LOAD postgres;")
        self._con.execute(
            f"ATTACH '{self._dsn}' AS {self._alias} (TYPE postgres)"
        )
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.execute(f"DETACH {self._alias}")
            self._con.close()
