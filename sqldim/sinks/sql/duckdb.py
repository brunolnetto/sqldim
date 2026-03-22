"""
sqldim/sinks/duckdb.py

DuckDB file sink — the simplest implementation.
Every operation is native DuckDB SQL.
Useful for local development, testing, and intermediate staging.
"""

import duckdb

from sqldim.sinks._connection import make_connection


class DuckDBSink:
    """
    Sink backed by a DuckDB file.
    All operations are native DuckDB — no translation layer.
    """

    def __init__(self, path: str, schema: str = "main"):
        self._path = path
        self._schema = schema
        self._alias = "sqldim_ddb"
        self._con: duckdb.DuckDBPyConnection | None = None
        self._hash_cache: dict[str, str] = {}

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """Return a DuckDB SQL fragment that reads the current dimension table."""
        if table_name in self._hash_cache:
            return f"SELECT * FROM {self._hash_cache[table_name]}"
        return f"SELECT * FROM {self._alias}.{self._schema}.{table_name}"

    def prefetch_hashes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_cols: list[str],
        hash_col: str = "checksum",
        where: str = "is_current = TRUE",
    ) -> int:
        """Pull a slim (NK + hash) fingerprint into a local DuckDB TABLE.

        For DuckDB-backed sinks this is mostly useful for API parity with
        :class:`~sqldim.sinks.sql.postgresql.PostgreSQLSink`.  Since the data is
        already local, the main benefit is pinning a consistent snapshot of the
        current state before streaming batches modify it.

        Returns the number of rows materialised.
        """
        nk_select = ", ".join(nk_cols)
        local = f"_sqldim_hashes_{table_name}"
        remote_sql = f"{self._alias}.{self._schema}.{table_name}"
        con.execute(f"""
            CREATE OR REPLACE TABLE {local} AS
            SELECT {nk_select}, {hash_col}
            FROM {remote_sql}
            WHERE {where}
        """)
        self._hash_cache[table_name] = local
        return con.execute(f"SELECT count(*) FROM {local}").fetchone()[0]

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
        per_thread_output: bool = False,
    ) -> int:
        """Stream all rows from *view_name* into *table_name*.

        Executes a single ``INSERT INTO … SELECT *`` so DuckDB's vectorised
        pipeline streams data without materialising the full result set.
        The total row count is logged before and the elapsed time after.

        Parameters
        ----------
        per_thread_output:
            When ``True``, hints DuckDB to use per-thread output mode which
            reduces memory pressure on xl/xxl tier inserts by not requiring
            the full result to be gathered before writing.
        """
        import logging
        import time

        _log = logging.getLogger(__name__)

        target = f"{self._alias}.{self._schema}.{table_name}"
        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0
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
        batch_size: int = 100_000,
    ) -> int:
        """Stream only the listed *columns* from *view_name* into *table_name*.

        Executes a single ``INSERT INTO … SELECT cols`` so DuckDB streams data
        without materialising the full result set.  Required when the target
        table has auto-generated columns (e.g. ``sk BIGSERIAL``) that must not
        appear in the ``INSERT`` column list.  All names in *columns* must have
        been validated by ``_safe()`` before being passed here.
        """
        import logging
        import time

        _log = logging.getLogger(__name__)

        cols = ", ".join(columns)
        target = f"{self._alias}.{self._schema}.{table_name}"
        total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
        if total == 0:
            return 0
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
        """Expire current rows whose natural key appears in *nk_view*, setting valid_to.

        Supports both single-column and composite natural keys.
        Performs a single SQL ``UPDATE`` — no Python-side loop over rows.
        """
        if isinstance(nk_col, list):
            nk_select = ", ".join(nk_col)
            where_clause = f"({nk_select}) IN (SELECT {nk_select} FROM {nk_view})"
        else:
            where_clause = f"{nk_col} IN (SELECT {nk_col} FROM {nk_view})"
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {where_clause}
               AND is_current = TRUE
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
        """Apply SCD-1 in-place attribute updates for rows matching *updates_view*."""
        set_clause = ",\n               ".join(
            f"{c} = (SELECT u.{c} FROM {updates_view} u WHERE u.{nk_col} = {self._alias}.{self._schema}.{table_name}.{nk_col})"
            for c in update_cols
        )
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name}
               SET {set_clause}
             WHERE {nk_col} IN (SELECT {nk_col} FROM {updates_view})
               AND is_current = TRUE
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
        """Apply SCD-3 column rotation for rows whose current values changed.

        For each ``(current_col, previous_col)`` pair, the existing current
        value is shifted to ``previous_col`` and the incoming value written to
        ``current_col``, all via a single ``UPDATE … FROM`` statement.
        """
        set_clause = ",\n               ".join(
            f"{prev} = t_old.{curr},\n               {curr} = r.{curr}"
            for curr, prev in column_pairs
        )
        # Two-step: read current then write, all inside one CTE view
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        con.execute(f"""
            UPDATE {tbl}
               SET {set_clause}
              FROM {rotations_view} r,
                   {tbl} t_old
             WHERE {tbl}.{nk_col} = r.{nk_col}
               AND t_old.{nk_col} = r.{nk_col}
               AND {tbl}.is_current = TRUE
               AND t_old.is_current = TRUE
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
        """Patch NULL milestone timestamps for accumulating-snapshot fact rows.

        Only columns that are currently NULL are updated so previously
        completed milestones are never overwritten.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        set_clause = ",\n               ".join(
            f"{c} = COALESCE((SELECT u.{c} FROM {updates_view} u WHERE u.{match_col} = {tbl}.{match_col}), {c})"
            for c in milestone_cols
        )
        con.execute(f"""
            UPDATE {tbl}
               SET {set_clause}
             WHERE {match_col} IN (SELECT {match_col} FROM {updates_view})
        """)
        return con.execute(f"SELECT count(*) FROM {updates_view}").fetchone()[0]

    def _upsert_sql(
        self,
        tbl: str,
        view_name: str,
        output_view: str,
        returning_col: str,
        cols_str: str,
        conflict_cols: list[str],
        inner_join: str,
        view_join: str,
    ) -> tuple[str, str]:
        src_cols = ", ".join(f"src.{c}" for c in conflict_cols)
        t_cols = ", ".join(f"t.{c}" for c in conflict_cols)
        insert_sql = (
            f"INSERT INTO {tbl} ({returning_col}, {cols_str})\n"
            f"SELECT (SELECT COALESCE(MAX({returning_col}), 0) FROM {tbl})"
            f" + row_number() OVER () AS {returning_col}, {src_cols}\n"
            f"FROM (SELECT DISTINCT {src_cols} FROM {view_name} src\n"
            f" WHERE NOT EXISTS (SELECT 1 FROM {tbl} t WHERE {inner_join})) src"
        )
        view_sql = (
            f"CREATE OR REPLACE VIEW {output_view} AS\n"
            f"SELECT t.{returning_col}, {t_cols}\n"
            f"FROM {tbl} t\n"
            f"INNER JOIN (SELECT DISTINCT {cols_str} FROM {view_name}) v ON {view_join}"
        )
        return insert_sql, view_sql

    def upsert(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        conflict_cols: list[str],
        returning_col: str,
        output_view: str,
    ) -> int:
        """
        Insert distinct combinations from *view_name* that do not yet exist in
        *table_name*, auto-assigning integer IDs via MAX+row_number.  Register
        *output_view* as a DuckDB view mapping (returning_col, *conflict_cols)
        for every combination present in *view_name*.  Returns row count.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        cols_str = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)
        insert_sql, view_sql = self._upsert_sql(
            tbl,
            view_name,
            output_view,
            returning_col,
            cols_str,
            conflict_cols,
            inner_join,
            view_join,
        )
        con.execute(insert_sql)
        con.execute(view_sql)
        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "DuckDBSink":
        self._con = make_connection()
        self._con.execute(f"ATTACH '{self._path}' AS {self._alias}")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.execute(f"DETACH {self._alias}")
            self._con.close()
