"""
sqldim/sinks/duckdb.py

DuckDB file sink — the simplest implementation.
Every operation is native DuckDB SQL.
Useful for local development, testing, and intermediate staging.
"""

import duckdb


class DuckDBSink:
    """
    Sink backed by a DuckDB file.
    All operations are native DuckDB — no translation layer.
    """

    def __init__(self, path: str, schema: str = "main"):
        self._path   = path
        self._schema = schema
        self._alias  = "sqldim_ddb"
        self._con: duckdb.DuckDBPyConnection | None = None

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        return f"SELECT * FROM {self._alias}.{self._schema}.{table_name}"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        con.execute(f"""
            INSERT INTO {self._alias}.{self._schema}.{table_name}
            SELECT * FROM {view_name}
        """)
        return con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        con.execute(f"""
            UPDATE {self._alias}.{self._schema}.{table_name}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
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
        # DuckDB UPDATE … FROM is supported — use it for clean rotation
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
        cols_str   = ", ".join(conflict_cols)
        inner_join = " AND ".join(f"src.{c} = t.{c}" for c in conflict_cols)
        view_join  = " AND ".join(f"t.{c} = v.{c}" for c in conflict_cols)
        insert_sql, view_sql = self._upsert_sql(
            tbl, view_name, output_view, returning_col,
            cols_str, conflict_cols, inner_join, view_join,
        )
        con.execute(insert_sql)
        con.execute(view_sql)
        return con.execute(f"SELECT count(*) FROM {output_view}").fetchone()[0]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "DuckDBSink":
        self._con = duckdb.connect()
        self._con.execute(f"ATTACH '{self._path}' AS {self._alias}")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.execute(f"DETACH {self._alias}")
            self._con.close()
