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
        return f"{self._alias}.{self._schema}.{table_name}"

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
        return con.execute("SELECT changes()").fetchone()[0]

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
        return con.execute("SELECT changes()").fetchone()[0]

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
        return con.execute("SELECT changes()").fetchone()[0]

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
        return con.execute("SELECT changes()").fetchone()[0]

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
        return con.execute("SELECT changes()").fetchone()[0]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "DuckDBSink":
        self._con = duckdb.connect()
        self._con.execute(f"ATTACH '{self._path}' AS {self._alias}")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.execute(f"DETACH {self._alias}")
            self._con.close()
