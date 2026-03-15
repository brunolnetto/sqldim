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
        # postgres_scan() is a DuckDB table function — fully lazy
        return (
            f"postgres_scan('{self._dsn}', '{self._schema}', '{table_name}')"
        )

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        # Single SQL statement — DuckDB translates to PG COPY protocol
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
            UPDATE {self._alias}.{self._schema}.{table_name} t
               SET is_current = FALSE,
                   valid_to   = '{valid_to}'
              FROM {nk_view} n
             WHERE t.{nk_col}   = n.{nk_col}
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
