"""
sqldim/sinks/motherduck.py

MotherDuck (cloud DuckDB) sink for sqldim.

Uses DuckDB's ATTACH pattern to connect to a MotherDuck database.
For local testing, pass a plain ``.duckdb`` file path as ``db`` — the
ATTACH mechanism is identical to the cloud path.

Usage
-----
::

    from sqldim.sinks import MotherDuckSink

    # Cloud target (requires MOTHERDUCK_TOKEN env var or explicit token=)
    with MotherDuckSink(db="my_warehouse") as sink:
        proc = LazySCDProcessor("id", ["name", "email"], sink, con=sink._con)
        proc.process(source, "dim_customer")

    # Local stand-in (useful for testing / CI)
    with MotherDuckSink(db="/tmp/local.duckdb") as sink:
        ...
"""

import os

import duckdb

from sqldim.sinks._connection import make_connection


class MotherDuckSink:
    """
    SinkAdapter implementation for MotherDuck (cloud DuckDB).

    All six ``SinkAdapter`` protocol methods are implemented; the connection
    is opened via DuckDB ``ATTACH`` so the exact same SQL works whether the
    target is MotherDuck cloud or a local ``.duckdb`` file.

    Parameters
    ----------
    db    : MotherDuck database name (e.g. ``"my_warehouse"``) **or** a
            local ``.duckdb`` file path for testing.
    token : MotherDuck API token.  Falls back to the ``MOTHERDUCK_TOKEN``
            environment variable when *db* looks like a cloud name.
    schema: Target schema inside the database (default: ``"main"``).
    """

    def __init__(self, db: str, token: str | None = None, schema: str = "main") -> None:
        # Only build an MD URI if the caller didn't pass a file path.
        if not db.startswith("/") and not db.endswith(".duckdb"):
            resolved_token = token or os.environ.get("MOTHERDUCK_TOKEN")
            self._path = (
                f"md:{db}?motherduck_token={resolved_token}"
                if resolved_token
                else f"md:{db}"
            )
        else:
            self._path = db
        self._schema = schema
        self._alias = "sqldim_md"
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
        """Pull a slim (NK + hash) fingerprint from MotherDuck into a local DuckDB TABLE.

        Call once per run before ``process()`` / ``process_stream()``.
        After calling this, ``current_state_sql()`` returns a reference to the
        local TABLE so all downstream joins read from local DuckDB storage, not
        MotherDuck.

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
        return (con.execute(f"SELECT count(*) FROM {local}").fetchone() or (0,))[0]

    def current_state_sql(self, table_name: str) -> str:
        """Return a SQL fragment that reads the current dimension table from MotherDuck."""
        if table_name in self._hash_cache:
            return f"SELECT * FROM {self._hash_cache[table_name]}"
        return f"SELECT * FROM {self._alias}.{self._schema}.{table_name}"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """Insert all rows from *view_name* into the MotherDuck table.

        The ``INSERT INTO … SELECT *`` flows from the local DuckDB session into
        MotherDuck via the ATTACH connection without Python-side serialisation.
        """
        con.execute(
            f"INSERT INTO {self._alias}.{self._schema}.{table_name} "
            f"SELECT * FROM {view_name}"
        )
        return (con.execute(f"SELECT count(*) FROM {view_name}").fetchone() or (0,))[0]

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        """Expire current rows whose natural key appears in *nk_view*.

        Performs an ``UPDATE`` against MotherDuck via the ATTACH connection;
        sets ``is_current = FALSE`` and ``valid_to`` for matching rows.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        con.execute(f"""
            UPDATE {tbl}
               SET is_current = FALSE, valid_to = '{valid_to}'
             WHERE {nk_col} IN (SELECT {nk_col} FROM {nk_view})
               AND is_current = TRUE
        """)
        return (con.execute(f"SELECT count(*) FROM {nk_view}").fetchone() or (0,))[0]

    # ── SinkAdapter extended ──────────────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Apply SCD1-style attribute updates to the MotherDuck table.

        Runs an ``UPDATE … SET`` against the fully-qualified MotherDuck table,
        patching each *update_cols* column on the is_current rows whose natural
        key appears in *updates_view*.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        set_clause = ",\n               ".join(
            f"{c} = (SELECT u.{c} FROM {updates_view} u WHERE u.{nk_col} = {tbl}.{nk_col})"
            for c in update_cols
        )
        con.execute(f"""
            UPDATE {tbl}
               SET {set_clause}
             WHERE {nk_col} IN (SELECT {nk_col} FROM {updates_view})
               AND is_current = TRUE
        """)
        return (con.execute(f"SELECT count(*) FROM {updates_view}").fetchone() or (0,))[
            0
        ]

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Rotate SCD3 current/previous column pairs in the MotherDuck table.

        Executes an ``UPDATE … SET`` that shifts each *current* column value to
        its paired *previous* column and writes the new value from *rotations_view*.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        set_clause = ",\n               ".join(
            f"{prev} = t_old.{curr},\n               {curr} = r.{curr}"
            for curr, prev in column_pairs
        )
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
        return (
            con.execute(f"SELECT count(*) FROM {rotations_view}").fetchone() or (0,)
        )[0]

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """Fill NULL milestone timestamps in the MotherDuck table.

        Uses ``COALESCE`` to patch only the columns that are currently NULL,
        leaving already-stamped milestones untouched.
        """
        tbl = f"{self._alias}.{self._schema}.{table_name}"
        set_clause = ",\n               ".join(
            f"{c} = COALESCE("
            f"(SELECT u.{c} FROM {updates_view} u WHERE u.{match_col} = {tbl}.{match_col})"
            f", {c})"
            for c in milestone_cols
        )
        con.execute(f"""
            UPDATE {tbl}
               SET {set_clause}
             WHERE {match_col} IN (SELECT {match_col} FROM {updates_view})
        """)
        return (con.execute(f"SELECT count(*) FROM {updates_view}").fetchone() or (0,))[
            0
        ]

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
        """Identical to DuckDBSink.upsert() but qualified to the MotherDuck alias."""
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
        return (con.execute(f"SELECT count(*) FROM {output_view}").fetchone() or (0,))[
            0
        ]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "MotherDuckSink":
        self._con = make_connection()
        self._con.execute(f"ATTACH '{self._path}' AS {self._alias}")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            try:
                self._con.execute(f"DETACH {self._alias}")
            except Exception:
                pass
            self._con.close()
            self._con = None
