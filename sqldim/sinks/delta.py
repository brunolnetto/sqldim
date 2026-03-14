"""
sqldim/sinks/delta.py

Delta Lake sink via DuckDB's delta extension.
MERGE INTO handles all SCD2 cases in one atomic statement.
"""

import duckdb


class DeltaLakeSink:
    """
    Sink backed by Delta Lake via DuckDB's delta extension.

    MERGE INTO handles all three SCD2 cases (new, changed, unchanged)
    in a single atomic operation. No separate INSERT + UPDATE needed.

    Mutable operations (update_attributes, rotate_attributes,
    update_milestones) also use MERGE INTO or UPDATE via the delta
    extension's DML support.
    """

    def __init__(self, path: str, natural_key: str = "id"):
        self._path = path
        self._natural_key = natural_key
        self._con: duckdb.DuckDBPyConnection | None = None

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        return f"delta_scan('{self._path}/{table_name}')"

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        nk = self._natural_key
        # Delta MERGE INTO: one statement handles new + changed rows
        con.execute(f"""
            MERGE INTO delta.`{self._path}/{table_name}` AS target
            USING {view_name} AS source
               ON target.{nk} = source.{nk}
                  AND target.is_current = TRUE
            WHEN MATCHED AND source._scd_class = 'changed' THEN
                UPDATE SET is_current = FALSE, valid_to = source.valid_from
            WHEN NOT MATCHED BY TARGET THEN
                INSERT *
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
        # Handled inside write() via MERGE INTO — no-op here
        return 0

    # ── SinkAdapter extended ──────────────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        set_clause = ", ".join(
            f"target.{c} = source.{c}" for c in update_cols
        )
        con.execute(f"""
            MERGE INTO delta.`{self._path}/{table_name}` AS target
            USING {updates_view} AS source
               ON target.{nk_col} = source.{nk_col}
                  AND target.is_current = TRUE
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
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
        # Rotation: target.prev = target.curr, target.curr = source.curr
        # Delta MERGE INTO sees both sides — use a self-join trick via view
        set_clause = ", ".join(
            f"target.{prev} = target.{curr},\n               target.{curr} = source.{curr}"
            for curr, prev in column_pairs
        )
        con.execute(f"""
            MERGE INTO delta.`{self._path}/{table_name}` AS target
            USING {rotations_view} AS source
               ON target.{nk_col} = source.{nk_col}
                  AND target.is_current = TRUE
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
        """)
        return con.execute(
            f"SELECT count(*) FROM {rotations_view}"
        ).fetchone()[0]

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        set_clause = ", ".join(
            f"target.{c} = COALESCE(source.{c}, target.{c})"
            for c in milestone_cols
        )
        con.execute(f"""
            MERGE INTO delta.`{self._path}/{table_name}` AS target
            USING {updates_view} AS source
               ON target.{match_col} = source.{match_col}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
        """)
        return con.execute(
            f"SELECT count(*) FROM {updates_view}"
        ).fetchone()[0]

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "DeltaLakeSink":
        self._con = duckdb.connect()
        self._con.execute("INSTALL delta; LOAD delta;")
        return self

    def __exit__(self, *_) -> None:
        if self._con:
            self._con.close()
