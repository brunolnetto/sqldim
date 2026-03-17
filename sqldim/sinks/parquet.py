"""
sqldim/sinks/parquet.py

Parquet sink — immutable storage on local disk, S3, GCS, or Azure.
close_versions(), update_attributes(), rotate_attributes(), and
update_milestones() all rewrite the affected partition entirely inside
DuckDB — never a Python loop over rows.
"""

from pathlib import Path
import duckdb


class ParquetSink:
    """
    Sink backed by Parquet files.

    Compatible with S3/GCS/Azure by passing a URI as base_path:
        ParquetSink("s3://my-bucket/dimensions/")

    Because Parquet is immutable, every mutation is a partition rewrite
    expressed as a single DuckDB COPY statement.
    """

    def __init__(self, base_path: str):
        self._base = base_path.rstrip("/")

    def _table_path(self, table_name: str) -> str:
        """Return the glob pattern that matches all Parquet files for *table_name*."""
        return f"{self._base}/{table_name}/**/*.parquet"

    def _table_out(self, table_name: str) -> str:
        """Return the root output directory for Parquet partitions of *table_name*."""
        return f"{self._base}/{table_name}"

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """Return a DuckDB FROM-source expression for the current Parquet table.

        Uses ``read_parquet`` with hive partitioning and a recursive glob so
        DuckDB streams data without loading the entire dataset into memory.
        """
        return (
            f"read_parquet('{self._table_path(table_name)}', "
            f"hive_partitioning=true)"
        )

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """Write *view_name* rows to Parquet, partitioned by is_current.

        Executes a single ``COPY … TO`` statement so DuckDB handles
        partitioning and file creation without any Python-side buffering.
        """
        out = self._table_out(table_name)
        if "://" not in out:
            Path(out).mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT * FROM {view_name})
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
        """)
        return con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]

    def write_named(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        columns: list[str],
        batch_size: int = 100_000,
    ) -> int:
        """Write only the listed *columns* from *view_name* to Parquet.

        Selects only the specified columns before writing so that
        auto-generated DB columns (e.g. ``sk``) do not appear in the output.
        """
        cols = ", ".join(columns)
        out  = self._table_out(table_name)
        con.execute(f"""
            COPY (SELECT {cols} FROM {view_name})
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
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
        """
        Immutable rewrite: read current partition, flip matching rows, write back.
        Entirely inside DuckDB — no Python loop.
        """
        out = self._table_out(table_name)
        con.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE (is_current, valid_to),
                    CASE WHEN {nk_col} IN (SELECT {nk_col} FROM {nk_view})
                         THEN FALSE
                         ELSE TRY_CAST(is_current AS BOOLEAN)
                    END AS is_current,
                    CASE WHEN {nk_col} IN (SELECT {nk_col} FROM {nk_view})
                         THEN '{valid_to}' ELSE valid_to END AS valid_to
                FROM read_parquet('{self._table_path(table_name)}',
                                  hive_partitioning=true)
                WHERE TRY_CAST(is_current AS BOOLEAN) = TRUE
            )
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
        """)
        return con.execute(
            f"SELECT count(*) FROM {nk_view}"
        ).fetchone()[0]


    # ── SinkAdapter extended ───────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Rewrite current partition with overwritten attribute columns.

        Reads the current Parquet partition, applies the overwrite via
        ``COALESCE(new, old)`` for each tracked column, and re-writes the
        full partition in a single ``COPY`` statement.
        """
        out = self._table_out(table_name)
        col_exprs = ",\n                    ".join(
            f"COALESCE(u.{c}, t.{c}) AS {c}" for c in update_cols
        )
        other_cols = f"t.* EXCLUDE ({', '.join(update_cols)})"
        con.execute(f"""
            COPY (
                SELECT
                    {other_cols},
                    {col_exprs}
                FROM read_parquet('{self._table_path(table_name)}',
                                  hive_partitioning=true) t
                LEFT JOIN {updates_view} u
                       ON t.{nk_col} = u.{nk_col}
                      AND TRY_CAST(t.is_current AS BOOLEAN) = TRUE
            )
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
        """)
        return con.execute(
            f"SELECT count(*) FROM {updates_view}"
        ).fetchone()[0]

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Rewrite current partition rotating current → previous columns."""
        out = self._table_out(table_name)
        rotate_exprs = ",\n                    ".join(
            f"CASE WHEN u.{nk_col} IS NOT NULL THEN t.{curr} ELSE t.{prev} END AS {prev},\n"
            f"                    CASE WHEN u.{nk_col} IS NOT NULL THEN u.{curr} ELSE t.{curr} END AS {curr}"
            for curr, prev in column_pairs
        )
        exclude_cols = ", ".join(
            col for pair in column_pairs for col in pair
        )
        con.execute(f"""
            COPY (
                SELECT
                    t.* EXCLUDE ({exclude_cols}),
                    {rotate_exprs}
                FROM read_parquet('{self._table_path(table_name)}',
                                  hive_partitioning=true) t
                LEFT JOIN {rotations_view} u
                       ON t.{nk_col} = u.{nk_col}
                      AND TRY_CAST(t.is_current AS BOOLEAN) = TRUE
            )
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
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
        """Rewrite partition patching non-null milestone columns."""
        out = self._table_out(table_name)
        col_exprs = ",\n                    ".join(
            f"COALESCE(u.{c}, t.{c}) AS {c}" for c in milestone_cols
        )
        exclude = ", ".join(milestone_cols)
        con.execute(f"""
            COPY (
                SELECT
                    t.* EXCLUDE ({exclude}),
                    {col_exprs}
                FROM read_parquet('{self._table_path(table_name)}',
                                  hive_partitioning=true) t
                LEFT JOIN {updates_view} u
                       ON t.{match_col} = u.{match_col}
            )
            TO '{out}'
            (FORMAT parquet, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
        """)
        return con.execute(
            f"SELECT count(*) FROM {updates_view}"
        ).fetchone()[0]
