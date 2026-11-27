"""
sqldim/sinks/parquet.py

Parquet sink — immutable storage on local disk, S3, GCS, or Azure.

Contract (#178)
---------------
* ``write()`` appends: every call adds new files (``COPY … APPEND``) and
  never replaces a file already written.
* Partition columns are also written into the files
  (``WRITE_PARTITION_COLUMNS``) and read back from them, never from the
  directory names, so ``is_current`` reads back as the BOOLEAN written.
* ``close_versions()``, ``update_attributes()``, ``rotate_attributes()`` and
  ``update_milestones()`` rewrite the whole table inside DuckDB into a
  staging directory, which then replaces the table's directory.  Every row
  is kept, historical versions included, and no file of the old state
  remains.  Replacing a directory needs a local filesystem: on a remote URI
  these operations raise :class:`~sqldim.sinks.SinkUnsupported` before
  anything is written.

Physical Partitioning ADR
--------------------------
``partition_by`` is configurable (default ``["is_current"]``).  Pass
``zorder_by`` columns to enable sort-based Z-ORDER approximation — rows are
sorted by those columns before writing, which co-locates them within Parquet
row groups for predicate-pushdown wins.
"""

import os
import shutil
from pathlib import Path
from uuid import uuid4

import duckdb

from sqldim.sinks._errors import SinkUnsupported


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return (con.execute(sql).fetchone() or (0,))[0]


class ParquetSink:
    """
    Sink backed by Parquet files.

    Compatible with S3/GCS/Azure by passing a URI as base_path:
        ParquetSink("s3://my-bucket/dimensions/")
    (appends only; a rewrite on a remote URI raises ``SinkUnsupported``).

    Parameters
    ----------
    base_path:
        Root directory or URI for Parquet output.
    partition_by:
        Hive partition columns.  Defaults to ``["is_current"]``.  Pass an
        empty list to skip partitioning.
    zorder_by:
        Columns to sort by before writing (Z-ORDER approximation).
        ``None`` (default) skips sorting and preserves current row order.
    target_file_size_mb:
        Soft target file size in MB.  Informational — used by
        :class:`~sqldim.io.Compactor` scheduling; the sink itself does not
        enforce exact file sizes.
    """

    def __init__(
        self,
        base_path: str,
        *,
        partition_by: list[str] | None = None,
        zorder_by: list[str] | None = None,
        target_file_size_mb: int = 256,
    ):
        self._base = base_path.rstrip("/")
        self._partition_by = (
            partition_by if partition_by is not None else ["is_current"]
        )
        self._zorder_by = zorder_by or []
        self._target_file_size_mb = target_file_size_mb

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _partition_clause(self) -> str:
        """The ``PARTITION_BY`` options of a COPY, or an empty string."""
        if not self._partition_by:
            return ""
        cols = ", ".join(self._partition_by)
        return f", PARTITION_BY ({cols}), WRITE_PARTITION_COLUMNS true"

    def _order_clause(self) -> str:
        """Return the ``ORDER BY ...`` clause for Z-ORDER or empty string."""
        if not self._zorder_by:
            return ""
        cols = ", ".join(self._zorder_by)
        return f" ORDER BY {cols}"

    def _table_path(self, table_name: str) -> str:
        """Return the glob pattern that matches all Parquet files for *table_name*."""
        return f"{self._base}/{table_name}/**/*.parquet"

    def _table_out(self, table_name: str) -> str:
        """Return the root output directory for Parquet partitions of *table_name*."""
        return f"{self._base}/{table_name}"

    def _copy(self, con: duckdb.DuckDBPyConnection, select: str, out: str) -> None:
        """Add the rows of *select* to directory *out* as new files."""
        if "://" not in out:
            Path(out).mkdir(parents=True, exist_ok=True)
        if self._partition_by:
            target = f"'{out}' (FORMAT parquet{self._partition_clause()}, APPEND true)"
        else:
            target = f"'{out}/data_{uuid4().hex}.parquet' (FORMAT parquet)"
        con.execute(f"COPY ({select}) TO {target}")

    def _rewrite(
        self, con: duckdb.DuckDBPyConnection, table_name: str, select: str, op: str
    ) -> None:
        """Replace the table's files by the rows of *select* (which reads them)."""
        out = self._table_out(table_name)
        if "://" in out:
            raise SinkUnsupported(
                f"ParquetSink.{op} on {out!r}: replacing the table's files needs "
                "a local filesystem; nothing was written"
            )
        tag = uuid4().hex
        stage, old = f"{out}.sqldim-stage-{tag}", f"{out}.sqldim-old-{tag}"
        os.makedirs(stage)
        try:
            self._copy(con, select, stage)
        except BaseException:
            shutil.rmtree(stage, ignore_errors=True)
            raise
        if os.path.exists(out):
            os.replace(out, old)
        os.replace(stage, out)
        shutil.rmtree(old, ignore_errors=True)

    # ── SinkAdapter core ──────────────────────────────────────────────────

    def current_state_sql(self, table_name: str) -> str:
        """Return a DuckDB FROM-source expression for the current Parquet table.

        Every column, partition columns included, is read from the files with
        the type written; directory names are not read
        (``hive_partitioning=false``).
        """
        return (
            f"read_parquet('{self._table_path(table_name)}', hive_partitioning=false)"
        )

    def write(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> int:
        """Append the rows of *view_name* as new Parquet files; returns their count."""
        self._copy(con, f"SELECT * FROM {view_name}{self._order_clause()}",
                   self._table_out(table_name))  # fmt: skip
        return _count(con, f"SELECT count(*) FROM {view_name}")

    def write_named(
        self,
        con: duckdb.DuckDBPyConnection,
        view_name: str,
        table_name: str,
        columns: list[str],
        batch_size: int = 100_000,
    ) -> int:
        """Append only the listed *columns* of *view_name*."""
        cols = ", ".join(columns)
        self._copy(con, f"SELECT {cols} FROM {view_name}{self._order_clause()}",
                   self._table_out(table_name))  # fmt: skip
        return _count(con, f"SELECT count(*) FROM {view_name}")

    def close_versions(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        nk_view: str,
        valid_to: str,
    ) -> int:
        """Close the current version of each key in *nk_view*; returns rows closed.

        Every other row — historical versions and other keys — is kept as
        it was.  The table is rewritten inside DuckDB, never in Python.
        """
        src = self.current_state_sql(table_name)
        match = f"({nk_col} IN (SELECT {nk_col} FROM {nk_view}) AND is_current = TRUE)"
        closed = _count(con, f"SELECT count(*) FROM {src} WHERE {match}")
        if not closed:
            return 0
        self._rewrite(
            con,
            table_name,
            f"SELECT * REPLACE ("
            f"CASE WHEN {match} THEN FALSE ELSE is_current END AS is_current, "
            f"CASE WHEN {match} THEN '{valid_to}' ELSE valid_to END AS valid_to"
            f") FROM {src}",
            "close_versions",
        )
        return closed

    # ── SinkAdapter extended ───────────────────────────────────────

    def update_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        updates_view: str,
        update_cols: list[str],
    ) -> int:
        """Overwrite *update_cols* on the current rows matched in *updates_view*.

        ``COALESCE(new, old)`` per column; the table is rewritten in one COPY.
        """
        exprs = ", ".join(f"COALESCE(u.{c}, t.{c}) AS {c}" for c in update_cols)
        self._rewrite(
            con,
            table_name,
            f"SELECT t.* REPLACE ({exprs}) "
            f"FROM {self.current_state_sql(table_name)} t "
            f"LEFT JOIN {updates_view} u "
            f"ON t.{nk_col} = u.{nk_col} AND t.is_current = TRUE",
            "update_attributes",
        )
        return _count(con, f"SELECT count(*) FROM {updates_view}")

    def rotate_attributes(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        nk_col: str,
        rotations_view: str,
        column_pairs: list[tuple[str, str]],
    ) -> int:
        """Rotate current → previous columns on the current rows matched."""
        hit = f"u.{nk_col} IS NOT NULL"
        exprs = ", ".join(
            f"CASE WHEN {hit} THEN t.{curr} ELSE t.{prev} END AS {prev}, "
            f"CASE WHEN {hit} THEN u.{curr} ELSE t.{curr} END AS {curr}"
            for curr, prev in column_pairs
        )
        self._rewrite(
            con,
            table_name,
            f"SELECT t.* REPLACE ({exprs}) "
            f"FROM {self.current_state_sql(table_name)} t "
            f"LEFT JOIN {rotations_view} u "
            f"ON t.{nk_col} = u.{nk_col} AND t.is_current = TRUE",
            "rotate_attributes",
        )
        return _count(con, f"SELECT count(*) FROM {rotations_view}")

    def update_milestones(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        match_col: str,
        updates_view: str,
        milestone_cols: list[str],
    ) -> int:
        """Patch the non-null milestone columns of the rows matched."""
        exprs = ", ".join(f"COALESCE(u.{c}, t.{c}) AS {c}" for c in milestone_cols)
        self._rewrite(
            con,
            table_name,
            f"SELECT t.* REPLACE ({exprs}) "
            f"FROM {self.current_state_sql(table_name)} t "
            f"LEFT JOIN {updates_view} u ON t.{match_col} = u.{match_col}",
            "update_milestones",
        )
        return _count(con, f"SELECT count(*) FROM {updates_view}")
