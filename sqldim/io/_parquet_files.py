"""Shared steps of the Parquet table rewrites in :mod:`sqldim.io` (#189).

A table is a directory of Parquet files in the layout
:class:`~sqldim.sinks.ParquetSink` writes: Hive partition directories, with
every partition column also stored inside the files.  A rewrite never
touches a file already written: it writes the new files into a staging
directory next to the table, checks them, and only then replaces the
table's directory.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Sequence
from uuid import uuid4

import duckdb

from sqldim.exceptions import SqldimError
from sqldim.sinks._errors import SinkUnsupported

__all__ = [
    "ParquetLayoutError",
    "file_list",
    "lit",
    "local_dir",
    "partition_options",
    "replace_table",
    "table_files",
]


class ParquetLayoutError(SqldimError):
    """The table's files are not in the layout the operation reads.

    Raised before anything is written: no file, a partition column missing
    from a file, a file appended while the rewrite ran, or a rewrite that
    would change the row count.
    """

    code = "INVALID_ARGUMENTS"


def lit(value: str) -> str:
    """*value* as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def file_list(files: Sequence[str]) -> str:
    """The SQL list literal of *files*, for ``read_parquet``."""
    return "[" + ", ".join(lit(f) for f in files) + "]"


def local_dir(path: str, op: str) -> str:
    """*path* without its trailing slash; a remote URI is refused."""
    if "://" in path:
        raise SinkUnsupported(
            f"{op} on {path!r}: replacing a table's files needs a local "
            "filesystem; nothing was written"
        )
    return path.rstrip("/")


def table_files(con: duckdb.DuckDBPyConnection, path: str) -> dict[str, set[str]]:
    """Every Parquet file under *path*, mapped to the names in its schema."""
    try:
        rows = con.execute(
            "SELECT file_name, list(name) FROM parquet_schema(?) GROUP BY file_name",
            [f"{path}/**/*.parquet"],
        ).fetchall()
    except duckdb.IOException as exc:
        raise ParquetLayoutError(
            f"no Parquet file under {path!r}; nothing to rewrite"
        ) from exc
    if not rows:
        raise ParquetLayoutError(f"no Parquet file under {path!r}; nothing to rewrite")
    return {name: set(cols) for name, cols in rows}


def _count(con: duckdb.DuckDBPyConnection, source: str) -> int:
    return int((con.execute(f"SELECT count(*) FROM {source}").fetchone() or (0,))[0])


def _check_unchanged(
    con: duckdb.DuckDBPyConnection, path: str, read: Sequence[str]
) -> None:
    now = sorted(table_files(con, path))
    if now != sorted(read):
        raise ParquetLayoutError(
            f"the files under {path!r} changed while they were rewritten; "
            "nothing was replaced, run it again without a concurrent writer"
        )


def replace_table(
    con: duckdb.DuckDBPyConnection,
    path: str,
    read: Sequence[str],
    source: str,
    write: Callable[[str], None],
) -> int:
    """Replace the files *read* under *path* by the rows *write* stages.

    *write* receives the staging directory and writes the new files there.
    The rows staged must be as many as *source* (the SQL source over *read*)
    holds, and *read* must still be every file under *path*; otherwise the
    staging directory is removed and :class:`ParquetLayoutError` raised,
    with the table untouched.  Returns the number of rows written.
    """
    tag = uuid4().hex
    stage, old = f"{path}.sqldim-stage-{tag}", f"{path}.sqldim-old-{tag}"
    os.makedirs(stage)
    try:
        write(stage)
        staged = _count(con, f"read_parquet({lit(stage + '/**/*.parquet')})")
        expected = _count(con, source)
        if staged != expected:
            raise ParquetLayoutError(
                f"the rewrite of {path!r} staged {staged} rows for {expected}; "
                "nothing was replaced"
            )
        _check_unchanged(con, path, read)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    os.replace(path, old)
    os.replace(stage, path)
    shutil.rmtree(old, ignore_errors=True)
    return staged


def partition_options(partition_by: Sequence[str]) -> str:
    """The ``PARTITION_BY`` options of the layout ``ParquetSink`` writes."""
    return f", PARTITION_BY ({', '.join(partition_by)}), WRITE_PARTITION_COLUMNS true"
