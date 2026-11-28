"""One-time migration of Parquet tables written by the sink before #178 (#189).

Before #178, :class:`~sqldim.sinks.ParquetSink` stored partition columns
(``is_current`` by default) only in the Hive directory names.  The sink now
also stores them inside the files and reads them from there
(``hive_partitioning=false``), so reading a file of the old layout fails
explicitly.  :func:`migrate_legacy_parquet` rewrites such a table once into
the current layout.

.. code-block:: python

    import duckdb
    from sqldim.io import migrate_legacy_parquet

    con = duckdb.connect()
    migrate_legacy_parquet(con, "/data/warehouse/dim_customer")
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

import duckdb

from sqldim.io._parquet_files import (
    ParquetLayoutError,
    file_list,
    lit,
    local_dir,
    partition_options,
    replace_table,
    table_files,
)

__all__ = ["migrate_legacy_parquet"]

#: The partition columns of ``ParquetSink``'s default layout, and their type.
DEFAULT_PARTITIONS: Mapping[str, str] = MappingProxyType({"is_current": "BOOLEAN"})


def _classify(
    files: Mapping[str, set[str]], keys: list[str]
) -> tuple[list[str], list[str]]:
    """The files lacking every partition column, and those holding all of them."""
    legacy, current = [], []
    for name, cols in sorted(files.items()):
        held = [k for k in keys if k in cols]
        if not held:
            legacy.append(name)
        elif len(held) == len(keys):
            current.append(name)
        else:
            raise ParquetLayoutError(
                f"{name!r} holds the partition columns {held} but not the others "
                f"of {keys}; it is in neither layout"
            )
    return legacy, current


def _require_directories(files: list[str], keys: list[str]) -> None:
    for name in files:
        parts = name.replace("\\", "/").split("/")
        missing = [k for k in keys if not any(p.startswith(f"{k}=") for p in parts)]
        if missing:
            raise ParquetLayoutError(
                f"{name!r} has no column {missing} and no {missing[0]}=… directory "
                "to read it from; it cannot be migrated"
            )


def _hive_types(partitions: Mapping[str, str]) -> str:
    return "{" + ", ".join(f"{lit(k)}: {t}" for k, t in partitions.items()) + "}"


def _source(
    legacy: list[str], current: list[str], partitions: Mapping[str, str]
) -> str:
    old = (
        f"SELECT * FROM read_parquet({file_list(legacy)}, hive_partitioning=true, "
        f"hive_types={_hive_types(partitions)})"
    )
    if not current:
        return f"({old})"
    new = f"SELECT * FROM read_parquet({file_list(current)}, hive_partitioning=false)"
    return f"({old} UNION ALL BY NAME {new})"


def migrate_legacy_parquet(
    con: duckdb.DuckDBPyConnection,
    path: str,
    *,
    partitions: Mapping[str, str] = DEFAULT_PARTITIONS,
) -> int:
    """Rewrite the table at *path* into the layout ``ParquetSink`` reads.

    Parameters
    ----------
    con:
        DuckDB connection.
    path:
        Local root directory of one table (``<base_path>/<table_name>``).
    partitions:
        The table's partition columns, in ``partition_by`` order, mapped to
        their SQL type, e.g. ``{"is_current": "BOOLEAN"}`` (the default
        layout).  The type is how the old directory value is read; it is
        never inferred.

    A file of the old layout (no partition column inside) is read with its
    column values from its ``key=value`` directories, cast to the declared
    type.  A file already in the current layout (appended after #178) is
    read as it is.  The rows are rewritten into a staging directory with
    the partition columns inside the files, which then replaces the table's
    directory; no file is overwritten in place.

    Returns the number of rows rewritten, or ``0`` without writing when no
    file is of the old layout.  Raises :class:`~sqldim.io.ParquetLayoutError`
    for a file in neither layout, a missing directory value or a value
    that does not cast, and :class:`~sqldim.sinks.SinkUnsupported` on a
    remote URI, always before the table is replaced.
    """
    keys = list(partitions)
    if not keys:
        raise ValueError("migrate_legacy_parquet needs the partition columns")
    root = local_dir(path, "migrate_legacy_parquet")
    files = table_files(con, root)
    legacy, current = _classify(files, keys)
    if not legacy:
        return 0
    _require_directories(legacy, keys)
    source = _source(legacy, current, partitions)

    def write(stage: str) -> None:
        options = partition_options(keys)
        con.execute(
            f"COPY (SELECT * FROM {source}) TO {lit(stage)} (FORMAT parquet{options})"
        )

    try:
        return replace_table(con, root, sorted(files), source, write)
    except (duckdb.ConversionException, duckdb.InvalidInputException) as exc:
        if "cast" not in str(exc).lower():
            raise
        raise ParquetLayoutError(
            f"a partition directory value under {root!r} does not cast to its "
            f"declared type {dict(partitions)}: {exc}"
        ) from exc
