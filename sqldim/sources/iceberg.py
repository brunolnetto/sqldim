"""sqldim/sources/iceberg.py

IcebergSource — read Apache Iceberg tables via DuckDB's native iceberg
extension, which supports filter pushdown at manifest, file, and row level.

This is the correct read path for sqldim.  The existing IcebergSink reads
back data via PyIceberg's ``.scan().to_arrow()`` for mutations, which loads
full partitions into Python.  IcebergSource bypasses that and keeps all
reads inside DuckDB.
"""
from __future__ import annotations


class IcebergSource:
    """
    Read from an Apache Iceberg table via DuckDB's native iceberg extension.

    DuckDB's ``iceberg_scan()`` supports:

    * Manifest-level pruning (skips manifest files for non-matching partitions)
    * Data file-level pruning (uses file statistics)
    * Row-level predicate pushdown to the underlying Parquet reader

    Parameters
    ----------
    path:
        Table location — local path or object storage URI.
        For metadata-file mode (faster for moved tables):
        ``'s3://bucket/table/metadata/v1.metadata.json'``.
    allow_moved_tables:
        When ``True``, allows reading tables that have been moved from their
        original location (metadata-file-path mode).  Default ``False`` —
        safe for standard catalog-registered tables.
    """

    def __init__(
        self,
        path: str,
        allow_moved_tables: bool = False,
    ) -> None:
        self._path               = path
        self._allow_moved_tables = allow_moved_tables

    def as_sql(self, con) -> str:
        """Return a DuckDB ``iceberg_scan`` fragment usable in a FROM clause."""
        con.execute("INSTALL iceberg; LOAD iceberg;")
        moved = "true" if self._allow_moved_tables else "false"
        return (
            f"iceberg_scan('{self._path}', "
            f"allow_moved_tables={moved})"
        )
