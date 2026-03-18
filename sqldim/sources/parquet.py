"""sqldim/sources/parquet.py"""


class ParquetSource:
    """
    Read one or more Parquet files.

        ParquetSource("data/empresa.parquet")
        ParquetSource("data/empresa/*.parquet")
        ParquetSource(["part1.parquet", "part2.parquet"])

    Parameters
    ----------
    path:
        File path, glob pattern, list of paths, or S3 URI.
    hive_partitioning:
        Enable hive-style partition pruning from directory names.
    union_by_name:
        Reconcile schemas across files by column name rather than position.
        Required for multi-file globs where files may have been written at
        different times with schema evolution.
    filename:
        Add a ``filename`` column to output — useful for debugging which file
        a row came from.
    remote_threads:
        For S3/httpfs paths, DuckDB uses synchronous IO (one HTTP request per
        thread).  Raise to 2-5× CPU cores to saturate network bandwidth.
        ``None`` = use connection default.  ``0`` = do not set.
    """

    def __init__(
        self,
        path: str | list[str],
        hive_partitioning: bool = False,
        union_by_name: bool = False,
        filename: bool = False,
        remote_threads: int | None = None,
    ):
        if isinstance(path, list):
            quoted = ", ".join(f"'{p}'" for p in path)
            self._expr = f"[{quoted}]"
        else:
            self._expr = f"'{path}'"
        self._hive = hive_partitioning
        self._union_by_name = union_by_name
        self._filename = filename
        self._remote_threads = remote_threads if remote_threads else None

    def _parquet_opts(self) -> list:
        """Build the list of read_parquet option strings from instance flags."""
        opts = []
        if self._hive:
            opts.append("hive_partitioning=true")
        if self._union_by_name:
            opts.append("union_by_name=true")
        if self._filename:
            opts.append("filename=true")
        if self._remote_threads:
            opts.append(f"threads={self._remote_threads}")
        return opts

    def as_sql(self, con) -> str:
        opts = self._parquet_opts()
        opts_str = (", " + ", ".join(opts)) if opts else ""
        return f"read_parquet({self._expr}{opts_str})"
