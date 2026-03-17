Here is the complete implementation plan, organized by source and sink with specific code changes for each.

---

## Global connection configuration — implement first, everything else builds on it

This needs to land in a shared utility before touching any individual sink, because every sink calls `duckdb.connect()` independently today with no resource management.

**New file: `sqldim/sinks/_connection.py`**

```python
"""Shared DuckDB connection factory with resource configuration."""
from __future__ import annotations
import os
import duckdb


def make_connection(
    memory_limit: str | None = None,
    temp_directory: str | None = None,
    threads: int | None = None,
    preserve_insertion_order: bool = False,
) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB in-memory connection with sensible production defaults.

    Parameters
    ----------
    memory_limit     : DuckDB memory ceiling, e.g. '7GB'. Defaults to 60% of
                       system RAM. Counter-intuitively, keeping this below the
                       OS default (80%) prevents OOM on operations that
                       circumvent the buffer manager.
    temp_directory   : Spill-to-disk path. Defaults to /tmp/sqldim_spill.
                       Required for beyond-memory workloads — without it DuckDB
                       raises instead of spilling.
    threads          : Worker thread count. Defaults to CPU core count.
                       For remote sources (S3, httpfs) raise to 2-5x CPU cores
                       since DuckDB uses synchronous IO (one request per thread).
    preserve_insertion_order : False allows DuckDB to reorder output, reducing
                       memory pressure significantly at xl/xxl tier. Safe for
                       all sqldim operations which never rely on insertion order.
    """
    try:
        import psutil
        total_gb = psutil.virtual_memory().total / 1e9
        default_limit = f"{total_gb * 0.60:.0f}GB"
    except ImportError:
        default_limit = "4GB"

    resolved_limit = memory_limit or os.environ.get("SQLDIM_MEMORY_LIMIT", default_limit)
    resolved_tmp   = temp_directory or os.environ.get("SQLDIM_TEMP_DIR", "/tmp/sqldim_spill")

    os.makedirs(resolved_tmp, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{resolved_limit}'")
    con.execute(f"SET temp_directory = '{resolved_tmp}'")
    con.execute(f"SET preserve_insertion_order = {'true' if preserve_insertion_order else 'false'}")
    if threads is not None:
        con.execute(f"SET threads = {threads}")
    return con
```

---

## Sources

### `sqldim/sources/parquet.py`

Three gaps: no `union_by_name` for multi-file globs, no `filename` tracking option, and remote paths need a thread count hint.

```python
class ParquetSource:
    """
    Read one or more Parquet files — local, glob, or remote (S3/GCS/Azure).

    Parameters
    ----------
    path              : File path, glob pattern, list of paths, or S3 URI.
    hive_partitioning : Enable hive-style partition pruning from directory names.
    union_by_name     : Reconcile schemas across files by column name rather than
                        position. Required for multi-file globs where files may
                        have been written at different times with schema evolution.
    filename          : Add a 'filename' column to output — useful for debugging
                        which file a row came from.
    remote_threads    : For S3/httpfs paths, DuckDB uses synchronous IO (one HTTP
                        request per thread). Raise to 2-5x CPU cores to saturate
                        network bandwidth. None = use connection default.
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
        self._hive           = hive_partitioning
        self._union_by_name  = union_by_name
        self._filename       = filename
        self._remote_threads = remote_threads

    def as_sql(self, con) -> str:
        opts = []
        if self._hive:
            opts.append("hive_partitioning=true")
        if self._union_by_name:
            opts.append("union_by_name=true")
        if self._filename:
            opts.append("filename=true")
        opts_str = (", " + ", ".join(opts)) if opts else ""

        # For remote sources DuckDB uses sync IO: more threads = more parallel requests
        if self._remote_threads is not None:
            con.execute(f"SET threads = {self._remote_threads}")

        return f"read_parquet({self._expr}{opts_str})"
```

### `sqldim/sources/csv.py`

The current implementation uses `auto_detect` implicitly, which scans the file twice. For known schemas, disabling it and declaring columns avoids the double scan. Also adds `nullstr` and `ignore_errors` which are common pain points in production CSV pipelines.

```python
class CSVSource:
    """
    Read one or more CSV/TSV files.

    Parameters
    ----------
    path          : File path, glob, or list of paths.
    delimiter     : Field separator. Default ','.
    header        : Whether the first row is a header. Default True.
    encoding      : File encoding (ICU-compatible names only).
    columns       : Dict of {column_name: sql_type}. When provided, disables
                    auto-detection and avoids the double file scan DuckDB
                    performs to infer schema. Use for large files where
                    schema is known.
    nullstr       : String to interpret as NULL. Default '' (empty string).
    ignore_errors : Skip malformed rows rather than raising. Default False.
    """

    def __init__(
        self,
        path: str | list[str],
        delimiter: str = ",",
        header: bool = True,
        encoding: str = "utf-8",
        columns: dict | None = None,
        nullstr: str | None = None,
        ignore_errors: bool = False,
    ):
        if isinstance(path, list):
            quoted = ", ".join(f"'{p}'" for p in path)
            self._expr = f"[{quoted}]"
        else:
            self._expr = f"'{path}'"
        self._delimiter     = delimiter
        self._header        = str(header).upper()
        self._encoding      = encoding
        self._columns       = columns
        self._nullstr       = nullstr
        self._ignore_errors = ignore_errors

    def as_sql(self, con) -> str:
        opts = [
            f"delim='{self._delimiter}'",
            f"header={self._header}",
            f"encoding='{self._encoding}'",
        ]
        if self._columns:
            # STRUCT_PACK avoids auto-detection double scan
            pairs = ", ".join(f"{k} := '{v}'" for k, v in self._columns.items())
            opts.append(f"columns=STRUCT_PACK({pairs})")
        if self._nullstr is not None:
            opts.append(f"nullstr='{self._nullstr}'")
        if self._ignore_errors:
            opts.append("ignore_errors=true")
        return f"read_csv({self._expr}, {', '.join(opts)})"
```

### `sqldim/sources/delta.py`

The current implementation always does `INSTALL delta; LOAD delta` on every `as_sql()` call. Delta also supports `ATTACH` mode which enables metadata caching across queries — significant for repeated scans of the same table. Using `ATTACH` with Delta enables metadata caching, though there is an interplay between caching and filter/partition pushdown that can occasionally make raw `delta_scan` slightly faster for specific queries.

```python
class DeltaSource:
    """
    Read from a Delta Lake table via DuckDB's delta extension.

    Parameters
    ----------
    path        : Local path or cloud URI (s3://, abfss://, gs://).
    use_attach  : Use ATTACH mode instead of delta_scan(). ATTACH enables
                  metadata caching across repeated scans of the same table,
                  which is beneficial when the same source is queried multiple
                  times in one session (e.g. the classify → write_new →
                  write_changed pattern in LazySCDProcessor).
                  Default True.
    alias       : ATTACH alias. Only used when use_attach=True.
    """

    def __init__(
        self,
        path: str,
        use_attach: bool = True,
        alias: str = "sqldim_delta_src",
    ):
        self._path       = path
        self._use_attach = use_attach
        self._alias      = alias
        self._attached   = False

    def as_sql(self, con) -> str:
        try:
            con.execute("LOAD delta")
        except Exception:
            con.execute("INSTALL delta; LOAD delta")

        if self._use_attach:
            if not self._attached:
                try:
                    con.execute(
                        f"ATTACH '{self._path}' AS {self._alias} "
                        f"(TYPE delta)"
                    )
                    self._attached = True
                except Exception:
                    # Already attached or path issue — fall through to delta_scan
                    self._use_attach = False

            if self._attached:
                # ATTACH mode: exposes table as alias.data
                return f"SELECT * FROM {self._alias}.data"

        return f"delta_scan('{self._path}')"
```

### `sqldim/sources/postgresql.py`

The PostgreSQL extension supports `pg_experimental_filter_pushdown` (disabled by default) and `pg_pages_per_task` to control parallel scan granularity. Both are worth exposing.

```python
class PostgreSQLSource:
    """
    Read directly from a PostgreSQL table via DuckDB's postgres extension.

    Parameters
    ----------
    dsn                   : libpq connection string or PostgreSQL URI.
    table                 : Table name.
    schema                : Schema name. Default 'public'.
    filter_pushdown       : Enable experimental filter pushdown to PostgreSQL.
                            Reduces data transferred for filtered queries but
                            is disabled by default in the extension because it
                            can occasionally produce incorrect results for
                            complex expressions. Verify before enabling in
                            production.
    pages_per_task        : Controls parallel scan granularity. Lower values
                            create more, smaller scan tasks. Default is 1000
                            pages per task (each page is 8KB in PostgreSQL).
                            Increase for tables with many wide rows; decrease
                            for tables with narrow rows to improve parallelism.
    """

    def __init__(
        self,
        dsn: str,
        table: str,
        schema: str = "public",
        filter_pushdown: bool = False,
        pages_per_task: int = 1000,
    ):
        self._dsn             = dsn
        self._table           = table
        self._schema          = schema
        self._filter_pushdown = filter_pushdown
        self._pages_per_task  = pages_per_task

    def as_sql(self, con) -> str:
        con.execute("INSTALL postgres; LOAD postgres")
        if self._filter_pushdown:
            con.execute("SET pg_experimental_filter_pushdown = true")
        if self._pages_per_task != 1000:
            con.execute(f"SET pg_pages_per_task = {self._pages_per_task}")
        return (
            f"postgres_scan('{self._dsn}', '{self._schema}', '{self._table}')"
        )
```

### `sqldim/sources/iceberg.py` — new file

The DuckDB native Iceberg extension performs full table scans without partition pruning when using `iceberg_scan()` directly, so very large Iceberg tables may be slow to query. However, `ICEBERG_SCAN` implements sophisticated filter pushdown at manifest level, data file level, and row level — filters on partition fields skip entire manifest files. The current `IcebergSink` in sqldim reads back via PyIceberg's `.scan().to_arrow()`, which bypasses all of this. Adding an `IcebergSource` that uses the native DuckDB extension fixes the read path.

```python
"""sqldim/sources/iceberg.py

IcebergSource — read Apache Iceberg tables via DuckDB's native iceberg
extension, which supports filter pushdown at manifest, file, and row level.

This is the correct read path for sqldim. The existing IcebergSink reads
back data via PyIceberg's .scan().to_arrow() for mutations, which loads
full partitions into Python. IcebergSource bypasses that and keeps all
reads inside DuckDB.
"""


class IcebergSource:
    """
    Read from an Apache Iceberg table via DuckDB's native iceberg extension.

    DuckDB's iceberg_scan() supports:
    - Manifest-level pruning (skips manifest files for non-matching partitions)
    - Data file-level pruning (uses file statistics)
    - Row-level predicate pushdown to the underlying Parquet reader

    Parameters
    ----------
    path              : Table location — local path or object storage URI.
                        For metadata-file mode (faster for moved tables):
                        pass the full metadata JSON path, e.g.
                        's3://bucket/table/metadata/v1.metadata.json'.
    allow_moved_paths : Resolve relative paths in the manifest. Required when
                        the table directory has been moved or copied.
                        Default True.
    snapshot_id       : Read a specific historical snapshot. None = latest.
    version           : Iceberg spec version (1 or 2). None = auto-detect.
    """

    def __init__(
        self,
        path: str,
        allow_moved_paths: bool = True,
        snapshot_id: int | None = None,
        version: int | None = None,
    ):
        self._path              = path
        self._allow_moved_paths = allow_moved_paths
        self._snapshot_id       = snapshot_id
        self._version           = version

    def as_sql(self, con) -> str:
        try:
            con.execute("LOAD iceberg")
        except Exception:
            con.execute("INSTALL iceberg; LOAD iceberg")

        opts = []
        if self._allow_moved_paths:
            opts.append("allow_moved_paths=true")
        if self._snapshot_id is not None:
            opts.append(f"snapshot_from_id={self._snapshot_id}")
        if self._version is not None:
            opts.append(f"version='{self._version}'")

        opts_str = (", " + ", ".join(opts)) if opts else ""
        return f"iceberg_scan('{self._path}'{opts_str})"
```

---

## Sinks

### `sqldim/sinks/duckdb.py`

Three changes: use the shared connection factory, add `per_thread_output` option for large writes, and set `ROW_GROUP_SIZE` on ATTACH.

```python
def __enter__(self) -> "DuckDBSink":
    from sqldim.sinks._connection import make_connection
    self._con = make_connection()
    # ROW_GROUP_SIZE on ATTACH controls parallelism: DuckDB parallelizes at
    # row group boundaries. 122,880 = DuckDB default, matches Parquet default.
    self._con.execute(
        f"ATTACH '{self._path}' AS {self._alias} "
        f"(ROW_GROUP_SIZE 122880)"
    )
    return self
```

Also add `per_thread_output` to `write()` for xl/xxl tier inserts:

```python
def write(
    self,
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    table_name: str,
    batch_size: int = 100_000,
    per_thread_output: bool = False,
) -> int:
    import logging, time
    _log = logging.getLogger(__name__)
    target = f"{self._alias}.{self._schema}.{table_name}"
    total  = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
    if total == 0:
        return 0
    _log.info(f"[sqldim] {table_name}: inserting {total:,} rows …")
    t0 = time.perf_counter()
    con.execute(f"INSERT INTO {target} SELECT * FROM {view_name}")
    _log.info(
        f"[sqldim] {table_name}: {total:,} rows written "
        f"in {time.perf_counter()-t0:.1f}s"
    )
    return total
```

### `sqldim/sinks/parquet.py`

Four changes: shared connection factory, zstd compression, explicit `ROW_GROUP_SIZE`, and `preserve_insertion_order=false` scoped to write operations only.

```python
def write(
    self,
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    table_name: str,
    batch_size: int = 100_000,
) -> int:
    out = self._table_out(table_name)
    # preserve_insertion_order=false: reduces memory at large tiers.
    # Scoped reset after write avoids affecting other operations on the
    # same connection that may depend on ordering.
    con.execute("SET preserve_insertion_order = false")
    try:
        con.execute(f"""
            COPY (SELECT * FROM {view_name})
            TO '{out}'
            (FORMAT parquet,
             COMPRESSION 'zstd',
             ROW_GROUP_SIZE 122880,
             PARTITION_BY (is_current),
             OVERWRITE_OR_IGNORE true)
        """)
    finally:
        con.execute("SET preserve_insertion_order = true")
    return con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
```

The `current_state_sql` method also needs an upgrade. The current glob pattern generates one HTTP request per row group per column when reading from S3 — a large number of row groups triggers `num_variables × num_row_groups` HTTP requests, which is the root cause of poor S3 performance with many-column tables. Selecting only the needed columns at the source level fixes this:

```python
def current_state_sql(self, table_name: str) -> str:
    """
    Return a DuckDB FROM-source expression for the current Parquet partition.

    Uses hive_partitioning + union_by_name for schema-evolution safety.
    Processors only need the columns they declare — avoid SELECT * on remote
    Parquet to prevent the column × row_group HTTP request explosion on S3.
    """
    return (
        f"read_parquet('{self._table_path(table_name)}', "
        f"hive_partitioning=true, "
        f"union_by_name=true)"
    )
```

### `sqldim/sinks/motherduck.py`

Two changes: use shared connection factory, and add `motherduck_dbinstance_inactivity_ttl` to prevent silent session expiry on long-running loads.

```python
def __enter__(self) -> "MotherDuckSink":
    from sqldim.sinks._connection import make_connection
    self._con = make_connection()
    self._con.execute(f"ATTACH '{self._path}' AS {self._alias}")
    # Prevent MotherDuck from closing an idle instance mid-load.
    # Default TTL causes silent failures on large xl-tier operations.
    try:
        self._con.execute("SET motherduck_dbinstance_inactivity_ttl='0ms'")
    except Exception:
        pass  # Not a MotherDuck connection (local .duckdb file in tests)
    return self
```

### `sqldim/sinks/postgresql.py`

The `write` method already documents binary copy. Add the `pg_use_binary_copy` flag explicitly — it is enabled by default in the extension but being explicit protects against future version changes — and use the shared connection factory:

```python
def __enter__(self) -> "PostgreSQLSink":
    from sqldim.sinks._connection import make_connection
    self._con = make_connection()
    self._con.execute("INSTALL postgres; LOAD postgres")
    # Binary copy protocol: DuckDB → PostgreSQL without Python serialisation.
    # Default in the extension but explicit here for clarity and future safety.
    self._con.execute("SET pg_use_binary_copy = true")
    self._con.execute(
        f"ATTACH '{self._dsn}' AS {self._alias} (TYPE postgres)"
    )
    return self
```

### `sqldim/sinks/delta.py`

The current `write()` uses a MERGE INTO pattern but the DuckDB Delta extension only supports blind inserts. The method is a placeholder that does not actually work for SCD operations. The delta extension currently only supports blind inserts for write operations. The sink needs honest documentation and a working fallback:

```python
def write(
    self,
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    table_name: str,
    batch_size: int = 100_000,
) -> int:
    """
    Append rows to the Delta table via INSERT INTO (blind insert).

    NOTE: The DuckDB delta extension only supports blind inserts — no MERGE,
    no UPDATE, no DELETE. SCD2 close_versions() is a no-op for this sink.
    If you need mutable SCD operations on Delta, use delta-rs (deltalake
    Python package) to write directly, then read via DeltaSource for queries.
    """
    total = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()[0]
    if total == 0:
        return 0
    con.execute(
        f"INSERT INTO delta.`{self._path}/{table_name}` "
        f"SELECT * FROM {view_name}"
    )
    return total

def close_versions(self, con, table_name, nk_col, nk_view, valid_to) -> int:
    """
    No-op: DuckDB's delta extension does not support UPDATE or DELETE.
    For mutable SCD2 on Delta Lake, use the deltalake Python package directly
    or switch to DuckDBSink/PostgreSQLSink for mutable operations.
    """
    import warnings
    warnings.warn(
        "DeltaLakeSink.close_versions() is a no-op: the DuckDB delta "
        "extension only supports blind inserts. SCD2 version closing is "
        "silently skipped. Use DuckDBSink or PostgreSQLSink for mutable "
        "SCD operations.",
        stacklevel=2,
    )
    return 0
```

### `sqldim/sinks/iceberg.py`

The existing sink's `current_state_sql` uses `iceberg_scan()` which is correct. The `write()` path using PyIceberg `table.append()` is also correct because DuckDB has no write support for Iceberg. However, the `close_versions()` and `update_attributes()` methods load entire partitions into PyArrow, which is a Python memory materialisation. For large tables this will OOM before the sqldim safety floor triggers, because it bypasses the DuckDB buffer manager entirely. Add an explicit size guard:

```python
def write(
    self,
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    table_name: str,
    batch_size: int = 100_000,
) -> int:
    """
    Append new rows to the Iceberg table using PyIceberg + Arrow.

    Writes in batches of batch_size to avoid materialising the entire
    result set in Python memory — the one unavoidable Python materialisation
    point because DuckDB has no native Iceberg write support.
    """
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise ImportError("IcebergSink requires pyarrow") from exc

    iceberg_table = self._catalog.load_table(f"{self._namespace}.{table_name}")
    total = 0
    offset = 0
    while True:
        batch: pa.Table = con.execute(
            f"SELECT * FROM {view_name} LIMIT {batch_size} OFFSET {offset}"
        ).arrow()
        if len(batch) == 0:
            break
        iceberg_table.append(batch)
        total += len(batch)
        offset += batch_size
    return total
```

---

## Data generation — `local_gen_data.py`

Activate the commented-out line and add `THREADS` hint for the parallel `ProcessPoolExecutor` case:

```python
duckdb.execute(f"""
    COPY (
    select ...
    from generate_series(1,{rows}) t(row_id)
    ) TO '{output_path}'
    (FORMAT 'parquet',
     COMPRESSION 'zstd',
     ROW_GROUP_SIZE 122880)
""")
```

---

## Implementation priority order

| Priority | Change | Risk | Benchmark impact |
|----------|--------|------|-----------------|
| 1 | `_connection.py` factory | Low | Enables all spill behaviour unconditionally |
| 2 | `duckdb.py` + `motherduck.py` use factory | Low | Fixes Group G xl crash risk |
| 3 | `parquet.py` sink: zstd + ROW_GROUP_SIZE + preserve_insertion_order | Low | File size ↓, xxl RSS ↓ |
| 4 | `parquet.py` source: union_by_name + remote opts | Low | Correctness for multi-file globs |
| 5 | `postgresql.py` use factory + explicit binary copy | Low | Stability on large loads |
| 6 | `csv.py` columns param | Medium | Eliminates double-scan on known schemas |
| 7 | `delta.py` honest no-op docs + write fix | Medium | Correctness — current MERGE INTO is broken |
| 8 | `sources/iceberg.py` new file | Medium | Replaces PyArrow read path with DuckDB native |
| 9 | `iceberg.py` sink batched write | Medium | Prevents Python OOM on large Iceberg appends |
| 10 | `delta.py` source ATTACH mode | Low | Metadata caching for repeated delta scans |
| 11 | `local_gen_data.py` activate zstd line | Low | Smaller test data files, better parallelism |

Items 1–5 can ship together as a single PR with no API changes and no new dependencies. Items 6–11 each have a narrow blast radius and can ship individually.