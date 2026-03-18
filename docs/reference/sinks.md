# Sinks Reference

Sinks are the write boundary between sqldim's DuckDB execution engine and your storage backend. Every sink implements the `SinkAdapter` protocol — loaders don't know or care whether they're writing to a local file, PostgreSQL, or a cloud data lake.

## Protocol Overview

`SinkAdapter` defines seven methods, grouped by function:

### Core (SCD Type 2)

| Method | Purpose |
|--------|---------|
| `current_state_sql(table_name)` | Returns a SQL fragment DuckDB can use in a `FROM` clause to read existing data |
| `write(con, view_name, table_name, batch_size)` | Stream rows from a DuckDB view into the sink — **the only materialization point** |
| `write_named(con, view_name, table_name, columns, batch_size)` | Like `write` but inserts only the listed columns (for tables with auto-generated columns like `BIGSERIAL`) |
| `close_versions(con, table_name, nk_col, nk_view, valid_to)` | Mark current rows as historical (set `is_current=FALSE`, `valid_to`) |

### Extended (SCD Type 1 / 3 / Accumulating)

| Method | Purpose |
|--------|---------|
| `update_attributes(con, table_name, nk_col, updates_view, update_cols)` | Overwrite columns in place — SCD Type 1 |
| `rotate_attributes(con, table_name, nk_col, rotations_view, column_pairs)` | Shift current→previous columns — SCD Type 3 |
| `update_milestones(con, table_name, match_col, updates_view, milestone_cols)` | Patch NULL milestone columns — accumulating snapshots |

### Additional methods (not in protocol, available on some sinks)

| Method | Sinks | Purpose |
|--------|-------|---------|
| `prefetch_hashes(con, table_name, nk_cols, hash_col, where)` | DuckDB, PostgreSQL, MotherDuck | Pull NK+hash fingerprint into a local DuckDB TABLE for fast joins |
| `upsert(con, view_name, table_name, conflict_cols, returning_col, output_view)` | DuckDB, MotherDuck | Insert distinct rows, auto-assign IDs, create a lookup view |

## Capabilities Matrix

| Capability | DuckDB | PostgreSQL | MotherDuck | Parquet | Iceberg | Delta |
|:-----------|:------:|:----------:|:----------:|:-------:|:-------:|:-----:|
| `write` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `write_named` | ✅ | ✅ | — | ✅ | — | — |
| `close_versions` | ✅ UPDATE | ✅ UPDATE | ✅ UPDATE | ✅ rewrite | ✅ overwrite | ⚠️ no-op |
| `update_attributes` | ✅ UPDATE | ✅ UPDATE | ✅ UPDATE | ✅ rewrite | ✅ overwrite | ✅ MERGE |
| `rotate_attributes` | ✅ UPDATE | ✅ UPDATE | ✅ UPDATE | ✅ rewrite | ✅ overwrite | ✅ MERGE |
| `update_milestones` | ✅ UPDATE | ✅ UPDATE | ✅ UPDATE | ✅ rewrite | ✅ overwrite | ✅ MERGE |
| `prefetch_hashes` | ✅ | ✅ | ✅ | — | — | — |
| `upsert` | ✅ | — | ✅ | — | — | — |
| Context manager | ✅ | ✅ | ✅ | — | ✅ | ✅ |
| S3/GCS/Azure | — | — | — | ✅ | ✅ | ✅ |
| Mutation strategy | In-place UPDATE | In-place UPDATE via pg extension | In-place UPDATE via ATTACH | Partition rewrite | Read-modify-write (Arrow) | MERGE INTO |

**Legend**: ✅ = supported, — = not implemented, ⚠️ = handled differently (see notes below)

## Sink Details

### DuckDBSink

Local file-based DuckDB database. The default choice for development, testing, and single-node workloads.

```python
from sqldim.sinks import DuckDBSink

with DuckDBSink("warehouse.duckdb") as sink:
    sink.write(con, "new_rows", "dim_customer")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | required | Path to the `.duckdb` file |
| `schema` | `str` | `"main"` | Target schema |

**Strengths**: Fastest option (no network), zero dependencies, all protocol methods implemented including `upsert`.

**Limitations**: Single-node only, not suitable for concurrent writes from multiple processes.

---

### PostgreSQLSink

PostgreSQL via DuckDB's `postgres` extension. All reads and writes are expressed as DuckDB SQL — no psycopg2 in the hot path. Uses `pg_use_binary_copy` for efficient streaming inserts.

```python
from sqldim.sinks import PostgreSQLSink

with PostgreSQLSink("postgresql://user:pass@host:5432/db", schema="dw") as sink:
    sink.write(con, "new_rows", "dim_customer")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dsn` | `str` | required | PostgreSQL connection string |
| `schema` | `str` | `"public"` | Target schema |

**Strengths**: Production-grade durability, supports concurrent readers/writers, `prefetch_hashes` avoids repeated cross-network scans.

**Limitations**: Requires DuckDB's postgres extension (auto-installed on `__enter__`). `upsert` not implemented — use `write_named` with pre-processed data instead.

**Tip**: Call `prefetch_hashes()` once before running SCD processors to pull the NK+checksum fingerprint into local DuckDB. This avoids repeated cross-network round-trips during change detection.

---

### MotherDuckSink

MotherDuck (cloud-hosted DuckDB) via the `ATTACH` pattern. The same SQL works whether targeting MotherDuck cloud or a local `.duckdb` file for testing.

```python
from sqldim.sinks import MotherDuckSink

# Cloud
with MotherDuckSink(db="my_warehouse") as sink:
    sink.write(con, "new_rows", "dim_customer")

# Local stand-in (for testing / CI)
with MotherDuckSink(db="/tmp/local.duckdb") as sink:
    sink.write(con, "new_rows", "dim_customer")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `db` | `str` | required | Database name (cloud) or `.duckdb` file path (local) |
| `token` | `str` | `None` | API token; falls back to `MOTHERDUCK_TOKEN` env var |
| `schema` | `str` | `"main"` | Target schema |

**Strengths**: Fully-featured DuckDB in the cloud, supports `upsert` and `prefetch_hashes`, drop-in replacement for `DuckDBSink`.

**Limitations**: Requires MotherDuck account. Network latency for large cross-network scans (mitigated by `prefetch_hashes`).

---

### ParquetSink

Immutable file-based storage. Supports local disk and cloud object storage (S3, GCS, Azure) via URIs. Because Parquet is immutable, every mutation is a **full partition rewrite** expressed as a DuckDB `COPY` statement.

```python
from sqldim.sinks import ParquetSink

# Local
sink = ParquetSink("/data/warehouse")

# S3
sink = ParquetSink("s3://my-bucket/warehouse/")

sink.write(con, "new_rows", "dim_customer")  # no context manager needed
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_path` | `str` | required | Root directory or URI (supports `s3://`, `gs://`, `az://`) |

**Strengths**: Cloud-native, works with any downstream engine (Spark, Trino, DuckDB, etc.), no database process to manage.

**Limitations**: No context manager. Partition rewrites are expensive for large datasets — every `close_versions` or `update_attributes` rewrites the entire `is_current=TRUE` partition. No `prefetch_hashes` or `upsert`. Not suitable for high-frequency incremental updates.

**Partitioning**: Data is automatically partitioned by `is_current` (each table gets `.../table_name/is_current=true/` and `.../table_name/is_current=false/` subdirectories).

---

### IcebergSink

Apache Iceberg via PyIceberg for catalog management + DuckDB's `iceberg_scan()` for reads. Writes go through PyIceberg's `Table.append()` (Arrow-backed) to keep the Iceberg commit chain consistent.

```python
from sqldim.sinks import IcebergSink

with IcebergSink(
    catalog_name="rest",
    namespace="warehouse",
    catalog_config={"uri": "http://localhost:8181"},
) as sink:
    sink.write(con, "new_rows", "dim_customer")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `catalog_name` | `str` | required | PyIceberg catalog name |
| `namespace` | `str` | required | Iceberg namespace (database) |
| `catalog_config` | `dict` | `{}` | Catalog properties (URI, S3 creds, etc.) |
| `table_location_base` | `str` | `None` | Base URI for table warehouse locations |
| `max_python_rows` | `int` | `500,000` | Safety limit for mutation operations |

**Strengths**: Full ACID transactions via Iceberg, time-travel queries, compatible with Spark/Trino/Flink.

**Limitations**: Mutation operations (`close_versions`, `update_attributes`, `rotate_attributes`, `update_milestones`) use a **read-modify-write pattern** — the full table is loaded into Arrow, modified in Python, and overwritten. This is gated by `max_python_rows` (raises `MemoryError` if exceeded). No `write_named`, `prefetch_hashes`, or `upsert`.

**Dependencies**: `pip install "pyiceberg[hive,s3]" pyarrow`

**Important**: For SCD-heavy workloads on large tables, prefer DuckDB, PostgreSQL, or Delta sinks where mutations are SQL-native. Use Iceberg when you need its catalog/time-travel features and your dimension tables fit within the `max_python_rows` limit.

---

### DeltaLakeSink

Delta Lake via DuckDB's `delta` extension. Uses `MERGE INTO` for writes — new rows are inserted and changed rows have their current version closed in a single atomic statement.

```python
from sqldim.sinks import DeltaLakeSink

with DeltaLakeSink("/data/delta_warehouse", natural_key="email") as sink:
    sink.write(con, "classified", "dim_customer")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | required | Base directory for Delta tables |
| `natural_key` | `str` | `"id"` | Column used for MERGE match condition |

**Strengths**: Atomic `MERGE INTO` handles insert + version-close in one statement. All mutations use `MERGE INTO` — efficient for Delta's file-based storage.

**Limitations**: `close_versions()` is a **no-op** — version closing is handled inside `write()` via the MERGE statement, so the source view must include an `_scd_class` column (`'new'`, `'changed'`, `'unchanged'`). No `write_named`, `prefetch_hashes`, or `upsert`.

**Important**: The `write()` method expects the source view to contain an `_scd_class` column that classifies each row. This is produced by `LazySCDProcessor` — if you're using `DimensionalLoader` (the standard path), use a different sink.

## Choosing a Sink

```
What's your target?
│
├── Local development / testing
│   └── DuckDBSink
│
├── Production relational database
│   └── PostgreSQLSink
│
├── Cloud DuckDB
│   └── MotherDuckSink
│
├── Data lake (immutable, any downstream engine)
│   └── ParquetSink
│
├── Data lake (ACID + time-travel needed)
│   ├── Small-to-medium dimensions (< 500K rows)
│   │   └── IcebergSink
│   └── Large dimensions / SCD-heavy workloads
│       └── DeltaLakeSink
```

## All Sinks Follow One Pattern

Every sink operation follows the same three-step flow inside the DuckDB execution engine:

```
1. CREATE VIEW ... AS (transform/source data)
2. sink.method(con, view_name, table_name, ...)
3. Data flows: DuckDB → Sink → Storage
```

The loader/processor creates a DuckDB VIEW representing the result of its transformation. The sink's method consumes that view and writes to storage. This means **the same processor code works with any sink** — swap `DuckDBSink` for `PostgreSQLSink` and everything just works.
