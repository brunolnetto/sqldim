# Big Data with sqldim

The lazy part of sqldim keeps the transformation work inside DuckDB: a source
is a SQL reference, a processor or loader issues SQL statements, and a sink
writes with SQL. Python coordinates and does not hold the rows. How large a
workload this handles depends on DuckDB and on your machine; sqldim publishes
no benchmark that states a row count.

---

## Architecture at a glance

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCES      ParquetSource · CSVSource · DeltaSource · DuckDBSource │
│               PostgreSQLSource · IcebergSource · …                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │  a SQL reference (no data in Python)
┌────────────────────────────▼────────────────────────────────────────┐
│  PROCESSORS   LazySCDProcessor · LazyType1/3/4/5/6Processor          │
│  LOADERS      LazyCumulativeLoader · LazyBitmaskLoader · …           │
└────────────────────────────┬────────────────────────────────────────┘
                             │  SQL statements over one connection
┌────────────────────────────▼────────────────────────────────────────┐
│  SINKS        DuckDBSink · PostgreSQLSink · MotherDuckSink           │
│               ParquetSink · DeltaLakeSink · IcebergSink              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Sources

A source produces the SQL a DuckDB connection reads it with:

```python
from sqldim.sources import DuckDBSource, ParquetSource

events = ParquetSource("s3://my-bucket/events/*.parquet")
products = DuckDBSource("products")  # a table or view of the connection
print(events.as_sql(None))    # read_parquet('s3://my-bucket/events/*.parquet')
print(products.as_sql(None))  # SELECT * FROM products
```

| Class | Reads with | Use case |
|---|---|---|
| `CSVSource` | DuckDB `read_csv` | Local or S3 CSV/TSV files |
| `ParquetSource` | DuckDB `read_parquet` | Columnar files, object storage |
| `DeltaSource` | DuckDB `delta` extension | Delta Lake tables |
| `DuckDBSource` | DuckDB SQL | A table or view of the connection |
| `PostgreSQLSource` | DuckDB's postgres extension | Operational databases |

---

## An SCD2 load end to end

`LazySCDProcessor` classifies incoming rows against the current table by a
checksum of the tracked columns and writes new rows and new versions with
SQL. Two things it needs:

- **an open sink.** A sink attaches its storage to its own DuckDB connection
  on `__enter__`. A processor given the sink and no `con` writes through
  that connection, `sink.connection`, which you also use for your own
  statements; outside the `with` block it raises `SinkNotOpen`.
- **an existing target table** with the source columns and `valid_from`,
  `valid_to`, `is_current`, `checksum`. `DuckDBSink.write` inserts by name,
  so the column order does not matter.

```python
import duckdb

from sqldim import DuckDBSink
from sqldim.core.kimball.dimensions.scd.processors import LazySCDProcessor
from sqldim.sources import ParquetSource

for version, tier in (("v1", "free"), ("v2", "pro")):
    duckdb.sql(
        f"COPY (SELECT * FROM (VALUES ('c1', '{tier}', 'BR'), ('c2', 'pro', 'US'))"
        f" t(customer_id, plan_tier, country)) TO 'customers_{version}.parquet'"
    )
duckdb.connect("analytics.duckdb").execute(
    "CREATE TABLE dim_customer (customer_id VARCHAR, plan_tier VARCHAR,"
    " country VARCHAR, valid_from VARCHAR, valid_to VARCHAR,"
    " is_current BOOLEAN, checksum VARCHAR)"
).close()

with DuckDBSink("analytics.duckdb") as sink:
    proc = LazySCDProcessor(
        natural_key="customer_id",
        track_columns=["plan_tier", "country"],
        sink=sink,
    )
    first = proc.process(ParquetSource("customers_v1.parquet"), "dim_customer")
    second = proc.process(ParquetSource("customers_v2.parquet"), "dim_customer")
    print(first.inserted, second.versioned, second.unchanged)  # 2 1 1
    rows = sink.connection.execute(
        "SELECT customer_id, plan_tier, is_current"
        " FROM sqldim_ddb.dim_customer ORDER BY 1, 3"
    ).fetchall()
    print(rows)  # [('c1', 'free', False), ('c1', 'pro', True), ('c2', 'pro', True)]
```

The other SCD types share the constructor and `process(source, table)`:

| Class | SCD Type | Description |
|---|---|---|
| `LazySCDProcessor` | Type 2 | Full history with valid-from / valid-to |
| `LazyType1Processor` | Type 1 | Overwrite — no history |
| `LazyType3Processor` | Type 3 | Previous + current column pair |
| `LazyType4Processor` | Type 4 | Mini-dimension split |
| `LazyType5Processor` | Type 5 | Extends Type 4 with current key |
| `LazyType6Processor` | Type 6 | Hybrid T1 + T2 + T3 |

All are importable from `sqldim.core.kimball.dimensions.scd.processors`.
`NarwhalsSCDProcessor` (`from sqldim import NarwhalsSCDProcessor`) is the
eager variant over a pandas or Polars DataFrame; it holds the data in
Python.

---

## Loaders

The lazy loaders (`LazyCumulativeLoader`, `LazyBitmaskLoader`,
`LazyArrayMetricLoader`, `LazyAccumulatingLoader`, `LazySnapshotLoader`,
`LazyTransactionLoader`, `LazyEdgeProjectionLoader`, all exported by
`sqldim`) follow the same pattern: a target table and an open sink.
[Lazy Loaders](lazy_loaders.md) documents each one.

---

## Sinks

| Sink | Constructor | Support (generated) |
|---|---|---|
| `DuckDBSink` | `DuckDBSink(path, schema="main")` | append, SCD2 close, merge — executed in CI |
| `PostgreSQLSink` | `PostgreSQLSink(dsn, schema="public")` | append, SCD2 close, merge — executed in CI against PostgreSQL |
| `MotherDuckSink` | `MotherDuckSink(db, token=None)` | supported; storage not executed in CI |
| `ParquetSink` | `ParquetSink(base_path, partition_by=None)` | append supported (new files, never an overwrite); SCD2 close and merge partial; not a context manager |
| `DeltaLakeSink` | `DeltaLakeSink(path, natural_key="id")` | append only — closing an SCD2 version or a merge raises `SinkUnsupported`; executed in CI on an empty Delta table |
| `IcebergSink` | `IcebergSink(catalog_name, namespace, catalog_config=None)` | partial; needs `pyiceberg` and a catalog; storage not executed in CI |

The support levels come from the generated
[capability page](../reference/capabilities.md#write-sinks-62-rule-8); the
[backend matrix](../reference/dgm-backend-matrix.md) says what ran.
