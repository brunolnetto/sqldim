# Big Data with sqldim

sqldim is designed as a **SQL-first, zero-copy dimensional engine**.  All
heavy transformation work stays inside DuckDB; Python is the coordinator, not
the data carrier.  This lets a single laptop process billions of rows with the
same code that runs on MotherDuck or a distributed Iceberg lake.

---

## Architecture at a glance

```
┌─────────────────────────────────────────────────────────────────────┐
│  Layer 1 — SOURCES                                                  │
│  CSVSource · ParquetSource · DeltaSource · DuckDBSource · PGSource  │
└────────────────────────────┬────────────────────────────────────────┘
                             │  lazy SQL reference (no data in Python)
┌────────────────────────────▼────────────────────────────────────────┐
│  Layer 2 — PROCESSORS                                               │
│  LazySCDProcessor (T1-T6) · NarwhalsSCDProcessor                    │
└────────────────────────────┬────────────────────────────────────────┘
                             │  SQL INSERT / MERGE (single statement)
┌────────────────────────────▼────────────────────────────────────────┐
│  Layer 3 — LOADERS                                                  │
│  AccumulatingLoader · CumulativeLoader · BitmaskerLoader            │
│  ArrayMetricLoader  · SnapshotLoader                                │
└────────────────────────────┬────────────────────────────────────────┘
                             │  batched writes (default 100 k rows)
┌────────────────────────────▼────────────────────────────────────────┐
│  Layer 4 — SINKS                                                    │
│  DuckDBSink · MotherDuckSink · ParquetSink · DeltaLakeSink          │
│  IcebergSink · PostgreSQLSink                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Layer 1 — Sources

Sources are **lazy SQL references**, not in-memory DataFrames.  They produce
a SQL string that runs inside DuckDB; Python never sees individual rows.

```python
from sqldim.sources import ParquetSource, DuckDBSource

# Read a 10 GB Parquet file — zero Python memory
events = ParquetSource("s3://my-bucket/events/*.parquet")

# Read from an existing DuckDB database
products = DuckDBSource("analytics.duckdb", query="SELECT * FROM products")
```

Available sources:

| Class | Backend | Use case |
|---|---|---|
| `CSVSource` | DuckDB `read_csv` | Local or S3 CSV/TSV files |
| `ParquetSource` | DuckDB `read_parquet` | Columnar files, object storage |
| `DeltaSource` | delta-kernel-python | Delta Lake tables |
| `DuckDBSource` | DuckDB SQL | Existing DuckDB / MotherDuck |
| `PostgreSQLSource` | psycopg2 streaming | Operational databases |

---

## Layer 2 — Processors

Processors implement dimensional patterns entirely in SQL.  A billion-row
SCD2 diff is **one JOIN** — no row-by-row Python loop.

### Lazy SCD processors (pure SQL)

```python
from sqldim.processors import LazySCDProcessor
from sqldim.sinks import DuckDBSink
import duckdb

con = duckdb.connect("analytics.duckdb")
with DuckDBSink(con) as sink:
    proc = LazySCDProcessor(
        natural_key="customer_id",
        track_columns=["plan_tier", "country"],
        sink=sink,
        con=con,
    )
    proc.process(customers.snapshot(), "dim_customer")   # initial load
    proc.process(customers.event_batch(1), "dim_customer")  # incremental
```

All SCD types share the same interface:

| Class | SCD Type | Description |
|---|---|---|
| `LazySCDProcessor` | Type 2 | Full history with valid-from / valid-to |
| `LazyType1Processor` | Type 1 | Overwrite — no history |
| `LazyType3Processor` | Type 3 | Previous + current column pair |
| `LazyType4Processor` | Type 4 | Mini-dimension split |
| `LazyType5Processor` | Type 5 | Extends Type 4 with current key |
| `LazyType6Processor` | Type 6 | Hybrid T1 + T2 + T3 |

### Narwhals vectorised processor

For single-process scale (up to ~100 M rows) where you want a Pandas or
Polars backend:

```python
from sqldim.processors.scd_engine import NarwhalsSCDProcessor

proc = NarwhalsSCDProcessor(
    natural_key=["email"],
    track_columns=["plan_tier", "meta"],
)
# Pass any Narwhals-compatible DataFrame (Pandas, Polars, Arrow …)
changed = proc.detect_changes(old_df, new_df)
```

---

## Layer 3 — Loaders

Loaders consume a Source and write dimension or fact tables, always in
`batch_size` chunks so Python memory stays bounded.

```python
from sqldim.loaders import CumulativeLoader, BitmaskerLoader

# Dense-history array loader — builds the "players_cumulated" pattern
loader = CumulativeLoader(
    source_model=PlayerSeasons,
    target_model=Player,
    session=session,
    batch_size=100_000,   # default; tune for your workload
)
loader.load()
```

| Loader | Pattern | When to use |
|---|---|---|
| `AccumulatingLoader` | Accumulating snapshot | Multi-milestone fact (order lifecycle) |
| `CumulativeLoader` | Dense history arrays | "All seasons so far" per entity |
| `BitmaskerLoader` | 32-bit bitmask datelist | L7/L28 retention metrics |
| `ArrayMetricLoader` | Month-partitioned arrays | Monthly cohort arrays |
| `SnapshotLoader` | Periodic snapshot | Daily/weekly dim snapshots |

---

## Layer 4 — Sinks

Sinks are context managers that buffer writes and flush in `batch_size` chunks.

```python
from sqldim.sinks import ParquetSink, MotherDuckSink

# Local Parquet — ideal for data-lake landing zones
with ParquetSink("output/dim_customer/", partition_by="country") as sink:
    proc = LazySCDProcessor("customer_id", ["plan_tier"], sink, con=con)
    proc.process(customers.snapshot(), "dim_customer")

# MotherDuck — cloud-scale DuckDB
with MotherDuckSink("md:analytics") as sink:
    proc.process(customers.event_batch(1), "dim_customer")
```

| Sink | Storage | Notes |
|---|---|---|
| `DuckDBSink` | Local DuckDB file | Development, CI |
| `MotherDuckSink` | Cloud DuckDB | Production analytics |
| `ParquetSink` | Columnar files | Data-lake, export |
| `DeltaLakeSink` | Delta Lake | ACID lakehouse |
| `IcebergSink` | Apache Iceberg | Open table format |
| `PostgreSQLSink` | PostgreSQL | Operational BI |

---

## Scale characteristics

| Characteristic | Approach |
|---|---|
| **1 B-row SCD diff** | Single SQL JOIN — database handles it |
| **Memory ceiling** | `batch_size` rows max in Python at any time |
| **Columnar reads** | ParquetSource / DeltaSource stream columns, not full rows |
| **Parallel ingestion** | Multiple `LazySCDProcessor` instances share one DuckDB connection |
| **Cloud scale** | MotherDuckSink routes the same SQL to the cloud |
| **Backend swap** | Narwhals lets you switch Pandas ↔ Polars with one import |

---

## Quick start — big-data pipeline

```python
import duckdb
from sqldim.sources import ParquetSource
from sqldim.processors import LazySCDProcessor
from sqldim.sinks import DeltaLakeSink

con = duckdb.connect()
customers = ParquetSource("s3://raw/customers/*.parquet")

with DeltaLakeSink("s3://lakehouse/dim_customer/") as sink:
    proc = LazySCDProcessor(
        natural_key="customer_id",
        track_columns=["plan_tier", "country", "email"],
        sink=sink,
        con=con,
        batch_size=500_000,
    )
    proc.process(customers, "dim_customer")
```

Run `sqldim bigdata features` from the command line for a quick in-terminal
summary of every layer.
