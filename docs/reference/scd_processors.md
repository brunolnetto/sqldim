# SCD Processors Reference

sqldim provides two families of SCD processors: **lazy** (pure SQL inside DuckDB) and **vectorised** (Narwhals in-memory with Polars/Pandas backend).

## Choosing a Processor

| Factor | Lazy Processors | NarwhalsSCDProcessor |
|---|---|---|
| Scale | Billions of rows | Up to ~100M rows |
| Memory | Zero Python rows | DataFrame in memory |
| Backend | DuckDB only | Polars, Pandas, Arrow |
| Use with | `DuckDBSink` + `SourceAdapter` | `DimensionalLoader` + DataFrames |
| SCD Types | 1, 2, 3, 4, 5, 6 | 2 (with T1/T6 columns handled by Field metadata) |

---

## Lazy SCD Processors

All lazy processors live under `sqldim.core.kimball.dimensions.scd.processors` and share a common interface: they accept a source (SQL reference), a sink, and a DuckDB connection, then execute change detection entirely in SQL.

### Common Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `natural_key` | `str` or `List[str]` | *required* | Column(s) that identify the business entity |
| `track_columns` | `List[str]` | *required* (varies) | Columns to monitor for changes |
| `sink` | `DuckDBSink` | *required* | Target sink for writes |
| `batch_size` | `int` | `100_000` | Rows per flush |
| `con` | `duckdb.DuckDBPyConnection` | `None` | DuckDB connection (required for SQL execution) |

### .process() Method

All lazy processors expose the same method signature:

```python
def process(self, source, table_name: str) -> SCDResult: ...
```

| Parameter | Type | Description |
|---|---|---|
| `source` | `SourceAdapter` or SQL string | Input data |
| `table_name` | `str` | Target table name in the sink |

Returns an `SCDResult` with counts of inserted, updated, and closed rows.

### LazySCDProcessor (Type 2)

Full history with valid-from / valid-to ranges.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazySCDProcessor
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
    proc.process(customers_source, "dim_customer")
```

**Logic:** Materialises a slim fingerprint (NK + checksum) from the sink, joins against incoming data, closes expired versions, and inserts new versions.

### LazyType1Processor (Type 1)

Overwrite in place — no history retained.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazyType1Processor

proc = LazyType1Processor(
    natural_key="customer_id",
    track_columns=["status"],
    sink=sink,
    con=con,
)
proc.process(customers_source, "dim_customer")
```

**Logic:** Issues SQL UPDATE statements to overwrite tracked columns where the natural key matches.

### LazyType3Processor (Type 3)

Previous + current column pairs for a fixed number of changes.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazyType3Processor

proc = LazyType3Processor(
    natural_key="customer_id",
    column_pairs=[("tier", "previous_tier"), ("region", "previous_region")],
    sink=sink,
    con=con,
)
proc.process(customers_source, "dim_customer")
```

| Parameter | Type | Description |
|---|---|---|
| `column_pairs` | `List[Tuple[str, str]]` | List of `(current_column, previous_column)` tuples |

**Logic:** On change, shifts the current value to the previous column and writes the new value to the current column.

### LazyType6Processor (Type 6 — Hybrid T1 + T2)

Combines Type 1 (overwrite) and Type 2 (version) in a single dimension.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazyType6Processor

proc = LazyType6Processor(
    natural_key="customer_id",
    type1_columns=["status"],          # overwritten in place
    type2_columns=["plan_tier", "country"],  # versioned
    sink=sink,
    con=con,
)
proc.process(customers_source, "dim_customer")
```

| Parameter | Type | Description |
|---|---|---|
| `type1_columns` | `List[str]` | Columns tracked with Type 1 (overwrite) |
| `type2_columns` | `List[str]` | Columns tracked with Type 2 (versioned) |

**Logic:** Computes separate checksums for T1 and T2 attribute groups. T1-only changes update in place; any T2 change triggers a new version row (with T1 values copied forward).

### LazyType4Processor (Type 4 — Mini-Dimension)

Splits rapidly changing attributes into a separate mini-dimension table.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazyType4Processor

proc = LazyType4Processor(
    natural_key="customer_id",
    base_columns=["name", "country"],
    mini_dim_columns=["tier", "credit_score"],
    base_dim_table="dim_customer",
    mini_dim_table="dim_customer_tier",
    mini_dim_fk_col="tier_key",
    mini_dim_id_col="id",
    sink=sink,
    con=con,
)
proc.process(customers_source, "dim_customer")
```

| Parameter | Type | Description |
|---|---|---|
| `base_columns` | `List[str]` | Slowly-changing columns on the base dimension |
| `mini_dim_columns` | `List[str]` | Rapidly-changing columns split to the mini-dimension |
| `base_dim_table` | `str` | Base dimension table name |
| `mini_dim_table` | `str` | Mini-dimension table name |
| `mini_dim_fk_col` | `str` | FK column name on the base table pointing to mini-dim |
| `mini_dim_id_col` | `str` | PK column name on the mini-dimension (default `"id"`) |

### LazyType5Processor (Type 5)

Extends Type 4 by adding a "current" FK reference on the base dimension.

```python
from sqldim.core.kimball.dimensions.scd.processors import LazyType5Processor

proc = LazyType5Processor(
    natural_key="customer_id",
    base_columns=["name", "country"],
    mini_dim_columns=["tier", "credit_score"],
    base_dim_table="dim_customer",
    mini_dim_table="dim_customer_tier",
    mini_dim_fk_col="tier_key",
    current_tier_key_col="current_tier_key",  # Type 5 addition
    sink=sink,
    con=con,
)
```

---

## NarwhalsSCDProcessor

In-memory vectorised SCD using Narwhals (Polars or Pandas backend). Designed for the `DimensionalLoader` vectorised path.

```python
from sqldim.core.processors.scd_engine import NarwhalsSCDProcessor

proc = NarwhalsSCDProcessor(
    natural_key=["email"],
    track_columns=["plan_tier", "country"],
)
```

### Constructor

```python
def __init__(
    self,
    natural_key: List[str],
    track_columns: List[str],
    ignore_columns: List[str] | None = None,
): ...
```

| Parameter | Type | Description |
|---|---|---|
| `natural_key` | `List[str]` | Column(s) identifying the business entity |
| `track_columns` | `List[str]` | Columns to monitor for changes |
| `ignore_columns` | `List[str]` \| `None` | Columns to exclude from hashing |

### .process() Method

```python
def process(
    self,
    incoming: nw.DataFrame,
    current_scd: nw.DataFrame,
    as_of: datetime | None = None,
) -> SCDResult: ...
```

| Parameter | Type | Description |
|---|---|---|
| `incoming` | `nw.DataFrame` | New data batch (Narwhals-compatible: Polars, Pandas, Arrow) |
| `current_scd` | `nw.DataFrame` | Current state of the dimension table |
| `as_of` | `datetime \| None` | Point-in-time for SCD validity |

Returns `SCDResult` with `.to_close` and `.to_insert` DataFrames for the caller to commit.

```python
import polars as pl
from sqldim.core.processors.scd_engine import NarwhalsSCDProcessor

proc = NarwhalsSCDProcessor(
    natural_key=["customer_id"],
    track_columns=["plan_tier", "country"],
)

incoming = pl.read_csv("daily_customers.csv")
current = pl.read_database("SELECT * FROM dim_customer WHERE is_current = TRUE", con)

result = proc.process(incoming, current)
# result.to_close  — rows to expire (set valid_to, is_current=False)
# result.to_insert  — new version rows to insert
```

### How It Works

1. **Hash** — compute MD5/SHA of tracked columns for all rows (vectorised, C/Rust native)
2. **Classify** — LEFT JOIN incoming on current by natural key
   - Right is `NULL` → **new record**
   - Hash mismatch → **changed record**
   - Otherwise → **unchanged** (skip)
3. **Return** — `SCDResult` with separate DataFrames for close and insert operations

## SCD Type Quick Reference

| Type | History | Lazy Processor | Use Case |
|---|---|---|---|
| 1 | None (overwrite) | `LazyType1Processor` | Correction fields, status flags |
| 2 | Full (valid-from/to) | `LazySCDProcessor` | Customer attributes, product hierarchy |
| 3 | Fixed (current + previous) | `LazyType3Processor` | Limited history windows (e.g., last 2 values) |
| 4 | Mini-dimension split | `LazyType4Processor` | Rapidly-changing attributes (credit score) |
| 5 | Type 4 + current FK | `LazyType5Processor` | Type 4 with easy "current state" joins |
| 6 | Hybrid T1 + T2 | `LazyType6Processor` | Mix of overwrite and versioned on one table |

## See Also

- [Dimensions Reference](dimensions.md) — `DimensionModel.__scd_type__`, `SCD2Mixin`, `Field(scd=...)`
- [Hybrid SCD Pattern](../patterns/hybrid_scd.md) — mixing typed columns with JSONB for flexible versioning
- [Loader Reference](loader.md) — how `DimensionalLoader` dispatches SCD logic
- [Lazy Loaders Guide](../guides/lazy_loaders.md) — DuckDB-native loading patterns
- [Big Data Guide](../guides/big_data.md) — end-to-end pipeline with lazy processors
