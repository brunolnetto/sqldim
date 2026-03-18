# Lazy Loaders — Big Data Without Python

The **lazy loaders** are sqldim's highest-performance components. They skip Python data structures entirely — instead, they generate DuckDB SQL views and stream results through a sink. No DataFrames in memory, no row-by-row loops, no OOM risk.

This guide explains when to use lazy loaders, how each one works, and how to set them up.

## Standard vs. Lazy: When to Use What

| Factor | Standard Loaders | Lazy Loaders |
|--------|-----------------|--------------|
| Data size | Up to a few million rows | Millions to billions of rows |
| Data location | Python dicts, Narwhals DataFrames | Parquet files, CSV files, DuckDB tables |
| Processing engine | Polars / Pandas (in-memory) | DuckDB SQL (on-disk) |
| Memory usage | Proportional to dataset size | Constant (streamed) |
| SCD support | Full Type 1/2/3/6 | Specialized patterns only |

**Use standard loaders** (`DimensionalLoader`) for your initial pipeline build and when SCD handling is required.

**Use lazy loaders** when your source data lives in Parquet/CSV on disk and the processing pattern matches one of the four supported strategies.

## Shared Setup Pattern

All lazy loaders follow the same structure:

```python
from sqldim.sinks.duckdb import DuckDBSink

with DuckDBSink("my_data.duckdb") as sink:
    loader = SomeLazyLoader(
        table_name="target_table",
        ...,
        sink=sink,
    )
    rows = loader.process("source_data.parquet")
```

### Key concepts

- **`sink`** — A `DuckDBSink` (required). The sink provides the connection to the target database and handles writing.
- **`source`** — The `process()` method accepts a file path string or a `SourceAdapter`. Paths are auto-detected: `.parquet` → `ParquetSource`, `.csv` → `CSVSource`, anything else → `DuckDBSource` (table/view name). For S3 or other remote paths without extensions, pass an explicit source adapter (e.g., `ParquetSource("s3://bucket/data/*.parquet")`).
- **`con`** — Optional. Pass an existing DuckDB connection instead of creating a new one (useful when chaining multiple loaders).
- **`batch_size`** — Defaults to 100,000. Controls the write batch size via the sink.

## LazyCumulativeLoader

**Pattern**: Array-of-Structs append via FULL OUTER JOIN.

Use this when tracking longitudinal history — e.g., a player's per-season stats, a user's monthly activity summary, or any scenario where each period adds a struct to a growing array.

### How it works

1. Reads yesterday's state from the sink (existing table)
2. Reads today's batch from the source (Parquet/CSV)
3. FULL OUTER JOIN on the partition key
4. For active members: `list_append(history, struct_pack(metrics, period))`
5. For dormant members: keeps history, increments `years_since_last_active`
6. Writes the merged result back

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Target table in the sink |
| `partition_key` | `str` | Column that identifies the entity (e.g., `"player_name"`) |
| `cumulative_column` | `str` | Column holding the history array (e.g., `"seasons"`) |
| `metric_columns` | `List[str]` | Columns to pack into each period's struct (e.g., `["pts", "ast", "reb"]`) |
| `sink` | `DuckDBSink` | Target sink |
| `current_period_column` | `str` | Name of the "current period" column (default: `"current_season"`) |
| `batch_size` | `int` | Write batch size (default: 100,000) |
| `con` | `DuckDBPyConnection` | Optional existing connection |

### `process(today_source, target_period) -> int`

| Parameter | Type | Description |
|-----------|------|-------------|
| `today_source` | `str` or `SourceAdapter` | Path to today's data file |
| `target_period` | `Any` | The period identifier (e.g., `2024`, `"Q1-2024"`) |

### Example

```python
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.core.loaders.cumulative import LazyCumulativeLoader

with DuckDBSink("nba.duckdb") as sink:
    loader = LazyCumulativeLoader(
        table_name="player_seasons_cumulated",
        partition_key="player_name",
        cumulative_column="seasons",
        metric_columns=["pts", "ast", "reb"],
        sink=sink,
    )
    rows = loader.process("2024_season_stats.parquet", target_period=2024)
```

After processing, each row's `seasons` column contains an array of structs:
```sql
[
    {pts: 22.3, ast: 5.1, reb: 4.8, period: 2022},
    {pts: 25.1, ast: 6.0, reb: 5.2, period: 2023},
    {pts: 27.4, ast: 7.2, reb: 5.9, period: 2024}
]
```

## LazyBitmaskLoader

**Pattern**: Date list → 32-bit integer bitmask via bitwise aggregation.

Use this for ultra-lightweight retention tracking. Instead of storing every active date, a 32-bit integer encodes up to 32 days of activity — perfect for L7/L28 metrics.

### How it works

1. Reads source with a date array column (e.g., `["2024-01-03", "2024-01-05", ...]`)
2. `UNNEST` the array to get individual dates
3. `DATEDIFF('day', date, reference_date)` → day offset
4. `SUM(1 << (window - 1 - offset))` → single integer per partition key

### Reading the bitmask

Bit positions are **most-significant first**: bit 31 = reference_date, bit 30 = reference_date - 1, etc.

```python
def count_active_days(bitmask: int, window: int = 32) -> int:
    return bin(bitmask).count("1")

def was_active_on(bitmask: int, day_offset: int, window: int = 32) -> bool:
    return bool(bitmask & (1 << (window - 1 - day_offset)))
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Target table |
| `partition_key` | `str` | Entity identifier (e.g., `"user_id"`) |
| `dates_column` | `str` | Column containing the date array |
| `reference_date` | `str` or `date` | The rightmost day of the window (e.g., `"2024-01-31"`) |
| `sink` | `DuckDBSink` | Target sink |
| `window_days` | `int` | Window size in days (default: 32) |
| `batch_size` | `int` | Write batch size (default: 100,000) |
| `con` | `DuckDBPyConnection` | Optional existing connection |

### `process(source) -> int`

### Example

```python
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.core.loaders.bitmask import LazyBitmaskLoader

with DuckDBSink("retention.duckdb") as sink:
    loader = LazyBitmaskLoader(
        table_name="fact_activity_bitmask",
        partition_key="user_id",
        dates_column="dates_active",
        reference_date="2024-01-31",
        window_days=32,
        sink=sink,
    )
    rows = loader.process("user_activity.parquet")
```

Source data format (each row has a list of active dates):
```
user_id | dates_active
--------|-------------------------------------------
U-001   | ["2024-01-01", "2024-01-03", "2024-01-15"]
U-002   | ["2024-01-31"]
```

Result:
```
user_id | datelist_int
--------|-------------
U-001   | 1610612737   (bits 31, 29, 17 set)
U-002   | 1            (bit 0 set)
```

## LazyArrayMetricLoader

**Pattern**: Daily value → 31-element monthly array via `list_transform`.

Use this for storing daily metric values compactly as a single array per month — e.g., daily revenue, daily active users, daily error counts.

### How it works

1. Reads source with a value column and a date context
2. Computes `day_offset = (target_date - month_start).days`
3. Uses DuckDB `list_transform(range(31), i -> CASE WHEN i = offset THEN value ELSE 0.0 END)` to build the array entirely in SQL

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Target table |
| `partition_key` | `str` | Entity identifier (e.g., `"user_id"`) |
| `value_column` | `str` | Column containing the metric value |
| `metric_name` | `str` | Name label for the metric (e.g., `"monthly_revenue"`) |
| `month_start` | `str` or `date` | First day of the month |
| `sink` | `DuckDBSink` | Target sink |
| `batch_size` | `int` | Write batch size (default: 100,000) |
| `con` | `DuckDBPyConnection` | Optional existing connection |

### `process(source, target_date) -> int`

| Parameter | Type | Description |
|-----------|------|-------------|
| `source` | `str` or `SourceAdapter` | Path to daily data |
| `target_date` | `str` or `date` | The specific day to place in the array |

### Example

```python
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.core.loaders.array_metric import LazyArrayMetricLoader

with DuckDBSink("metrics.duckdb") as sink:
    loader = LazyArrayMetricLoader(
        table_name="fact_monthly_metrics",
        partition_key="user_id",
        value_column="revenue",
        metric_name="monthly_revenue",
        month_start="2024-01-01",
        sink=sink,
    )
    rows = loader.process("daily_revenue.parquet", target_date="2024-01-15")
```

Result — each row gets a 31-element array with the value at index 14:
```
user_id | metric_name       | month_start | metric_array
--------|-------------------|-------------|------------------------------------------
U-001   | monthly_revenue   | 2024-01-01  | [0.0, 0.0, ..., 149.99, ..., 0.0]
                                                     ^ index 14
```

**Note**: This loader performs an insert, not an upsert. For incremental daily builds, manage deduplication at the source or via a pre-processing view.

## LazyEdgeProjectionLoader

**Pattern**: Fact table → Graph edge table via SQL projection or self-join.

Use this to project your fact data into graph edges for the dual-paradigm traversal engine. Two modes:

- **Direct**: Extract (subject, object) pairs from a fact table (e.g., player → game)
- **Self-join**: Generate co-occurrence edges (e.g., player ↔ player who shared a game)

### Direct mode

Simple column projection — reads subject_key and object_key from the source.

### Self-join mode

Joins the source with itself on `self_join_key`, using `f1.subject_key < f2.subject_key` as a symmetry breaker to avoid duplicate edges. Property columns are aggregated with `+` (e.g., shared points = player1 points + player2 points).

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Target edge table |
| `subject_key` | `str` | Column for the source entity |
| `object_key` | `str` | Column for the target entity |
| `sink` | `DuckDBSink` | Target sink |
| `property_map` | `Dict[str, str]` | Map source columns to edge columns (default: `{}`) |
| `self_join` | `bool` | Enable self-join mode (default: `False`) |
| `self_join_key` | `str` | Join key for self-join mode (required when `self_join=True`) |
| `batch_size` | `int` | Write batch size (default: 100,000) |
| `con` | `DuckDBPyConnection` | Optional existing connection |

### `process(source) -> int`

### Example — Direct edges

```python
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.core.loaders.edge_projection import LazyEdgeProjectionLoader

with DuckDBSink("graph.duckdb") as sink:
    loader = LazyEdgeProjectionLoader(
        table_name="graph_player_game",
        subject_key="player_id",
        object_key="game_id",
        property_map={"pts": "points_scored"},
        sink=sink,
    )
    rows = loader.process("player_game_facts.parquet")
```

### Example — Self-join edges (co-players)

```python
with DuckDBSink("graph.duckdb") as sink:
    loader = LazyEdgeProjectionLoader(
        table_name="graph_player_player",
        subject_key="player_id",
        object_key="player_id",
        self_join=True,
        self_join_key="game_id",
        property_map={"pts": "shared_points"},
        sink=sink,
    )
    rows = loader.process("player_game_facts.parquet")
```

The self-join generates one edge per unique player pair per game, with `shared_points` being the sum of both players' scores.

## Chaining Multiple Lazy Loaders

When running multiple lazy loaders against the same database, pass a shared connection to avoid attach/detach overhead:

```python
import duckdb
from sqldim.sinks.duckdb import DuckDBSink
from sqldim.core.loaders.bitmask import LazyBitmaskLoader
from sqldim.core.loaders.array_metric import LazyArrayMetricLoader

con = duckdb.connect()

with DuckDBSink("pipeline.duckdb") as sink:
    # Bitmask first
    bitmask = LazyBitmaskLoader(
        table_name="fact_activity_bitmask",
        partition_key="user_id",
        dates_column="dates_active",
        reference_date="2024-01-31",
        sink=sink,
        con=con,
    )
    bitmask.process("user_activity.parquet")

    # Then array metrics — reuses same connection
    metrics = LazyArrayMetricLoader(
        table_name="fact_monthly_metrics",
        partition_key="user_id",
        value_column="revenue",
        metric_name="monthly_revenue",
        month_start="2024-01-01",
        sink=sink,
        con=con,
    )
    metrics.process("daily_revenue.parquet", target_date="2024-01-15")

con.close()
```

## DuckDB-Only Constraint

Lazy loaders generate DuckDB-specific SQL (`struct_pack`, `list_append`, `list_transform`, `UNNEST`, bitwise operators). They **only work with `DuckDBSink`** — they cannot be used with PostgreSQL, Iceberg, or Delta sinks. If you need to target other databases, use the standard (Narwhals-based) loaders instead.
