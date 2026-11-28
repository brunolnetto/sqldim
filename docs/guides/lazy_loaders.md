# Lazy Loaders — Big Data Without Python

The **lazy loaders** keep the data out of Python: they generate DuckDB SQL views and write the result through a sink, with no DataFrame and no row-by-row loop. Memory use is then DuckDB's, not Python's; sqldim publishes no benchmark of how far that scales.

This guide explains when to use lazy loaders, how each one works, and how to set them up.

## Standard vs. Lazy: When to Use What

| Factor | Standard Loaders | Lazy Loaders |
|--------|-----------------|--------------|
| Data size | What fits in Python memory | What DuckDB can process |
| Data location | Python dicts, Narwhals DataFrames | Parquet files, CSV files, DuckDB tables |
| Processing engine | Polars / Pandas (in-memory) | DuckDB SQL (on-disk) |
| Memory usage | Proportional to dataset size | DuckDB's (may spill to disk) |
| SCD support | Full Type 1/2/3/6 | Specialized patterns only |

**Use standard loaders** (`DimensionalLoader`) for your initial pipeline build and when SCD handling is required.

**Use lazy loaders** when your source data lives in Parquet/CSV on disk and the processing pattern matches one of the loaders below.

## Shared Setup Pattern

All lazy loaders take a target `table` and a `sink`, and load with
`process(source, ...)`. The examples on this page run as written; they share
these inputs:

```python
from datetime import date

import duckdb

duckdb.sql(
    "COPY (SELECT * FROM (VALUES ('LeBron', 27.4::DOUBLE, 7.2::DOUBLE, 5.9::DOUBLE),"
    " ('Curry', 26.4, 5.1, 4.5)) t(player_name, pts, ast, reb)) TO '2024_season_stats.parquet'"
)
duckdb.sql(
    "COPY (SELECT * FROM (VALUES"
    " ('U-001', ['2024-01-01', '2024-01-03', '2024-01-15']),"
    " ('U-002', ['2024-01-31'])) t(user_id, dates_active)) TO 'user_activity.parquet'"
)
duckdb.sql(
    "COPY (SELECT * FROM (VALUES ('U-001', 149.99), ('U-002', 20.0))"
    " t(user_id, revenue)) TO 'daily_revenue.parquet'"
)
duckdb.sql(
    "COPY (SELECT * FROM (VALUES ('p1', 'g1', 10), ('p2', 'g1', 12), ('p1', 'g2', 8))"
    " t(player_id, game_id, pts)) TO 'player_game_facts.parquet'"
)
```

### What the loaders need

- **An open sink.** A `DuckDBSink` attaches its file to its own connection
  on `__enter__`. A loader given the sink and no `con` writes through that
  connection, `sink.connection`; outside the `with` block reading it raises
  `SinkNotOpen`. Pass `con=` only to use another connection that has the
  sink's database attached.
- **An existing target table.** `DuckDBSink.write` inserts by name
  (`INSERT INTO … BY NAME`), so column order does not matter; a target
  column the loader does not produce takes its default, and a produced
  column the table lacks raises `SchemaError` before anything is written.
  Each example below creates its table.
- **A source.** `process()` accepts a `SourceAdapter`, a Parquet or CSV path,
  or the name of a table or view of the connection.
- **`batch_size`** defaults to 100,000 and is passed to the sink.

## LazyCumulativeLoader

**Pattern**: Array-of-Structs append via FULL OUTER JOIN.

Use this when tracking longitudinal history — e.g., a player's per-season stats, a user's monthly activity summary, or any scenario where each period adds a struct to a growing array.

### How it works

The table holds one row per entity **per period**, as in the
`pipeline_query.sql` pattern it reproduces: the latest state of an entity is
its row with the latest `current_period_column`.

1. Reads yesterday's state from the sink: the rows of the previous period
2. Reads today's batch from the source (Parquet/CSV)
3. FULL OUTER JOIN on the partition key
4. For active members: `list_append(history, struct_pack(metrics, period))`
5. For dormant members: keeps history, increments `years_since_last_active`
6. Appends the merged rows as the rows of `target_period`

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

### `process(today_source, target_period, previous_period=None) -> int`

| Parameter | Type | Description |
|-----------|------|-------------|
| `today_source` | `str` or `SourceAdapter` | Path to today's data file |
| `target_period` | `Any` | The period identifier (e.g., `2024`, `"Q1-2024"`), stored as text |
| `previous_period` | `Any` | The period read as yesterday; may be omitted while the table holds at most one period |

Periods are compared as text, so the loader never guesses which period is
the previous one. It raises `LoadError` when the table holds several periods
and `previous_period` is not given, or holds no row of the one given, and
`IdempotencyError` when `target_period` is already stored, before anything
is written.

### Example

```python
from sqldim import DuckDBSink, LazyCumulativeLoader

duckdb.connect("nba.duckdb").execute(
    "CREATE TABLE player_seasons_cumulated (player_name VARCHAR,"
    " seasons STRUCT(pts DOUBLE, ast DOUBLE, reb DOUBLE, period VARCHAR)[],"
    " is_active BOOLEAN, years_since_last_active INTEGER, current_season VARCHAR)"
).close()

duckdb.sql(
    "COPY (SELECT 'LeBron' AS player_name, 25.7::DOUBLE AS pts, 8.3::DOUBLE AS ast,"
    " 7.8::DOUBLE AS reb) TO '2025_season_stats.parquet'"
)

with DuckDBSink("nba.duckdb") as sink:
    loader = LazyCumulativeLoader(
        table="player_seasons_cumulated",
        partition_key="player_name",
        cumulative_column="seasons",
        metric_columns=["pts", "ast", "reb"],
        sink=sink,
    )
    print(loader.process("2024_season_stats.parquet", target_period=2024))  # 2
    loader.process("2025_season_stats.parquet", target_period=2025, previous_period=2024)
    print(sink.connection.execute(
        "SELECT player_name, len(seasons), years_since_last_active"
        " FROM sqldim_ddb.player_seasons_cumulated"
        " WHERE current_season = '2025' ORDER BY 1"
    ).fetchall())  # [('Curry', 1, 1), ('LeBron', 2, 0)]
```

Each row's `seasons` column holds one struct per period, and `period` is
stored as text. After two periods the table holds four rows: one per player
for 2024 and one for 2025. Curry, absent from the 2025 batch, keeps his
history and counts one season since he was last active.

## LazyBitmaskLoader

**Pattern**: Date list → 32-bit integer bitmask via bitwise aggregation.

Use this for ultra-lightweight retention tracking. Instead of storing every active date, a 32-bit integer encodes up to 32 days of activity — perfect for L7/L28 metrics.

### How it works

1. Reads source with a date array column (e.g., `["2024-01-03", "2024-01-05", ...]`)
2. `UNNEST` the array to get individual dates
3. `DATEDIFF('day', date, reference_date)` → day offset
4. `SUM(1 << (window - 1 - offset))` → single integer per partition key

### Reading the bitmask

Bit positions are **most-significant first**: with a 32-day window, bit 31 is `reference_date`, bit 30 the day before, and so on.

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
from sqldim import LazyBitmaskLoader

duckdb.connect("retention.duckdb").execute(
    "CREATE TABLE fact_activity_bitmask (user_id VARCHAR, datelist_int BIGINT)"
).close()

with DuckDBSink("retention.duckdb") as sink:
    loader = LazyBitmaskLoader(
        table="fact_activity_bitmask",
        partition_key="user_id",
        dates_column="dates_active",
        reference_date="2024-01-31",
        window_days=32,
        sink=sink,
    )
    loader.process("user_activity.parquet")
    print(sink.connection.execute(
        "SELECT * FROM sqldim_ddb.fact_activity_bitmask ORDER BY 1"
    ).fetchall())  # [('U-001', 32778), ('U-002', 2147483648)]
```

Source data format (each row has a list of active dates):
```
user_id | dates_active
--------|-------------------------------------------
U-001   | ["2024-01-01", "2024-01-03", "2024-01-15"]
U-002   | ["2024-01-31"]
```

Result: U-001 was active 30, 28 and 16 days before the reference date
(bits 1, 3 and 15: 2 + 8 + 32768 = 32778); U-002 on the reference date
itself (bit 31: 2147483648).

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
| `month_start` | `date` | First day of the month (a `str` fails: the offset is computed in Python) |
| `sink` | `DuckDBSink` | Target sink |
| `batch_size` | `int` | Write batch size (default: 100,000) |
| `con` | `DuckDBPyConnection` | Optional existing connection |

### `process(source, target_date) -> int`

| Parameter | Type | Description |
|-----------|------|-------------|
| `source` | `str` or `SourceAdapter` | Path to daily data |
| `target_date` | `date` | The specific day to place in the array |

### Example

```python
from sqldim import LazyArrayMetricLoader

duckdb.connect("metrics.duckdb").execute(
    "CREATE TABLE fact_monthly_metrics (user_id VARCHAR, metric_name VARCHAR,"
    " month_start DATE, metric_array DOUBLE[])"
).close()

with DuckDBSink("metrics.duckdb") as sink:
    loader = LazyArrayMetricLoader(
        table="fact_monthly_metrics",
        partition_key="user_id",
        value_column="revenue",
        metric_name="monthly_revenue",
        month_start=date(2024, 1, 1),
        sink=sink,
    )
    loader.process("daily_revenue.parquet", target_date=date(2024, 1, 15))
    print(sink.connection.execute(
        "SELECT user_id, metric_array[15] FROM sqldim_ddb.fact_monthly_metrics ORDER BY 1"
    ).fetchall())  # [('U-001', 149.99), ('U-002', 20.0)]
```

Each row gets a 31-element array with the value at the day's position (the
15th element, `metric_array[15]` in DuckDB's 1-based indexing).

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
from sqldim import LazyEdgeProjectionLoader

ddl = duckdb.connect("graph.duckdb")
ddl.execute("CREATE TABLE graph_player_game (subject_id VARCHAR, object_id VARCHAR, points_scored INTEGER)")
ddl.execute("CREATE TABLE graph_player_player (subject_id VARCHAR, object_id VARCHAR, shared_points INTEGER)")
ddl.close()

with DuckDBSink("graph.duckdb") as sink:
    loader = LazyEdgeProjectionLoader(
        table="graph_player_game",
        subject_key="player_id",
        object_key="game_id",
        property_map={"pts": "points_scored"},
        sink=sink,
    )
    print(loader.process("player_game_facts.parquet"))  # 3
```

### Example — Self-join edges (co-players)

```python
with DuckDBSink("graph.duckdb") as sink:
    loader = LazyEdgeProjectionLoader(
        table="graph_player_player",
        subject_key="player_id",
        object_key="player_id",
        self_join=True,
        self_join_key="game_id",
        property_map={"pts": "shared_points"},
        sink=sink,
    )
    loader.process("player_game_facts.parquet")
    print(sink.connection.execute(
        "SELECT * FROM sqldim_ddb.graph_player_player"
    ).fetchall())  # [('p1', 'p2', 22)]
```

The self-join generates one edge per unique player pair per game, with `shared_points` being the sum of both players' scores.

## Chaining Multiple Lazy Loaders

Loaders that share a sink share its connection, `sink.connection`: several
loaders can run inside one `with DuckDBSink(…)` block, and your own
statements in that block go through `sink.connection` too. A separate
`duckdb.connect()` does not see the sink's database.

## DuckDB-Only Constraint

Lazy loaders generate DuckDB-specific SQL (`struct_pack`, `list_append`, `list_transform`, `UNNEST`, bitwise operators). Only `DuckDBSink` is exercised with them here; whether another sink works depends on whether its connection runs that SQL, and nothing tests it. If you need to target other databases, use the standard (Narwhals-based) loaders instead.
