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

All lazy loaders take a target `table`, a `sink`, the sink's connection as
`con` and load with `process(source, ...)`. The examples on this page run as
written; they share these inputs:

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

### What the loaders need today

- **The sink's connection.** A `DuckDBSink` attaches its file to its own
  connection on `__enter__`, and a loader writes through the connection it
  is given. Pass `con=sink._con`; there is no public accessor yet. A loader
  given no `con` opens a connection without the sink's database and fails.
- **An existing target table.** `DuckDBSink.write` runs `INSERT INTO … SELECT
  *`, so the table must exist with the columns in the order the loader
  produces them; each example below creates it.
- **A table or view as the source.** `process()` accepts a `SourceAdapter`
  or a string, but a Parquet or CSV path currently fails with a DuckDB parser
  error (the loaders write `FROM (read_parquet(…))`). Register a view over
  the file and pass its name, as the examples do.
- **`batch_size`** defaults to 100,000 and is passed to the sink.

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
from sqldim import DuckDBSink, LazyCumulativeLoader

duckdb.connect("nba.duckdb").execute(
    "CREATE TABLE player_seasons_cumulated (player_name VARCHAR,"
    " seasons STRUCT(pts DOUBLE, ast DOUBLE, reb DOUBLE, period VARCHAR)[],"
    " is_active BOOLEAN, years_since_last_active INTEGER, current_season VARCHAR)"
).close()

with DuckDBSink("nba.duckdb") as sink:
    sink._con.execute("CREATE VIEW season_2024 AS SELECT * FROM '2024_season_stats.parquet'")
    loader = LazyCumulativeLoader(
        table="player_seasons_cumulated",
        partition_key="player_name",
        cumulative_column="seasons",
        metric_columns=["pts", "ast", "reb"],
        sink=sink,
        con=sink._con,
    )
    print(loader.process("season_2024", target_period=2024))  # 2
    print(sink._con.execute(
        "SELECT seasons FROM sqldim_ddb.player_seasons_cumulated WHERE player_name = 'LeBron'"
    ).fetchone()[0])
    # [{'pts': 27.4, 'ast': 7.2, 'reb': 5.9, 'period': '2024'}]
```

Each row's `seasons` column holds one struct per period, and `period` is
stored as text. With `DuckDBSink` the merged rows are *inserted*: after a
second period the table holds both the old and the new row of each player,
so read the latest `current_season`, or replace the table between periods.

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
    sink._con.execute("CREATE VIEW user_activity AS SELECT * FROM 'user_activity.parquet'")
    loader = LazyBitmaskLoader(
        table="fact_activity_bitmask",
        partition_key="user_id",
        dates_column="dates_active",
        reference_date="2024-01-31",
        window_days=32,
        sink=sink,
        con=sink._con,
    )
    loader.process("user_activity")
    print(sink._con.execute(
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
    sink._con.execute("CREATE VIEW daily_revenue AS SELECT * FROM 'daily_revenue.parquet'")
    loader = LazyArrayMetricLoader(
        table="fact_monthly_metrics",
        partition_key="user_id",
        value_column="revenue",
        metric_name="monthly_revenue",
        month_start=date(2024, 1, 1),
        sink=sink,
        con=sink._con,
    )
    loader.process("daily_revenue", target_date=date(2024, 1, 15))
    print(sink._con.execute(
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
    sink._con.execute(
        "CREATE VIEW player_game_facts AS SELECT * FROM 'player_game_facts.parquet'"
    )
    loader = LazyEdgeProjectionLoader(
        table="graph_player_game",
        subject_key="player_id",
        object_key="game_id",
        property_map={"pts": "points_scored"},
        sink=sink,
        con=sink._con,
    )
    print(loader.process("player_game_facts"))  # 3
```

### Example — Self-join edges (co-players)

```python
with DuckDBSink("graph.duckdb") as sink:
    sink._con.execute(
        "CREATE VIEW player_game_facts AS SELECT * FROM 'player_game_facts.parquet'"
    )
    loader = LazyEdgeProjectionLoader(
        table="graph_player_player",
        subject_key="player_id",
        object_key="player_id",
        self_join=True,
        self_join_key="game_id",
        property_map={"pts": "shared_points"},
        sink=sink,
        con=sink._con,
    )
    loader.process("player_game_facts")
    print(sink._con.execute(
        "SELECT * FROM sqldim_ddb.graph_player_player"
    ).fetchall())  # [('p1', 'p2', 22)]
```

The self-join generates one edge per unique player pair per game, with `shared_points` being the sum of both players' scores.

## Chaining Multiple Lazy Loaders

Loaders that share a sink share its connection, `sink._con`: every example
above passes it, and several loaders can run inside one `with DuckDBSink(…)`
block. A separate `duckdb.connect()` does not see the sink's database.

## DuckDB-Only Constraint

Lazy loaders generate DuckDB-specific SQL (`struct_pack`, `list_append`, `list_transform`, `UNNEST`, bitwise operators). Only `DuckDBSink` is exercised with them here; whether another sink works depends on whether its connection runs that SQL, and nothing tests it. If you need to target other databases, use the standard (Narwhals-based) loaders instead.
