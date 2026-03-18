# Sources Reference

Sources provide lazy SQL references to external data. They produce SQL strings that execute inside DuckDB — Python never materialises individual rows.

All sources implement the `SourceAdapter` protocol and are designed for use with lazy SCD processors and DuckDB sinks.

## Source Classes

### ParquetSource

Read Parquet files (local or S3). Uses DuckDB `read_parquet`.

```python
from sqldim.sources import ParquetSource

events = ParquetSource("s3://my-bucket/events/*.parquet")
orders = ParquetSource("data/orders/", hive_partitioning=True)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | *required* | File path or glob pattern. Supports local paths and `s3://` URIs. |
| `hive_partitioning` | `bool` | `False` | Parse Hive-style partition directories (`year=2024/month=01/`) |
| `union_by_name` | `bool` | `False` | Union schemas by column name when reading multiple files |
| `filename` | `bool` | `False` | Include source filename as a column |
| `remote_threads` | `int \| None` | `None` | Thread count for S3 reads (DuckDB default if unset) |

### CSVSource

Read CSV/TSV files. Uses DuckDB `read_csv`.

```python
from sqldim.sources import CSVSource

raw = CSVSource("data/customers.csv")
tsv = CSVSource("data/events.tsv", delimiter="\t")
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | *required* | File path or glob pattern |
| `delimiter` | `str` | `","` | Column separator |
| `header` | `bool` | `True` | First row contains column names |
| `encoding` | `str` | `"utf-8"` | Character encoding |
| `columns` | `dict \| None` | `None` | Explicit column types (disables auto-detection scan — **recommended for large files**) |
| `nullstr` | `str` | `""` | String that represents NULL values |
| `ignore_errors` | `bool` | `False` | Skip malformed rows instead of raising |

**Tip:** For files over 1 GB, always pass `columns` to skip the expensive type-inference scan:

```python
CSVSource(
    "data/huge.csv",
    columns={"id": "VARCHAR", "amount": "DOUBLE", "date": "DATE"},
)
```

### DeltaSource

Read Delta Lake tables. Uses DuckDB `delta` extension.

```python
from sqldim.sources import DeltaSource

delta = DeltaSource("s3://lakehouse/customers/")
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | *required* | Path to Delta table (local or S3) |
| `use_attach` | `bool` | `True` | Attach table to DuckDB for metadata caching |
| `alias` | `str` | `"sqldim_delta_src"` | Alias for the attached table |

### DuckDBSource

Read from an existing DuckDB table or view. Used for chaining internal views.

```python
from sqldim.sources import DuckDBSource

staged = DuckDBSource("staging.customers_clean")
filtered = DuckDBSource("v_active_products", schema="analytics")
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `table_or_view` | `str` | *required* | Table or view name (optionally schema-qualified) |
| `schema` | `str \| None` | `None` | Schema name if not included in `table_or_view` |

### PostgreSQLSource

Stream from a PostgreSQL table via DuckDB `postgres` extension.

```python
from sqldim.sources import PostgreSQLSource

pg = PostgreSQLSource(
    dsn="postgresql://user:pass@host:5432/oltp",
    table="orders",
    schema="public",
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | `str` | *required* | PostgreSQL connection string |
| `table` | `str` | *required* | Table name to read |
| `schema` | `str` | `"public"` | Schema name |
| `filter_pushdown` | `bool` | `False` | Push WHERE clauses to PostgreSQL (experimental) |
| `pages_per_task` | `int` | `1000` | Pages per read task (tune for throughput) |

### KafkaSource

Read from a Kafka topic. Uses DuckDB `kafka_scan` or falls back to `confluent-kafka` micro-batches.

```python
from sqldim.sources import KafkaSource

kafka = KafkaSource(
    brokers="kafka:9092",
    topic="order_events",
    group_id="sqldim-loader",
    format="json",
    start_from="latest",
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `brokers` | `str` | *required* | Comma-separated broker addresses |
| `topic` | `str` | *required* | Kafka topic name |
| `group_id` | `str` | *required* | Consumer group ID |
| `format` | `str` | `"json"` | Message format (`"json"` or `"avro"`) |
| `start_from` | `str` | `"latest"` | Offset start position (`"latest"` or `"earliest"`) |

## Source Selection Guide

| Scenario | Recommended Source |
|---|---|
| Data lake landing zone | `ParquetSource` |
| Legacy CSV extracts | `CSVSource` (with explicit `columns`) |
| Delta Lake lakehouse | `DeltaSource` |
| Internal DuckDB staging | `DuckDBSource` |
| Operational PostgreSQL | `PostgreSQLSource` |
| Real-time event stream | `KafkaSource` |

## See Also

- [Big Data Guide](../guides/big_data.md) — end-to-end big-data pipeline using sources, processors, and sinks
- [Lazy Loaders Guide](../guides/lazy_loaders.md) — loaders that consume sources directly
- [Sinks Reference](sinks.md) — write-side adapters
