# ADR: Physical Partitioning and Bucketing Strategy

**Date**: 2026-03-18  
**POC**: @pingu  
**TL;DR**: Move from hardcoded `is_current` Hive partitioning to a per-layer, query-aware physical data organization strategy combining coarse partitioning with Z-ORDER clustering — aligned with DuckDB's strengths.

## Status

Proposed

## Context

sqldim's physical data layout treats partitioning as an SCD2 artifact. `ParquetSink` hardcodes `PARTITION_BY (is_current)`, separating current rows from history. This was designed for ETL correctness, not query performance.

An audit of the current state:

| Capability | Status | Detail |
|-----------|--------|-------|
| Hive partitioning | Implemented | Hardcoded to `is_current` in `ParquetSink` |
| Partition pruning | Implemented | DuckDB `hive_partitioning=true` on read |
| Predicate pushdown | Partial | IcebergSource (full), PostgreSQLSource (experimental) |
| Delta metadata caching | Implemented | `DeltaSource(use_attach=True)` via DuckDB `ATTACH` |
| Custom partition columns | Missing | Cannot partition by `order_date`, `region`, etc. |
| Hash bucketing | Missing | No co-located file grouping by column hash |
| Z-ORDER / CLUSTER BY | Missing | No row-group-level column co-location |
| Compaction | Missing | No file merge for streaming-generated small files |
| Partition evolution | Missing | Cannot change scheme without full rewrite |
| Per-layer partitioning | Missing | Bronze/Silver/Gold all use `is_current` |

### The core problem

In a real analytical pipeline, each medallion layer has a different access pattern:

- **Bronze** — append-only, scanned by ingestion time (`WHERE ingest_date BETWEEN ...`)
- **Silver** — keyed lookups and joins, accessed by natural key (`WHERE customer_code = ...`)
- **Gold** — star-join range scans, accessed by date dimension (`WHERE order_date BETWEEN ...`)
- **Bridges** — always joined with their parent facts, should be co-located

One-size-fits-all `is_current` partitioning doesn't serve any of these patterns well.

### Why traditional deep partitioning doesn't apply

DuckDB's Parquet reader changes the math compared to Hive/Spark:

- **Predicate pushdown** on non-partition columns is efficient for moderate datasets
- **Over-partitioning** (many small directories) hurts more than under-partitioning — DuckDB has per-file overhead
- **Z-ORDER within files** can substitute for fine-grained partitioning at lower cost
- **Hash bucketing** (Spark-style) adds complexity DuckDB doesn't optimize for — it doesn't prune within a partition by bucket ID

## Decision

Adopt a **coarse partitioning + Z-ORDER clustering** strategy, configured per medallion layer.

### Layer-Specific Partition Schemes

| Layer | Partition Column | Z-ORDER Columns | Rationale |
|-------|-----------------|-----------------|-----------|
| Bronze | `ingest_date` (day) | `source`, `natural_key_cols` | Time-scanned, append-only |
| Silver | `is_current` (boolean) | Natural key columns, `valid_from` | Keyed lookups, SCD state filtering |
| Gold (Facts) | Primary date key (month) | Dimension FK columns | Star-join range scans |
| Gold (Dimensions) | `is_current` (boolean) | Natural key columns | Current-state lookups |
| Bridges | Same as parent fact | FK pair columns | Co-located with parent joins |

### Configuration via Sink Metadata

Partitioning becomes a property of the sink, not hardcoded in the sink implementation:

```python
ParquetSink(
    path="s3://warehouse/gold/order_fact",
    partition_by=["order_month"],       # coarse Hive partitioning
    zorder_by=["customer_id", "product_id"],  # row-group clustering
    target_file_size_mb=256,            # compaction target
)
```

### Z-ORDER Implementation

DuckDB doesn't natively support Z-ORDER at write time, but can achieve the same effect via:

1. **Sort-then-write**: `ORDER BY zorder_cols` before writing Parquet — DuckDB's Parquet writer preserves sort order within row groups
2. **Post-write rewrite**: Read + sort + rewrite for existing datasets (compaction opportunity)
3. **Future**: DuckDB may add native Z-ORDER; implementation abstracts behind a `ClusteringStrategy` protocol

```python
class ClusteringStrategy(Protocol):
    def cluster_sql(self, table: str, columns: list[str]) -> str: ...
```

Default implementation: `ORDER BY {cols}`. Future implementations can plug in `ICEBERG_WRITE_WITH_ORDERING` or Delta `OPTIMIZE ZORDER BY`.

### Compaction

Add a `Compactor` utility for streaming-induced small file accumulation:

```python
Compactor.compact(
    path="s3://warehouse/bronze/events",
    partition_by=["ingest_date"],
    target_file_size_mb=256,
    max_files_per_partition=10,
)
```

Reads existing files per partition, sorts by Z-ORDER columns, writes merged files, deletes originals. Runs as a post-load or scheduled maintenance step.

### Hash Bucketing — Explicitly Excluded

Hash bucketing (e.g., `hash(id) % 64`) is **not** adopted because:

- DuckDB doesn't prune Parquet files by bucket ID
- Adds directory depth without query benefit
- Complicates partition evolution
- Z-ORDER on the join key achieves better co-location within row groups

If a future backend (Spark, Trino) is added that benefits from hash buckets, the `PartitionStrategy` protocol can accommodate it without changing the model layer.

## Consequences

**Positive**
- Time-range queries on gold facts skip irrelevant month partitions
- Z-ORDER on FK columns minimizes row-group reads during star joins
- Bronze partitioned by ingest date enables efficient late-arrival handling and reprocessing
- Compaction prevents streaming degradation over time
- Per-layer strategy matches real access patterns instead of ETL internals

**Neutral**
- `is_current` partitioning remains for silver (where it belongs — SCD state filtering)
- Z-ORDER via sort-then-write is a best-effort approximation — not as precise as true Z-ORDER (adequate for DuckDB Parquet)
- `target_file_size_mb` defaults to 256MB (DuckDB sweet spot) — configurable per sink

**Negative**
- Custom partition columns require modelers to think about query patterns at sink definition time
- Compaction is an operational concern — must be scheduled or triggered explicitly (not automatic)
- Z-ORDER rewrite is I/O-intensive for large datasets — should run during maintenance windows
- Changing partition scheme on existing data requires a full rewrite (no in-place evolution)

## Migration Path

1. **Phase 1**: Make `partition_by` configurable on `ParquetSink` (currently hardcoded). Default to `["is_current"]` for backward compat.
2. **Phase 2**: Add Z-ORDER via sort-then-write. Add `target_file_size_mb`.
3. **Phase 3**: Add `Compactor` utility. Integrate with medallion pipeline post-load hooks.
4. **Phase 4**: Add per-layer partition defaults in medallion layer definitions.

## Affected Files (Planned)

| File | Change |
|------|--------|
| `sqldim/sinks/parquet.py` | Replace hardcoded `is_current` with configurable `partition_by` |
| `sqldim/sinks/base.py` | Add `partition_by`, `zorder_by`, `target_file_size_mb` to base sink |
| `sqldim/io/compaction.py` | New `Compactor` utility |
| `sqldim/medallion/layers.py` | Per-layer partition defaults |
| `sqldim/io/clustering.py` | New `ClusteringStrategy` protocol + sort-based implementation |

## See Also

- [Semantic Bucketing for Measures and Dimensions](./semantic-bucketing.md) — value-level bucketing (distinct concern)
- [Analytical Indexing Strategy](./analytical-indexing-strategy.md) — in-memory index prerequisites
- [Graph Analytics Roadmap](./graph-analytics-roadmap.md) — graph traversal performance
