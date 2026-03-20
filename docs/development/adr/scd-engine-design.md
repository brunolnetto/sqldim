# ADR: SCD Engine Design

**Date**: 2026-03-18
**POC**: @pingu
**TL;DR**: Dual-engine SCD architecture — eager row-by-row (`SCDHandler`) for OLTP-side inserts and lazy vectorized (`LazySCDProcessor` family) for bulk DuckDB-native processing. Supports Types 0–6 plus metadata-bag variant. MD5 checksums on tracked columns drive change detection.

## Status

Accepted (implemented)

## Context

Slowly Changing Dimensions are the most architecturally complex part of sqldim. The library must handle seven SCD strategies, each with different state management semantics, while supporting both streaming (row-at-a-time) and batch (set-based) processing patterns.

Key constraints:
- DuckDB is the primary analytical backend — SQL-native processing is preferred over Python loops
- Some deployments need in-memory processing (Polars/Pandas) without DuckDB
- SCD Type 6 (hybrid) requires per-column strategy dispatch within a single row
- Checksum computation must be deterministic across runs and backends
- Batch deduplication is essential for streaming sources that may replay events

## Decision

### Dual-Engine Architecture

Two parallel processing paths, selectable by use case:

| Engine | Class | Execution | Best For |
|--------|-------|-----------|----------|
| Eager | `SCDHandler` | Python row-by-row | Small batches, OLTP inserts, non-DuckDB backends |
| Lazy | `LazySCDProcessor` variants | DuckDB SQL set-based | Bulk loads, streaming sinks, large datasets |

The eager engine is the original implementation. The lazy engine was added for performance — it avoids round-tripping data between DuckDB and Python by computing checksums, classifying changes, and applying mutations entirely in SQL.

### Processor Inventory

All lazy processors live in `sqldim/core/kimball/dimensions/scd/processors/`:

| Processor | SCD Type | Strategy |
|-----------|----------|----------|
| `LazyType1Processor` | 1 | Overwrite in place via `sink.update_attributes` |
| `LazySCDProcessor` | 2 | Close old version (`valid_to=now`, `is_current=False`), insert new |
| `LazyType3Processor` | 3 | Rotate `(current, previous)` attribute pairs via `sink.rotate_attributes` |
| `LazyType6Processor` | 6 | Hybrid: compute separate T1 and T2 checksums, dispatch per-column |
| `LazyType4Processor` | 4 | Mini-dimension split: upsert profile SK, then run Type 2 on base dimension |
| `LazyType5Processor` | 5 | Type 4 + Type 1 "current profile" pointer on base dimension |
| `LazySCDMetadataProcessor` | 2 (metadata) | Specialized for JSON/struct "metadata bag" column layouts |
| `NarwhalsSCDProcessor` | 2 | In-memory vectorized for Polars/Pandas (non-DuckDB) |

### Change Detection Pipeline (Lazy Engine)

```
Input rows
    │
    ▼
_register_source ─── Coerce to SQL, compute MD5 _checksum on track_columns
    │
    ▼
_register_current_checksums ─── Pull (natural_key, checksum) for is_current=TRUE rows
    │
    ▼
_classify ─── LEFT JOIN incoming × current on natural_key
    │          Tags: new (no match), changed (checksum differs), unchanged (match)
    │
    ├─ new      → _write_new (INSERT with valid_from=now, is_current=TRUE)
    ├─ changed  → sink.close_versions (UPDATE valid_to, is_current) + _write_new
    └─ unchanged → discard
```

### Checksum Computation

- **Algorithm**: MD5 hex digest
- **Input**: All columns listed in `track_columns`, sorted alphabetically
- **Serialization**: Values are JSON-serialized before hashing (handles complex types consistently)
- **Location**: `handler.py:54-71` (eager), `_scd_base.py:94` (lazy)
- **Determinism**: Alphabetical sort + JSON serialization ensures identical input → identical checksum across runs

### Type 6 Hybrid Logic

Type 6 requires per-column strategy dispatch. The `LazyType6Processor`:

1. Splits `track_columns` into T1 columns and T2 columns (based on `Field(scd=1)` vs `Field(scd=2)`)
2. Computes **two separate checksums**: `_t1_checksum` (T1 columns only) and `_t2_checksum` (T2 columns only)
3. Classification logic:
   - T2 checksum changed → close version + insert new row (T1 columns also carried forward)
   - Only T1 checksum changed → overwrite in place (no new version)
   - Neither changed → discard

This preserves T2 history while allowing T1 corrections without version bloat.

### Batch Deduplication

Streaming sources may replay events. The `process_stream` methods accept a `deduplicate_by` parameter that wraps the source in a window function:

```sql
SELECT * FROM (source_sql) 
QUALIFY ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY deduplicate_by DESC) = 1
```

This ensures only the latest version of each natural key within a batch is processed, preventing duplicate version creation.

### Orchestration

- **Eager engine**: `SCDHandler` auto-dispatches to `_handle_type1`, `_handle_type2`, etc. based on `model.__scd_type__`
- **Lazy engine**: No auto-dispatch — user explicitly instantiates `LazyType2Processor(model, sink, track_columns=[...])`. This is a known gap (see Future Work).

## Consequences

**Positive**
- DuckDB-native lazy processing avoids Python round-trips — orders of magnitude faster for bulk loads
- Deterministic checksums enable idempotent reprocessing
- Type 6 hybrid correctly separates T1/T2 semantics without version bloat
- Deduplication handles streaming replay at the engine level
- Metadata-bag variant supports flexible JSON column schemas

**Neutral**
- Two engines mean two code paths to maintain — shared logic is in utility functions, not a common base
- Eager engine is simpler but slower — appropriate for its use case (small batches)
- Lazy engine requires DuckDB-specific SQL — the Narwhals processor covers non-DuckDB but only for Type 2

**Negative**
- No lazy auto-dispatch — user must know which processor to instantiate
- MD5 is not collision-resistant for adversarial inputs (acceptable for analytical ETL, not for security)
- Missing natural key in input causes silent `None` classification — no explicit validation
- Type 4/5 mini-dimension management adds schema complexity (profile table must exist alongside base)
- Checksum includes ALL tracked columns — adding a tracked column changes all checksums, triggering false "changes" on first run after schema change

## Future Work

| Item | Description |
|------|-------------|
| Lazy auto-dispatch | `LazySCDHandler` that reads `__scd_type__` and instantiates the correct processor |
| Natural key validation | Explicit error on missing NK in input records (currently silent failure) |
| Checksum migration | Strategy for handling checksum changes when `track_columns` is modified |
| Type 4/5 profile lifecycle | Automated profile table creation/deletion alongside base dimension |
| Eager engine deprecation path | Evaluate whether eager engine is still needed once lazy auto-dispatch exists |

## Key Files

| File | Responsibility |
|------|---------------|
| `sqldim/core/kimball/dimensions/scd/handler.py` | Eager `SCDHandler` + checksum utility |
| `sqldim/core/kimball/dimensions/scd/processors/_lazy_type2.py` | Type 1 and Type 2 lazy processors |
| `sqldim/core/kimball/dimensions/scd/processors/_lazy_type3_6.py` | Type 3 and Type 6 lazy processors |
| `sqldim/core/kimball/dimensions/scd/processors/_lazy_type4_5.py` | Type 4 and Type 5 lazy processors |
| `sqldim/core/kimball/dimensions/scd/processors/_lazy_metadata.py` | Metadata-bag Type 2 variant |
| `sqldim/core/kimball/dimensions/scd/processors/_scd_base.py` | Narwhals in-memory Type 2 processor |
| `sqldim/core/kimball/mixins.py` | `SCD2Mixin` (column definitions + indexes) |

## See Also

- [Analytical Indexing Strategy](./analytical-indexing-strategy.md) — SCD2 temporal index pair
- [Late Arriving Dimensions](./late-arriving-dimensions.md) — inferred member interaction with SCD pipeline
