# ADR: Semantic Bucketing for Measures and Dimensions

**Date**: 2026-03-18  
**POC**: @pingu  
**TL;DR**: Introduce a generic bucketing layer for numeric measures (ntile, width_bucket) and time dimensions (grain truncation) to enable segmentation, rollups, and semi-additive handling — currently absent from sqldim.

## Status

Proposed

## Context

sqldim can validate additive vs. non-additive measures and apply bridge weights during aggregation, but has no concept of **bucketing** — grouping continuous values into discrete tiers. What exists is fragmented:

| Existing | Limitation |
|----------|-----------|
| `TimeDimension._time_of_day()` | Hardcoded 4-bin split (Morning/Afternoon/Evening/Night) |
| `DatelistMixin.to_bitmask()` | Binary bit bins for L7/L28 windows only |
| `Field(additive=True/False)` | Binary flag — no semi-additive middle ground |
| `QualityGate` | Boolean checks only — no statistical distribution profiling |

Users must write raw SQL for any segmentation (deciles, price bands, time rollups). This blocks the most common analytical operations.

## Decision

Introduce semantic bucketing as metadata-driven configuration on `Field()` and as a query builder capability, prioritized in four tiers:

### P0: Numeric Measure Bucketizer

Add `bucket_count` and `bucket_strategy` to `Field()` metadata. The query builder emits appropriate SQL expressions:

```python
# Ntile (equal-depth) — e.g., customer spend deciles
total_spend: float = Field(
    measure=True,
    bucket_count=10,
    bucket_strategy="ntile",
)

# Width bucket (equal-width) — e.g., price bands
unit_price: float = Field(
    measure=True,
    bucket_count=5,
    bucket_strategy="width_bucket",
    bucket_bounds=(0, 50, 100, 250, 500, 1000),
)
```

Emitted SQL:
```sql
-- ntile
NTILE(10) OVER (ORDER BY total_spend) AS spend_decile
-- width_bucket
WIDTH_BUCKET(unit_price, ARRAY[0, 50, 100, 250, 500, 1000]) AS price_band
```

### P1: Time Grain Bucketing

Replace hardcoded time-of-day logic with configurable grain truncation:

```python
order_date: date = Field(
    dimension=DateDimension,
    bucket_grain="week",  # day, week, month, quarter, year, fiscal_quarter
)
```

Emitted SQL (DuckDB/PostgreSQL):
```sql
DATE_TRUNC('week', order_date) AS order_week
```

Custom periods (fiscal quarters, rolling windows) via a `TimeBucketStrategy` protocol.

### P2: Semi-Additive Bucket Logic

Extend `additive` from `bool` to accept `"semi_additive"` with a `forbidden_dimension` hint:

```python
account_balance: float = Field(
    measure=True,
    additive="semi_additive",
    semi_additive_fallback="last",  # or "avg", "max"
    semi_additive_forbidden=["order_date"],
)
```

The query builder applies `SUM()` by default but switches to `LAST()` or `AVG()` when the GROUP BY includes a forbidden dimension. This closes the gap between the current binary flag and real-world measure semantics (Account Balance, Inventory Level, Exchange Rate).

### P3: Profiling Buckets in Data Contracts

Extend `QualityGate` with bucket-aware statistical rules:

```python
QualityGate(
    name="spend_distribution",
    rule="histogram_check",
    column="total_spend",
    bucket_count=10,
    max_bucket_skew=0.3,  # no bucket should hold > 30% of rows
)
```

Enables distribution shift detection between loads without external profiling tools.

## Consequences

**Positive**
- Segmentation becomes a model-level concern, not raw SQL
- Time rollups are declarative — the query builder handles `DATE_TRUNC` dialect differences
- Semi-additive measures get correct aggregation without user-side logic
- Data contracts gain statistical depth

**Neutral**
- Bucket metadata is additive — existing `Field()` calls without bucket params are unaffected
- `additive` type change from `bool` to `bool | str` is backward compatible (`True`/`False` still work)
- Bucket strategies are pluggable via protocol — default implementations cover ntile, width_bucket, grain truncation

**Negative**
- Ntile requires window functions — may not work in all SQL dialects (mitigated by dialect-aware builder)
- Profiling bucket rules add complexity to `QualityGate` evaluation (P3 — low urgency)
- `bucket_bounds` on width_bucket requires domain knowledge from the modeler

## Affected Files (Planned)

| File | Change |
|------|--------|
| `sqldim/core/kimball/fields.py` | Add `bucket_count`, `bucket_strategy`, `bucket_bounds`, `bucket_grain`, `semi_additive_*` params |
| `sqldim/core/query/builder.py` | Bucket expression emission + semi-additive aggregation switching |
| `sqldim/core/kimball/dimensions/time.py` | Replace hardcoded `_time_of_day()` with grain-based bucketing |
| `sqldim/contracts/gates.py` | Histogram/profiling rules (P3) |

## See Also

- [Analytical Indexing Strategy](./analytical-indexing-strategy.md) — query performance prerequisites
- [Graph Analytics Roadmap](./graph-analytics-roadmap.md) — graph aggregation bucketing (P3 overlap)
