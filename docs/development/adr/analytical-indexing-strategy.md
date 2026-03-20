# ADR: Analytical Indexing Strategy

**Date**: 2026-03-18  
**POC**: @pingu  
**TL;DR**: Auto-index foreign keys on facts/edges, add composite unique indexes on bridges, and index `valid_from` on SCD2 dimensions — shifting from ETL-only indexing to query-aware indexing.

## Status

Accepted

## Context

sqldim's indexing was built around ETL concerns: SCD change detection (`checksum`), current-version lookups (`is_current`), and natural key resolution (`date_value`, `time_value`). An audit revealed four gaps that degrade analytical query and graph traversal performance:

| # | Concern | Impact |
|---|---------|--------|
| 1 | Fact table FK columns unindexed | Star joins scan full fact table per dimension |
| 2 | Graph edge endpoints (`subject_id`, `object_id`) unindexed | Recursive CTEs degrade O(n) per hop |
| 3 | Bridge composite FK pairs unindexed | M2M lookups scan full bridge table |
| 4 | SCD2 `valid_from` unindexed | As-of queries can't use full range scan (`valid_from <= X AND valid_to > X`) |

Only `valid_to` was indexed — the DB could only use half of the temporal predicate.

## Decision

Implement four targeted indexing mechanisms, all at the model-definition layer (no manual DDL):

### 1. FK Auto-Index via `Field()` Sentinel

Changed `Field(index=...)` default from `False` to an `_UNSET` sentinel. When `foreign_key=` is provided and `index` is not explicitly set, `index=True` is applied automatically. Explicit `index=False` still opts out.

```python
# Before — required manual index=True on every FK
customer_id: int = Field(foreign_key="customer.id", index=True)

# After — automatic
customer_id: int = Field(foreign_key="customer.id")

# Opt out when needed
legacy_id: int = Field(foreign_key="old.id", index=False)
```

**Covers**: Fact FKs (concern 1) and graph edge endpoints (concern 2) — same mechanism since both use `foreign_key=`.

### 2. Bridge Composite Unique Index via `__bridge_keys__`

`BridgeModel.__init_subclass__` reads `__bridge_keys__` and creates a composite unique index. Merges cleanly with existing `__table_args__` (preserves trailing options dict). Raises `TypeError` if `__table_args__` is a bare dict (can't merge).

```python
class SalesRepBridge(BridgeModel, table=True):
    __bridge_keys__ = ["sale_id", "rep_id"]  # → uq_salesrepbridge_sale_id_rep_id
    sale_id: int
    rep_id: int
```

### 3. SCD2 `valid_from` Index

Added `index=True` to the `valid_from` field in `SCD2Mixin`. Completes the temporal range index pair (`valid_from` + `valid_to`) for point-in-time queries.

```python
# Before
valid_from: datetime = Field(default_factory=...)
valid_to: Optional[datetime] = Field(default=None, index=True)

# After
valid_from: datetime = Field(default_factory=..., index=True)
valid_to: Optional[datetime] = Field(default=None, index=True)
```

## Consequences

**Positive**
- Star joins on fact tables now use index lookups instead of full scans
- Multi-hop graph traversals scale with index cardinality, not table size
- Bridge M2M queries have enforced uniqueness and indexed lookups
- SCD2 as-of queries can leverage a full range scan on both endpoints

**Neutral**
- `_UNSET` sentinel in `Field()` is a private implementation detail; users see standard `bool` semantics
- Bridge `__bridge_keys__` is optional — existing bridges without it are unaffected (backward compatible)
- Index naming follows convention: `uq_{table}_{col1}_{col2}` for bridges

**Negative**
- Additional indexes increase write overhead during ETL loads (acceptable trade-off for analytical workloads where reads dominate)
- Users who previously relied on no FK indexes (e.g., tiny lookup tables) must now explicitly opt out with `index=False`

## Affected Files

| File | Change |
|------|--------|
| `sqldim/core/kimball/fields.py` | `_UNSET` sentinel + FK auto-index logic |
| `sqldim/core/kimball/models.py` | `BridgeModel.__init_subclass__` with `__bridge_keys__` support |
| `sqldim/core/kimball/mixins.py` | `valid_from` index in `SCD2Mixin` |
| `tests/core/test_indexing.py` | 19 tests covering all four concerns + edge cases |

## See Also

- [Graph Analytics Roadmap](./graph-analytics-roadmap.md) — Tier 1 prerequisite
