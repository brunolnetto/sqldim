# ADR: Late Arriving Dimensions (Inferred Members)

**Date**: 2026-03-18
**POC**: @pingu
**TL;DR**: When a fact arrives before its dimension row exists, sqldim currently fails with a broken FK or `SKResolutionError`. Introduce inferred member generation — a placeholder dimension row created on-the-fly during fact loading, later reconnected when the real dimension data arrives.

## Status

Proposed

## Context

In real-world ETL pipelines, facts frequently arrive before their corresponding dimension data. Common causes: source system extraction ordering, event-driven architectures where fact events fire before dimension master data syncs, and late-binding data marts.

sqldim currently handles this poorly. The failure chain:

1. `SKResolver.resolve()` looks up the natural key in the dimension table
2. Natural key not found → returns `None` (default) or raises `SKResolutionError` (if `raise_on_missing=True`)
3. `DimensionalLoader._resolve_fks` leaves the FK column as `None` or the raw natural key value
4. Fact insert fails with `IntegrityError` or stores a broken link

There is no inferred member generation anywhere in the codebase — no placeholder creation, no reconnection logic, no audit trail for late arrivals.

### Kimball's Inferred Member Pattern

Ralph Kimball's standard approach has three phases:

1. **Infer**: Create a placeholder dimension row with the natural key, default/unknown values for all attributes, and a flag marking it as inferred
2. **Load**: The fact row links to the inferred member's surrogate key — no data loss
3. **Reconnect**: When the real dimension data arrives, update the inferred member's attributes (in-place for SCD1, close+insert for SCD2) and clear the inferred flag

## Decision

Implement inferred member generation as an opt-in behavior on `SKResolver`, integrated with the existing SCD pipeline.

### Inferred Member Row Shape

```python
class CustomerDimension(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["customer_code"]
    id: int = Field(primary_key=True)
    customer_code: str = Field(natural_key=True)
    name: str = "UNKNOWN"
    region: str = "UNKNOWN"
    is_inferred: bool = False  # Flag column — required when inferred members are enabled
```

When an inferred member is generated:
- `customer_code` = the natural key from the fact
- All non-key attributes = their column default (or `"UNKNOWN"` if no default)
- `is_inferred` = `True`
- `valid_from` = now, `valid_to` = `None`, `is_current` = `True`

### Configuration

```python
SKResolver(
    dimension=CustomerDimension,
    session=session,
    infer_missing=True,           # Enable inferred member generation
    inferred_defaults={           # Override defaults for specific columns
        "name": "INFERRED",
        "region": "UNASSIGNED",
    },
)
```

When `infer_missing=False` (default), behavior is unchanged — returns `None` or raises.

### Reconnection Strategy

When the real dimension data arrives via the normal SCD pipeline:

**For SCD Type 1 (overwrite):**
```sql
UPDATE customer_dim
SET name = :real_name, region = :real_region, is_inferred = FALSE
WHERE customer_code = :nk AND is_inferred = TRUE AND is_current = TRUE
```

**For SCD Type 2 (versioned):**
```sql
-- Close the inferred version
UPDATE customer_dim
SET valid_to = NOW(), is_current = FALSE
WHERE customer_code = :nk AND is_inferred = TRUE AND is_current = TRUE;

-- Insert the real version
INSERT INTO customer_dim (customer_code, name, region, is_inferred, valid_from, ...)
VALUES (:nk, :real_name, :real_region, FALSE, NOW(), ...);
```

The reconnection happens transparently within `LazySCDProcessor._classify` — when a `changed` row is detected and the current version has `is_inferred = TRUE`, the reconnection SQL is used instead of the standard close+insert.

### Audit Trail

Inferred member creation and reconnection emit lineage events:

```python
LineageEvent(
    type="inferred_member_created",
    dimension="customer_dim",
    natural_key="C123",
    fact_table="order_fact",
)

LineageEvent(
    type="inferred_member_reconnected",
    dimension="customer_dim",
    natural_key="C123",
    versions_merged=1,  # How many inferred versions were replaced
)
```

### Requirements for Opt-In

To use inferred members, a dimension model must:

1. Have an `is_inferred: bool` column (enforced at resolver construction time)
2. Have defaults for all non-key attributes (enforced at resolver construction time)
3. Be loaded with the lazy SCD engine (reconnection is SQL-native)

If these requirements aren't met, `SKResolver(infer_missing=True)` raises `ConfigurationError` with a clear message.

## Consequences

**Positive**
- Facts are never dropped due to missing dimensions — zero data loss
- Reconnection is transparent — the SCD pipeline handles it without user code
- Audit trail tracks which dimension rows were inferred and when they were reconnected
- Opt-in — existing pipelines unaffected
- `is_inferred` flag enables downstream filtering (exclude inferred rows from reports)

**Neutral**
- Inferred member adds a required column to participating dimensions
- Default values for attributes are a modeling decision (not automatable)
- Reconnection for SCD2 creates a "phantom version" — the inferred row exists in history even though it was never "real"

**Negative**
- Inferred members can accumulate if dimension data never arrives — needs monitoring
- Reconnection in SCD2 changes surrogate keys for facts loaded against the inferred member — facts must use natural key resolution, not stored SKs, or a SK remapping step is needed
- Adds complexity to `SKResolver` and `LazySCDProcessor` — two of the most critical paths
- `is_inferred` column is a modeling convention, not a framework-enforced constraint — forgetting it causes silent incorrect behavior

## Future Work

| Item | Description |
|------|-------------|
| Inferred member expiry | Auto-delete inferred members older than N days if never reconnected |
| Bulk reconciliation | Post-load job that scans for unreconnected inferred members and alerts |
| SK remapping | For fact tables that store SKs (not NKs), a remapping step after reconnection |
| Multi-source inference | When the same natural key arrives from multiple fact sources before the dimension |

## Key Files (Planned)

| File | Change |
|------|--------|
| `sqldim/core/loaders/resolution.py` | Add `infer_missing`, `inferred_defaults`, validation logic |
| `sqldim/core/kimball/dimensions/scd/processors/_lazy_type2.py` | Reconnection branch in `_classify` |
| `sqldim/lineage/events.py` | Add `inferred_member_created` and `inferred_member_reconnected` event types |

## See Also

- [SCD Engine Design](./scd-engine-design.md) — change detection pipeline where reconnection hooks in
- [Column-Level Lineage Architecture](./column-level-lineage.md) — tracking inferred attribute provenance
