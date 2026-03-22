# ADR: Schema Evolution Policy

**Date**: 2026-03-18
**POC**: @pingu
**TL;DR**: Formalize what schema changes are safe, additive, or breaking across Parquet/Delta/Postgres backends. Leverage existing `ColumnSpec.is_breaking_change` detection and `union_by_name` Parquet reads. Define a migration workflow for breaking changes.

## Status

Proposed

## Context

sqldim models evolve over time — new columns are added, types change, natural keys get renamed. Each backend handles these differently, and the current behavior is implicit rather than policy-driven.

### Current State

| Backend | New Column | Type Change | Column Rename | NK Rename |
|---------|-----------|-------------|---------------|-----------|
| Parquet | ✅ `union_by_name=True` | 🟡 DuckDB auto-cast | ❌ Data loss | ❌ Destructive |
| Delta | 🟡 `MERGE INTO` handles | ❌ Fails at Delta boundary | ❌ Data loss | ❌ Destructive |
| Postgres | 🟡 Requires `ALTER TABLE` | ❌ Requires `ALTER TABLE` | ❌ Requires `ALTER TABLE` | ❌ Requires `ALTER TABLE` |

Existing detection:
- `ColumnSpec.is_breaking_change` (`contracts/schema.py:17`) correctly identifies type changes and nullable narrowing
- `BackfillHint` (`migrations/ops.py:4`) flags required backfills before `NOT NULL` changes
- `migrations/generator.py` warns on NK renames but doesn't block them

What's missing: a formal policy that answers "is this change safe to deploy?" before the user hits a runtime error.

## Decision

Classify all schema changes into three tiers and define the required workflow for each.

### Tier 1: Safe (No Action Required)

Changes that work automatically across all backends without data loss:

| Change | Why Safe |
|--------|----------|
| Add nullable column with default | `union_by_name` fills `NULL` for old files; `ALTER TABLE` not needed for nullable |
| Add column to SQLModel model only (no DB column) | Pure Python — no physical schema impact |
| Change `Field(description=...)` | Metadata-only — no physical impact |
| Change `Field(additive=...)` | Metadata-only — no physical impact |

**User action**: Deploy model change. No migration needed.

### Tier 2: Additive (Requires Explicit Migration)

Changes that need a migration step but don't break existing data:

| Change | Migration |
|--------|-----------|
| Add `NOT NULL` column | `BackfillHint` → fill default values → `ALTER TABLE ADD COLUMN ... NOT NULL DEFAULT ...` |
| Widen type (`int` → `bigint`, `float` → `double`) | `ALTER TABLE ALTER COLUMN TYPE` (safe widening) |
| Add index | `CREATE INDEX` (non-blocking) |
| Add `__bridge_keys__` | `CREATE UNIQUE INDEX` (fails if duplicates exist — validate first) |
| Add `is_inferred` column for late-arriving dims | `ALTER TABLE ADD COLUMN` with `DEFAULT FALSE` |

**User action**: Run migration script before deploying model change. Parquet files auto-adapt via `union_by_name`. Delta/Postgres need explicit DDL.

### Tier 3: Breaking (Requires Backfill + Rewrite)

Changes that cannot be applied incrementally and require data rewriting:

| Change | Why Breaking | Required Workflow |
|--------|-------------|-------------------|
| Narrow type (`bigint` → `int`) | Data truncation risk | Backfill: validate no overflow → Rewrite: `ALTER TABLE` or Parquet rewrite |
| Nullable → `NOT NULL` without default | Existing NULLs violate constraint | Backfill: fill all NULLs → `ALTER TABLE` |
| Rename column | Old files/rows reference old name | Rewrite: Parquet rewrite with new schema, or `ALTER TABLE RENAME` + backfill |
| Rename natural key | Breaks NK-based joins, SCD checksums | Backfill: update all referencing facts + recompute checksums |
| Remove column | Data loss | Backfill: verify column is unused → Rewrite: drop from model + files |
| Change SCD type (e.g., 1 → 2) | Different state management semantics | `BackfillHint` + `ops.py` migration helpers |

**User action**: Plan a maintenance window. Run backfill validation. Deploy migration. Verify. Deploy model change.

### Enforcement via Contract Engine

Extend `ContractEngine` to validate model-to-physical schema alignment:

```python
ContractEngine.check_evolution_safety(
    model=CustomerDimension,
    backend="parquet",  # or "delta", "postgres"
    path="s3://warehouse/silver/customer_dim/",
)
```

Returns a structured report:
```python
EvolutionReport(
    safe_changes=[AddedColumn("email", nullable=True)],
    additive_changes=[WidenedType("id", int, bigint)],
    breaking_changes=[RenamedColumn("code", "customer_code")],
    required_migrations=["ALTER TABLE ... ALTER COLUMN id TYPE bigint"],
)
```

### Natural Key Rename — Special Handling

NK renames are the most dangerous change because they affect:
1. SCD checksum computation (column name in `track_columns`)
2. `SKResolver` lookups
3. Fact table FK references
4. Bridge table composite keys
5. Schema graph role-playing resolution

Required workflow:
1. Add new NK column alongside old one (dual-key period)
2. Backfill new column from old column
3. Update all fact FK references to new column
4. Run full checksum recompute on dimension
5. Remove old column (Tier 3 rewrite)

This is too complex for automation — the ADR documents the steps but does not automate them.

## Consequences

**Positive**
- Users know before deploying whether a change is safe
- `ColumnSpec.is_breaking_change` gets used instead of being dead code
- Parquet's `union_by_name` is formally recognized as a first-class evolution mechanism
- NK rename danger is documented with a concrete workflow

**Neutral**
- Tier classification is advisory — not enforced at runtime (framework can't prevent `ALTER TABLE`)
- `check_evolution_safety` is a new capability on `ContractEngine` — opt-in
- Parquet is the most forgiving backend by design — Delta/Postgres require more manual work

**Negative**
- No automatic migration generation — user must write DDL for Tier 2/3 changes
- Tier 3 changes require downtime or dual-write periods (inherent to the operation)
- Schema evolution checking adds latency to pipeline startup if run on every load

## Future Work

| Item | Description |
|------|-------------|
| Auto-migration generator | Generate `ALTER TABLE` DDL from `EvolutionReport.additive_changes` |
| Parquet rewrite utility | Automated Parquet file rewriting for Tier 3 changes on file-based backends |
| Dual-key transition helper | First-class support for the NK rename workflow |
| Schema versioning | Track schema version in model metadata, compare across deployments |

## Key Files

| File | Current Role | Planned Change |
|------|-------------|----------------|
| `sqldim/contracts/schema.py` | `ColumnSpec.is_breaking_change` | Used by `check_evolution_safety` |
| `sqldim/migrations/ops.py` | `BackfillHint`, SCD2 init | Expanded with Tier 2/3 migration helpers |
| `sqldim/migrations/generator.py` | NK rename warnings | Enhanced with dual-key transition support |
| `sqldim/contracts/engine.py` | Rule evaluation | Add `check_evolution_safety` method |
| `sqldim/sources/parquet.py` | `union_by_name=True` | No change — already correct |

## See Also

- [SCD Engine Design](./scd-engine-design.md) — checksum recomputation on schema change
- [Late Arriving Dimensions](./late-arriving-dimensions.md) — `is_inferred` column as Tier 2 additive change
- [Physical Partitioning and Bucketing](./physical-partitioning-and-bucketing.md) — partition scheme changes as Tier 3
