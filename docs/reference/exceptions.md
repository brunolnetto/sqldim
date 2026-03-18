# Exceptions Reference

All sqldim errors inherit from `SqldimError`. The hierarchy is organised into five groups: schema, SCD, load, migration, and semantic errors.

## Hierarchy

```
SqldimError
├── SchemaError
│   ├── GrainViolationError
│   └── NaturalKeyError
├── SCDError
│   └── DestructiveOperationError
├── LoadError
│   ├── SKResolutionError
│   ├── IdempotencyError
│   └── TransformError
│       └── TransformTypeError
├── MigrationError
│   └── DestructiveMigrationError
└── SemanticError
    └── InvalidJoinError
```

## Schema Errors

Raised at model definition or introspection time.

### `SchemaError`

Base for schema-definition errors. Raised when a model violates dimensional modeling rules.

```python
from sqldim.exceptions import SchemaError
```

### `GrainViolationError`

Raised when a `FactModel` is missing a `__grain__` declaration.

```python
from sqldim.exceptions import GrainViolationError

class MyFact(FactModel, table=True):
    pass  # raises GrainViolationError — no __grain__ defined
```

### `NaturalKeyError`

Raised when a `DimensionModel` has a missing or ambiguous natural key (`__natural_key__`).

```python
from sqldim.exceptions import NaturalKeyError

class MyDim(DimensionModel, table=True):
    __natural_key__ = []  # raises NaturalKeyError — empty natural key
```

## SCD Errors

Raised at load time during SCD processing.

### `SCDError`

Base for SCD-related errors.

```python
from sqldim.exceptions import SCDError
```

### `DestructiveOperationError`

Raised when a destructive SCD operation is attempted without explicit opt-in. Protects against accidental data loss during SCD Type 1 overwrites or similar operations.

```python
from sqldim.exceptions import DestructiveOperationError

try:
    processor.apply_scd_type1(source_df, allow_destructive=False)
except DestructiveOperationError:
    print("Set allow_destructive=True to proceed.")
```

## Load Errors

Raised during ETL loading.

### `LoadError`

Base for loading errors.

```python
from sqldim.exceptions import LoadError
```

### `SKResolutionError`

Raised when a natural key cannot be resolved to a surrogate key in the dimension table. Typically means a fact row references a dimension member that does not exist.

```python
from sqldim.exceptions import SKResolutionError

try:
    loader.load(fact_records)
except SKResolutionError as e:
    print(f"Unresolved dimension key: {e}")
    # Load missing dimension members first, then retry
```

### `IdempotencyError`

Raised when a duplicate record is inserted without an idempotency strategy (e.g., no `__strategy__` set and a duplicate primary key is encountered).

```python
from sqldim.exceptions import IdempotencyError

# Fix: set __strategy__ = "upsert" on the fact model
class MyFact(FactModel, table=True):
    __strategy__ = "upsert"  # enables idempotent loads
```

### `TransformError`

Base for transform pipeline errors (Narwhals-based transforms).

```python
from sqldim.exceptions import TransformError
```

### `TransformTypeError`

Raised when a transform produces a dtype incompatible with the target model column. Includes detailed diagnostics.

```python
from sqldim.exceptions import TransformTypeError

try:
    transformed = pipeline.run(source_df)
except TransformTypeError as e:
    print(e)
    # Column 'revenue': expected Float64, got String
    # Hint: use .cast(pl.Float64) before loading
    print(f"Column: {e.column}")
    print(f"Expected: {e.expected}, Got: {e.got}")
    print(f"Hint: {e.hint}")
```

| Attribute | Type | Description |
|---|---|---|
| `column` | `str` | Column name with the type mismatch |
| `expected` | `Any` | Expected dtype |
| `got` | `Any` | Actual dtype produced by the transform |
| `hint` | `str` | Optional remediation hint |

## Migration Errors

### `MigrationError`

Base for migration errors.

```python
from sqldim.exceptions import MigrationError
```

### `DestructiveMigrationError`

Raised when a migration would cause data loss (e.g., dropping a column) without the `SQLDIM_ALLOW_DESTRUCTIVE=true` environment variable set.

```bash
# Allow destructive migrations
export SQLDIM_ALLOW_DESTRUCTIVE=true
sqldim migrations generate "drop legacy column"
```

```python
from sqldim.exceptions import DestructiveMigrationError

try:
    apply_migration(migration)
except DestructiveMigrationError:
    print("Set SQLDIM_ALLOW_DESTRUCTIVE=true to allow this migration.")
```

## Semantic Errors

Raised when a query violates dimensional modeling semantics.

### `SemanticError`

Base for semantic query errors. Raised when a query violates dimensional semantics (e.g., summing a non-additive measure).

```python
from sqldim.exceptions import SemanticError
```

### `InvalidJoinError`

Raised when no foreign-key path exists between a fact table and a requested dimension in a query.

```python
from sqldim.exceptions import InvalidJoinError

try:
    result = query.join("dim_region")  # no FK path from fact to dim_region
except InvalidJoinError as e:
    print(f"No join path: {e}")
```

## Handling Patterns

### Catch-all

```python
from sqldim.exceptions import SqldimError

try:
    loader.load(records)
except SqldimError as e:
    logger.error("sqldim error: %s", e)
    raise
```

### Granular handling

```python
from sqldim.exceptions import (
    SKResolutionError,
    IdempotencyError,
    TransformTypeError,
    DestructiveMigrationError,
)

try:
    transformed = pipeline.run(source)
    loader.load(transformed)
except TransformTypeError as e:
    fix_dtypes_and_retry(e)
except SKResolutionError:
    load_missing_dimensions()
    loader.load(transformed)
except IdempotencyError:
    loader.upsert(transformed)
```

## See Also

- [Fact Types Reference](fact_types.md) — `__grain__` and `__strategy__` that trigger `GrainViolationError` / `IdempotencyError`
- [Sinks Reference](sinks.md) — sink-specific error behavior
- [Lazy Loaders](../guides/lazy_loaders.md) — loader error handling patterns
