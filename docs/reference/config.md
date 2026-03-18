# Config Reference

`SqldimConfig` provides global defaults for SCD behaviour, batch sizes, caching, and migration paths. It uses Pydantic `BaseSettings`, so values can be overridden via environment variables.

## SqldimConfig

```python
from sqldim.config import SqldimConfig

config = SqldimConfig()  # uses defaults
config = SqldimConfig(default_batch_size=50_000)  # override programmatically
```

### Fields

| Field | Type | Default | Env Override | Description |
|---|---|---|---|---|
| `scd_default_type` | `int` | `2` | `SQLDIM_SCD_DEFAULT_TYPE` | Default SCD type when `__scd_type__` is not set on a `DimensionModel` |
| `scd_epoch` | `datetime` | `1970-01-01` | `SQLDIM_SCD_EPOCH` | Minimum valid-from timestamp for SCD history |
| `scd_infinity` | `datetime` | `9999-12-31` | `SQLDIM_SCD_INFINITY` | Open-ended valid-to sentinel for current SCD rows |
| `checksum_algorithm` | `Literal["md5", "sha1", "sha256"]` | `"md5"` | `SQLDIM_CHECKSUM_ALGORITHM` | Hash algorithm for SCD change detection |
| `default_batch_size` | `int` | `10_000` | `SQLDIM_DEFAULT_BATCH_SIZE` | Row count per flush for sinks and loaders |
| `sk_lookup_cache` | `bool` | `True` | `SQLDIM_SK_LOOKUP_CACHE` | Enable in-memory surrogate-key lookup cache |
| `sk_lookup_cache_ttl` | `int` | `3600` | `SQLDIM_SK_LOOKUP_CACHE_TTL` | Cache TTL in seconds for SK lookups |
| `allow_destructive` | `bool` | `False` | `SQLDIM_ALLOW_DESTRUCTIVE` | Allow destructive SCD operations and migrations without explicit opt-in |
| `migration_dir` | `str` | `"migrations"` | `SQLDIM_MIGRATION_DIR` | Path to the migration directory |

### Usage with Session

`AsyncDimensionalSession` accepts an optional `SqldimConfig`:

```python
from sqldim.config import SqldimConfig
from sqldim.session import AsyncDimensionalSession

config = SqldimConfig(default_batch_size=50_000, checksum_algorithm="sha256")

async with AsyncDimensionalSession.from_url("sqlite:///dw.db", config=config) as session:
    # loader uses config.default_batch_size
    ...
```

If no config is passed, `SqldimConfig()` is instantiated with defaults (which may include env variable overrides).

### Environment Variable Overrides

All fields can be set via environment variables. Pydantic `BaseSettings` handles type coercion automatically:

```bash
export SQLDIM_DEFAULT_BATCH_SIZE=50000
export SQLDIM_CHECKSUM_ALGORITHM=sha256
export SQLDIM_ALLOW_DESTRUCTIVE=true

python my_pipeline.py
```

### Common Overrides by Scenario

| Scenario | Fields to Change |
|---|---|
| Large dimensions (>10M rows) | `default_batch_size=100_000`, `sk_lookup_cache=True` |
| High-security compliance | `checksum_algorithm="sha256"` |
| CI/test environments | `default_batch_size=1_000`, `allow_destructive=True` |
| Production with strict governance | `allow_destructive=False`, `sk_lookup_cache_ttl=600` |

## See Also

- [Session Reference](session.md) — passing config to `AsyncDimensionalSession`
- [Loader Reference](loader.md) — how batch size affects loading
- [Exceptions Reference](exceptions.md) — `DestructiveOperationError`, `DestructiveMigrationError`
- [Observability](../features/observability.md) — exporter configuration (OTLP endpoints, console)
- [Lineage Reference](lineage.md) — emitter configuration (Console, OpenLineage)
