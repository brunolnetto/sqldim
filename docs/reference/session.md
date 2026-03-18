# Session Reference

`AsyncDimensionalSession` manages database connections and provides an async context manager for running loaders and queries.

## AsyncDimensionalSession

```python
from sqldim.session import AsyncDimensionalSession

async with AsyncDimensionalSession.from_url("sqlite:///dw.db") as session:
    loader = DimensionalLoader(session, models=[CustomerDim])
    # ...
```

### Constructor

```python
class AsyncDimensionalSession:
    def __init__(self, engine: AsyncEngine, config: SqldimConfig | None = None): ...
```

| Parameter | Type | Description |
|---|---|---|
| `engine` | `AsyncEngine` | A SQLAlchemy `AsyncEngine` instance |
| `config` | `SqldimConfig \| None` | Optional configuration override. Uses defaults if not provided. |

### from_url()

Create a session from a database URL. The most common entry point.

```python
@classmethod
def from_url(cls, url: str, **kwargs) -> "AsyncDimensionalSession": ...
```

| Parameter | Type | Description |
|---|---|---|
| `url` | `str` | SQLAlchemy connection string (e.g., `"sqlite:///dw.db"`, `"postgresql+asyncpg://user:pass@host/db"`) |
| `**kwargs` | — | Passed to `sqlalchemy.ext.asyncio.create_async_engine()` (e.g., `echo=True`, `pool_size=10`) |

```python
# SQLite (local development)
session = AsyncDimensionalSession.from_url("sqlite:///dw.db")

# PostgreSQL (production)
session = AsyncDimensionalSession.from_url(
    "postgresql+asyncpg://user:pass@localhost:5432/datawarehouse",
    pool_size=20,
    max_overflow=10,
)

# DuckDB (via SQLAlchemy dialect)
session = AsyncDimensionalSession.from_url("duckdb:///analytics.duckdb")
```

### Context Manager

Use as an async context manager to get a sync `Session` for `DimensionalLoader`:

```python
async with AsyncDimensionalSession.from_url("sqlite:///dw.db") as session:
    # session is a SQLAlchemy SyncSession here
    loader = DimensionalLoader(session, models=[CustomerDim, SalesFact])
    loader.register(CustomerDim, source=data)
    await loader.run()
```

The context manager handles:
- Entering the session on `__aenter__`
- Committing on clean exit
- Rolling back on exception in `__aexit__`
- Closing the session

### Common Connection Strings

| Backend | Connection String | Notes |
|---|---|---|
| SQLite | `sqlite:///dw.db` | Local file, no async driver needed |
| PostgreSQL | `postgresql+asyncpg://user:pass@host/db` | Requires `asyncpg` package |
| DuckDB | `duckdb:///analytics.duckdb` | Requires DuckDB SQLAlchemy dialect |
| MySQL | `mysql+aiomysql://user:pass@host/db` | Requires `aiomysql` package |

## Complete Example

```python
from sqldim import DimensionModel, SCD2Mixin, Field, DimensionalLoader
from sqldim.session import AsyncDimensionalSession

class CustomerDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["customer_id"]
    customer_key: int = Field(default=None, primary_key=True)
    customer_id: str = Field(max_length=50)
    name: str = Field(max_length=200)

async def main():
    async with AsyncDimensionalSession.from_url("sqlite:///dw.db") as session:
        loader = DimensionalLoader(session, models=[CustomerDim])
        loader.register(CustomerDim, source=[
            {"customer_id": "C001", "name": "Alice"},
            {"customer_id": "C002", "name": "Bob"},
        ])
        await loader.run()

import asyncio
asyncio.run(main())
```

## See Also

- [Config Reference](config.md) — `SqldimConfig` defaults and environment overrides
- [Loader Reference](loader.md) — using sessions with `DimensionalLoader`
- [Dimensions Reference](dimensions.md) — defining models to load
