# Loader Reference

`DimensionalLoader` is the primary ETL orchestrator. It accepts registered models and data sources, then dispatches dimension SCD logic and fact loading strategies.

## DimensionalLoader

```python
from sqldim import DimensionalLoader

loader = DimensionalLoader(session, models=[CustomerDim, SalesFact])
```

### Constructor

```python
class DimensionalLoader:
    def __init__(self, session: Session, models: List[Type[Any]]): ...
```

| Parameter | Type | Description |
|---|---|---|
| `session` | `Session` | A SQLAlchemy sync session (from `AsyncDimensionalSession` or created manually) |
| `models` | `List[Type[Any]]` | List of `DimensionModel` and `FactModel` classes to load. Order matters — dimensions must precede facts that reference them. |

### register()

Bind a data source to a model before calling `.run()`.

```python
def register(
    self,
    model: Type[Any],
    source: List[Dict[str, Any]],
    key_map: Optional[Dict[str, Tuple[Type[DimensionModel], str]]] = None,
): ...
```

| Parameter | Type | Description |
|---|---|---|
| `model` | `Type` | The `DimensionModel` or `FactModel` to load into |
| `source` | `List[Dict[str, Any]]` | List of row dictionaries. For vectorised path, pass a Narwhals-compatible DataFrame instead. |
| `key_map` | `Dict[str, Tuple[Type[DimensionModel], str]] \| None` | Maps fact column names to `(DimensionModel, natural_key_column)` for surrogate-key resolution. Required for facts with foreign keys. |

```python
loader.register(
    CustomerDim,
    source=[
        {"customer_id": "C001", "name": "Alice", "country": "US"},
        {"customer_id": "C002", "name": "Bob", "country": "UK"},
    ],
)

loader.register(
    SalesFact,
    source=[
        {"order_id": 1, "customer_id": "C001", "amount": 99.99},
    ],
    key_map={"customer_id": (CustomerDim, "customer_id")},
)
```

### run()

Execute all registered loads. Async.

```python
async def run(self) -> None: ...
```

```python
await loader.run()
```

Internally, `.run()` processes models in registration order:

1. **Dimensions** — resolves natural keys to surrogate keys, applies SCD logic based on `__scd_type__`
2. **Facts** — resolves foreign keys via `key_map`, dispatches loading based on `__strategy__`

### Strategy Dispatch for Facts

When `.run()` encounters a `FactModel`, it checks `__strategy__`:

| `__strategy__` | Loader/Strategy | Behavior |
|---|---|---|
| `"bulk"` | `BulkInsertStrategy` | Fast batch insert |
| `"upsert"` | `UpsertStrategy` | Insert-or-update on conflict |
| `"merge"` | `MergeStrategy` | SQL MERGE with match column |
| `"accumulating"` | `AccumulatingLoader` | Patch nullable milestone columns |
| *(none)* | Default | Row-by-row `session.add()` |

See [Fact Types Reference](fact_types.md) for strategy selection guidance and [Sinks Reference](sinks.md) for sink-specific support.

## Surrogate Key Resolution

When a fact row references a dimension, the loader looks up the surrogate key:

1. Read the natural key value from the fact row (e.g., `"customer_id": "C001"`)
2. Look up `key_map["customer_id"]` → `(CustomerDim, "customer_id")`
3. Query `dim_customer` where `customer_id = "C001"` and `is_current = True`
4. Replace `"C001"` with the resolved `customer_key` integer

If the natural key is not found, raises [`SKResolutionError`](exceptions.md).

## Vectorised Path

When a Narwhals-compatible DataFrame (Polars, Pandas, Arrow) is passed as `source`, the loader automatically switches to the vectorised path:

```python
import polars as pl

df = pl.read_csv("daily_extract.csv")
loader.register(ProductDim, source=df)
await loader.run()
```

The vectorised path:
1. Computes column hashes in bulk (C/Rust native)
2. Performs a single LEFT JOIN to classify rows (new / unchanged / changed)
3. Commits changes via bulk insert

See [Vectorized ETL Guide](../guides/vectorized_etl.md) for performance benchmarks.

## Lazy Loaders

For DuckDB-specific high-performance patterns (bitmask, cumulative, accumulating), use the dedicated lazy loaders directly instead of `DimensionalLoader`:

| Loader | Use Case |
|---|---|
| `BitmaskerLoader` | 32-bit bitmask datelist encoding |
| `CumulativeLoader` | Dense history arrays |
| `AccumulatingLoader` | Milestone patching |
| `ArrayMetricLoader` | Month-partitioned arrays |
| `SnapshotLoader` | Periodic snapshot facts |

See [Lazy Loaders Guide](../guides/lazy_loaders.md) for detailed usage.

## Complete Example

```python
from sqldim import DimensionModel, SCD2Mixin, Field, DimensionalLoader
from sqldim.core.kimball.facts import TransactionFact
from sqldim.session import AsyncDimensionalSession

class CustomerDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["customer_id"]
    customer_key: int = Field(default=None, primary_key=True)
    customer_id: str = Field(max_length=50)
    name: str = Field(max_length=200)

class SalesFact(TransactionFact, table=True):
    __grain__ = "one row per sale"
    __strategy__ = "bulk"
    sale_key: int = Field(default=None, primary_key=True)
    customer_key: int = Field(foreign_key="dim_customer.customer_key")
    amount: float = Field(default=0.0)

async def load_data():
    async with AsyncDimensionalSession.from_url("sqlite:///dw.db") as session:
        loader = DimensionalLoader(session, models=[CustomerDim, SalesFact])
        loader.register(CustomerDim, source=[
            {"customer_id": "C001", "name": "Alice"},
        ])
        loader.register(SalesFact, source=[
            {"customer_key": 1, "amount": 99.99},
        ], key_map={"customer_key": (CustomerDim, "customer_id")})
        await loader.run()
```

## See Also

- [Dimensions Reference](dimensions.md) — defining `DimensionModel` with SCD mixins
- [Fact Types Reference](fact_types.md) — `__strategy__` values and Kimball patterns
- [Session Reference](session.md) — creating database sessions
- [Lazy Loaders Guide](../guides/lazy_loaders.md) — DuckDB-native loaders
- [Exceptions Reference](exceptions.md) — `SKResolutionError`, `IdempotencyError`
