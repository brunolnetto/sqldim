# Reference: Example Datasets

`sqldim.examples.datasets` ships a library of OLTP-style source adapters for use in tests, benchmarks, and tutorials.  The public API has two layers:

| Class | Purpose |
|---|---|
| `BaseSource` | Single-table OLTP adapter |
| `Dataset` | FK-ordered collection of related `BaseSource` instances |

---

## `BaseSource`

Base class for every OLTP table adapter.  Subclasses override the methods they support; callers iterate what is available.

```python
class BaseSource:
    provider: str          # human-readable label, e.g. "ecommerce"
    OLTP_DDL: str          # CREATE TABLE statement
    DIM_DDL:  str          # CREATE TABLE for the dimensional projection

    def setup(self, con: duckdb.DuckDBPyConnection) -> None: ...
    def teardown(self, con: duckdb.DuckDBPyConnection) -> None: ...
    def snapshot(self) -> list[dict]: ...         # static fixture rows
    def event_batch(self, n: int) -> list[dict]:  # generated CDC events
    def provider_name(self) -> str: ...
```

Sources that only have static data raise `NotImplementedError` in `event_batch`.  Sources that do not have a dimensional projection raise it in
`snapshot`.  `Dataset` handles these gracefully (`snapshots()` and `event_batches()` skip sources that raise).

### `SchematicSource`

A `BaseSource` subclass that requires `OLTP_DDL`/`DIM_DDL` to be defined. Used for sources that represent a full schema with both OLTP and dimensional projections.

---

## `DatasetFactory`

A registry for sources and datasets, used in tests and benchmarks to look up registered factories by name.

```python
from sqldim.examples.datasets.base import DatasetFactory

# register a source
@DatasetFactory.register("my_source")
def _make():
    return MySource(n=1000)

# retrieve it later
source = DatasetFactory.get("my_source")
```

---

## `Dataset`

A named, FK-ordered collection of `BaseSource` instances.  Sources are listed **parent-first** (dimensions before facts) so that `setup` and `teardown` never break FK constraints.

### Constructor

```python
from sqldim.examples.datasets import Dataset

pipeline = Dataset(
    name="ecommerce",
    sources=[
        (CustomersSource(n=100), "raw_customers"),
        (ProductsSource(n=50),   "raw_products"),
        (OrdersSource(n=200),    "raw_orders"),   # references customers + products
    ],
)
```

`sources` is a `list[tuple[BaseSource, str]]` where the second element is the DuckDB table name used by `setup`/`teardown`.

### Methods

| Method | Description |
|---|---|
| `setup(con)` | Creates all tables in FK order |
| `teardown(con)` | Drops all tables in reverse order |
| `snapshots()` | Returns `{table: rows}` for every source that has a `snapshot()` |
| `event_batches(n=1)` | Returns `{table: rows}` for every source that has `event_batch(n)` |
| `table_names()` | Ordered list of table names |
| `dataset[table_name]` | `BaseSource` lookup by table name |
| `describe()` | One-line human-readable summary (provider names) |

### Example

```python
import duckdb
from sqldim.examples.datasets import Dataset
from sqldim.examples.datasets.domains.ecommerce import (
    CustomersSource, ProductsSource, OrdersSource,
)

pipeline = Dataset("ecommerce", [
    (CustomersSource(n=100), "raw_customers"),
    (ProductsSource(n=50),   "raw_products"),
    (OrdersSource(n=200),    "raw_orders"),
])

con = duckdb.connect()
pipeline.setup(con)

for table, rows in pipeline.snapshots().items():
    print(f"{table}: {len(rows)} initial rows")

batches = pipeline.event_batches(n=5)
for table, events in batches.items():
    print(f"{table}: {len(events)} events")

pipeline.teardown(con)
con.close()
```

---

## Domain Catalogue

| Domain module | Sources | Description |
|---|---|---|
| `domains.ecommerce` | `CustomersSource`, `ProductsSource`, `StoresSource`, `OrdersSource` | Retail pipeline with FK-linked orders |
| `domains.enterprise` | `AccountsSource`, `EmployeesSource`, `EventsSource` | B2B accounts, headcount, and activity |
| `domains.fintech` | `TransactionsSource`, `PortfolioSource` | Financial transactions and holdings |
| `domains.media` | `MoviesSource`, `ViewingSource` | Streaming viewing events |
| `domains.devops` | `GithubSource`, `DeploySource` | CI/CD events and releases |
| `domains.saas_growth` | `UsersSource`, `SubscriptionsSource` | SaaS funnel and MRR |
| `domains.supply_chain` | `ShipmentsSource`, `WarehouseSource` | Logistics events |
| `domains.user_activity` | `SessionSource`, `PageViewSource` | Clickstream events |
| `domains.nba_analytics` | `GamesSource`, `PlayersSource` | Sports analytics |
| `domains.hierarchy` | `OrgSource` | Organisational hierarchy |
| `domains.dgm` | `DGMSource` | Graph-model entity/relationship data |

Each domain's `sources.py` holds the full `DatasetSpec` objects and vocabularies.
The domain `__init__.py` re-exports public classes only.

---

## See Also

- [BaseSource implementation](../../sqldim/examples/datasets/base.py)
- [Dataset implementation](../../sqldim/examples/datasets/dataset.py)
- [Domain sources](../../sqldim/examples/datasets/domains/)
