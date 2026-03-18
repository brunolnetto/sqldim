# Fact Types Reference

sqldim implements the five core Kimball fact table patterns as typed base classes. Each carries an automatic `__fact_type__` tag used by the schema registry for categorization, visualization, and documentation.

## Fact Type Enumeration

```python
from sqldim.core.kimball.facts import FactType

FactType.TRANSACTION          # "transaction"
FactType.PERIODIC_SNAPSHOT     # "periodic_snapshot"
FactType.ACCUMULATING_SNAPSHOT # "accumulating_snapshot"
FactType.CUMULATIVE            # "cumulative"
FactType.ACTIVITY              # "activity"
```

## Base Classes

All five classes inherit from [`FactModel`](../../sqldim/core/kimball/models.py) and set `__fact_type__` automatically:

| Base Class | `__fact_type__` | Grain Description |
|---|---|---|
| `TransactionFact` | `transaction` | One row per business event |
| `PeriodicSnapshotFact` | `periodic_snapshot` | One row per grain per time period |
| `AccumulatingFact` | `accumulating_snapshot` | One row per pipeline instance (milestones) |
| `CumulativeFact` | `cumulative` | Running totals aggregated over time |
| `ActivityFact` | `activity` | Sparse, entity-centric event log |

## Defining a Fact Table

Subclass one of the typed bases and declare your columns. The `__fact_type__` tag is inherited automatically:

```python
from sqldim.core.kimball.facts import TransactionFact
from sqldim.core.kimball.fields import Field
from sqlmodel import Column
import sqlalchemy as sa

class OrderLine(TransactionFact, table=True):
    """One row per line item in an order."""
    __grain__ = "one row per order line"
    __strategy__ = "upsert"

    order_id: int = Field(primary_key=True)
    line_number: int = Field(primary_key=True)
    product_key: int = Field(foreign_key="dim_product.product_key")
    quantity: int = Field(default=0)
    unit_price: float = Field(default=0.0)
    line_total: float = Field(default=0.0)
```

### Metadata Class Variables

| Variable | Type | Purpose |
|---|---|---|
| `__fact_type__` | `FactType` | Auto-set by base class. Used by the schema registry for ER diagrams and categorization. |
| `__grain__` | `str \| None` | Human-readable grain declaration (e.g. `"one row per game"`). Captured by `SchemaGraph` for documentation. |
| `__strategy__` | `str \| None` | **Critical for execution.** Dispatches loading behavior in `DimensionalLoader`. See [Loading Strategies](#loading-strategies) below. |

## The Five Kimball Patterns

### Transaction Fact

Records indivisible business events. The most common fact type — one row per event.

```python
from sqldim.core.kimball.facts import TransactionFact

class PageView(TransactionFact, table=True):
    __grain__ = "one row per page view"
    __strategy__ = "bulk"

    view_id: int = Field(primary_key=True)
    user_key: int = Field(foreign_key="dim_user.user_key")
    page_key: int = Field(foreign_key="dim_page.page_key")
    session_duration_secs: int = Field(default=0)
```

### Periodic Snapshot Fact

Captures the state of a process at regular intervals (daily, weekly, monthly). One row per grain per period.

```python
from sqldim.core.kimball.facts import PeriodicSnapshotFact

class DailyAccountBalance(PeriodicSnapshotFact, table=True):
    __grain__ = "one row per account per day"
    __strategy__ = "upsert"

    account_key: int = Field(primary_key=True)
    snapshot_date: date = Field(primary_key=True)
    closing_balance: float = Field(default=0.0)
    num_transactions: int = Field(default=0)
```

### Accumulating Snapshot Fact

Tracks a pipeline or process with milestone columns that are patched over time. One row per pipeline instance.

```python
from sqldim.core.kimball.facts import AccumulatingFact

class OrderPipeline(AccumulatingFact, table=True):
    __grain__ = "one row per order"
    __strategy__ = "accumulating"

    order_id: int = Field(primary_key=True)
    order_date: Optional[datetime] = None
    packed_date: Optional[datetime] = None
    shipped_date: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
```

When `__strategy__` is set to `"accumulating"`, the `DimensionalLoader` instantiates an `AccumulatingLoader` that patches only the non-null milestone columns on each load.

### Cumulative Fact

Stores running totals aggregated over time. Often used with [lazy loaders](../guides/lazy_loaders.md) for efficient DuckDB-based computation.

```python
from sqldim.core.kimball.facts import CumulativeFact
from sqldim.core.mixins import DatelistMixin

class UserCumulated(CumulativeFact, DatelistMixin, table=True):
    __grain__ = "one row per user per date"
    __strategy__ = "merge"

    user_key: int = Field(primary_key=True)
    date_id: int = Field(primary_key=True)
    active_days_mask: str = Field(default="")  # bitmask from lazy loader
    total_events: int = Field(default=0)
    total_session_secs: int = Field(default=0)
```

> **Tip:** Cumulative facts pair naturally with the `DatelistMixin` and bitmask lazy loaders for high-performance daily engagement tracking. See the [Lazy Loaders guide](../guides/lazy_loaders.md) for details.

### Activity Fact

A sparse, entity-centric event log. Unlike transaction facts, activity facts are optimized for querying "what did this entity do?" rather than "what happened at this time?"

```python
from sqldim.core.kimball.facts import ActivityFact

class UserActivity(ActivityFact, table=True):
    __grain__ = "one row per user action"
    __strategy__ = "bulk"

    activity_id: int = Field(primary_key=True)
    user_key: int = Field(foreign_key="dim_user.user_key")
    action_type: str = Field(max_length=50)
    action_detail: Optional[str] = None
    occurred_at: datetime = Field(default_factory=datetime.utcnow)
```

## Loading Strategies

The `__strategy__` class variable controls how `DimensionalLoader` writes fact rows:

| Strategy | Behavior | Requirements |
|---|---|---|
| `"bulk"` | Fast batch insert via `BulkInsertStrategy` | None |
| `"upsert"` | Insert-or-update via `UpsertStrategy` | A natural key / conflict column |
| `"merge"` | SQL MERGE via `MergeStrategy` | A match column |
| `"accumulating"` | Patch milestone columns via `AccumulatingLoader` | Nullable milestone columns |
| *(none / default)* | Row-by-row `session.add()` | None (slowest) |

**Choosing a strategy:**

- **New events** (append-only) → `"bulk"`
- **Idempotent reloads** with a natural key → `"upsert"`
- **Complex merge logic** (e.g., dedup) → `"merge"`
- **Pipeline tracking** with milestone patches → `"accumulating"`

See [Sinks Reference](sinks.md) for sink-specific strategy support.

## Alternative: Direct FactModel Subclass

You may also subclass `FactModel` directly and set `__fact_type__` manually. This pattern is used in some example projects:

```python
from sqldim.core.kimball.models import FactModel
from sqldim.core.kimball.facts import FactType

class Game(FactModel, table=True):
    __fact_type__ = FactType.PERIODIC_SNAPSHOT
    __grain__ = "one row per game"
    __strategy__ = "bulk"
    # ... fields
```

This is functionally equivalent but loses the semantic clarity of the typed base classes. **Prefer the typed bases** (`TransactionFact`, etc.) in production code.

## See Also

- [Getting Started](../getting-started.md) — end-to-end tutorial including a fact table
- [Lazy Loaders](../guides/lazy_loaders.md) — DuckDB-native loaders for cumulative and activity facts
- [Sinks Reference](sinks.md) — which sinks support which loading strategies
- [CLI Reference](cli.md) — `sqldim schema graph` visualizes fact tables in Mermaid ER diagrams
