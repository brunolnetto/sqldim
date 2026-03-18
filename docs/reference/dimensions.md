# Dimensions Reference

Defines dimension tables using `DimensionModel` with SCD tracking, natural keys, and optional graph projection.

## DimensionModel

Base class for all dimension tables. Inherits from `SQLModel`.

```python
from sqldim import DimensionModel

class CustomerDim(DimensionModel, table=True):
    __natural_key__ = ["customer_id"]
    __scd_type__ = 2
    # ... columns
```

### Class Variables

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `__natural_key__` | `List[str]` | `[]` | Column names that uniquely identify the business entity. Used for change detection and surrogate-key resolution. |
| `__scd_type__` | `int` | `2` | SCD tracking strategy. Supported: `1` (overwrite), `2` (full history), `3` (previous/current pair), `6` (hybrid T1+T2+T3). |

## SCD2Mixin

Adds temporal tracking fields to a `DimensionModel`. Include it in your dimension's inheritance chain for SCD Type 2 or Type 6.

```python
from sqldim import DimensionModel, SCD2Mixin

class CustomerDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["customer_id"]
    __scd_type__ = 2

    customer_id: str = Field(primary_key=True)
    name: str = Field(max_length=200)
    country: str = Field(max_length=100)
```

### Fields Added by SCD2Mixin

| Field | Type | Description |
|---|---|---|
| `valid_from` | `datetime` | Row effective start time (defaults to current UTC) |
| `valid_to` | `Optional[datetime]` | Row effective end time (`None` = currently valid) |
| `is_current` | `bool` | Whether this is the active record (`True` by default) |
| `checksum` | `Optional[str]` | MD5/SHA hash of tracked columns for change detection |

## BridgeModel

Base class for Kimball bridge tables handling many-to-many relationships between dimensions. Includes a weighting factor to prevent double-counting.

```python
from sqldim import BridgeModel

class ProductCustomerBridge(BridgeModel, table=True):
    product_key: int = Field(foreign_key="dim_product.product_key", primary_key=True)
    customer_key: int = Field(foreign_key="dim_customer.customer_key", primary_key=True)
    # weight: float is inherited (default 1.0)
```

### Inherited Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `weight` | `float` | `1.0` | Allocation factor (0.0 to 1.0). Use when a fact must be split across multiple dimension members. |

## Field

A wrapper around `sqlmodel.Field` that adds dimensional metadata. Use `sqldim.core.kimball.fields.Field` or import via `from sqldim import Field`.

```python
from sqldim import DimensionModel, SCD2Mixin, Field

class ProductDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["sku"]

    sku: str = Field(primary_key=True, natural_key=True)
    name: str = Field(max_length=300)
    category: str = Field(max_length=100, dimension=CategoryDim)
    gross_margin: float = Field(default=0.0, measure=True, additive=True)
    list_price: float = Field(default=0.0, measure=True, additive=False)
    previous_category: Optional[str] = Field(default=None, scd=3, previous_column="category")
```

### Dimensional Metadata Arguments

These are stored in `sa_column_kwargs["info"]` and consumed by the schema registry, loaders, and semantic query layer.

| Argument | Type | Purpose |
|---|---|---|
| `surrogate_key` | `bool` | Mark as surrogate key column (auto-generated) |
| `natural_key` | `bool` | Mark as part of the business natural key |
| `measure` | `bool` | Mark as a fact measure (not a dimension attribute) |
| `additive` | `bool` | Whether the measure is safe to sum across dimensions (`True` by default) |
| `dimension` | `Type \| None` | Reference to the related `DimensionModel` class |
| `role` | `str \| None` | Role-playing dimension alias (e.g., `"order_date"` vs `"ship_date"`) |
| `scd` | `int \| None` | Per-column SCD override (e.g., `scd=3` for Type 3 tracking on one column) |
| `previous_column` | `str \| None` | Name of the "previous value" column for SCD Type 3 pairs |

All standard `sqlmodel.Field` arguments (`default`, `primary_key`, `foreign_key`, `index`, `max_length`, etc.) are passed through unchanged.

## Complete Dimension Example

```python
from typing import Optional
from sqldim import DimensionModel, SCD2Mixin, Field

class CustomerDim(DimensionModel, SCD2Mixin, table=True):
    """Customer dimension with SCD Type 2 tracking."""
    __tablename__ = "dim_customer"
    __natural_key__ = ["customer_id"]
    __scd_type__ = 2

    customer_key: Optional[int] = Field(default=None, primary_key=True, surrogate_key=True)
    customer_id: str = Field(max_length=50, natural_key=True)
    name: str = Field(max_length=200)
    email: str = Field(max_length=300)
    tier: str = Field(max_length=20)
    # Track tier changes with SCD Type 3 on this column
    previous_tier: Optional[str] = Field(default=None, scd=3, previous_column="tier")
```

## See Also

- [Fact Types Reference](fact_types.md) — `FactModel` base class and Kimball fact patterns
- [Loader Reference](loader.md) — how `DimensionalLoader` processes dimensions
- [SCD Processors Reference](scd_processors.md) — lazy and vectorised SCD change detection
- [Exceptions Reference](exceptions.md) — `GrainViolationError`, `NaturalKeyError`
