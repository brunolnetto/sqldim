# Pattern: Hybrid SCD (Typed Columns + JSONB)

One of the greatest frictions in dimensional modeling is "Homogenization"—the loss of flexibility as you force diverse source data into a rigid table structure.

## The Problem
You have a `Product` dimension. 90% of products share `name` and `sku`, but the other 10% have unique metadata (e.g., `fabric_density` for textiles, `octane_rating` for fuel). Creating 100 nullable columns is messy; using only a JSON blob prevents easy filtering and type safety.

## The Solution: The Hybrid Model
`sqldim` allows you to define core "High-Density" attributes as columns and "Low-Density" sparse attributes in a `properties` JSONB bag. Crucially, **changes inside the JSON bag trigger SCD2 versioning.**

```python
from sqldim import DimensionModel, SCD2Mixin, Field
from sqlalchemy import JSON, Column

class ProductDim(DimensionModel, SCD2Mixin, table=True):
    # Typed Columns (BI friendly, high performance)
    sku: str = Field(primary_key=True)
    name: str
    category: str

    # Property Bag (Flexible, sparse data)
    # track_columns=["properties"] ensures versioning on any JSON change
    properties: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, info={"scd": 2})
    )
```

## Why this works
1. **Deterministic Hashing**: `sqldim` sorts JSON keys before hashing. Changing `{"a": 1, "b": 2}` to `{"b": 2, "a": 1}` does **not** trigger a new version.
2. **Indexing**: You get the speed of B-Tree indexes on your core columns while keeping the flexibility of GIN indexes (on Postgres) for your metadata.
3. **Clean ETL**: Your ingestion pipeline doesn't need to break every time a new attribute appears in the source JSON.
