# Guide: Semantic Query Layer

The Semantic Layer is where `sqldim` turns "Data into Information." It abstracts away the complexity of point-in-time joins and historical state resolution.

## The Pain Point: The "Point-in-Time" Join Nightmare
In an SCD2 world, joining a Fact table to a Dimension table requires checking date ranges:
```sql
SELECT ... FROM facts f
JOIN dimensions d ON f.dim_id = d.dim_id
AND f.event_date BETWEEN d.valid_from AND COALESCE(d.valid_to, '9999-12-31')
```
Writing this for every join is exhausting and prone to "off-by-one" date errors.

## The Solution: Fluent Temporal Joins
The `sqldim` query builder understands your schema graph and handles the temporal logic automatically.

```python
from sqldim.query import DimensionalQuery

query = (
    DimensionalQuery(session)
    .from_fact(SalesFact)
    .join_dim(ProductDim)
    .as_of("2024-01-01") # Automatically applies PIT filters to ALL joins
    .filter(ProductDim.category == "Electronics")
    .aggregate(SalesFact.amount, "sum")
)

results = await query.execute()
```

## Key Capabilities
1. **Grain Validation**: Prevents you from joining a Fact to a Dimension that would result in a "Fan Trap" (incorrect row multiplication).
2. **Automatic Role Playing**: If you have `departure_date` and `arrival_date` pointing to the same Date dimension, `sqldim` resolves the aliases correctly.
3. **Aggregation Safety**: Only allows "Sum" on additive measures and "Average" on non-additive ones based on your model metadata.
