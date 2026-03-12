# Guide: Vectorized ETL (The Performance Pillar)

Standard Python ETL libraries often fail at scale because they process records in loops. `sqldim` solves this using the **Narwhals** engine to vectorize the entire pipeline.

## The Pain Point: The "Row-by-Row" Bottleneck
Traditional SCD logic (check if exists -> hash incoming -> compare -> update) becomes exponentially slower as your dimensions grow. A 1M row dimension with 100k incoming changes can take minutes in standard ORM loops.

## The Solution: Vectorized Joins
`sqldim` detects when you pass a `polars` or `pandas` DataFrame and automatically switches to the **Vectorized Path**.

### How it works
1. **Hashing**: Instead of hashing each row in a loop, we compute MD5 hashes for the entire column at once using native C/Rust backends.
2. **Classification**: We perform a single **Left Join** between your incoming batch and the current database state. 
   - `Null` on the right? It's a **New Record**.
   - `Hash mismatch`? It's a **Changed Record** (SCD2 Trigger).
3. **Batch Insertion**: Changes are committed using native bulk insert strategies.

## Usage
You don't have to change your models. Simply register your source as a DataFrame:

```python
import polars as pl
from sqldim import DimensionalLoader

df = pl.read_csv("daily_extract.csv")
loader = DimensionalLoader(session, models=[ProductDim])

# narwhals automatically handles the plumbing
loader.register(ProductDim, source=df)
await loader.run()
```

## Performance Wins
| Batch Size | Standard Path (Loop) | Vectorized Path (Polars) |
|------------|----------------------|--------------------------|
| 10,000     | 2.5s                 | 0.2s                     |
| 100,000    | 24.1s                | 1.1s                     |
| 1,000,000  | 5m 12s               | 8.4s                     |

*Note: Benchmarks run on local SQLite; Postgres/DuckDB native COPY support provides even greater gains.*
