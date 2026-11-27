# Guide: Vectorized ETL

`NarwhalsSCDProcessor` runs the SCD2 classification on a whole DataFrame at
once — pandas, Polars, or any backend [Narwhals](https://narwhals-dev.github.io/narwhals/)
supports — instead of record by record. The example uses Polars (`pip install polars`).

## How it works

1. **Hashing.** A checksum of the tracked columns is computed for every row
   of the incoming batch and of the open rows of the current table, as
   column expressions.
2. **Classification.** One join on the natural key splits the batch into new
   rows (no current row), changed rows (checksum differs) and unchanged rows.
3. **Result.** `to_close` holds the current rows to close (`valid_to` set,
   `is_current` false) and `to_insert` the new rows and new versions, both in
   the backend of the input. Writing them is up to you.

## Usage

```python
from datetime import datetime, timezone

import narwhals as nw
import polars as pl

from sqldim import NarwhalsSCDProcessor

current = pl.DataFrame(
    {
        "sku": ["A", "B"],
        "price": [10.0, 20.0],
        "valid_from": ["2024-01-01", "2024-01-01"],
        "valid_to": [None, None],
        "is_current": [True, True],
    }
)
incoming = pl.DataFrame({"sku": ["A", "B", "C"], "price": [10.0, 25.0, 5.0]})

proc = NarwhalsSCDProcessor(natural_key=["sku"], track_columns=["price"])
result = proc.process(
    nw.from_native(incoming),
    nw.from_native(current),
    as_of=datetime(2024, 6, 1, tzinfo=timezone.utc),
)
print(result.inserted, result.versioned, result.unchanged)  # 1 1 1
print(type(result.to_insert).__name__)                       # DataFrame (polars)
print(sorted(result.to_insert["sku"].to_list()))             # ['B', 'C']
print(result.to_close["sku"].to_list())                      # ['B']
```

`DimensionalLoader` does not take a DataFrame: `register()` takes a list of
dicts. For data that should not pass through Python at all, see the lazy,
DuckDB-SQL processors in [Big Data](big_data.md).

sqldim publishes no benchmark of this path.
