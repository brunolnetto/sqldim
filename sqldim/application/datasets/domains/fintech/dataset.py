"""
fintech domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.fintech.dataset import fintech_dataset

    con = duckdb.connect()
    fintech_dataset.setup(con)

    for table, rows in fintech_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    fintech_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.fintech.sources import (
    AccountsSource,
    CounterpartiesSource,
    TransactionsSource,
)

fintech_dataset = Dataset(
    "fintech",
    [
        (AccountsSource(), "accounts"),
        (CounterpartiesSource(), "counterparties"),
        (TransactionsSource(), "transactions"),
    ],
)

DATASET_METADATA = {
    "name": "fintech",
    "title": "Fintech transactions",
    "description": "Bank accounts, consumers, merchants, and payment transactions",
    "dataset_attr": "fintech_dataset",
}

__all__ = ["fintech_dataset", "DATASET_METADATA"]
