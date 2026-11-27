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
from sqldim.application.datasets.domains.fintech.model import build_model
from sqldim.application.datasets.domains.fintech.sources import (
    AccountsSource,
    CounterpartiesSource,
    TransactionsSource,
)

_accounts = AccountsSource()
_counterparties = CounterpartiesSource()

fintech_dataset = Dataset(
    "fintech",
    [
        (_accounts, "accounts"),
        (_counterparties, "counterparties"),
        # Every transaction references a generated account and counterparty.
        (
            TransactionsSource(
                n_accounts=len(_accounts.initial),
                n_counterparties=len(_counterparties.initial),
            ),
            "transactions",
        ),
    ],
    model=build_model,
)

DATASET_METADATA = {
    "name": "fintech",
    "title": "Fintech transactions",
    "description": "Bank accounts, consumers, merchants, and payment transactions",
    "dataset_attr": "fintech_dataset",
}

__all__ = ["fintech_dataset", "DATASET_METADATA"]
