"""
enterprise domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.enterprise.dataset import enterprise_dataset

    con = duckdb.connect()
    enterprise_dataset.setup(con)

    for table, rows in enterprise_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    enterprise_dataset.teardown(con)
"""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.enterprise.sources import (
    EmployeesSource, AccountsSource,
)

enterprise_dataset = Dataset(
    "enterprise",
    [
        (EmployeesSource(), "employees"),
        (AccountsSource(), "accounts"),
    ],
)

__all__ = ["enterprise_dataset"]
