"""
growth domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.growth.dataset import growth_dataset

    con = duckdb.connect()
    growth_dataset.setup(con)

    for table, rows in growth_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    growth_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.growth.model import build_model
from sqldim.application.datasets.domains.growth.sources import (
    EmployeesSource,
    AccountsSource,
    SaaSUsersSource,
    SaaSSessionsSource,
)
from sqldim.application.datasets.domains.growth.sources.users import N_SAAS_USERS

_users = SaaSUsersSource(n=N_SAAS_USERS)

growth_dataset = Dataset(
    "growth",
    [
        (EmployeesSource(), "employees"),
        (AccountsSource(), "accounts"),
        (_users, "saas_users"),
        # Every session references a generated user.
        (SaaSSessionsSource(n=200, n_users=len(_users.initial)), "saas_sessions"),
    ],
    model=build_model,
)

DATASET_METADATA = {
    "name": "growth",
    "title": "Growth (B2B lifecycle)",
    "description": "Employees, accounts, SaaS users, and session activity",
    "dataset_attr": "growth_dataset",
}

__all__ = ["growth_dataset", "DATASET_METADATA"]
