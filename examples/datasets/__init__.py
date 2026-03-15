"""
examples/datasets
==================
Re-exports from ``sqldim.examples.datasets`` for convenience.

The canonical home is ``sqldim.examples.datasets``; these names are
kept here purely for backwards compatibility with older example paths.

New code should use::

    from sqldim.examples.datasets import ProductsSource, CustomersSource, ...
"""
from sqldim.examples.datasets import (
    ProductsSource,
    CustomersSource,
    StoresSource,
    OrdersSource,
    EmployeesSource,
    AccountsSource,
    MoviesSource,
    GitHubIssuesSource,
)

# Backward-compat aliases (old *Dataset names)
ProductsDataset     = ProductsSource
CustomersDataset    = CustomersSource
StoresDataset       = StoresSource
OrdersDataset       = OrdersSource
EmployeesDataset    = EmployeesSource
AccountsDataset     = AccountsSource
MoviesDataset       = MoviesSource
GitHubIssuesDataset = GitHubIssuesSource

__all__ = [
    # New names
    "ProductsSource",
    "CustomersSource",
    "StoresSource",
    "OrdersSource",
    "EmployeesSource",
    "AccountsSource",
    "MoviesSource",
    "GitHubIssuesSource",
    # Backward-compat aliases
    "ProductsDataset",
    "CustomersDataset",
    "StoresDataset",
    "OrdersDataset",
    "EmployeesDataset",
    "AccountsDataset",
    "MoviesDataset",
    "GitHubIssuesDataset",
]
