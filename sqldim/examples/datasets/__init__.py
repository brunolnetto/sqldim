"""
sqldim.examples.datasets
=========================
OLTP-style Faker-backed dataset generators.

Each *Source class:
  - OLTP_DDL        — transactional source schema (what the app DB holds)
  - DIM_DDL/FACT_DDL— sqldim-managed analytical target schema
  - provider        — SourceProvider descriptor for the live data source
  - setup()         — create the target table (empty)
  - teardown()      — drop the target table
  - snapshot()      — full initial OLTP extract
  - event_batch(n)  — incremental change events (CDC-style)

Schema-driven sources use ``EntitySchema`` / ``FieldSpec`` / ``EventSpec``
to declare field definitions, vocabulary, and DDL in one place.

Domains
-------
ecommerce  — ProductsSource, CustomersSource, StoresSource, OrdersSource
enterprise — EmployeesSource, AccountsSource
media      — MoviesSource
devops     — GitHubIssuesSource
"""
from sqldim.examples.datasets.base import (
    BaseSource,
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)
from sqldim.examples.datasets.ecommerce import (
    ProductsSource,
    CustomersSource,
    StoresSource,
    OrdersSource,
)
from sqldim.examples.datasets.enterprise import (
    EmployeesSource,
    AccountsSource,
)
from sqldim.examples.datasets.media import MoviesSource
from sqldim.examples.datasets.devops import GitHubIssuesSource

__all__ = [
    # base abstractions
    "BaseSource",
    "DatasetFactory",
    "SchematicSource",
    "SourceProvider",
    # schema primitives
    "FieldSpec",
    "EntitySchema",
    "DatasetSpec",
    "ChangeRule",
    "EventSpec",
    # e-commerce
    "ProductsSource",
    "CustomersSource",
    "StoresSource",
    "OrdersSource",
    # enterprise
    "EmployeesSource",
    "AccountsSource",
    # media
    "MoviesSource",
    # devops
    "GitHubIssuesSource",
]
