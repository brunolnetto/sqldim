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
dgm           — DGMShowcaseSource (static fixture; no Faker dependency)
ecommerce     — ProductsSource, CustomersSource, StoresSource, OrdersSource
enterprise    — EmployeesSource, AccountsSource
fintech       — AccountsSource, CounterpartiesSource, TransactionsSource
hierarchy     — OrgChartSource (static fixture; no Faker dependency)
media         — MoviesSource
nba_analytics — PlayerSeasonsSource
saas_growth   — SaaSUsersSource
supply_chain  — SuppliersSource, WarehousesSource, SKUsSource, ReceiptsSource
user_activity — DevicesSource, EventsSource
devops        — GitHubIssuesSource
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

# Faker-free sources — always importable
from sqldim.examples.datasets.domains.hierarchy import OrgChartSource
from sqldim.examples.datasets.domains.dgm import DGMShowcaseSource

# Faker-dependent sources — imported only when faker is available
try:
    from sqldim.examples.datasets.domains.ecommerce import (
        ProductsSource,
        CustomersSource,
        StoresSource,
        OrdersSource,
    )
    from sqldim.examples.datasets.domains.enterprise import (
        EmployeesSource,
        AccountsSource,
    )
    from sqldim.examples.datasets.domains.media import MoviesSource
    from sqldim.examples.datasets.domains.devops import GitHubIssuesSource

    # Real-world domain datasets
    from sqldim.examples.datasets.domains.fintech import (
        AccountsSource as FintechAccountsSource,
        CounterpartiesSource,
        TransactionsSource,
    )
    from sqldim.examples.datasets.domains.nba_analytics import PlayerSeasonsSource
    from sqldim.examples.datasets.domains.saas_growth import SaaSUsersSource
    from sqldim.examples.datasets.domains.supply_chain import (
        SuppliersSource,
        WarehousesSource,
        SKUsSource,
        ReceiptsSource,
    )
    from sqldim.examples.datasets.domains.user_activity import (
        DevicesSource,
        EventsSource,
    )
    from sqldim.examples.datasets.domains.ecommerce.star import (
        CustomersSource as EcommerceStarCustomersSource,
        ProductsSource as EcommerceStarProductsSource,
        OrdersSource as EcommerceStarOrdersSource,
    )
except ImportError:  # pragma: no cover
    pass

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
    # dgm showcase (no Faker required)
    "DGMShowcaseSource",
    # hierarchy (no Faker required)
    "OrgChartSource",
    # e-commerce (requires faker)
    "ProductsSource",
    "CustomersSource",
    "StoresSource",
    "OrdersSource",
    # enterprise (requires faker)
    "EmployeesSource",
    "AccountsSource",
    # media (requires faker)
    "MoviesSource",
    # devops (requires faker)
    "GitHubIssuesSource",
    # fintech (requires faker)
    "FintechAccountsSource",
    "CounterpartiesSource",
    "TransactionsSource",
    # nba_analytics (requires faker)
    "PlayerSeasonsSource",
    # saas_growth (requires faker)
    "SaaSUsersSource",
    # supply_chain (requires faker)
    "SuppliersSource",
    "WarehousesSource",
    "SKUsSource",
    "ReceiptsSource",
    # user_activity (requires faker)
    "DevicesSource",
    "EventsSource",
    # ecommerce_star (requires faker)
    "EcommerceStarCustomersSource",
    "EcommerceStarProductsSource",
    "EcommerceStarOrdersSource",
]
