"""
sqldim.application.datasets
=========================
OLTP-style Faker-backed dataset generators.

**Dataset** groups multiple related sources (FK-ordered)::

    from sqldim.application.datasets import Dataset
    from sqldim.application.datasets.domains.ecommerce import CustomersSource, OrdersSource

    pipeline = Dataset("ecommerce", [
        (CustomersSource(n=100), "raw_customers"),
        (OrdersSource(n=200),    "raw_orders"),
    ])

Each *Source class implements:
  - OLTP_DDL        — transactional source schema
  - DIM_DDL/FACT_DDL— sqldim-managed analytical target schema
  - provider        — SourceProvider descriptor
  - setup()         — create the target table (empty)
  - teardown()      — drop the target table
  - snapshot()      — full initial OLTP extract
  - event_batch(n)  — incremental CDC events

Domains
-------
dgm           — DGMShowcaseSource (static fixture; no Faker dependency)
ecommerce     — LoyaltyCustomersSource, CatalogProductsSource, FulfillmentOrdersSource, StoresSource, CustomersSource, ProductsSource, OrdersSource
enterprise    — EmployeesSource, AccountsSource
fintech       — AccountsSource, CounterpartiesSource, TransactionsSource
hierarchy     — OrgChartSource (static fixture; no Faker dependency)
media         — MoviesSource
nba_analytics — PlayerSeasonsSource
saas_growth   — SaaSUsersSource
supply_chain  — SuppliersSource, WarehousesSource, SKUsSource, ReceiptsSource
user_activity — DevicesSource, EventsSource
devops        — GitHubIssuesSource
observability — PipelineSpanSource, MetricSampleSource
"""

from sqldim.application.datasets.base import (
    BaseSource,
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.events import (
    AggregateState,
    DomainEvent,
    EventRepository,
    rows_to_sql,
)
from sqldim.application.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# Faker-free sources — always importable
from sqldim.application.datasets.domains.hierarchy import OrgChartSource
from sqldim.application.datasets.domains.dgm import DGMShowcaseSource

# Faker-dependent sources — imported only when faker is available
try:
    from sqldim.application.datasets.domains.ecommerce import (
        LoyaltyCustomersSource,
        CatalogProductsSource,
        FulfillmentOrdersSource,
        ProductsSource,
        CustomersSource,
        StoresSource,
        OrdersSource,
    )
    from sqldim.application.datasets.domains.enterprise import (
        EmployeesSource,
        AccountsSource,
    )
    from sqldim.application.datasets.domains.media import MoviesSource
    from sqldim.application.datasets.domains.devops import GitHubIssuesSource

    # Real-world domain datasets
    from sqldim.application.datasets.domains.fintech import (
        AccountsSource as FintechAccountsSource,
        CounterpartiesSource,
        TransactionsSource,
    )
    from sqldim.application.datasets.domains.nba_analytics import PlayerSeasonsSource
    from sqldim.application.datasets.domains.saas_growth import SaaSUsersSource
    from sqldim.application.datasets.domains.supply_chain import (
        SuppliersSource,
        WarehousesSource,
        SKUsSource,
        ReceiptsSource,
    )
    from sqldim.application.datasets.domains.user_activity import (
        DevicesSource,
        EventsSource,
    )
    from sqldim.application.datasets.domains.observability import (
        PipelineSpanSource,
        MetricSampleSource,
    )
except ImportError:  # pragma: no cover
    pass

__all__ = [
    # base abstractions
    "Dataset",
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
    "LoyaltyCustomersSource",
    "CatalogProductsSource",
    "FulfillmentOrdersSource",
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
    # observability (real execution telemetry)
    "PipelineSpanSource",
    "MetricSampleSource",
    # domain-event infrastructure
    "AggregateState",
    "DomainEvent",
    "EventRepository",
    "rows_to_sql",
]
