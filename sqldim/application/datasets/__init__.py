"""
sqldim.application.datasets
=========================
OLTP-style Faker-backed dataset generators.

**Dataset** groups multiple related sources (FK-ordered)::

    from sqldim.application.datasets import Dataset
    from sqldim.application.datasets.domains.commerce import CustomersSource, OrdersSource

    pipeline = Dataset("commerce", [
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
ecommerce     — (merged into commerce — use commerce domain instead)
commerce      — LoyaltyCustomersSource, CatalogProductsSource, FulfillmentOrdersSource, StoresSource, CustomersSource, ProductsSource, OrdersSource (OLTP) + DimCustomerSource, FctDailySalesSource, FctCohortRetentionSource (gold) + SuppliersSource, WarehousesSource, SKUsSource, ReceiptsSource (supply-chain)
growth        — EmployeesSource, AccountsSource, SaaSUsersSource, SaaSSessionsSource
fintech       — AccountsSource, CounterpartiesSource, TransactionsSource
data_models   — DGMShowcaseSource, OrgChartSource (static fixtures; no Faker dependency)
entertainment — MoviesSource, SocialNetworkSource
nba_analytics — PlayerSeasonsSource
telemetry     — DevicesSource, EventsSource, PipelineSpanSource, MetricSampleSource
devops        — GitHubIssuesSource
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
from sqldim.application.datasets.domains.data_models import (
    DGMShowcaseSource,
    OrgChartSource,
)

# Faker-dependent sources — imported only when faker is available
try:
    from sqldim.application.datasets.domains.commerce import (
        LoyaltyCustomersSource,
        CatalogProductsSource,
        FulfillmentOrdersSource,
        ProductsSource,
        CustomersSource,
        StoresSource,
        OrdersSource,
    )
    from sqldim.application.datasets.domains.growth import (
        EmployeesSource,
        AccountsSource,
    )
    from sqldim.application.datasets.domains.entertainment import (
        MoviesSource,
        SocialNetworkSource,
    )
    from sqldim.application.datasets.domains.devops import GitHubIssuesSource

    # Real-world domain datasets
    from sqldim.application.datasets.domains.fintech import (
        AccountsSource as FintechAccountsSource,
        CounterpartiesSource,
        TransactionsSource,
    )
    from sqldim.application.datasets.domains.nba_analytics import PlayerSeasonsSource
    from sqldim.application.datasets.domains.growth import SaaSUsersSource
    from sqldim.application.datasets.domains.commerce import (
        SuppliersSource,
        WarehousesSource,
        SKUsSource,
        ReceiptsSource,
    )
    from sqldim.application.datasets.domains.telemetry import (
        DevicesSource,
        EventsSource,
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
    # data_models (no Faker required)
    "DGMShowcaseSource",
    "OrgChartSource",
    # e-commerce (requires faker)
    "LoyaltyCustomersSource",
    "CatalogProductsSource",
    "FulfillmentOrdersSource",
    "ProductsSource",
    "CustomersSource",
    "StoresSource",
    "OrdersSource",
    # growth (requires faker)
    "EmployeesSource",
    "AccountsSource",
    # entertainment (requires faker)
    "MoviesSource",
    "SocialNetworkSource",
    # devops (requires faker)
    "GitHubIssuesSource",
    # fintech (requires faker)
    "FintechAccountsSource",
    "CounterpartiesSource",
    "TransactionsSource",
    # nba_analytics (requires faker)
    "PlayerSeasonsSource",
    # growth (requires faker)
    "SaaSUsersSource",
    # supply-chain sources (now part of commerce domain; requires faker)
    "SuppliersSource",
    "WarehousesSource",
    "SKUsSource",
    "ReceiptsSource",
    # telemetry (devices/events + pipeline spans/metrics)
    "DevicesSource",
    "EventsSource",
    "PipelineSpanSource",
    "MetricSampleSource",
    # domain-event infrastructure
    "AggregateState",
    "DomainEvent",
    "EventRepository",
    "rows_to_sql",
]
