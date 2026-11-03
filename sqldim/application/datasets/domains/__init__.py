"""
sqldim.application.datasets.domains
==================================
Domain-specific OLTP dataset generators, organised one folder per domain.

Each sub-package has the following layout::

    domains/<name>/
        __init__.py  — re-imports only (no functional code)
        sources.py   — Source class definitions, DatasetSpec objects, DDL

Import directly from the sub-package or let the parent
``sqldim.application.datasets`` hub re-export everything::

    from sqldim.application.datasets import ProductsSource    # hub re-export
    from sqldim.application.datasets.domains.commerce import ProductsSource
    from sqldim.application.datasets.domains.commerce.sources import ProductsSource

Domains
-------
commerce      — ProductsSource, CustomersSource, StoresSource, OrdersSource, LoyaltyCustomersSource, CatalogProductsSource, FulfillmentOrdersSource (OLTP) + DimCustomerSource, FctDailySalesSource, FctCohortRetentionSource (gold) + SuppliersSource, WarehousesSource, SKUsSource, ReceiptsSource (supply-chain)
growth        — EmployeesSource, AccountsSource, SaaSUsersSource, SaaSSessionsSource
fintech       — AccountsSource, CounterpartiesSource, TransactionsSource
data_models   — DGMShowcaseSource, OrgChartSource (static fixtures; no Faker dependency)
entertainment — MoviesSource, SocialNetworkSource
nba_analytics — PlayerSeasonsSource
telemetry     — DevicesSource, EventsSource, PipelineSpanSource, MetricSampleSource
devops        — GitHubIssuesSource
"""
