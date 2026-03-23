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
    from sqldim.application.datasets.domains.ecommerce import ProductsSource
    from sqldim.application.datasets.domains.ecommerce.sources import ProductsSource

Domains
-------
dgm           — DGMShowcaseSource (static fixture; no Faker dependency)
ecommerce     — ProductsSource, CustomersSource, StoresSource, OrdersSource
                sources.star  — LoyaltyCustomersSource, CatalogProductsSource, FulfillmentOrdersSource
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
