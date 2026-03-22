"""
sqldim.examples.datasets.domains
==================================
Domain-specific OLTP dataset generators, organised one folder per domain.

Each sub-package exposes one or more *Source classes built on BaseSource /
SchematicSource.  Import directly from the sub-package or let the parent
``sqldim.examples.datasets`` hub re-export everything for you.

Domains
-------
ecommerce     — ProductsSource, CustomersSource, StoresSource, OrdersSource
                (ecommerce.orders — OrdersSource accumulating milestone pipeline)
                (ecommerce.star   — Faker-backed SCD2 + bridge star schema sources)
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
