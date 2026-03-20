"""
sqldim.examples
================
Ready-to-run showcase examples and OLTP dataset generators for sqldim.

Import datasets::

    from sqldim.examples.datasets.domains.ecommerce  import ProductsSource, CustomersSource
    from sqldim.examples.datasets.domains.enterprise import EmployeesSource, AccountsSource
    from sqldim.examples.datasets.domains.media      import MoviesSource
    from sqldim.examples.datasets.domains.devops     import GitHubIssuesSource

Run a complete showcase::

    from sqldim.examples.features.scd_types import run_showcase
    run_showcase()
"""
