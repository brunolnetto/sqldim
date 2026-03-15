"""
sqldim.examples
================
Ready-to-run showcase examples and OLTP dataset generators for sqldim.

Import datasets::

    from sqldim.examples.datasets.ecommerce  import ProductsSource, CustomersSource
    from sqldim.examples.datasets.enterprise import EmployeesSource, AccountsSource
    from sqldim.examples.datasets.media      import MoviesSource
    from sqldim.examples.datasets.devops     import GitHubIssuesSource

Run a complete showcase::

    from sqldim.examples.features.scd_types import run_showcase
    run_showcase()
"""
