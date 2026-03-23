"""
sqldim.application.benchmarks
==================
Performance benchmark suite for the sqldim library.

Entry point::

    python -m sqldim.application.benchmarks.runner                         # all groups
    python -m sqldim.application.benchmarks.runner A B C                   # specific groups
    python -m sqldim.application.benchmarks.runner A.products B.employees  # specific elements

Groups A–I cover SCD processing; J–N dimensional model infrastructure;
O–S DGM query algebra.  See :mod:`sqldim.application.benchmarks.runner` for full CLI help.
"""
