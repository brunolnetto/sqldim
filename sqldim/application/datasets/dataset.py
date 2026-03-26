"""
sqldim/examples/datasets/dataset.py
=====================================
``Dataset`` — a named, FK-ordered collection of related OLTP sources.

Unlike a bare :class:`~sqldim.application.datasets.base.BaseSource` (which
represents a single OLTP table), a ``Dataset`` groups **multiple** sources
that are connected through foreign-key relationships — exactly as they
exist in a real operational system.

Design
------
Sources are supplied in **dependency order**: parent tables (referenced
dimensions) before children (facts and bridges).  ``setup()`` creates
tables in that order; ``teardown()`` drops them in reverse, so FK
constraints are never violated.

Individual access
~~~~~~~~~~~~~~~~~
``dataset["table_name"]`` returns the :class:`BaseSource` for that table,
letting callers load, probe, or inspect a single source in isolation.

Joint actions
~~~~~~~~~~~~~
* ``setup(con)``         — create all tables (ordered by FK dependency)
* ``teardown(con)``      — drop all tables (reverse order)
* ``snapshots(con)``     — dict mapping table name → initial data rows
* ``event_batches(con)`` — dict mapping table name → event rows (CDC)

Registration
~~~~~~~~~~~~
Datasets can be registered in :class:`~sqldim.application.datasets.base.DatasetFactory`
the same way as individual sources::

    @DatasetFactory.register_dataset("ecommerce_pipeline")
    def _make():
        return Dataset("ecommerce_pipeline", [
            (CustomersSource(), "raw_customers"),
            (ProductsSource(),  "raw_products"),
            (OrdersSource(),    "raw_orders"),
        ])

Example
-------
::

    import duckdb
    from sqldim.application.datasets import Dataset
    from sqldim.application.datasets.domains.ecommerce import (
        CustomersSource, ProductsSource, OrdersSource,
    )

    pipeline = Dataset("ecommerce", [
        (CustomersSource(n=100), "raw_customers"),
        (ProductsSource(n=50),   "raw_products"),
        (OrdersSource(n=200),    "raw_orders"),    # references customers + products
    ])

    con = duckdb.connect()
    pipeline.setup(con)              # creates all three tables

    for table, rows in pipeline.snapshots().items():
        print(f"{table}: {len(rows)} initial rows")

    pipeline.teardown(con)           # drops in reverse order
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import duckdb

from sqldim.application.datasets.base import BaseSource


@dataclass
class Dataset:
    """
    A named collection of related OLTP sources with coordinated lifecycle.

    Parameters
    ----------
    name    : Unique human-readable name (used as registry key and label).
    sources : List of ``(source, table_name)`` pairs in FK dependency order.
              Parents before children — ``setup()`` processes this list
              front-to-back; ``teardown()`` reverses it.

    Attributes
    ----------
    name    : str
    sources : list[tuple[BaseSource, str]]
    """

    name: str
    sources: list[tuple[BaseSource, str]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def __getitem__(self, table_name: str) -> BaseSource:
        """Return the source registered under *table_name*.

        Raises ``KeyError`` if no source is registered for that table.
        """
        for src, tname in self.sources:
            if tname == table_name:
                return src
        raise KeyError(
            f"No source registered for table '{table_name}' in dataset "
            f"'{self.name}'.  Available: {self.table_names()}"
        )

    def table_names(self) -> list[str]:
        """Return table names in dependency order."""
        return [tname for _, tname in self.sources]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create all tables in FK dependency order (parents first)."""
        for src, table in self.sources:
            src.setup(con, table)

    def teardown(self, con: duckdb.DuckDBPyConnection) -> None:
        """Drop all tables in reverse FK dependency order (children first)."""
        for src, table in reversed(self.sources):
            src.teardown(con, table)

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def snapshots(self) -> dict[str, Any]:
        """
        Return a mapping of ``table_name → snapshot`` for every source.

        Sources that raise :exc:`NotImplementedError` from ``snapshot()``
        (i.e. static fixtures that self-populate in ``setup()``) are
        silently excluded.
        """
        result: dict[str, Any] = {}
        for src, table in self.sources:
            try:
                result[table] = src.snapshot()
            except NotImplementedError:
                pass
        return result

    def event_batches(self, n: int = 1) -> dict[str, Any]:
        """
        Return a mapping of ``table_name → event_batch(n)`` for every source
        that supports incremental events.

        Sources that raise :exc:`NotImplementedError` are silently excluded.
        """
        result: dict[str, Any] = {}
        for src, table in self.sources:
            try:
                result[table] = src.event_batch(n)
            except NotImplementedError:
                pass
        return result

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def describe(self) -> str:
        """Return a human-readable summary of this dataset."""
        lines = [f"Dataset '{self.name}' ({len(self.sources)} sources):"]
        for src, table in self.sources:
            provider_name = (
                src.provider.name
                if src.provider is not None
                else type(src).__name__
            )
            lines.append(f"  {table:35s}  ← {provider_name}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        tables = ", ".join(self.table_names())
        return f"Dataset(name={self.name!r}, tables=[{tables}])"
