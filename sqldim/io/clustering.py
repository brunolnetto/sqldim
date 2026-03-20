"""Z-ORDER / clustering strategies for Parquet row-group co-location.

DuckDB doesn't natively support Z-ORDER at write time, but achieves the same
effect by sorting before writing.  The Parquet writer preserves row ordering
within row groups, so ``ORDER BY zorder_cols`` before ``COPY TO`` is a
best-effort approximation.

Protocol
--------
All implementations satisfy the :class:`ClusteringStrategy` protocol:

.. code-block:: python

    class ClusteringStrategy(Protocol):
        def cluster_sql(self, table: str, columns: list[str]) -> str: ...

Usage
-----
.. code-block:: python

    from sqldim.io import SortBasedClustering

    strategy = SortBasedClustering()
    sql = strategy.cluster_sql("my_view", ["customer_id", "product_id"])
    # → "SELECT * FROM my_view ORDER BY customer_id, product_id"
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ClusteringStrategy(Protocol):
    """Protocol for row-group clustering strategies.

    Implementations return a SQL ``SELECT`` statement that reads *table* and
    produces rows in the desired clustering order.  The output is passed
    directly to DuckDB's ``COPY (…) TO`` so it must be a valid SELECT.
    """

    def cluster_sql(self, table: str, columns: list[str]) -> str:
        """Return a SQL SELECT that orders *table* by *columns*.

        Parameters
        ----------
        table:
            Table, view, or sub-expression name registered in DuckDB.
        columns:
            List of column names to cluster/sort by.
        """
        ...


class SortBasedClustering:
    """Default clustering via ``ORDER BY`` — best-effort Z-ORDER approximation.

    DuckDB's Parquet writer preserves sort order within row groups, so sorting
    by the clustering columns before writing achieves row-group co-location
    for those columns.  This is a practical substitute for true Z-ORDER
    (which DuckDB does not yet support natively) at near-zero overhead.

    Provides the best read performance for workloads that consistently filter
    on the first one or two clustering columns; multi-column filter benefit
    degrades for columns beyond position 2–3 (the same trade-off as Spark's
    Z-ORDER).
    """

    def cluster_sql(self, table: str, columns: list[str]) -> str:
        """Return ``SELECT * FROM {table} ORDER BY {columns}``."""
        if not columns:
            return f"SELECT * FROM {table}"
        order_expr = ", ".join(columns)
        return f"SELECT * FROM {table} ORDER BY {order_expr}"


class NoOpClustering:
    """Pass-through strategy — writes rows in their natural order.

    Use when clustering is not wanted (e.g. append-only bronze tables where
    ingestion order is the natural access pattern).
    """

    def cluster_sql(self, table: str, columns: list[str]) -> str:
        return f"SELECT * FROM {table}"
