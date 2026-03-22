"""
sqldim/examples/datasets/hierarchy.py
=======================================
Org-chart hierarchy dataset — a static 7-node HR fixture used by the
hierarchy strategy showcase.

Provides three tables that together form a small Kimball star with a
self-referential dimension:

* ``org_dim``         — adjacency-list + materialized-path dimension
* ``sales_fact``      — one revenue row per employee
* ``org_dim_closure`` — pre-computed closure bridge for ClosureTableStrategy

``OrgChartSource`` wraps these three tables behind the standard
:class:`~sqldim.examples.datasets.base.BaseSource` interface and is
registered with :class:`~sqldim.examples.datasets.base.DatasetFactory`
under the key ``"org_chart"``.

The corresponding dimensional model classes (``OrgDimModel``,
``OrgSalesFact``, ``OrgDimClosureModel``) live in
:mod:`sqldim.examples.features.hierarchy.models` so that showcase-level
SQLModel metadata stays separate from the dataset fixture.

Usage::

    from sqldim.examples.datasets import DatasetFactory
    import duckdb

    src = DatasetFactory.create("org_chart")
    con = duckdb.connect()
    src.setup(con)
    # … run hierarchy queries …
    src.teardown(con)
"""

from __future__ import annotations

import duckdb

from sqldim.examples.datasets.base import BaseSource, DatasetFactory, SourceProvider


# ---------------------------------------------------------------------------
# Inline DDL (avoids SQLModel dependency in the dataset layer)
# ---------------------------------------------------------------------------

_ORG_DIM_DDL = """
CREATE TABLE IF NOT EXISTS org_dim (
    id              INTEGER NOT NULL PRIMARY KEY,
    parent_id       INTEGER,
    employee_code   VARCHAR NOT NULL,
    name            VARCHAR NOT NULL,
    hierarchy_level INTEGER NOT NULL,
    hierarchy_path  VARCHAR
)
"""

_SALES_FACT_DDL = """
CREATE TABLE IF NOT EXISTS sales_fact (
    sale_id INTEGER NOT NULL PRIMARY KEY,
    dim_id  INTEGER NOT NULL,
    revenue FLOAT NOT NULL
)
"""

_ORG_CLOSURE_DDL = """
CREATE TABLE IF NOT EXISTS org_dim_closure (
    ancestor_id   INTEGER NOT NULL,
    descendant_id INTEGER NOT NULL,
    depth         INTEGER NOT NULL,
    PRIMARY KEY (ancestor_id, descendant_id)
)
"""


# ---------------------------------------------------------------------------
# Static fixture data
# ---------------------------------------------------------------------------

# Columns: id, parent_id, employee_code, name, hierarchy_level, hierarchy_path
_ORG_DATA = [
    (1, None, "EMP-001", "Alice (CEO)", 0, "/1/"),
    (2, 1, "EMP-002", "Bob (VP Eng)", 1, "/1/2/"),
    (3, 1, "EMP-003", "Carol (VP Sales)", 1, "/1/3/"),
    (4, 2, "EMP-004", "Dave (SWE)", 2, "/1/2/4/"),
    (5, 2, "EMP-005", "Eve (SWE)", 2, "/1/2/5/"),
    (6, 3, "EMP-006", "Frank (AE)", 2, "/1/3/6/"),
    (7, 4, "EMP-007", "Grace (Intern)", 3, "/1/2/4/7/"),
]

# Columns: sale_id, dim_id, revenue
_SALES_DATA = [
    (1, 4, 5000.0),  # Dave
    (2, 5, 8000.0),  # Eve
    (3, 6, 12000.0),  # Frank
    (4, 7, 1500.0),  # Grace (intern)
    (5, 2, 200.0),  # Bob (direct deal)
    (6, 3, 300.0),  # Carol (direct deal)
]

# Columns: ancestor_id, descendant_id, depth
_CLOSURE_DATA = [
    # Reflexive (depth 0)
    (1, 1, 0),
    (2, 2, 0),
    (3, 3, 0),
    (4, 4, 0),
    (5, 5, 0),
    (6, 6, 0),
    (7, 7, 0),
    # Alice → descendants
    (1, 2, 1),
    (1, 3, 1),
    (1, 4, 2),
    (1, 5, 2),
    (1, 6, 2),
    (1, 7, 3),
    # Bob → descendants
    (2, 4, 1),
    (2, 5, 1),
    (2, 7, 2),
    # Carol → descendants
    (3, 6, 1),
    # Dave → descendants
    (4, 7, 1),
]


# ---------------------------------------------------------------------------
# DatasetFactory-registered source
# ---------------------------------------------------------------------------


@DatasetFactory.register("org_chart")
class OrgChartSource(BaseSource):
    """
    Static 7-node org-chart fixture — adjacency list + closure bridge.

    This source represents data as it would appear in an HR Management
    System: a reporting hierarchy of employees together with revenue
    transactions per employee.

    All three tables are fully populated by :meth:`setup`; no OLTP
    extraction step is needed for this static fixture.
    """

    provider = SourceProvider(
        name="HR Management System (synthetic)",
        description=(
            "7-node reporting hierarchy (CEO → VP → IC → intern) with "
            "revenue transactions per employee.  All data is a static fixture."
        ),
    )

    # ── Schema (informational) ───────────────────────────────────────────────
    OLTP_DDL = (
        "-- org_dim: id, parent_id, employee_code, name, "
        "hierarchy_level, hierarchy_path\n"
        "-- sales_fact: sale_id, dim_id, revenue\n"
        "-- org_dim_closure: ancestor_id, descendant_id, depth"
    )

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def setup(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "org_dim",
    ) -> None:
        """
        Create and populate ``org_dim``, ``sales_fact``, and
        ``org_dim_closure`` using the static fixture data.

        The *table* parameter is accepted for API compatibility but the
        table names are fixed by the model declarations.
        """
        for ddl in (_ORG_DIM_DDL, _SALES_FACT_DDL, _ORG_CLOSURE_DDL):
            con.execute(ddl)

        con.executemany(
            "INSERT INTO org_dim"
            " (id, parent_id, employee_code, name, hierarchy_level, hierarchy_path)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            _ORG_DATA,
        )
        con.executemany(
            "INSERT INTO sales_fact (sale_id, dim_id, revenue) VALUES (?, ?, ?)",
            _SALES_DATA,
        )
        con.executemany(
            "INSERT INTO org_dim_closure (ancestor_id, descendant_id, depth)"
            " VALUES (?, ?, ?)",
            _CLOSURE_DATA,
        )

    def teardown(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "org_dim",
    ) -> None:
        """Drop all three fixture tables."""
        for tbl in ("org_dim_closure", "sales_fact", "org_dim"):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")

    # ── Data interface ───────────────────────────────────────────────────────

    def snapshot(self):
        raise NotImplementedError(
            "OrgChartSource is a static fixture — call setup() to populate "
            "the tables, then query them directly."
        )
