"""
sqldim/examples/datasets/domains/dgm/__init__.py
=================================================
Static DGM showcase fixture — a minimal star schema used by
:mod:`sqldim.examples.features.dgm.showcase`.

Five tables are populated with a small fixed dataset:

* ``dgm_showcase_customer``   — 3 customers (SCD-2 columns present, all current)
* ``dgm_showcase_product``    — 3 products across three categories
* ``dgm_showcase_segment``    — 2 segments (premium, standard)
* ``dgm_showcase_sale``       — 6 transactions across those customers/products
* ``dgm_showcase_prod_seg``   — 2 product–segment assignments (bridge)

:class:`DGMShowcaseSource` is registered with
:class:`~sqldim.examples.datasets.base.DatasetFactory`
under the key ``"dgm_showcase"`` and follows the static-fixture pattern
(``setup()`` populates all tables; ``snapshot()`` is not applicable).

Usage::

    from sqldim.examples.datasets import DatasetFactory
    import duckdb

    src = DatasetFactory.create("dgm_showcase")
    con = duckdb.connect()
    src.setup(con)
    # … run DGM queries …
    src.teardown(con)
"""

from __future__ import annotations

import duckdb

from sqldim.examples.datasets.base import BaseSource, DatasetFactory, SourceProvider


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS dgm_showcase_customer (
    id          INTEGER PRIMARY KEY,
    email       VARCHAR NOT NULL,
    segment     VARCHAR NOT NULL,
    region      VARCHAR NOT NULL,
    valid_from  DATE,
    valid_to    DATE
)
"""

_PRODUCT_DDL = """
CREATE TABLE IF NOT EXISTS dgm_showcase_product (
    id       INTEGER PRIMARY KEY,
    sku      VARCHAR NOT NULL,
    category VARCHAR NOT NULL
)
"""

_SEGMENT_DDL = """
CREATE TABLE IF NOT EXISTS dgm_showcase_segment (
    id   INTEGER PRIMARY KEY,
    code VARCHAR NOT NULL,
    tier VARCHAR NOT NULL
)
"""

_SALE_DDL = """
CREATE TABLE IF NOT EXISTS dgm_showcase_sale (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    revenue     DOUBLE  NOT NULL,
    quantity    INTEGER NOT NULL,
    sale_year   INTEGER NOT NULL
)
"""

_BRIDGE_DDL = """
CREATE TABLE IF NOT EXISTS dgm_showcase_prod_seg (
    id         INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    segment_id INTEGER NOT NULL,
    weight     DOUBLE  NOT NULL DEFAULT 1.0
)
"""


# ---------------------------------------------------------------------------
# Static fixture data
# ---------------------------------------------------------------------------

# (id, email, segment, region, valid_from, valid_to)
_CUSTOMER_ROWS = [
    (1, "alice@x", "retail",    "US", "2020-01-01", None),
    (2, "bob@x",   "wholesale", "EU", "2020-01-01", None),
    (3, "carol@x", "retail",    "US", "2020-01-01", None),
]

# (id, sku, category)
_PRODUCT_ROWS = [
    (1, "W-001", "electronics"),
    (2, "G-002", "clearance"),
    (3, "D-003", "food"),
]

# (id, code, tier)
_SEGMENT_ROWS = [
    (1, "elec", "premium"),
    (2, "food", "standard"),
]

# (id, customer_id, product_id, revenue, quantity, sale_year)
_SALE_ROWS = [
    (1, 1, 1, 1500.0, 3, 2024),
    (2, 1, 2,  200.0, 1, 2024),
    (3, 1, 3, 3500.0, 5, 2024),
    (4, 2, 1, 4000.0, 8, 2024),
    (5, 3, 1, 2000.0, 4, 2024),
    (6, 3, 3,  600.0, 2, 2024),
]

# (id, product_id, segment_id, weight)
_BRIDGE_ROWS = [
    (1, 1, 1, 1.0),
    (2, 3, 2, 1.0),
]


# ---------------------------------------------------------------------------
# Source class
# ---------------------------------------------------------------------------

@DatasetFactory.register("dgm_showcase")
class DGMShowcaseSource(BaseSource):
    """
    Static five-table star schema fixture for the DGM feature showcase.

    Represents what would, in a real deployment, be sourced from an OLTP
    order-management system: customers, products, segments, sales
    transactions, and a product-to-segment bridge.

    All five tables are populated by :meth:`setup`; no OLTP extraction
    step is needed for this static fixture.
    """

    provider = SourceProvider(
        name="Order-Management System (synthetic)",
        description=(
            "Minimal star schema — 3 customers, 3 products, 2 segments, "
            "6 sale transactions, and a product-segment bridge.  "
            "Static fixture used by the DGM feature showcase."
        ),
    )

    OLTP_DDL = (
        "-- dgm_showcase_customer : id, email, segment, region, valid_from, valid_to\n"
        "-- dgm_showcase_product  : id, sku, category\n"
        "-- dgm_showcase_segment  : id, code, tier\n"
        "-- dgm_showcase_sale     : id, customer_id, product_id, revenue, quantity, sale_year\n"
        "-- dgm_showcase_prod_seg : id, product_id, segment_id, weight"
    )

    def setup(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "dgm_showcase_customer",
    ) -> None:
        """Create and populate all five showcase tables."""
        for ddl in (_CUSTOMER_DDL, _PRODUCT_DDL, _SEGMENT_DDL, _SALE_DDL, _BRIDGE_DDL):
            con.execute(ddl)

        con.executemany(
            "INSERT INTO dgm_showcase_customer"
            " (id, email, segment, region, valid_from, valid_to)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            _CUSTOMER_ROWS,
        )
        con.executemany(
            "INSERT INTO dgm_showcase_product (id, sku, category) VALUES (?, ?, ?)",
            _PRODUCT_ROWS,
        )
        con.executemany(
            "INSERT INTO dgm_showcase_segment (id, code, tier) VALUES (?, ?, ?)",
            _SEGMENT_ROWS,
        )
        con.executemany(
            "INSERT INTO dgm_showcase_sale"
            " (id, customer_id, product_id, revenue, quantity, sale_year)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            _SALE_ROWS,
        )
        con.executemany(
            "INSERT INTO dgm_showcase_prod_seg"
            " (id, product_id, segment_id, weight)"
            " VALUES (?, ?, ?, ?)",
            _BRIDGE_ROWS,
        )

    def teardown(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "dgm_showcase_customer",
    ) -> None:
        """Drop all five showcase tables."""
        for tbl in (
            "dgm_showcase_prod_seg",
            "dgm_showcase_sale",
            "dgm_showcase_segment",
            "dgm_showcase_product",
            "dgm_showcase_customer",
        ):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")

    def snapshot(self):
        raise NotImplementedError(
            "DGMShowcaseSource is a static fixture — call setup() to populate "
            "the tables, then query them directly."
        )
