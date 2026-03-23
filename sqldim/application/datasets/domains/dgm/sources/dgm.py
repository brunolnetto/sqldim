"""DGMShowcaseSource — dgm domain source."""

from __future__ import annotations


import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider


# ---------------------------------------------------------------------------
# DDL templates  (use table prefix so the source honours the Dataset alias)
# ---------------------------------------------------------------------------

_CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}_customer (
    id          INTEGER PRIMARY KEY,
    email       VARCHAR NOT NULL,
    segment     VARCHAR NOT NULL,
    region      VARCHAR NOT NULL,
    valid_from  DATE,
    valid_to    DATE
)
"""

_PRODUCT_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}_product (
    id       INTEGER PRIMARY KEY,
    sku      VARCHAR NOT NULL,
    category VARCHAR NOT NULL
)
"""

_SEGMENT_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}_segment (
    id   INTEGER PRIMARY KEY,
    code VARCHAR NOT NULL,
    tier VARCHAR NOT NULL
)
"""

_SALE_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}_sale (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    revenue     DOUBLE  NOT NULL,
    quantity    INTEGER NOT NULL,
    sale_year   INTEGER NOT NULL
)
"""

_BRIDGE_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}_prod_seg (
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
        "-- {prefix}_customer : id, email, segment, region, valid_from, valid_to\n"
        "-- {prefix}_product  : id, sku, category\n"
        "-- {prefix}_segment  : id, code, tier\n"
        "-- {prefix}_sale     : id, customer_id, product_id, revenue, quantity, sale_year\n"
        "-- {prefix}_prod_seg : id, product_id, segment_id, weight"
    )

    def setup(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "dgm_showcase",
    ) -> None:
        """Create and populate all five showcase tables, using *table* as prefix.

        For example, with the default ``table="dgm_showcase"`` the created
        tables are ``dgm_showcase_customer``, ``dgm_showcase_product``, etc.
        """
        p = table  # prefix shorthand
        for ddl in (_CUSTOMER_DDL, _PRODUCT_DDL, _SEGMENT_DDL, _SALE_DDL, _BRIDGE_DDL):
            con.execute(ddl.format(prefix=p))

        con.executemany(
            f"INSERT INTO {p}_customer"
            " (id, email, segment, region, valid_from, valid_to)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            _CUSTOMER_ROWS,
        )
        con.executemany(
            f"INSERT INTO {p}_product (id, sku, category) VALUES (?, ?, ?)",
            _PRODUCT_ROWS,
        )
        con.executemany(
            f"INSERT INTO {p}_segment (id, code, tier) VALUES (?, ?, ?)",
            _SEGMENT_ROWS,
        )
        con.executemany(
            f"INSERT INTO {p}_sale"
            " (id, customer_id, product_id, revenue, quantity, sale_year)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            _SALE_ROWS,
        )
        con.executemany(
            f"INSERT INTO {p}_prod_seg"
            " (id, product_id, segment_id, weight)"
            " VALUES (?, ?, ?, ?)",
            _BRIDGE_ROWS,
        )

    def teardown(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "dgm_showcase",
    ) -> None:
        """Drop all five showcase tables (reverse FK order)."""
        for suffix in ("_prod_seg", "_sale", "_segment", "_product", "_customer"):
            con.execute(f"DROP TABLE IF EXISTS {table}{suffix}")

    def snapshot(self):
        raise NotImplementedError(
            "DGMShowcaseSource is a static fixture — call setup() to populate "
            "the tables, then query them directly."
        )
