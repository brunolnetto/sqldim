"""
sqldim/examples/datasets/ecommerce.py
=======================================
E-commerce domain: Products, Customers, Stores, and Orders.

Each class simulates an **OLTP source table**.  sqldim sits between the
OLTP system and the analytical dimension/fact table:

    OLTP system        sqldim processor          Analytical layer
    (OLTP_DDL)  ──►  (LazySCDProcessor etc.)  ──►  (DIM_DDL / FACT_DDL)

Products, Customers, and Stores use the schema-driven ``SchematicSource``
base class — field definitions, vocabulary (choices, ranges), and DDL are
all declared together in ``EntitySchema`` / ``FieldSpec`` objects.

``OrdersSource`` is kept manual because it models milestone-timestamp
progressions (placed → paid → shipped → delivered) that don't fit the
simple ChangeRule pattern.

Public interface for every *Source class
-----------------------------------------
* ``setup(con, table)``       — create the sqldim-managed target DDL (empty)
* ``teardown(con, table)``    — drop the target table
* ``snapshot()``              — SQLSource of the full OLTP current state (T0)
* ``event_batch(n=1)``        — SQLSource of records that changed at event T_n

For ``OrdersSource`` the progression is expressed via named event methods:
    placed_events(), paid_events(), shipped_events(), delivered_events()
"""
from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

from faker import Faker

from sqldim.examples.datasets.base import (
    BaseSource,
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)


# ── ProductsSource ────────────────────────────────────────────────────────────

_PRODUCTS_SPEC = DatasetSpec(
    name="product",
    schemas={
        "source": EntitySchema(
            name="product",
            fields=[
                FieldSpec("product_id", "INTEGER", kind="seq"),
                FieldSpec(
                    "name", "VARCHAR", kind="computed",
                    fn=lambda fake, i: (
                        f"{fake.word().title()} "
                        f"{random.choice(['Pro', 'Plus', 'Max', 'Lite', 'Ultra', 'Air', 'Mini', 'Elite']) if random.random() > 0.4 else ''}"
                    ).strip(),
                ),
                FieldSpec(
                    "category", "VARCHAR", kind="choices",
                    choices=["widgets", "gadgets", "tools", "accessories", "consumables"],
                ),
                FieldSpec("price", "DOUBLE", kind="uniform", low=4.99, high=199.99, precision=2),
                FieldSpec("updated_at", "TIMESTAMP", kind="const",
                          value="2024-01-01 00:00:00", sql_export=False),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "price",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: round(v * random.uniform(1.05, 1.30), 2),
            ),
            ChangeRule(
                "name",
                condition=lambda i, r: i % 3 == 0,
                mutate=lambda v, r, fake: v + " v2",
            ),
        ],
        timestamp_field="updated_at",
        event_ts="2024-03-01 09:00:00",
    ),
)


@DatasetFactory.register("products")
class ProductsSource(SchematicSource):
    """
    OLTP product catalog — feeds ``LazySCDProcessor`` to build ``dim_product``.

    Events (driven by ``_PRODUCTS_SPEC.events``):
      * Even-indexed products get a price increase (+5–30 %).
      * Every third product gets a name revision (``"… v2"``).

    sqldim output schema (DIM_DDL) adds::

        valid_from  VARCHAR
        valid_to    VARCHAR
        is_current  BOOLEAN
        checksum    VARCHAR
    """

    _spec = _PRODUCTS_SPEC

    provider = SourceProvider(
        name="E-commerce platform (Shopify / WooCommerce)",
        description="Product master catalog from REST API.",
        url="https://shopify.dev/docs/api/admin-rest/products",
        auth_required=True,
        requires=["dlt", "dlt-shopify"],
    )


# ── CustomersSource ───────────────────────────────────────────────────────────

_CUSTOMERS_SPEC = DatasetSpec(
    name="customer",
    schemas={
        "source": EntitySchema(
            name="customer",
            fields=[
                FieldSpec("customer_id", "INTEGER", kind="seq"),
                FieldSpec("full_name", "VARCHAR", kind="faker", method="name"),
                FieldSpec("email", "VARCHAR", kind="faker", method="email"),
                FieldSpec("address", "VARCHAR", kind="faker", method="street_address"),
                FieldSpec("city", "VARCHAR", kind="faker", method="city"),
                FieldSpec("updated_at", "TIMESTAMP", kind="const",
                          value="2024-01-01 00:00:00", sql_export=False),
            ],
            # SCD Type-3 previous-value columns in the dimension target
            dim_extra=[("prev_address", "VARCHAR"), ("prev_city", "VARCHAR")],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "address",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: fake.street_address(),
            ),
            ChangeRule(
                "city",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: fake.city(),
            ),
        ],
        timestamp_field="updated_at",
        event_ts="2024-04-01 10:00:00",
    ),
)


@DatasetFactory.register("customers")
class CustomersSource(SchematicSource):
    """
    OLTP customer registry — feeds ``LazyType3SCDProcessor`` (SCD Type 3).

    The dimension retains one layer of address history in ``prev_address`` /
    ``prev_city`` so analysts can compare *current* vs *previous* location
    without full SCD2 versioning.

    Events: every even-indexed customer has moved (address + city changed).

    OLTP schema::

        customer_id  INTEGER  PRIMARY KEY
        full_name    VARCHAR
        email        VARCHAR
        address      VARCHAR
        city         VARCHAR
        updated_at   TIMESTAMP

    sqldim output (DIM_DDL) adds::

        prev_address VARCHAR  — rotated in by LazyType3SCDProcessor
        prev_city    VARCHAR
        + SCD2 audit columns
    """

    _spec = _CUSTOMERS_SPEC

    provider = SourceProvider(
        name="CRM / e-commerce customer DB (Salesforce / Klaviyo)",
        description="Customer PII and address records from CRM REST API.",
        url="https://developer.salesforce.com/docs/apis",
        auth_required=True,
        requires=["simple-salesforce"],
    )

    def moved_count(self) -> int:
        """Number of customers who moved since the initial snapshot."""
        return len(self._events1)


# ── StoresSource ──────────────────────────────────────────────────────────────

def get_us_states():
    return ["WA", "OR", "CA", "TX", "NY", "IL", "FL", "CO", "AZ", "GA"]

_STORES_SPEC = DatasetSpec(
    name="store",
    schemas={
        "source": EntitySchema(
            name="store",
            fields=[
                FieldSpec("store_id", "INTEGER", kind="seq", start=10, step=10),
                FieldSpec(
                    "store_name", "VARCHAR", kind="faker", method="company",
                    post=lambda v, fake, i: v.split(",")[0][:20],
                ),
                FieldSpec("phone", "VARCHAR", kind="faker", method="numerify", pattern="555-####"),
                FieldSpec("email", "VARCHAR", kind="faker", method="company_email"),
                FieldSpec("city", "VARCHAR", kind="faker", method="city"),
                FieldSpec("state", "VARCHAR", kind="choices", choices=get_us_states()),
                FieldSpec("updated_at", "TIMESTAMP", kind="const",
                          value="2024-01-01 00:00:00", sql_export=False),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "city",
                condition=lambda i, r: i % 3 == 0,
                mutate=lambda v, r, fake: fake.city(),
            ),
            ChangeRule(
                "state",
                condition=lambda i, r: i % 3 == 0,
                mutate=lambda v, r, fake: random.choice(get_us_states()),
            ),
            ChangeRule(
                "phone",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: fake.numerify("555-####"),
            ),
        ],
        timestamp_field="updated_at",
        event_ts="2024-05-01 08:30:00",
    ),
)


@DatasetFactory.register("stores")
class StoresSource(SchematicSource):
    """
    OLTP retail-store directory — feeds a ``LazyType6SCDProcessor``
    (SCD Type 1 for phone updates, SCD Type 2 for relocations).

    Events:
      * Every third store relocates (city + state change).
      * Every second store updates its phone number.

    OLTP schema::

        store_id    INTEGER  PRIMARY KEY
        store_name  VARCHAR
        phone       VARCHAR
        email       VARCHAR
        city        VARCHAR
        state       VARCHAR
        updated_at  TIMESTAMP
    """

    _spec = _STORES_SPEC

    provider = SourceProvider(
        name="Retail POS / franchise management system",
        description="Store directory with location and contact data.",
        url="https://www.lightspeedhq.com/api/",
        auth_required=True,
        requires=["requests"],
    )


# ── OrdersSource ──────────────────────────────────────────────────────────────

_ORDERS_SPEC = DatasetSpec("orders", {
    "source": EntitySchema(
        name="order",
        fields=[
            FieldSpec("order_id",     "INTEGER"),
            FieldSpec("customer_id",  "INTEGER"),
            FieldSpec("amount",       "DOUBLE"),
            FieldSpec("status",       "VARCHAR"),
            FieldSpec("placed_at",    "VARCHAR"),
            FieldSpec("paid_at",      "VARCHAR"),
            FieldSpec("shipped_at",   "VARCHAR"),
            FieldSpec("delivered_at", "VARCHAR"),
        ],
    ),
    "fact": EntitySchema(
        name="order_fact",
        fields=[
            FieldSpec("order_id",     "INTEGER"),
            FieldSpec("customer_id",  "INTEGER"),
            FieldSpec("amount",       "DOUBLE"),
            FieldSpec("placed_at",    "VARCHAR"),
            FieldSpec("paid_at",      "VARCHAR"),
            FieldSpec("shipped_at",   "VARCHAR"),
            FieldSpec("delivered_at", "VARCHAR"),
        ],
    ),
})


@DatasetFactory.register("orders")
class OrdersSource(BaseSource):
    """
    OLTP order-fulfilment pipeline — feeds ``LazyAccumulatingLoader``.

    Each order is a row in the OLTP ``orders`` table with four progressive
    milestone timestamps.  Events arrive in batches as the order moves
    through the pipeline:

        T1 placed_events()   → placed_at set; paid/shipped/delivered NULL
        T2 paid_events()     → paid_at set for settled orders
        T3 shipped_events()  → shipped_at set for despatched orders
        T4 delivered_events()→ delivered_at set for completed orders

    Kept manual because the milestone-progression model requires knowing the
    full set of timestamps at construction time, which doesn't map naturally
    to a ChangeRule pattern.

    OLTP schema::

        order_id      INTEGER  PRIMARY KEY
        customer_id   INTEGER  FK
        amount        DOUBLE
        status        VARCHAR   -- 'placed' | 'paid' | 'shipped' | 'delivered'
        placed_at     TIMESTAMP
        paid_at       TIMESTAMP  NULL
        shipped_at    TIMESTAMP  NULL
        delivered_at  TIMESTAMP  NULL
    """

    provider = SourceProvider(
        name="Order management system (OMS / ERP)",
        description="Order fulfilment pipeline with milestone timestamps.",
        url="https://docs.kladana.com/api/",
        auth_required=True,
        requires=["dlt", "requests"],
    )

    @property
    def OLTP_DDL(self) -> str:  # noqa: N802
        return _ORDERS_SPEC.source.oltp_ddl()

    @property
    def DIM_DDL(self) -> str:  # noqa: N802
        """Fact target DDL for LazyAccumulatingLoader (no SCD audit columns)."""
        return _ORDERS_SPEC.fact.oltp_ddl()

    def __init__(self, n: int = 6, seed: int = 42) -> None:
        fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        base = date(2024, 3, 1)
        self._orders: list[dict[str, Any]] = []
        for i in range(n):
            placed    = base + timedelta(days=random.randint(0, 10))
            paid      = placed   + timedelta(days=random.randint(0, 2))
            shipped   = paid     + timedelta(days=random.randint(1, 3))
            delivered = shipped  + timedelta(days=random.randint(1, 4))
            self._orders.append({
                "order_id":    1000 + i + 1,
                "customer_id": random.randint(1, 50),
                "amount":      round(random.uniform(9.99, 499.99), 2),
                "_placed":     placed,
                "_paid":       paid      if i < n * 3 // 4 else None,   # 75%
                "_shipped":    shipped   if i < n * 2 // 4 else None,   # 50%
                "_delivered":  delivered if i < n * 1 // 4 else None,   # 25%
            })

    # ── Progressive event sources ─────────────────────────────────────────

    def snapshot(self):
        """Initial order state — all orders at placement (T0)."""
        return self.placed_events()

    def placed_events(self):
        """All orders just created — only placed_at is known."""
        from sqldim.sources import SQLSource
        return SQLSource(self._batch_sql(self._orders, []))

    def paid_events(self):
        """Orders that have been paid (75% of total)."""
        from sqldim.sources import SQLSource
        return SQLSource(self._batch_sql(
            [o for o in self._orders if o["_paid"] is not None], ["paid_at"]
        ))

    def shipped_events(self):
        """Orders that have shipped (50% of total)."""
        from sqldim.sources import SQLSource
        return SQLSource(self._batch_sql(
            [o for o in self._orders if o["_shipped"] is not None], ["paid_at", "shipped_at"]
        ))

    def delivered_events(self):
        """Fully delivered orders (25% of total)."""
        from sqldim.sources import SQLSource
        return SQLSource(self._batch_sql(
            [o for o in self._orders if o["_delivered"] is not None],
            ["paid_at", "shipped_at", "delivered_at"],
        ))

    def _null(self, v: Any) -> str:
        return f"'{v.isoformat()}'" if v is not None else "NULL"

    def _batch_sql(self, orders: list[dict], include_milestones: list[str]) -> str:
        def row_sql(o: dict) -> str:
            milestones = {
                "paid_at":      self._null(o["_paid"]),
                "shipped_at":   self._null(o["_shipped"]),
                "delivered_at": self._null(o["_delivered"]),
            }
            parts = [
                f"SELECT {o['order_id']} AS order_id",
                f"{o['customer_id']} AS customer_id",
                f"{o['amount']} AS amount",
                f"'{o['_placed'].isoformat()}' AS placed_at",
            ]
            for col in ["paid_at", "shipped_at", "delivered_at"]:
                parts.append(
                    f"{milestones[col] if col in include_milestones else 'NULL'} AS {col}"
                )
            return ", ".join(parts)
        return " UNION ALL ".join(row_sql(o) for o in orders)

    @property
    def orders(self) -> list[dict[str, Any]]:
        return list(self._orders)
