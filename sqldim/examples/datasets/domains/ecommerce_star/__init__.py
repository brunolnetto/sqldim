"""
sqldim/examples/datasets/ecommerce_star.py
================================================
Faker-backed OLTP simulator for the e-commerce real-world example.

Provides synthetic customer, product, campaign, and order data matching the
models declared in ``models.py``.  Feeds the SCD2 + Bridge + Accumulating
fact pipeline demonstrated in ``showcase.py``.

Sources
-------
``CustomersSource``  — customer sign-ups and loyalty-tier upgrades.
``ProductsSource``   — product catalogue (price / stock changes).
``OrdersSource``     — raw order events (placed → paid → shipped → delivered).

OLTP flow::

    CustomersSource / ProductsSource / OrdersSource    (synthetic OLTP)
        │  .snapshot()      full initial extract
        │  .event_batch(n)  tier-upgrades / new order stages
        ▼
    SCDHandler / LazyAccumulatingLoader
        ▼
    dim_customer (SCD2) + dim_product (SCD1) + fact_order (accumulating)
"""

from __future__ import annotations

import random

from sqldim.examples.datasets.base import (
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

# ── Vocabulary ────────────────────────────────────────────────────────────────

_TIERS = ["bronze", "bronze", "silver", "gold", "platinum"]
_CHANNELS = ["organic", "email", "social", "referral", "ads"]
_CATEGORIES = ["Electronics", "Apparel", "Home", "Sports", "Books"]
_ORDER_IDS = [f"ORD-{i:05d}" for i in range(1, 201)]

# ── DatasetSpecs ──────────────────────────────────────────────────────────────

_CUSTOMERS_SPEC = DatasetSpec(
    name="customers",
    schemas={
        "source": EntitySchema(
            name="customers",
            fields=[
                FieldSpec("customer_id", "INTEGER", kind="seq"),
                FieldSpec("email", "VARCHAR", kind="faker", method="email"),
                FieldSpec("full_name", "VARCHAR", kind="faker", method="name"),
                FieldSpec("loyalty_tier", "VARCHAR", kind="choices", choices=_TIERS),
                FieldSpec(
                    "country_code",
                    "VARCHAR",
                    kind="choices",
                    choices=["US", "US", "CA", "GB", "AU"],
                ),
                FieldSpec(
                    "acquisition_channel", "VARCHAR", kind="choices", choices=_CHANNELS
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # ~20 % of bronze customers upgrade to silver
            ChangeRule(
                field="loyalty_tier",
                condition=lambda i, row: (
                    row.get("loyalty_tier") == "bronze" and random.random() < 0.2
                ),
                mutate=lambda old, row, fake: "silver",
            ),
            # ~15 % of silver customers upgrade to gold
            ChangeRule(
                field="loyalty_tier",
                condition=lambda i, row: (
                    row.get("loyalty_tier") == "silver" and random.random() < 0.15
                ),
                mutate=lambda old, row, fake: "gold",
            ),
        ],
        timestamp_field=None,
    ),
)

_PRODUCTS_SPEC = DatasetSpec(
    name="products",
    schemas={
        "source": EntitySchema(
            name="products",
            fields=[
                FieldSpec("product_id", "INTEGER", kind="seq"),
                FieldSpec("sku", "VARCHAR", kind="faker", method="ean8"),
                FieldSpec(
                    "product_name", "VARCHAR", kind="faker", method="catch_phrase"
                ),
                FieldSpec("category", "VARCHAR", kind="choices", choices=_CATEGORIES),
                FieldSpec(
                    "unit_price",
                    "DOUBLE",
                    kind="uniform",
                    low=4.99,
                    high=299.99,
                    precision=2,
                ),
                FieldSpec("stock_units", "INTEGER", kind="randint", low=0, high=5000),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Price fluctuations
            ChangeRule(
                field="unit_price",
                condition=lambda i, row: random.random() < 0.25,
                mutate=lambda old, row, fake: round(
                    max(0.99, float(old) * random.uniform(0.85, 1.15)), 2
                ),
            ),
        ],
    ),
)

_ORDERS_SPEC = DatasetSpec(
    name="orders",
    schemas={
        "source": EntitySchema(
            name="orders",
            fields=[
                FieldSpec("order_id", "VARCHAR", kind="choices", choices=_ORDER_IDS),
                FieldSpec("customer_id", "INTEGER", kind="randint", low=1, high=100),
                FieldSpec("product_id", "INTEGER", kind="randint", low=1, high=50),
                FieldSpec("quantity", "INTEGER", kind="randint", low=1, high=10),
                FieldSpec(
                    "unit_price_at_order",
                    "DOUBLE",
                    kind="uniform",
                    low=4.99,
                    high=299.99,
                    precision=2,
                ),
                FieldSpec(
                    "placed_at",
                    "TIMESTAMP",
                    kind="faker",
                    method="date_time_this_year",
                    post=lambda v, f, i: str(v),
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Orders progress through stages as event batches arrive
            ChangeRule(
                field="placed_at",
                condition=lambda i, row: random.random() < 0.8,
                mutate=lambda old, row, fake: old,  # placed_at stays fixed
            ),
        ],
    ),
)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("ecommerce_customers")
class CustomersSource(SchematicSource):
    """Synthetic customer OLTP source keyed on ``email``.

    Example
    -------
    ::

        import duckdb
        src = CustomersSource(n_entities=50)
        con = duckdb.connect()
        print(src.snapshot().as_sql(con))
    """

    spec = _CUSTOMERS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 80, *, seed: int = 42) -> None:
        super().__init__(
            spec=_CUSTOMERS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )


@DatasetFactory.register("ecommerce_products")
class ProductsSource(SchematicSource):
    """Synthetic product catalogue OLTP source keyed on ``sku``."""

    spec = _PRODUCTS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 40, *, seed: int = 42) -> None:
        super().__init__(
            spec=_PRODUCTS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )


@DatasetFactory.register("ecommerce_orders")
class OrdersSource(SchematicSource):
    """Synthetic order event stream keyed on ``order_id``."""

    spec = _ORDERS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 100, *, seed: int = 42) -> None:
        super().__init__(
            spec=_ORDERS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
