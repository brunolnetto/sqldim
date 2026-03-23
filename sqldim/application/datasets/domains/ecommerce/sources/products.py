"""ProductsSource — ecommerce domain source."""

from __future__ import annotations

import random


from sqldim.application.datasets.base import (
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.application.datasets.schema import (
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
                    "name",
                    "VARCHAR",
                    kind="computed",
                    fn=lambda fake, i: (
                        f"{fake.word().title()} "
                        f"{random.choice(['Pro', 'Plus', 'Max', 'Lite', 'Ultra', 'Air', 'Mini', 'Elite']) if random.random() > 0.4 else ''}"
                    ).strip(),
                ),
                FieldSpec(
                    "category",
                    "VARCHAR",
                    kind="choices",
                    choices=[
                        "widgets",
                        "gadgets",
                        "tools",
                        "accessories",
                        "consumables",
                    ],
                ),
                FieldSpec(
                    "price",
                    "DOUBLE",
                    kind="uniform",
                    low=4.99,
                    high=199.99,
                    precision=2,
                ),
                FieldSpec(
                    "updated_at",
                    "TIMESTAMP",
                    kind="const",
                    value="2024-01-01 00:00:00",
                    sql_export=False,
                ),
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
