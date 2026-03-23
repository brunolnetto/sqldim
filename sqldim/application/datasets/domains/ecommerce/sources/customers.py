"""CustomersSource — ecommerce domain source."""

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
                FieldSpec(
                    "updated_at",
                    "TIMESTAMP",
                    kind="const",
                    value="2024-01-01 00:00:00",
                    sql_export=False,
                ),
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
