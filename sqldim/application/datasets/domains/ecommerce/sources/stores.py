"""StoresSource — ecommerce domain source."""

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
                    "store_name",
                    "VARCHAR",
                    kind="faker",
                    method="company",
                    post=lambda v, fake, i: v.split(",")[0][:20],
                ),
                FieldSpec(
                    "phone",
                    "VARCHAR",
                    kind="faker",
                    method="numerify",
                    pattern="555-####",
                ),
                FieldSpec("email", "VARCHAR", kind="faker", method="company_email"),
                FieldSpec("city", "VARCHAR", kind="faker", method="city"),
                FieldSpec("state", "VARCHAR", kind="choices", choices=get_us_states()),
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
