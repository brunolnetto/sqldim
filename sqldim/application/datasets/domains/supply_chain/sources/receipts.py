"""ReceiptsSource — supply_chain domain source."""

from __future__ import annotations


import random

from sqldim.application.datasets.base import (
    DatasetFactory,
    SchematicSource,
)
from sqldim.application.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# ── Vocabulary ────────────────────────────────────────────────────────────────

_REGIONS = ["EMEA", "APAC", "AMER"]
_CATEGORIES = ["Raw Materials", "Components", "Finished Goods", "Packaging"]
_CARRIERS = ["freight", "air", "courier", "rail"]
_COUNTRIES = ["CN", "US", "DE", "IN", "BR", "JP", "MX"]

# ── DatasetSpecs ──────────────────────────────────────────────────────────────

_SUPPLIERS_SPEC = DatasetSpec(
    name="suppliers",
    schemas={
        "source": EntitySchema(
            name="suppliers",
            fields=[
                FieldSpec("supplier_id", "INTEGER", kind="seq"),
                FieldSpec("supplier_code", "VARCHAR", kind="faker", method="ein"),
                FieldSpec("supplier_name", "VARCHAR", kind="faker", method="company"),
                FieldSpec(
                    "country_code", "VARCHAR", kind="choices", choices=_COUNTRIES
                ),
                FieldSpec("lead_time_days", "INTEGER", kind="randint", low=1, high=60),
                FieldSpec(
                    "reliability_score",
                    "DOUBLE",
                    kind="uniform",
                    low=0.5,
                    high=1.0,
                    precision=2,
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Reliability scores drift slightly each period
            ChangeRule(
                field="reliability_score",
                condition=lambda i, row: random.random() < 0.3,
                mutate=lambda old, row, fake: round(
                    min(1.0, max(0.0, float(old) + random.uniform(-0.1, 0.1))), 2
                ),
            ),
        ],
    ),
)

_WAREHOUSES_SPEC = DatasetSpec(
    name="warehouses",
    schemas={
        "source": EntitySchema(
            name="warehouses",
            fields=[
                FieldSpec("warehouse_id", "INTEGER", kind="seq"),
                FieldSpec("warehouse_code", "VARCHAR", kind="faker", method="bban"),
                FieldSpec(
                    "warehouse_name",
                    "VARCHAR",
                    kind="faker",
                    method="city",
                    post=lambda v, f, i: f"{v} DC",
                ),
                FieldSpec("region", "VARCHAR", kind="choices", choices=_REGIONS),
                FieldSpec(
                    "max_capacity_units",
                    "INTEGER",
                    kind="randint",
                    low=10_000,
                    high=500_000,
                ),
                FieldSpec(
                    "is_active",
                    "BOOLEAN",
                    kind="choices",
                    choices=[True, True, True, False],
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Capacity expansions after refits
            ChangeRule(
                field="max_capacity_units",
                condition=lambda i, row: random.random() < 0.15,
                mutate=lambda old, row, fake: int(old) + random.randint(5_000, 50_000),
            ),
        ],
    ),
)

_SKUS_SPEC = DatasetSpec(
    name="skus",
    schemas={
        "source": EntitySchema(
            name="skus",
            fields=[
                FieldSpec("sku_id", "INTEGER", kind="seq"),
                FieldSpec("sku_code", "VARCHAR", kind="faker", method="ean13"),
                FieldSpec(
                    "product_name", "VARCHAR", kind="faker", method="catch_phrase"
                ),
                FieldSpec("category", "VARCHAR", kind="choices", choices=_CATEGORIES),
                FieldSpec(
                    "weight_kg",
                    "DOUBLE",
                    kind="uniform",
                    low=0.1,
                    high=50.0,
                    precision=2,
                ),
                FieldSpec(
                    "hs_code",
                    "VARCHAR",
                    kind="randint",
                    low=100000,
                    high=999999,
                    post=lambda v, f, i: str(v),
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Weight corrections after measurement audits
            ChangeRule(
                field="weight_kg",
                condition=lambda i, row: random.random() < 0.1,
                mutate=lambda old, row, fake: round(
                    max(0.01, float(old) + random.uniform(-0.5, 0.5)), 2
                ),
            ),
        ],
    ),
)

_RECEIPTS_SPEC = DatasetSpec(
    name="receipts",
    schemas={
        "source": EntitySchema(
            name="receipts",
            fields=[
                FieldSpec("receipt_id", "INTEGER", kind="seq"),
                FieldSpec("warehouse_id", "INTEGER", kind="randint", low=1, high=10),
                FieldSpec("supplier_id", "INTEGER", kind="randint", low=1, high=20),
                FieldSpec("sku_id", "INTEGER", kind="randint", low=1, high=50),
                FieldSpec(
                    "receipt_date",
                    "VARCHAR",
                    kind="faker",
                    method="date",
                    post=lambda v, f, i: str(v),
                ),
                FieldSpec(
                    "quantity_received", "INTEGER", kind="randint", low=10, high=5_000
                ),
                FieldSpec(
                    "unit_cost_usd",
                    "DOUBLE",
                    kind="uniform",
                    low=0.50,
                    high=500.0,
                    precision=2,
                ),
            ],
        ),
    },
    events=EventSpec(changes=[]),
)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("supply_chain_receipts")
class ReceiptsSource(SchematicSource):
    """Synthetic goods-receipt event stream."""

    spec = _RECEIPTS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 150, *, seed: int = 42) -> None:
        super().__init__(
            spec=_RECEIPTS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
