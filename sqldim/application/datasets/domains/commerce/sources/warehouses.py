"""WarehousesSource — commerce domain (supply-chain tier)."""

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
_COUNTRIES = ["CN", "US", "DE", "IN", "BR", "JP", "MX"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────

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


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("supply_chain_warehouses")
class WarehousesSource(SchematicSource):
    """Synthetic warehouse OLTP source keyed on ``warehouse_code``."""

    spec = _WAREHOUSES_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 10, *, seed: int = 42) -> None:
        super().__init__(
            spec=_WAREHOUSES_SPEC,
            schema_name="source",
            n_entities=n_entities,
            seed=seed,
        )
