"""SuppliersSource — commerce domain (supply-chain tier)."""

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


# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("supply_chain_suppliers")
class SuppliersSource(SchematicSource):
    """Synthetic supplier master OLTP source keyed on ``supplier_code``."""

    spec = _SUPPLIERS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 20, *, seed: int = 42) -> None:
        super().__init__(
            spec=_SUPPLIERS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
