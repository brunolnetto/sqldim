"""SKUsSource — commerce domain (supply-chain tier)."""

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

_CATEGORIES = ["Raw Materials", "Components", "Finished Goods", "Packaging"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────

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


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("supply_chain_skus")
class SKUsSource(SchematicSource):
    """Synthetic SKU catalogue OLTP source keyed on ``sku_code``."""

    spec = _SKUS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 50, *, seed: int = 42) -> None:
        super().__init__(
            spec=_SKUS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
