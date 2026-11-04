"""ReceiptsSource — commerce domain (supply-chain tier)."""

from __future__ import annotations


from sqldim.application.datasets.base import (
    DatasetFactory,
    SchematicSource,
)
from sqldim.application.datasets.schema import (
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# ── DatasetSpec ───────────────────────────────────────────────────────────────

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


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("supply_chain_receipts")
class ReceiptsSource(SchematicSource):
    """Synthetic goods-receipt event stream."""

    spec = _RECEIPTS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 150, *, seed: int = 42) -> None:
        super().__init__(
            spec=_RECEIPTS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
