"""CounterpartiesSource — fintech domain source."""

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

N_COUNTERPARTIES = 25
"""Counterparties the bundled dataset generates; ``cp_id`` runs 1..N."""

# ── DatasetSpecs ──────────────────────────────────────────────────────────────

_COUNTERPARTIES_SPEC = DatasetSpec(
    name="counterparties",
    schemas={
        "source": EntitySchema(
            name="counterparties",
            fields=[
                FieldSpec("cp_id", "INTEGER", kind="seq"),
                FieldSpec("bic_code", "VARCHAR", kind="faker", method="swift11"),
                FieldSpec(
                    "institution_name", "VARCHAR", kind="faker", method="company"
                ),
                FieldSpec(
                    "country_code",
                    "VARCHAR",
                    kind="choices",
                    choices=["US", "GB", "DE", "JP", "SG", "CH"],
                ),
                FieldSpec(
                    "is_sanctioned",
                    "BOOLEAN",
                    kind="choices",
                    choices=[False, False, False, False, True],
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # Institutions occasionally get added to / removed from sanctions
            ChangeRule(
                field="is_sanctioned",
                condition=lambda i, row: random.random() < 0.05,
                mutate=lambda old, row, fake: not old,
            ),
        ],
    ),
)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("fintech_counterparties")
class CounterpartiesSource(SchematicSource):
    """Synthetic counterparty OLTP source keyed on ``bic_code``."""

    spec = _COUNTERPARTIES_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = N_COUNTERPARTIES, *, seed: int = 42) -> None:
        super().__init__(
            spec=_COUNTERPARTIES_SPEC,
            schema_name="source",
            n_entities=n_entities,
            seed=seed,
        )
