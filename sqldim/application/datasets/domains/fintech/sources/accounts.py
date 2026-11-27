"""AccountsSource — fintech domain source."""

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

_RISK_TIERS = ["low", "low", "low", "medium", "high"]
_ACCOUNT_TYPES = ["checking", "checking", "savings", "investment"]
_CURRENCIES = ["USD", "USD", "EUR", "GBP", "JPY"]

N_ACCOUNTS = 60
"""Accounts the bundled dataset generates; ``account_id`` runs 1..N."""

# ── DatasetSpecs ──────────────────────────────────────────────────────────────

_ACCOUNTS_SPEC = DatasetSpec(
    name="accounts",
    schemas={
        "source": EntitySchema(
            name="accounts",
            fields=[
                FieldSpec("account_id", "INTEGER", kind="seq"),
                FieldSpec("account_number", "VARCHAR", kind="faker", method="bban"),
                FieldSpec(
                    "account_type", "VARCHAR", kind="choices", choices=_ACCOUNT_TYPES
                ),
                FieldSpec("risk_tier", "VARCHAR", kind="choices", choices=_RISK_TIERS),
                FieldSpec("currency", "VARCHAR", kind="choices", choices=_CURRENCIES),
                FieldSpec("owner_name", "VARCHAR", kind="faker", method="name"),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # ~10 % of low-risk accounts escalate to medium after review
            ChangeRule(
                field="risk_tier",
                condition=lambda i, row: (
                    row.get("risk_tier") == "low" and random.random() < 0.10
                ),
                mutate=lambda old, row, fake: "medium",
            ),
            # ~5 % of medium-risk accounts escalate to high
            ChangeRule(
                field="risk_tier",
                condition=lambda i, row: (
                    row.get("risk_tier") == "medium" and random.random() < 0.05
                ),
                mutate=lambda old, row, fake: "high",
            ),
        ],
        timestamp_field=None,
    ),
)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("fintech_accounts")
class AccountsSource(SchematicSource):
    """Synthetic bank-account OLTP source keyed on ``account_number``."""

    spec = _ACCOUNTS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = N_ACCOUNTS, *, seed: int = 42) -> None:
        super().__init__(
            spec=_ACCOUNTS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
