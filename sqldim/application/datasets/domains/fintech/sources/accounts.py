"""AccountsSource — fintech domain source."""

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

# ── Vocabulary ────────────────────────────────────────────────────────────────

_RISK_TIERS = ["low", "low", "low", "medium", "high"]
_ACCOUNT_TYPES = ["checking", "checking", "savings", "investment"]
_CURRENCIES = ["USD", "USD", "EUR", "GBP", "JPY"]
_TXN_TYPES = ["payment", "payment", "refund", "fee", "interest"]
_CHANNELS = ["online", "mobile", "branch", "api"]

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

_TRANSACTIONS_SPEC = DatasetSpec(
    name="transactions",
    schemas={
        "source": EntitySchema(
            name="transactions",
            fields=[
                FieldSpec("txn_id", "INTEGER", kind="seq"),
                FieldSpec("account_id", "INTEGER", kind="randint", low=1, high=100),
                FieldSpec("counterparty_id", "INTEGER", kind="randint", low=1, high=30),
                FieldSpec(
                    "txn_date",
                    "VARCHAR",
                    kind="faker",
                    method="date",
                    post=lambda v, f, i: str(v),
                ),
                FieldSpec(
                    "amount_usd",
                    "DOUBLE",
                    kind="uniform",
                    low=-5_000.0,
                    high=50_000.0,
                    precision=2,
                ),
                FieldSpec("txn_type", "VARCHAR", kind="choices", choices=_TXN_TYPES),
                FieldSpec("channel", "VARCHAR", kind="choices", choices=_CHANNELS),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                field="amount_usd",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: round(
                    float(old) * random.uniform(0.5, 1.5), 2
                ),
            ),
        ],
    ),
)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("fintech_accounts")
class AccountsSource(SchematicSource):
    """Synthetic bank-account OLTP source keyed on ``account_number``."""

    spec = _ACCOUNTS_SPEC
    schema_name = "source"

    def __init__(self, n_entities: int = 60, *, seed: int = 42) -> None:
        super().__init__(
            spec=_ACCOUNTS_SPEC, schema_name="source", n_entities=n_entities, seed=seed
        )
