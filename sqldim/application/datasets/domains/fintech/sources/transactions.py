"""TransactionsSource — fintech domain source.

Every transaction references an existing account and counterparty: the
``account_id`` and ``counterparty_id`` ranges are the parents' key ranges
(``1..n_accounts`` and ``1..n_counterparties``), which default to the sizes
the bundled dataset generates (#168).
"""

from __future__ import annotations


import random

from sqldim.application.datasets.base import (
    DatasetFactory,
    SchematicSource,
)
from sqldim.application.datasets.domains.fintech.sources.accounts import N_ACCOUNTS
from sqldim.application.datasets.domains.fintech.sources.counterparties import (
    N_COUNTERPARTIES,
)
from sqldim.application.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# ── Vocabulary ────────────────────────────────────────────────────────────────

_TXN_TYPES = ["payment", "payment", "refund", "fee", "interest"]
_CHANNELS = ["online", "mobile", "branch", "api"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────


def _transactions_spec(n_accounts: int, n_counterparties: int) -> DatasetSpec:
    """The ledger spec whose foreign keys stay inside the parents' key ranges."""
    if n_accounts < 1 or n_counterparties < 1:
        raise ValueError(
            "transactions need at least one account and one counterparty "
            f"(got n_accounts={n_accounts}, n_counterparties={n_counterparties})"
        )
    return DatasetSpec(
        name="transactions",
        schemas={
            "source": EntitySchema(
                name="transactions",
                fields=[
                    FieldSpec("txn_id", "INTEGER", kind="seq"),
                    FieldSpec(
                        "account_id", "INTEGER", kind="randint", low=1, high=n_accounts
                    ),
                    FieldSpec(
                        "counterparty_id",
                        "INTEGER",
                        kind="randint",
                        low=1,
                        high=n_counterparties,
                    ),
                    FieldSpec(
                        "txn_date",
                        "VARCHAR",
                        kind="date",
                        low=-730,
                        high=0,
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
                    FieldSpec(
                        "txn_type", "VARCHAR", kind="choices", choices=_TXN_TYPES
                    ),
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


_TRANSACTIONS_SPEC = _transactions_spec(N_ACCOUNTS, N_COUNTERPARTIES)

# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("fintech_transactions")
class TransactionsSource(SchematicSource):
    """Synthetic payment ledger OLTP source.

    ``n_accounts`` and ``n_counterparties`` are the sizes of the parent
    tables; every generated reference falls inside them.
    """

    spec = _TRANSACTIONS_SPEC
    schema_name = "source"

    def __init__(
        self,
        n_entities: int = 200,
        *,
        seed: int = 42,
        n_accounts: int = N_ACCOUNTS,
        n_counterparties: int = N_COUNTERPARTIES,
    ) -> None:
        super().__init__(
            spec=_transactions_spec(n_accounts, n_counterparties),
            schema_name="source",
            n_entities=n_entities,
            seed=seed,
        )
