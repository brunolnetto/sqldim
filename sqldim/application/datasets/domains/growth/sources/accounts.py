"""AccountsSource — growth domain source."""

from __future__ import annotations


import random
from typing import Any

from faker import Faker

from sqldim.application.datasets.base import (
    BaseSource,
    DatasetFactory,
    SourceProvider,
)
from sqldim.application.datasets.schema import (
    DatasetSpec,
    EntitySchema,
    FieldSpec,
)


# ── AccountsSource ────────────────────────────────────────────────────────────

_ACCOUNTS_SPEC = DatasetSpec(
    "accounts",
    {
        "source": EntitySchema(
            name="account",
            fields=[
                FieldSpec("account_id", "INTEGER"),
                FieldSpec("account_type", "VARCHAR"),
                FieldSpec("balance", "DOUBLE"),
                FieldSpec("as_of_date", "DATE"),
            ],
        ),
        "snapshot": EntitySchema(
            name="account_fact",
            fields=[
                FieldSpec("account_id", "INTEGER"),
                FieldSpec("account_type", "VARCHAR"),
                FieldSpec("balance", "DOUBLE"),
                FieldSpec("snapshot_date", "DATE"),
            ],
        ),
    },
)


@DatasetFactory.register("accounts")
class AccountsSource(BaseSource):
    """
    OLTP daily account-balance snapshot — feeds ``LazySnapshotLoader``.

    The OLTP system emits one row per account per day.  sqldim's
    ``LazySnapshotLoader`` appends each daily extract to the fact table,
    creating a time-series of balances.

    Kept manual because each call to ``snapshot_for_date`` applies random
    drift to produce a fresh snapshot — a pattern incompatible with EventSpec.

    OLTP schema::

        account_id    INTEGER  PRIMARY KEY
        account_type  VARCHAR
        balance       DOUBLE
        as_of_date    DATE      -- the snapshot date emitted by the OLTP system

    Fact table target (FACT_DDL)::

        account_id    INTEGER
        account_type  VARCHAR
        balance       DOUBLE
        snapshot_date DATE      -- injected by LazySnapshotLoader
    """

    provider = SourceProvider(
        name="Core banking / ERP (SAP S/4HANA / Oracle Financials)",
        description="Daily account balance snapshots from financial ledger.",
        url="https://api.sap.com/",
        auth_required=True,
        requires=["pyrfc", "requests"],
    )

    @property
    def OLTP_DDL(self) -> str:  # type: ignore[override]  # noqa: N802
        return _ACCOUNTS_SPEC.source.oltp_ddl()

    @property
    def DIM_DDL(self) -> str:  # type: ignore[override]  # noqa: N802
        """Periodic-snapshot fact target DDL for LazySnapshotLoader."""
        return _ACCOUNTS_SPEC.snapshot.oltp_ddl()

    def __init__(self, n: int = 5, seed: int = 42) -> None:
        Faker()
        Faker.seed(seed)
        random.seed(seed)

        account_types = ["checking", "savings", "money_market", "cd"]
        self._accounts: list[dict[str, Any]] = [
            {
                "account_id": 1000 + i + 1,
                "account_type": random.choice(account_types),
                "balance": round(random.uniform(250.0, 150_000.0), 2),
            }
            for i in range(n)
        ]

    def snapshot(self):
        """Initial balance snapshot (today's extract)."""
        return self.snapshot_for_date("2024-01-01")

    def snapshot_for_date(self, snapshot_date: str):
        """
        OLTP daily balance extract for a given date.

        Each call adds a small random ±3 % drift to simulate realistic
        daily balance movement.
        """
        from sqldim.sources import SQLSource

        rows = " UNION ALL ".join(
            f"SELECT {a['account_id']} AS account_id,"
            f" '{a['account_type']}' AS account_type,"
            f" {round(a['balance'] * random.uniform(0.97, 1.03), 2)} AS balance"
            for a in self._accounts
        )
        return SQLSource(rows)

    @property
    def accounts(self) -> list[dict[str, Any]]:
        return list(self._accounts)
