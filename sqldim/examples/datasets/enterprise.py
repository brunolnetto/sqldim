"""
sqldim/examples/datasets/enterprise.py
========================================
Enterprise domain: Employees (HR) and Accounts (Finance).

``EmployeesSource`` uses the schema-driven ``SchematicSource`` base class.
``AccountsSource`` is kept manual because it models periodic daily-balance
snapshots (one row per account per day with random drift), which doesn't
map naturally to the CDC ChangeRule pattern.

Public interface for every *Source class:
  OLTP_DDL  — schema as it exists in the transactional system
  DIM_DDL / FACT_DDL — sqldim-managed analytical target
  snapshot()         — full initial OLTP extract
  event_batch(n=1)   — incremental change events
"""
from __future__ import annotations

import random
from typing import Any

from faker import Faker

from sqldim.examples.datasets.base import (
    BaseSource,
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)


# ── EmployeesSource ───────────────────────────────────────────────────────────

def get_dept_choices():
    return ["Engineering", "Finance", "Marketing", "Product", "HR", "Legal", "Sales"]

def get_title_ladder():
    return {
        "Engineer":  "Senior Engineer",
        "Analyst":   "Senior Analyst",
        "Manager":   "Senior Manager",
        "Developer": "Lead Developer",
        "Associate": "Senior Associate",
    }


_EMPLOYEES_SPEC = DatasetSpec(
    name="employee",
    schemas={
        "source": EntitySchema(
            name="employee",
            fields=[
                FieldSpec("employee_id", "INTEGER", kind="seq", start=100),
                FieldSpec("full_name", "VARCHAR", kind="faker", method="name"),
                FieldSpec("title", "VARCHAR", kind="choices",
                          choices=list(get_title_ladder().keys())),
                FieldSpec("department", "VARCHAR", kind="choices", choices=get_dept_choices()),
                FieldSpec(
                    "hire_date", "DATE", kind="computed", sql_export=False,
                    fn=lambda fake, i: (
                        f"202{random.randint(0, 3)}-{random.randint(1, 12):02d}-01"
                    ),
                ),
                FieldSpec("updated_at", "TIMESTAMP", kind="const",
                          value="2024-01-01 00:00:00", sql_export=False),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "title",
                condition=lambda i, r: i % 3 == 0,
                mutate=lambda v, r, fake: get_title_ladder().get(v, v),
            ),
            ChangeRule(
                "department",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: random.choice(
                    [d for d in get_dept_choices() if d != v]
                ),
            ),
        ],
        timestamp_field="updated_at",
        event_ts="2024-06-01 09:00:00",
    ),
)


@DatasetFactory.register("employees")
class EmployeesSource(SchematicSource):
    """
    OLTP HR employee directory — feeds a ``LazyType6SCDProcessor``.

    Events (driven by ``_EMPLOYEES_SPEC.events``):
      * Every third employee is promoted (title ascends the ladder).
      * Every second employee transfers to a different department.

    Changes modelled:
      * Promotions   — title upgrades tracked as SCD Type 1 (in-place overwrite)
      * Transfers    — department changes tracked as SCD Type 2 (new version)

    OLTP schema::

        employee_id  INTEGER  PRIMARY KEY
        full_name    VARCHAR
        title        VARCHAR
        department   VARCHAR
        hire_date    DATE
        updated_at   TIMESTAMP

    sqldim output adds::

        valid_from  VARCHAR
        valid_to    VARCHAR
        is_current  BOOLEAN
        checksum    VARCHAR
    """

    _spec = _EMPLOYEES_SPEC

    provider = SourceProvider(
        name="HR Information System (Workday / BambooHR)",
        description="Employee directory with titles, departments, and hire dates.",
        url="https://developer.workday.com/",
        auth_required=True,
        requires=["requests", "oauthlib"],
    )


# ── AccountsSource ────────────────────────────────────────────────────────────

_ACCOUNTS_SPEC = DatasetSpec("accounts", {
    "source": EntitySchema(
        name="account",
        fields=[
            FieldSpec("account_id",   "INTEGER"),
            FieldSpec("account_type", "VARCHAR"),
            FieldSpec("balance",      "DOUBLE"),
            FieldSpec("as_of_date",   "DATE"),
        ],
    ),
    "snapshot": EntitySchema(
        name="account_fact",
        fields=[
            FieldSpec("account_id",    "INTEGER"),
            FieldSpec("account_type",  "VARCHAR"),
            FieldSpec("balance",       "DOUBLE"),
            FieldSpec("snapshot_date", "DATE"),
        ],
    ),
})


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
    def OLTP_DDL(self) -> str:  # noqa: N802
        return _ACCOUNTS_SPEC.source.oltp_ddl()

    @property
    def DIM_DDL(self) -> str:  # noqa: N802
        """Periodic-snapshot fact target DDL for LazySnapshotLoader."""
        return _ACCOUNTS_SPEC.snapshot.oltp_ddl()

    def __init__(self, n: int = 5, seed: int = 42) -> None:
        fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        account_types = ["checking", "savings", "money_market", "cd"]
        self._accounts: list[dict[str, Any]] = [
            {
                "account_id":   1000 + i + 1,
                "account_type": random.choice(account_types),
                "balance":      round(random.uniform(250.0, 150_000.0), 2),
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
