"""
sqldim/examples/real_world/saas_growth/dataset.py
==================================================
Faker-backed OLTP simulator for the SaaS Growth real-world example.

Provides synthetic SaaS user and subscription data matching the ``User``
SQLModel schema.  Feeds the sqldim SCD2 + graph + bitmask pipeline
demonstrated in ``showcase.py``.

Domains covered
---------------
``SaaSUsersSource``      — user sign-ups with plan_tier and acquisition source.
``SubscriptionSource``   — plan tier changes / upgrade events (CDC feed).

OLTP flow::

    SaaSUsersSource / SubscriptionSource    (this module — synthetic OLTP)
        │  .snapshot()
        │  .event_batch(n=1)               plan upgrades (free → pro)
        ▼
    NarwhalsSCDProcessor / LazySCDProcessor
        ▼
    dim_user (SCD2)

Public interface (matches all BaseSource subclasses):
    setup(con, table)     — create the sqldim-managed target DDL
    teardown(con, table)  — drop the target table
    snapshot()            — SQLSource of full initial user extract
    event_batch(n=1)      — SQLSource of plan-upgrade events
"""
from __future__ import annotations

import random

from sqldim.examples.datasets.base import DatasetFactory, SchematicSource, SourceProvider
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)

# ── Vocabulary ────────────────────────────────────────────────────────────────

_PLAN_TIERS  = ["free", "free", "free", "pro", "enterprise"]
_ACQ_SOURCES = ["organic", "ads", "referral", "content", "social"]
_DEVICES     = ["desktop", "mobile", "tablet"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────

_SAAS_USERS_SPEC = DatasetSpec(
    name="saas_users",
    schemas={
        "source": EntitySchema(
            name="saas_users",
            fields=[
                FieldSpec("user_id",      "INTEGER", kind="seq"),
                FieldSpec("email",        "VARCHAR", kind="faker", method="email"),
                FieldSpec("plan_tier",    "VARCHAR", kind="choices", choices=_PLAN_TIERS),
                FieldSpec("acq_source",   "VARCHAR", kind="choices", choices=_ACQ_SOURCES),
                FieldSpec("device",       "VARCHAR", kind="choices", choices=_DEVICES),
                FieldSpec("signup_date",  "VARCHAR", kind="faker", method="date",
                          post=lambda v, f, i: str(v)),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            # ~30 % of free users upgrade to pro on first event batch
            ChangeRule(
                field="plan_tier",
                condition=lambda i, row: row.get("plan_tier") == "free" and random.random() < 0.3,
                mutate=lambda old, row, fake: "pro",
            ),
            # ~10 % of pro users upgrade further to enterprise
            ChangeRule(
                field="plan_tier",
                condition=lambda i, row: row.get("plan_tier") == "pro" and random.random() < 0.1,
                mutate=lambda old, row, fake: "enterprise",
            ),
        ],
        timestamp_field=None,
    ),
)


# ── Source classes ────────────────────────────────────────────────────────────

@DatasetFactory.register("saas_users")
class SaaSUsersSource(SchematicSource):
    """
    Synthetic SaaS user OLTP source.

    Provides user sign-up data keyed on ``email``.  Plan-tier upgrades are
    modelled as event batches so the SCD2 processor sees genuine changes.

    Example
    -------
    ::

        import duckdb
        from sqldim.examples.real_world.saas_growth.dataset import SaaSUsersSource
        from sqldim.core.kimball.dimensions.scd.processors.scd_engine import LazySCDProcessor
        from sqldim.sinks import DuckDBSink

        con = duckdb.connect()
        src = SaaSUsersSource(n=200, seed=42)
        with DuckDBSink(con) as sink:
            proc = LazySCDProcessor("email", ["plan_tier"], sink, con=con)
            proc.process(src.snapshot(), "dim_user")
            proc.process(src.event_batch(1), "dim_user")
    """

    _spec = _SAAS_USERS_SPEC

    provider = SourceProvider(
        name="Application Database",
        url="postgresql://app-db/users",
        description=(
            "Transactional user table from the SaaS application database. "
            "Real deployment would use the PostgreSQL source adapter."
        ),
    )
