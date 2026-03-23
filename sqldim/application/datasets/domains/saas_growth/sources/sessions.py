"""SaaSSessionsSource — saas_growth domain daily activity source."""
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

_EVENT_TYPES = ["login", "feature_use", "dashboard_view", "api_call", "export"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────

_SAAS_SESSIONS_SPEC = DatasetSpec(
    name="saas_sessions",
    schemas={
        "source": EntitySchema(
            name="saas_sessions",
            fields=[
                FieldSpec("session_id", "INTEGER", kind="seq"),
                # FK into saas_users — must match SaaSUsersSource(n=200) range
                FieldSpec("user_id", "INTEGER", kind="randint", low=1, high=200),
                FieldSpec(
                    "event_date",
                    "VARCHAR",
                    kind="faker",
                    method="date_this_year",
                    post=lambda v, f, i: str(v),
                ),
                FieldSpec(
                    "event_type",
                    "VARCHAR",
                    kind="choices",
                    choices=_EVENT_TYPES,
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                field="event_date",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: str(fake.date_this_year()),
            ),
            ChangeRule(
                field="event_type",
                condition=lambda i, row: random.random() < 0.5,
                mutate=lambda old, row, fake: random.choice(_EVENT_TYPES),
            ),
        ],
    ),
)


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("saas_sessions")
class SaaSSessionsSource(SchematicSource):
    """
    Synthetic SaaS session activity source.

    Each row is a user session (login, feature usage, API call, etc.).
    The ``event_date`` field provides daily activity granularity for growth
    accounting state transitions (New / Retained / Resurrected / Churned / Stale).

    Designed to work with ``SaaSUsersSource(n=200)`` — ``user_id`` values are
    sampled from the same 1–200 range.

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.saas_growth.sources import (
            SaaSUsersSource, SaaSSessionsSource,
        )

        con = duckdb.connect()
        src_u = SaaSUsersSource(n=200, seed=42)
        src_s = SaaSSessionsSource(n=3_000, seed=42)
        con.execute(f"CREATE TABLE saas_users AS SELECT * FROM ({src_u.snapshot().as_sql(con)})")
        con.execute(f"CREATE TABLE saas_sessions AS SELECT * FROM ({src_s.snapshot().as_sql(con)})")
    """

    _spec = _SAAS_SESSIONS_SPEC

    provider = SourceProvider(
        name="Application Session Log",
        url="postgresql://app-db/sessions",
        description=(
            "Daily session events emitted by the SaaS application. "
            "Real deployment would stream from the application event bus."
        ),
    )
