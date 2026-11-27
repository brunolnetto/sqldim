"""SaaSSessionsSource — growth domain daily activity source."""

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
    seeded_date,
)
from sqldim.application.datasets.domains.growth.sources.users import N_SAAS_USERS

# ── Vocabulary ────────────────────────────────────────────────────────────────

_EVENT_TYPES = ["login", "feature_use", "dashboard_view", "api_call", "export"]

# ── DatasetSpec ───────────────────────────────────────────────────────────────


def _saas_sessions_spec(n_users: int) -> DatasetSpec:
    """The session spec whose ``user_id`` stays inside ``1..n_users``."""
    if n_users < 1:
        raise ValueError(f"sessions need at least one user (got n_users={n_users})")
    return DatasetSpec(
        name="saas_sessions",
        schemas={
            "source": EntitySchema(
                name="saas_sessions",
                fields=[
                    FieldSpec("session_id", "INTEGER", kind="seq"),
                    # FK into saas_users: 1..n_users, the parent's key range.
                    FieldSpec(
                        "user_id", "INTEGER", kind="randint", low=1, high=n_users
                    ),
                    # The session year: the 366 days ending on REFERENCE_DATE.
                    FieldSpec(
                        "event_date",
                        "VARCHAR",
                        kind="date",
                        low=-365,
                        high=0,
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
                    mutate=lambda old, row, fake: str(seeded_date(-365, 0)),
                ),
                ChangeRule(
                    field="event_type",
                    condition=lambda i, row: random.random() < 0.5,
                    mutate=lambda old, row, fake: random.choice(_EVENT_TYPES),
                ),
            ],
        ),
    )


_SAAS_SESSIONS_SPEC = _saas_sessions_spec(N_SAAS_USERS)


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("saas_sessions")
class SaaSSessionsSource(SchematicSource):
    """
    Synthetic SaaS session activity source.

    Each row is a user session (login, feature usage, API call, etc.).
    The ``event_date`` field provides daily activity granularity for growth
    accounting state transitions (New / Retained / Resurrected / Churned / Stale).

    ``user_id`` values are sampled from ``1..n_users``, the key range of the
    paired ``SaaSUsersSource(n=n_users)`` (200 by default).

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.growth.sources import (
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

    def __init__(
        self, n: int = 5, seed: int = 42, *, n_users: int = N_SAAS_USERS
    ) -> None:
        super().__init__(n, seed, spec=_saas_sessions_spec(n_users))
