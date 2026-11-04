"""EventsSource — telemetry domain source."""

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

_BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Brave"]
_OS_TYPES = ["Windows", "macOS", "Linux", "iOS", "Android"]
_DEVICES = ["Desktop", "Mobile", "Tablet"]
_URL_PATHS = [
    "/home",
    "/dashboard",
    "/pricing",
    "/docs",
    "/blog",
    "/login",
    "/signup",
    "/settings",
]

_REFERRERS = [
    "https://google.com/search",
    "https://twitter.com",
    "https://linkedin.com",
    "https://news.ycombinator.com",
    "https://reddit.com/r/python",
    None,  # direct traffic
    None,
    None,
]

_HOSTS = [
    "app.example.com",
    "www.example.com",
    "staging.example.com",
]

# ── EventsSource spec ─────────────────────────────────────────────────────────

_EVENTS_SPEC = DatasetSpec(
    name="events",
    schemas={
        "source": EntitySchema(
            name="events",
            fields=[
                FieldSpec("event_id", "INTEGER", kind="seq"),
                FieldSpec("url", "VARCHAR", kind="choices", choices=_URL_PATHS),
                FieldSpec(
                    "referrer",
                    "VARCHAR",
                    kind="choices",
                    choices=_REFERRERS,
                ),
                FieldSpec("user_id", "INTEGER", kind="randint", low=1, high=500),
                FieldSpec("device_id", "INTEGER", kind="randint", low=1, high=50),
                FieldSpec("host", "VARCHAR", kind="choices", choices=_HOSTS),
                FieldSpec(
                    "event_time",
                    "TIMESTAMP",
                    kind="faker",
                    method="date_time_this_year",
                    post=lambda v, f, i: str(v),
                ),
            ],
        ),
    },
    # New events arrive each batch (page-views for the next day)
    events=EventSpec(
        changes=[
            ChangeRule(
                field="url",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: random.choice(_URL_PATHS),
            ),
            ChangeRule(
                field="referrer",
                condition=lambda i, row: random.random() < 0.4,
                mutate=lambda old, row, fake: random.choice(_REFERRERS),
            ),
            ChangeRule(
                field="event_time",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: str(fake.date_time_this_year()),
            ),
        ],
    ),
)


# ── Source class ──────────────────────────────────────────────────────────────


@DatasetFactory.register("events")
class EventsSource(SchematicSource):
    """
    Synthetic page-view event source.

    Matches the ``Event`` SQLModel schema.  Use with ``LazyBitmaskLoader``
    to compute daily activity bitmasks for ``users_cumulated``.

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.telemetry.sources import EventsSource

        con = duckdb.connect()
        src = EventsSource(n=10_000, seed=0)
        src.setup(con, "page_events")
    """

    _spec = _EVENTS_SPEC

    provider = SourceProvider(
        name="Page-View Event Stream",
        url="kafka://analytics-kafka/pageviews",
        description=(
            "Raw page-view events from the application front-end. "
            "Real deployment would use a streaming source adapter."
        ),
    )
