"""EventsSource — user_activity domain source."""

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

# ── DevicesSource spec ────────────────────────────────────────────────────────

_DEVICES_SPEC = DatasetSpec(
    name="devices",
    schemas={
        "source": EntitySchema(
            name="devices",
            fields=[
                FieldSpec("device_id", "INTEGER", kind="seq"),
                FieldSpec("browser_type", "VARCHAR", kind="choices", choices=_BROWSERS),
                FieldSpec("os_type", "VARCHAR", kind="choices", choices=_OS_TYPES),
                FieldSpec("device_type", "VARCHAR", kind="choices", choices=_DEVICES),
            ],
        ),
    },
    # Devices rarely change; OS upgrades modelled as event
    events=EventSpec(
        changes=[
            ChangeRule(
                field="os_type",
                condition=lambda i, row: random.random() < 0.1,
                mutate=lambda old, row, fake: random.choice(_OS_TYPES),
            ),
        ],
    ),
)


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


# ── Source classes ────────────────────────────────────────────────────────────


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
        from sqldim.application.datasets.domains.user_activity.sources import EventsSource
        from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader
        from sqldim.sinks import DuckDBSink

        con = duckdb.connect()
        src = EventsSource(n=10_000, seed=0)
        with DuckDBSink(con) as sink:
            loader = LazyBitmaskLoader(
                table="fact_activity_bitmask",
                partition_key="user_id",
                dates_column="dates_active",
                reference_date=date.today(),
                sink=sink,
            )
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
