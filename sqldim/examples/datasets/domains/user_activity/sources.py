"""
sqldim/examples/datasets/user_activity.py
====================================================
Faker-backed OLTP simulators for the User Activity real-world example.

Provides synthetic device and raw event data matching the ``Device`` and
``Event`` SQLModel schemas in ``models.py``.  Feeds the bitmask + datelist
pipeline demonstrated in ``showcase.py``.

Sources
-------
``DevicesSource``   — device dimension data (browser, OS, form-factor).
``EventsSource``    — raw page-view event stream (url, user_id, device_id).

OLTP flow::

    DevicesSource / EventsSource    (this module — synthetic OLTP)
        │  .snapshot()
        │  .event_batch(n=1)        new events for the next day
        ▼
    DimensionalLoader / BitmaskerLoader
        ▼
    dim_device + fact_event + users_cumulated

Public interface (matches all BaseSource subclasses):
    setup(con, table)     — create the sqldim-managed target DDL
    teardown(con, table)  — drop the target table
    snapshot()            — SQLSource of full initial extract
    event_batch(n=1)      — SQLSource of next-day events
"""

from __future__ import annotations

import random

from sqldim.examples.datasets.base import (
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
                FieldSpec("user_id", "INTEGER", kind="randint", low=1, high=500),
                FieldSpec("device_id", "INTEGER", kind="randint", low=1, high=50),
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
                field="event_time",
                condition=lambda i, row: True,
                mutate=lambda old, row, fake: str(fake.date_time_this_year()),
            ),
        ],
    ),
)


# ── Source classes ────────────────────────────────────────────────────────────


@DatasetFactory.register("devices")
class DevicesSource(SchematicSource):
    """
    Synthetic device dimension source.

    Matches the ``Device`` SQLModel schema.  Use with ``DimensionalLoader``
    to populate the ``dim_device`` table.

    Example
    -------
    ::

        import duckdb
        from sqldim.examples.real_world.user_activity.dataset import DevicesSource

        con = duckdb.connect()
        src = DevicesSource(n=50, seed=0)
        src.setup(con, "dim_device")
        con.execute("INSERT INTO dim_device SELECT * FROM (" + src.snapshot().sql + ")")
    """

    _spec = _DEVICES_SPEC

    provider = SourceProvider(
        name="Analytics Event Stream",
        url="kafka://analytics-kafka/device-registry",
        description=(
            "Device registry enriched from the raw event stream. "
            "Real deployment would use a Kafka source or dlt pipeline."
        ),
    )


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
        from sqldim.examples.datasets.domains.user_activity.sources import EventsSource
        from sqldim.core.loaders.bitmask import LazyBitmaskLoader
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
