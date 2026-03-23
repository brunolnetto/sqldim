"""DevicesSource — user_activity domain source."""

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
