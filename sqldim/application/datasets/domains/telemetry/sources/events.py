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
    seeded_datetime,
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

N_DEVICES = 50
"""Default device count a standalone ``EventsSource`` references."""


def _events_spec(n_devices: int) -> DatasetSpec:
    """The page-event spec whose ``device_id`` stays inside ``1..n_devices``."""
    if n_devices < 1:
        raise ValueError(f"events need at least one device (got n_devices={n_devices})")
    return DatasetSpec(
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
                    # FK into devices: 1..n_devices, the parent's key range.
                    FieldSpec(
                        "device_id", "INTEGER", kind="randint", low=1, high=n_devices
                    ),
                    FieldSpec("host", "VARCHAR", kind="choices", choices=_HOSTS),
                    FieldSpec(
                        "event_time",
                        "TIMESTAMP",
                        kind="datetime",
                        low=-365,
                        high=0,
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
                    mutate=lambda old, row, fake: str(seeded_datetime(-365, 0)),
                ),
            ],
        ),
    )


_EVENTS_SPEC = _events_spec(N_DEVICES)


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

    def __init__(
        self, n: int = 5, seed: int = 42, *, n_devices: int = N_DEVICES
    ) -> None:
        super().__init__(n, seed, spec=_events_spec(n_devices))
