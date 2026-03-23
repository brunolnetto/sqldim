"""
user_activity domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.user_activity.dataset import user_activity_dataset

    con = duckdb.connect()
    user_activity_dataset.setup(con)

    for table, rows in user_activity_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    user_activity_dataset.teardown(con)
"""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.user_activity.sources import (
    DevicesSource, EventsSource,
)

user_activity_dataset = Dataset(
    "user_activity",
    [
        (DevicesSource(), "devices"),
        (EventsSource(), "page_events"),
    ],
)

__all__ = ["user_activity_dataset"]
