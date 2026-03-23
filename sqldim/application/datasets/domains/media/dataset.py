"""
media domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.media.dataset import media_dataset

    con = duckdb.connect()
    media_dataset.setup(con)

    for table, rows in media_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    media_dataset.teardown(con)
"""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.media.sources import (
    MoviesSource,
)

media_dataset = Dataset(
    "media",
    [
        (MoviesSource(), "movies"),
    ],
)

__all__ = ["media_dataset"]
