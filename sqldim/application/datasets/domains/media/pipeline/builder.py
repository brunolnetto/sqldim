"""Flat OLTP pipeline builder for the media domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.media.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``actors``, ``movies``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['actors', 'movies']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **media** OLTP tables in *con*.

    Tables created: ``actors``, ``movies``.
    """
    from sqldim.application.datasets.domains.media.dataset import media_dataset

    media_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]
