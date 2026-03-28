"""Flat OLTP pipeline builder for the nba_analytics domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.nba_analytics.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``teams``, ``player_seasons``, ``games``, ``game_details``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['teams', 'player_seasons', 'games', 'game_details']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **nba_analytics** OLTP tables in *con*.

    Tables created: ``teams``, ``player_seasons``, ``games``, ``game_details``.
    """
    from sqldim.application.datasets.domains.nba_analytics.dataset import nba_analytics_dataset

    nba_analytics_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]
