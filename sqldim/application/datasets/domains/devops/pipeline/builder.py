"""Flat OLTP pipeline builder for the devops domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables directly in the supplied DuckDB connection.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.devops.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # tables now available: ``github_issues``
"""

from __future__ import annotations

import duckdb

#: OLTP tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = ['github_issues']


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **devops** OLTP tables in *con*.

    Tables created: ``github_issues``.
    """
    from sqldim.application.datasets.domains.devops.dataset import devops_dataset

    devops_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]
