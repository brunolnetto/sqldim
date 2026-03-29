"""OLTP pipeline builder for the social_network domain.

There is no medallion layer — :func:`build_pipeline` populates the OLTP source
tables in the supplied DuckDB connection.  Graph analytics are expressed as
DGM queries over these tables rather than materialised graph tables.

Usage
-----
    import duckdb
    from sqldim.application.datasets.domains.social_network.pipeline.builder import build_pipeline

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    # OLTP tables: pessoa, livro, genero, leitura, conexao_social,
    #              livro_genero, pessoa_preferencia
"""

from __future__ import annotations

import duckdb

#: All tables created by :func:`build_pipeline`.
TABLE_NAMES: list[str] = [
    "pessoa",
    "livro",
    "genero",
    "leitura",
    "conexao_social",
    "livro_genero",
    "pessoa_preferencia",
]


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **social_network** OLTP tables in *con*."""
    from sqldim.application.datasets.domains.social_network.dataset import (
        social_network_dataset,
    )

    social_network_dataset.setup(con)


__all__ = ["build_pipeline", "TABLE_NAMES"]
