"""entertainment pipeline builder — films/cast + BookClub social network."""

from __future__ import annotations

import duckdb

#: Film cast tables.
MEDIA_TABLE_NAMES: list[str] = ["actors", "movies"]

#: BookClub social reading network tables.
SOCIAL_TABLE_NAMES: list[str] = [
    "pessoa",
    "livro",
    "genero",
    "leitura",
    "conexao_social",
    "livro_genero",
    "pessoa_preferencia",
]

#: All tables for the entertainment domain.
TABLE_NAMES: list[str] = MEDIA_TABLE_NAMES + SOCIAL_TABLE_NAMES


def build_pipeline(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
    """Populate the **entertainment** tables in *con*.

    Tables created: ``actors``, ``movies`` (film cast) and
    ``pessoa``, ``livro``, ``genero``, ``leitura``, ``conexao_social``,
    ``livro_genero``, ``pessoa_preferencia`` (BookClub social network).
    """
    from sqldim.application.datasets.domains.entertainment.dataset import (
        entertainment_dataset,
    )

    entertainment_dataset.setup(con)
