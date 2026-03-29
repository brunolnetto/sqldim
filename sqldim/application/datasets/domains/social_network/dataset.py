"""
social_network domain Dataset — BookClub social reading network.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.social_network.dataset import social_network_dataset

    con = duckdb.connect()
    social_network_dataset.setup(con)
    # tables: pessoa, livro, genero, leitura, conexao_social,
    #         livro_genero, pessoa_preferencia, grafo_vertices, grafo_arestas
    social_network_dataset.teardown(con)
"""

from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.social_network.sources import (
    SocialNetworkSource,
)

social_network_dataset = Dataset(
    "social_network",
    [
        (SocialNetworkSource(), "social_network"),
    ],
)

DATASET_METADATA = {
    "name": "social_network",
    "title": "BookClub Social Network",
    "description": (
        "Social reading network — 10 readers, 8 books, 7 genres, "
        "social connections, reading reviews, and derived recommendation graph "
        "(25 vertices, 69 edges with 5 relation types)."
    ),
    "dataset_attr": "social_network_dataset",
}

__all__ = ["social_network_dataset", "DATASET_METADATA"]
