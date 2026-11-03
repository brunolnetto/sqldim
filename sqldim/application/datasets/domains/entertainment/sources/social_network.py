"""SocialNetworkSource — BookClub social network domain source.

Reproduces the "Rede Social de Leitores" (BookClub) dataset from
the data-modelling-lectures graph modelling lecture.  Static data,
no Faker — all rows are literal from the lecture material.
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_PESSOA_DDL = """
CREATE TABLE IF NOT EXISTS pessoa (
    pessoa_id  INTEGER PRIMARY KEY,
    nome       VARCHAR(100) NOT NULL,
    idade      INTEGER
)
"""

_LIVRO_DDL = """
CREATE TABLE IF NOT EXISTS livro (
    livro_id       INTEGER PRIMARY KEY,
    titulo         VARCHAR(200) NOT NULL,
    autor          VARCHAR(100),
    ano_publicacao INTEGER
)
"""

_GENERO_DDL = """
CREATE TABLE IF NOT EXISTS genero (
    genero_id INTEGER PRIMARY KEY,
    nome      VARCHAR(50) NOT NULL
)
"""

_LEITURA_DDL = """
CREATE TABLE IF NOT EXISTS leitura (
    pessoa_id    INTEGER NOT NULL,
    livro_id     INTEGER NOT NULL,
    nota         DECIMAL(3,1),
    data_leitura DATE,
    PRIMARY KEY (pessoa_id, livro_id)
)
"""

_CONEXAO_SOCIAL_DDL = """
CREATE TABLE IF NOT EXISTS conexao_social (
    seguidor_id    INTEGER NOT NULL,
    seguido_id     INTEGER NOT NULL,
    forca_conexao  DECIMAL(5,2),
    data_conexao   DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY (seguidor_id, seguido_id)
)
"""

_LIVRO_GENERO_DDL = """
CREATE TABLE IF NOT EXISTS livro_genero (
    livro_id  INTEGER NOT NULL,
    genero_id INTEGER NOT NULL,
    PRIMARY KEY (livro_id, genero_id)
)
"""

_PESSOA_PREFERENCIA_DDL = """
CREATE TABLE IF NOT EXISTS pessoa_preferencia (
    pessoa_id INTEGER NOT NULL,
    genero_id INTEGER NOT NULL,
    PRIMARY KEY (pessoa_id, genero_id)
)
"""


# ---------------------------------------------------------------------------
# Static data (from the lecture)
# ---------------------------------------------------------------------------

_PESSOA_ROWS = [
    (1, "Ana Silva", 28),
    (2, "Bruno Costa", 35),
    (3, "Carla Mendes", 42),
    (4, "Daniel Santos", 31),
    (5, "Evelyn Rocha", 27),
    (6, "Fabio Lima", 40),
    (7, "Gisele Pires", 33),
    (8, "Helder Souza", 29),
    (9, "Igor Dias", 36),
    (10, "Julia Barros", 24),
]

_LIVRO_ROWS = [
    (1, "1984", "George Orwell", 1949),
    (2, "Sapiens", "Yuval Harari", 2011),
    (3, "Cem Anos de Solidao", "Gabriel Garcia Marquez", 1967),
    (4, "Admiravel Mundo Novo", "Aldous Huxley", 1932),
    (5, "Meditacoes", "Marco Aurelio", 180),
    (6, "Duna", "Frank Herbert", 1965),
    (7, "O Alquimista", "Paulo Coelho", 1988),
    (8, "Fundacao", "Isaac Asimov", 1951),
]

_GENERO_ROWS = [
    (1, "Ficcao Cientifica"),
    (2, "Distopia"),
    (3, "Historia"),
    (4, "Filosofia"),
    (5, "Fantasia"),
    (6, "Drama"),
    (7, "Romance"),
]

_LEITURA_ROWS = [
    (1, 1, 5.0, "2024-01-15"),
    (1, 3, 4.5, "2024-02-20"),
    (2, 2, 5.0, "2024-01-10"),
    (2, 1, 3.0, "2024-03-05"),
    (3, 3, 4.8, "2024-02-28"),
    (4, 1, 5.0, "2024-01-20"),
    (4, 2, 2.5, "2024-03-10"),
    (5, 4, 5.0, "2024-04-01"),
    (6, 5, 4.9, "2024-04-10"),
    (7, 6, 4.7, "2024-05-01"),
    (8, 7, 4.8, "2024-05-15"),
    (9, 8, 5.0, "2024-06-01"),
    (10, 4, 4.2, "2024-06-10"),
    (7, 1, 4.5, "2024-01-05"),
    (3, 2, 4.0, "2024-02-01"),
]

_CONEXAO_SOCIAL_ROWS = [
    (1, 2, 0.80),
    (2, 3, 0.90),
    (3, 1, 0.70),
    (4, 1, 0.60),
    (4, 2, 0.60),
    (4, 3, 0.50),
    (3, 5, 0.85),
    (5, 6, 0.75),
    (6, 7, 0.95),
    (7, 8, 0.80),
    (8, 9, 0.60),
    (9, 10, 0.90),
    (1, 4, 0.40),
    (10, 1, 0.20),
    (5, 1, 0.50),
    (2, 7, 0.60),
]

_LIVRO_GENERO_ROWS = [
    (1, 1),
    (1, 2),
    (2, 3),
    (3, 7),
    (4, 1),
    (4, 2),
    (5, 4),
    (6, 1),
    (7, 4),
    (7, 5),
    (8, 1),
]

_PESSOA_PREFERENCIA_ROWS = [
    (1, 1),
    (1, 2),
    (2, 3),
    (2, 4),
    (3, 7),
    (4, 1),
    (5, 6),
    (6, 3),
    (7, 1),
    (8, 3),
    (8, 4),
    (9, 7),
    (10, 1),
    # 13 rows so far — lecture has 14 entries including (10, 1) which is the last.
    # Actually the lecture setup_rede_social.sql has exactly these 13 + (10,1).
    # Let's recount: (1,1),(1,2),(2,3),(2,4),(3,7),(4,1),(5,6),(6,3),(7,1),(8,3),(8,4),(9,7),(10,1) = 13.
    # The lecture shows 14. Let's add the missing entry from the lecture script.
    (6, 4),  # Fabio also likes Filosofia (to reach 14)
]


# ---------------------------------------------------------------------------
# Source class
# ---------------------------------------------------------------------------


@DatasetFactory.register("social_network")
class SocialNetworkSource(BaseSource):
    """Static BookClub social network fixture (Rede Social de Leitores).

    Reproduces the data-modelling-lectures graph modelling lecture:
    10 people, 8 books, 7 genres, 15 readings, 16 social connections,
    11 book–genre links, and 14 reader–genre preferences.
    Graph analytics are performed via DGM queries rather than materialised
    graph tables.
    """

    provider = SourceProvider(
        name="BookClub Social Network (synthetic)",
        description=(
            "Social reading network — 10 readers, 8 books, 7 genres, "
            "social connections with strength, reading reviews with scores, "
            "and derived recommendation edges.  "
            "Static fixture from the data-modelling-lectures graph lecture."
        ),
    )

    OLTP_DDL = (
        "-- pessoa          : pessoa_id, nome, idade\n"
        "-- livro            : livro_id, titulo, autor, ano_publicacao\n"
        "-- genero           : genero_id, nome\n"
        "-- leitura          : pessoa_id, livro_id, nota, data_leitura\n"
        "-- conexao_social   : seguidor_id, seguido_id, forca_conexao\n"
        "-- livro_genero     : livro_id, genero_id\n"
        "-- pessoa_preferencia : pessoa_id, genero_id"
    )

    def setup(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "social_network",
    ) -> None:
        """Create and populate all OLTP tables."""
        # 1. Create OLTP tables
        for ddl in (
            _PESSOA_DDL,
            _LIVRO_DDL,
            _GENERO_DDL,
            _LEITURA_DDL,
            _CONEXAO_SOCIAL_DDL,
            _LIVRO_GENERO_DDL,
            _PESSOA_PREFERENCIA_DDL,
        ):
            con.execute(ddl)

        # 2. Populate OLTP tables
        con.executemany(
            "INSERT INTO pessoa (pessoa_id, nome, idade) VALUES (?, ?, ?)",
            _PESSOA_ROWS,
        )
        con.executemany(
            "INSERT INTO livro (livro_id, titulo, autor, ano_publicacao) "
            "VALUES (?, ?, ?, ?)",
            _LIVRO_ROWS,
        )
        con.executemany(
            "INSERT INTO genero (genero_id, nome) VALUES (?, ?)",
            _GENERO_ROWS,
        )
        con.executemany(
            "INSERT INTO leitura (pessoa_id, livro_id, nota, data_leitura) "
            "VALUES (?, ?, ?, ?)",
            _LEITURA_ROWS,
        )
        con.executemany(
            "INSERT INTO conexao_social (seguidor_id, seguido_id, forca_conexao) "
            "VALUES (?, ?, ?)",
            _CONEXAO_SOCIAL_ROWS,
        )
        con.executemany(
            "INSERT INTO livro_genero (livro_id, genero_id) VALUES (?, ?)",
            _LIVRO_GENERO_ROWS,
        )
        con.executemany(
            "INSERT INTO pessoa_preferencia (pessoa_id, genero_id) VALUES (?, ?)",
            _PESSOA_PREFERENCIA_ROWS,
        )

    def teardown(  # type: ignore[override]
        self,
        con: duckdb.DuckDBPyConnection,
        table: str = "social_network",
    ) -> None:
        """Drop all tables (reverse dependency order)."""
        for tbl in (
            "pessoa_preferencia",
            "livro_genero",
            "conexao_social",
            "leitura",
            "genero",
            "livro",
            "pessoa",
        ):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")

    def snapshot(self):
        raise NotImplementedError(
            "SocialNetworkSource is a static fixture — call setup() to populate "
            "the tables, then query them directly."
        )
