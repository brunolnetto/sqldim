"""The DGM model the entertainment dataset declares (§2.6).

Written from ``sources/``.  Two parts, unrelated to each other:

* films: ``actors`` is a dimension.  The table ``movies`` holds the cast: one
  row per actor per film, naming the actor (``actor_id``).  No film table
  exists, so ``movie_id``, ``title`` and ``released_at`` are properties of the
  credit, as is the copied ``actor_name``.  A credit carries no measure, so it
  is a factless fact;
* the BookClub network: ``pessoa``, ``livro`` and ``genero`` are dimensions.
  Each ``leitura`` row is one reading, keyed by reader and book, with a score
  (``nota``) and a date.  ``conexao_social`` relates a follower to the person
  followed with a strength: an N:N bridge edge type weighted by
  ``forca_conexao``.  ``livro_genero`` and ``pessoa_preferencia`` are N:N
  bridge tables from a book or a person to a genre.
  ``conexao_social.data_conexao`` is not mapped: the fixture never sets it,
  so every row holds the ``CURRENT_DATE`` of the load, not a date of the
  connection.

Grain (§2.4): a credit and a reading are events.  Following someone and
belonging to or liking a genre carry no order in time between their ends:
STRUCTURAL (§2.3).  No dimension holds verb edge types into two fact types,
so nothing is Conformed.
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    DATE,
    INTEGER,
    REAL,
    STRING,
    bridge_table,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import (
    BridgeSemantics,
    BridgeSemanticsKind,
    FactlessFact,
    Grain,
    GrainKind,
)
from sqldim.core.query.dgm.model import DGMModel, EdgeType, NodeKind, NodeType

__all__ = ["build_model"]

_STRUCTURAL = BridgeSemanticsKind.STRUCTURAL


def _films() -> tuple[NodeType, ...]:
    return (
        table_node("Actor", NodeKind.DIM, "actors", ("id",), {"name": STRING}),
        table_node(
            "CastCredit",
            NodeKind.FACT,
            "movies",
            ("movie_id", "actor_id"),
            {
                "actor_name": STRING,
                "movie_id": INTEGER,
                "title": STRING,
                "released_at": DATE,
            },
        ),
    )


def _book_club() -> tuple[NodeType, ...]:
    return (
        table_node(
            "Pessoa",
            NodeKind.DIM,
            "pessoa",
            ("pessoa_id",),
            {"nome": STRING, "idade": INTEGER},
        ),
        table_node(
            "Livro",
            NodeKind.DIM,
            "livro",
            ("livro_id",),
            {"titulo": STRING, "autor": STRING, "ano_publicacao": INTEGER},
        ),
        table_node("Genero", NodeKind.DIM, "genero", ("genero_id",), {"nome": STRING}),
        table_node(
            "Leitura",
            NodeKind.FACT,
            "leitura",
            ("pessoa_id", "livro_id"),
            {"nota": REAL, "data_leitura": DATE},
            event_time="data_leitura",
        ),
    )


def _edges() -> tuple[EdgeType, ...]:
    return (
        verb_fk("Actor", "CastCredit", "credited_in", "actor_id"),
        verb_fk("Pessoa", "Leitura", "leu", "pessoa_id"),
        verb_fk("Livro", "Leitura", "lido_em", "livro_id"),
        bridge_table(
            ("Pessoa", "seguidor_id"),
            ("Pessoa", "seguido_id"),
            "segue",
            "conexao_social",
            weight="forca_conexao",
        ),
        bridge_table(
            ("Livro", "livro_id"),
            ("Genero", "genero_id"),
            "pertence_ao_genero",
            "livro_genero",
        ),
        bridge_table(
            ("Pessoa", "pessoa_id"),
            ("Genero", "genero_id"),
            "interesse_em",
            "pessoa_preferencia",
        ),
    )


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(*_films(), *_book_club()),
        edge_types=_edges(),
        sigma=(
            Grain("CastCredit", GrainKind.EVENT),
            Grain("Leitura", GrainKind.EVENT),
            FactlessFact("CastCredit"),
            BridgeSemantics("segue", _STRUCTURAL),
            BridgeSemantics("pertence_ao_genero", _STRUCTURAL),
            BridgeSemantics("interesse_em", _STRUCTURAL),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )
