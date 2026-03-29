"""Tests for the social_network domain pipeline builder.

Red-green: these tests are written first, then the domain implementation
makes them pass.  Each test asserts exactly one thing.
"""

from __future__ import annotations

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Fixture: build the pipeline once per module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def social_network_con() -> duckdb.DuckDBPyConnection:
    """Build the social_network pipeline in-memory and return the connection."""
    from sqldim.application.datasets.domains.social_network.pipeline.builder import (
        build_pipeline,
    )

    con = duckdb.connect(":memory:")
    build_pipeline(con)
    return con


def _table_names(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }


def _row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[index]


# ---------------------------------------------------------------------------
# OLTP table existence
# ---------------------------------------------------------------------------

_EXPECTED_OLTP = {
    "pessoa",
    "livro",
    "genero",
    "leitura",
    "conexao_social",
    "livro_genero",
    "pessoa_preferencia",
}


class TestSocialNetworkPipelineStructure:
    """Verify all expected tables are created."""

    def test_build_creates_oltp_tables(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        tables = _table_names(social_network_con)
        assert _EXPECTED_OLTP.issubset(tables)

    def test_no_graph_tables(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        tables = _table_names(social_network_con)
        assert "grafo_vertices" not in tables
        assert "grafo_arestas" not in tables


# ---------------------------------------------------------------------------
# OLTP row counts (match lecture data exactly)
# ---------------------------------------------------------------------------


class TestSocialNetworkRowCounts:
    """Each OLTP table must have the exact row count from the lecture data."""

    def test_pessoa_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "pessoa") == 10

    def test_livro_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "livro") == 8

    def test_genero_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "genero") == 7

    def test_leitura_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "leitura") == 15

    def test_conexao_social_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "conexao_social") == 16

    def test_livro_genero_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "livro_genero") == 11

    def test_pessoa_preferencia_row_count(self, social_network_con: duckdb.DuckDBPyConnection) -> None:
        assert _row_count(social_network_con, "pessoa_preferencia") == 14


# ---------------------------------------------------------------------------
# Dataset metadata discovery
# ---------------------------------------------------------------------------


class TestSocialNetworkDiscovery:
    """The domain must be discoverable by the ask CLI."""

    def test_dataset_metadata_discoverable(self) -> None:
        from sqldim.application.ask import KNOWN_DATASETS

        assert "social_network" in KNOWN_DATASETS
