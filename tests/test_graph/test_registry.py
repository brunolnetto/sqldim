"""Tests for GraphModel registry — Task 6.3."""
import pytest
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock
from sqlmodel import Field

from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.graph.registry import GraphModel
from sqldim.exceptions import SchemaError


# ---------------------------------------------------------------------------
# Vertex / Edge fixtures
# ---------------------------------------------------------------------------

class RPlayer(VertexModel, table=True):
    __tablename__ = "r_player"
    __vertex_type__ = "r_player"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class RGame(VertexModel, table=True):
    __tablename__ = "r_game"
    __vertex_type__ = "r_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_id: int


class RPlaysIn(EdgeModel, table=True):
    __tablename__ = "r_plays_in"
    __edge_type__ = "r_plays_in"
    __subject__ = RPlayer
    __object__ = RGame

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="r_player.id")
    object_id: int = Field(foreign_key="r_game.id")
    pts: float = 0.0


class RSelfEdge(EdgeModel, table=True):
    __tablename__ = "r_self_edge"
    __edge_type__ = "r_plays_against"
    __subject__ = RPlayer
    __object__ = RPlayer
    __directed__ = False

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="r_player.id")
    object_id: int = Field(foreign_key="r_player.id")
    num_games: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(rows: list[dict]) -> Any:
    """Build a minimal async session mock that returns given rows."""
    session = AsyncMock()
    execute_result = MagicMock()
    # mappings().first() — for get_vertex
    execute_result.mappings.return_value.first.return_value = rows[0] if rows else None
    # fetchall() — for neighbors / paths / degree
    execute_result.fetchall.return_value = [(r.get("id", r.get("neighbor_id", 0)),) for r in rows]
    execute_result.fetchone.return_value = (len(rows),)
    # mappings().fetchall() — for hydrating neighbor vertices
    execute_result.mappings.return_value.fetchall.return_value = rows
    session.execute.return_value = execute_result
    return session


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_graph_model_registers_vertices():
    session = _make_session([])
    g = GraphModel(RPlayer, RGame, session=session)
    assert RPlayer in g.vertex_models
    assert RGame in g.vertex_models


def test_graph_model_registers_edges():
    session = _make_session([])
    g = GraphModel(RPlayer, RGame, RPlaysIn, session=session)
    assert RPlaysIn in g.edge_models


def test_graph_model_rejects_non_graph_models():
    from sqldim.core.models import DimensionModel

    class Plain(DimensionModel, table=True):
        __tablename__ = "plain_dim"
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str

    with pytest.raises(SchemaError):
        GraphModel(Plain, session=_make_session([]))


# ---------------------------------------------------------------------------
# get_vertex
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_vertex_returns_instance():
    row = {"id": 1, "name": "Jordan"}
    session = _make_session([row])
    g = GraphModel(RPlayer, session=session)
    v = await g.get_vertex(RPlayer, id=1)
    assert v is not None
    assert v.id == 1
    assert v.name == "Jordan"


@pytest.mark.asyncio
async def test_get_vertex_not_found():
    session = _make_session([])
    # mappings().first() returns None for missing
    execute_result = MagicMock()
    execute_result.mappings.return_value.first.return_value = None
    session = AsyncMock()
    session.execute.return_value = execute_result

    g = GraphModel(RPlayer, session=session)
    v = await g.get_vertex(RPlayer, id=999)
    assert v is None


@pytest.mark.asyncio
async def test_get_vertex_unregistered_raises():
    session = _make_session([])
    g = GraphModel(RGame, session=session)
    with pytest.raises(SchemaError):
        await g.get_vertex(RPlayer, id=1)


# ---------------------------------------------------------------------------
# neighbors
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_neighbors_returns_vertex_instances():
    neighbor_rows = [{"id": 5, "game_id": 100}]
    session = AsyncMock()
    # First execute: get neighbor IDs → fetchall returns [(5,)]
    first_result = MagicMock()
    first_result.fetchall.return_value = [(5,)]
    # Second execute: hydrate neighbors → mappings().fetchall()
    second_result = MagicMock()
    second_result.mappings.return_value.fetchall.return_value = neighbor_rows
    session.execute.side_effect = [first_result, second_result]

    g = GraphModel(RPlayer, RGame, RPlaysIn, session=session)
    player = RPlayer(id=1, name="Jordan")
    neighbors = await g.neighbors(player, edge_type=RPlaysIn, direction="out")
    assert len(neighbors) == 1
    assert isinstance(neighbors[0], RGame)


@pytest.mark.asyncio
async def test_neighbors_empty_result():
    session = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = []
    session.execute.return_value = result

    g = GraphModel(RPlayer, RGame, RPlaysIn, session=session)
    player = RPlayer(id=1, name="Jordan")
    neighbors = await g.neighbors(player, edge_type=RPlaysIn)
    assert neighbors == []


# ---------------------------------------------------------------------------
# neighbor_aggregation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_neighbor_aggregation_returns_float():
    session = AsyncMock()
    result = MagicMock()
    result.fetchone.return_value = (150.5,)
    session.execute.return_value = result

    g = GraphModel(RPlayer, RGame, RPlaysIn, session=session)
    player = RPlayer(id=1, name="Jordan")
    total = await g.neighbor_aggregation(player, edge_type=RPlaysIn, measure="pts", agg="sum")
    assert total == 150.5


@pytest.mark.asyncio
async def test_neighbor_aggregation_null_returns_zero():
    session = AsyncMock()
    result = MagicMock()
    result.fetchone.return_value = (None,)
    session.execute.return_value = result

    g = GraphModel(RPlayer, RGame, RPlaysIn, session=session)
    player = RPlayer(id=1, name="Jordan")
    total = await g.neighbor_aggregation(player, edge_type=RPlaysIn, measure="pts")
    assert total == 0.0


# ---------------------------------------------------------------------------
# degree
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_degree_returns_int():
    session = AsyncMock()
    result = MagicMock()
    result.fetchone.return_value = (7,)
    session.execute.return_value = result

    g = GraphModel(RPlayer, RSelfEdge, session=session)
    player = RPlayer(id=1, name="Jordan")
    d = await g.degree(player, edge_type=RSelfEdge)
    assert d == 7


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

def test_explain_neighbors():
    g = GraphModel(RPlayer, RGame, RPlaysIn, session=AsyncMock())
    sql = g.explain("neighbors", edge_model=RPlaysIn, start_id=1, direction="out")
    assert "r_plays_in" in sql
    assert isinstance(sql, str)


def test_explain_paths():
    g = GraphModel(RPlayer, RGame, RPlaysIn, session=AsyncMock())
    sql = g.explain("paths", edge_model=RPlaysIn, start_id=1, target_id=5, max_hops=2)
    assert "WITH RECURSIVE" in sql.upper()


def test_explain_unknown_operation_raises():
    g = GraphModel(RPlayer, session=AsyncMock())
    with pytest.raises(SchemaError):
        g.explain("nonexistent_op")
