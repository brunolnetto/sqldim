"""Tests for GraphModel registry — Task 6.3."""
import pytest
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.pool import StaticPool
from sqlmodel import Field, Session, SQLModel, create_engine

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


# ---------------------------------------------------------------------------
# Models and fixture for tests needing a real SQLite session
# ---------------------------------------------------------------------------

class RegGVertex(VertexModel, table=True):
    __tablename__ = "reg_gvertex"
    __vertex_type__ = "reg_gv"
    id: int = Field(primary_key=True)
    name: str = "test"


class RegHeteroVertex(VertexModel, table=True):
    __tablename__ = "reg_hetero_vertex"
    __vertex_type__ = "reg_hv"
    id: int = Field(primary_key=True)


class RegGEdge(EdgeModel, table=True):
    __tablename__ = "reg_gedge"
    __edge_type__ = "reg_ge"
    __subject__ = RegGVertex
    __object__ = RegGVertex
    id: int = Field(primary_key=True)
    subject_id: int = Field(foreign_key="reg_gvertex.id")
    object_id: int = Field(foreign_key="reg_gvertex.id")


@pytest.fixture
def sqlite_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


# ---------------------------------------------------------------------------
# Multi-edge / no-edge error branches
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_graph_registry_multi_edge_error(sqlite_session):
    class ExtraEdge(EdgeModel, table=True):
        __tablename__ = "reg_extra_edge"
        __edge_type__ = "reg_ee"
        __subject__ = RegGVertex
        __object__ = RegGVertex
        id: int = Field(primary_key=True)
        subject_id: int = Field(foreign_key="reg_gvertex.id")
        object_id: int = Field(foreign_key="reg_gvertex.id")

    graph = GraphModel(RegGVertex, RegGEdge, ExtraEdge, session=sqlite_session)
    v = RegGVertex(id=1)
    with pytest.raises(SchemaError, match="Multiple edge types connect"):
        await graph.neighbors(v)


@pytest.mark.asyncio
async def test_graph_registry_no_edge_error(sqlite_session):
    graph = GraphModel(RegGVertex, session=sqlite_session)
    v = RegGVertex(id=1)
    with pytest.raises(SchemaError, match="No edge type registered"):
        await graph.neighbors(v)


def test_pick_neighbor_class_hetero():
    cls = GraphModel._pick_neighbor_class(RegGVertex, RegGVertex, RegHeteroVertex, "both")
    assert cls == RegHeteroVertex
    cls2 = GraphModel._pick_neighbor_class(RegHeteroVertex, RegGVertex, RegHeteroVertex, "both")
    assert cls2 == RegGVertex


# ---------------------------------------------------------------------------
# _assert_edge_registered error branch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_graph_registry_assert_edge_unregistered(sqlite_session):
    class GVUnique(VertexModel, table=True):
        __tablename__ = "reg_gvu"
        __vertex_type__ = "reg_gvu"
        id: int = Field(primary_key=True)

    class GEUnique(EdgeModel, table=True):
        __tablename__ = "reg_geu"
        __edge_type__ = "reg_geu"
        __subject__ = GVUnique
        __object__ = GVUnique
        id: int = Field(primary_key=True)
        subject_id: int = Field(foreign_key="reg_gvu.id")
        object_id: int = Field(foreign_key="reg_gvu.id")

    class Unregistered(EdgeModel, table=True):
        __tablename__ = "reg_unregistered"
        __edge_type__ = "reg_unregistered"
        __subject__ = GVUnique
        __object__ = GVUnique
        id: int = Field(primary_key=True)
        subject_id: int = Field(foreign_key="reg_gvu.id")
        object_id: int = Field(foreign_key="reg_gvu.id")

    graph = GraphModel(GVUnique, GEUnique, session=sqlite_session)
    with pytest.raises(SchemaError, match="not registered in this GraphModel"):
        graph._assert_edge_registered(Unregistered)

    cls = graph._pick_neighbor_class(GVUnique, GVUnique, GVUnique, "both")
    assert cls == GVUnique


# ---------------------------------------------------------------------------
# _pick_neighbor_class direction branches
# ---------------------------------------------------------------------------

def test_pick_neighbor_class_in_direction():
    cls_in = GraphModel._pick_neighbor_class(RegGVertex, RegGVertex, RegGVertex, "in")
    assert cls_in == RegGVertex

    cls_self = GraphModel._pick_neighbor_class(RegGVertex, RegGVertex, RegGVertex, "both")
    assert cls_self == RegGVertex


# ---------------------------------------------------------------------------
# paths() with mocked async session
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_graph_paths_execution():
    class PVertex(VertexModel, table=True):
        __tablename__ = "reg_pv"
        __vertex_type__ = "reg_pv"
        id: int = Field(primary_key=True)

    class PEdge(EdgeModel, table=True):
        __tablename__ = "reg_pe"
        __edge_type__ = "reg_pe"
        __subject__ = PVertex
        __object__ = PVertex
        id: int = Field(primary_key=True)
        subject_id: int = Field(foreign_key="reg_pv.id")
        object_id: int = Field(foreign_key="reg_pv.id")

    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [([1, 2, 3],)]
    mock_session.execute.return_value = mock_result

    graph = GraphModel(PVertex, PEdge, session=mock_session)
    v1, v2 = PVertex(id=1), PVertex(id=3)
    paths = await graph.paths(v1, v2)
    assert paths == [[1, 2, 3]]
