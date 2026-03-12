"""Tests for TraversalEngine SQL generation — Task 6.4."""
import pytest
from typing import Optional
from sqlmodel import Field

from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.graph.traversal import TraversalEngine, _build_filters


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class TPlayer(VertexModel, table=True):
    __tablename__ = "t_player"
    __vertex_type__ = "t_player"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class TGame(VertexModel, table=True):
    __tablename__ = "t_game"
    __vertex_type__ = "t_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_id: int


class DirectedEdge(EdgeModel, table=True):
    __tablename__ = "directed_edge"
    __edge_type__ = "directed"
    __subject__ = TPlayer
    __object__ = TGame
    __directed__ = True

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="t_player.id")
    object_id: int = Field(foreign_key="t_game.id")
    score: float = 0.0


class UndirectedEdge(EdgeModel, table=True):
    __tablename__ = "undirected_edge"
    __edge_type__ = "undirected"
    __subject__ = TPlayer
    __object__ = TPlayer
    __directed__ = False

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="t_player.id")
    object_id: int = Field(foreign_key="t_player.id")
    num_games: int = 0


@pytest.fixture
def engine():
    return TraversalEngine()


# ---------------------------------------------------------------------------
# neighbors_sql
# ---------------------------------------------------------------------------

def test_neighbors_sql_out(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="out")
    assert "object_id" in sql
    assert "subject_id = 1" in sql
    assert "directed_edge" in sql


def test_neighbors_sql_in(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="in")
    assert "subject_id" in sql
    assert "object_id = 1" in sql


def test_neighbors_sql_both(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="both")
    # Should produce a UNION of out + in
    assert "UNION" in sql.upper()
    assert "subject_id = 1" in sql
    assert "object_id = 1" in sql


def test_neighbors_sql_undirected_always_both(engine):
    # Undirected edges always produce symmetric UNION regardless of direction arg
    sql_out = engine.neighbors_sql(UndirectedEdge, start_id=2, direction="out")
    sql_in  = engine.neighbors_sql(UndirectedEdge, start_id=2, direction="in")
    for sql in (sql_out, sql_in):
        assert "UNION" in sql.upper()
        assert "subject_id = 2" in sql
        assert "object_id = 2" in sql


def test_neighbors_sql_with_filters(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="out", filters={"score": 30})
    assert "score = 30" in sql


def test_neighbors_sql_string_filter(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="out", filters={"team": "LAL"})
    assert "team = 'LAL'" in sql


def test_neighbors_sql_none_filter(engine):
    sql = engine.neighbors_sql(DirectedEdge, start_id=1, direction="out", filters={"pos": None})
    assert "IS NULL" in sql


# ---------------------------------------------------------------------------
# paths_sql
# ---------------------------------------------------------------------------

def test_paths_sql_contains_recursive(engine):
    sql = engine.paths_sql(DirectedEdge, start_id=1, target_id=5, max_hops=3)
    upper = sql.upper()
    assert "WITH RECURSIVE" in upper
    assert "UNION ALL" in upper


def test_paths_sql_cycle_prevention(engine):
    sql = engine.paths_sql(DirectedEdge, start_id=1, target_id=5, max_hops=3)
    assert "ANY(t.path)" in sql


def test_paths_sql_max_hops(engine):
    sql = engine.paths_sql(DirectedEdge, start_id=1, target_id=5, max_hops=2)
    assert "2" in sql


def test_paths_sql_target_in_where(engine):
    sql = engine.paths_sql(DirectedEdge, start_id=1, target_id=99, max_hops=3)
    assert "99" in sql


def test_paths_sql_undirected_symmetric_join(engine):
    sql = engine.paths_sql(UndirectedEdge, start_id=1, target_id=5, max_hops=3)
    # Undirected paths should join on both subject and object
    assert "subject_id = t.current_id OR e.object_id = t.current_id" in sql
    assert "CASE WHEN" in sql


# ---------------------------------------------------------------------------
# aggregate_sql
# ---------------------------------------------------------------------------

def test_aggregate_sql_sum(engine):
    sql = engine.aggregate_sql(DirectedEdge, start_id=1, measure="score", agg="sum", direction="out")
    assert "SUM(score)" in sql
    assert "subject_id = 1" in sql


def test_aggregate_sql_avg(engine):
    sql = engine.aggregate_sql(DirectedEdge, start_id=1, measure="score", agg="avg", direction="in")
    assert "AVG(score)" in sql
    assert "object_id = 1" in sql


def test_aggregate_sql_both_directions(engine):
    sql = engine.aggregate_sql(DirectedEdge, start_id=1, measure="score", agg="count", direction="both")
    assert "subject_id = 1 OR object_id = 1" in sql


# ---------------------------------------------------------------------------
# degree_sql
# ---------------------------------------------------------------------------

def test_degree_sql(engine):
    sql = engine.degree_sql(DirectedEdge, start_id=1, direction="out")
    assert "COUNT(*)" in sql
    assert "subject_id = 1" in sql


# ---------------------------------------------------------------------------
# _build_filters helper
# ---------------------------------------------------------------------------

def test_build_filters_multiple():
    result = _build_filters({"a": 1, "b": "x"})
    assert "a = 1" in result
    assert "b = 'x'" in result
    assert "AND" in result
