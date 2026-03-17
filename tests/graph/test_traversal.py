"""Tests for TraversalEngine SQL generation — Task 6.4."""
import pytest
from typing import Optional
from sqlmodel import Field

from sqldim.core.graph import VertexModel, EdgeModel
from sqldim.core.graph.traversal import TraversalEngine, DuckDBTraversalEngine, _build_filters


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


# ---------------------------------------------------------------------------
# Migrated from test_coverage_100.py and test_coverage_gap_v2.py
# ---------------------------------------------------------------------------

class TravPlayer(VertexModel, table=True):
    __tablename__ = "trav_player"
    __natural_key__ = ["code"]
    __vertex_type__ = "trav_player"
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


class TravGame(VertexModel, table=True):
    __tablename__ = "trav_game"
    __natural_key__ = ["game_code"]
    __vertex_type__ = "trav_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_code: str


class TravDirectedEdge(EdgeModel, table=True):
    __tablename__ = "trav_directed_edge"
    __edge_type__ = "tde"
    __subject__ = TravPlayer
    __object__ = TravGame
    __directed__ = True
    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="trav_player.id")
    object_id: int = Field(foreign_key="trav_game.id")
    weight: float = 1.0


class TravUndirectedEdge(EdgeModel, table=True):
    __tablename__ = "trav_undirected_edge"
    __edge_type__ = "tue"
    __subject__ = TravPlayer
    __object__ = TravGame
    __directed__ = False
    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="trav_player.id")
    object_id: int = Field(foreign_key="trav_game.id")
    weight: float = 1.0


@pytest.fixture
def ddb_trav_con():
    """DuckDB connection with test edge data for DuckDBTraversalEngine."""
    import duckdb
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE trav_directed_edge "
        "(id INT, subject_id INT, object_id INT, weight DOUBLE)"
    )
    con.execute(
        "INSERT INTO trav_directed_edge VALUES "
        "(1,1,2,1.0),(2,1,3,2.0),(3,2,3,1.5)"
    )
    con.execute(
        "CREATE TABLE trav_undirected_edge "
        "(id INT, subject_id INT, object_id INT, weight DOUBLE)"
    )
    con.execute(
        "INSERT INTO trav_undirected_edge VALUES "
        "(1,1,2,1.0),(2,2,3,2.0)"
    )
    yield con
    con.close()


class TestTraversalEngineSQLExtended:
    def test_neighbors_sql_undirected_with_filters(self):
        """Undirected neighbors SQL with filters uses UNION."""
        te = TraversalEngine()
        sql = te.neighbors_sql(TravUndirectedEdge, 1, direction="both", filters={"weight": 1.0})
        assert "UNION" in sql
        assert "weight = 1.0" in sql

    def test_aggregate_sql_undirected(self, ddb_trav_con):
        """aggregate_sql with undirected edge uses OR clause."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        sql = te.aggregate_sql(TravUndirectedEdge, 1, "weight", "sum")
        assert "OR object_id" in sql

    def test_degree_sql_delegates_to_aggregate(self, ddb_trav_con):
        """degree_sql delegates to aggregate_sql with COUNT."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        sql = te.degree_sql(TravDirectedEdge, 1, "out")
        assert "COUNT" in sql.upper()


class TestDuckDBTraversalEngine:
    def test_neighbors_executes_query(self, ddb_trav_con):
        """DuckDBTraversalEngine.neighbors() executes the query."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        neighbors = te.neighbors(TravDirectedEdge, 1, direction="out")
        assert isinstance(neighbors, list)
        assert len(neighbors) == 2

    def test_paths_returns_path_lists(self, ddb_trav_con):
        """DuckDBTraversalEngine.paths() returns path lists."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        paths = te.paths(TravDirectedEdge, 1, 3, max_hops=3)
        assert isinstance(paths, list)
        assert len(paths) >= 1

    def test_aggregate_returns_scalar(self, ddb_trav_con):
        """DuckDBTraversalEngine.aggregate() returns a scalar value."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        total = te.aggregate(TravDirectedEdge, 1, "weight", "sum")
        assert total == pytest.approx(3.0)

    def test_degree_returns_edge_count(self, ddb_trav_con):
        """DuckDBTraversalEngine.degree() returns edge count."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        d = te.degree(TravDirectedEdge, 1, "out")
        assert d == 2

    def test_register_neighbor_view(self, ddb_trav_con):
        """register_neighbor_view() creates a DuckDB VIEW."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        view = te.register_neighbor_view(TravDirectedEdge, 1, "trav_nbrs_view")
        assert view == "trav_nbrs_view"
        rows = ddb_trav_con.execute("SELECT * FROM trav_nbrs_view").fetchall()
        assert len(rows) >= 1

    def test_register_paths_view(self, ddb_trav_con):
        """register_paths_view() creates a DuckDB paths VIEW."""
        te = DuckDBTraversalEngine(ddb_trav_con)
        view = te.register_paths_view(TravDirectedEdge, 1, 3, "trav_paths_view")
        assert view == "trav_paths_view"
        rows = ddb_trav_con.execute("SELECT * FROM trav_paths_view").fetchall()
        assert len(rows) >= 1


def test_traversal_engine_undirected_sql():
    """Undirected TraversalEngine generates UNION SQL."""
    class UEdge2(EdgeModel, table=True):
        __tablename__ = "uedge2"
        __edge_type__ = "ue2"
        __subject__ = TravPlayer
        __object__ = TravGame
        __directed__ = False
        id: int = Field(primary_key=True)

    te = TraversalEngine()
    sql = te.neighbors_sql(UEdge2, start_id=1, direction="out")
    assert "UNION" in sql
