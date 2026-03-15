"""
test_coverage_100.py — closes the remaining 4% coverage gap.
Covers:
  - sqldim/core/mixins.py           (52, 72, 85-86, 90-91, 109)
  - sqldim/dimensions/date.py       (96-123)
  - sqldim/dimensions/junk.py       (38-64)
  - sqldim/dimensions/time.py       (68-90)
  - sqldim/graph/schema_graph.py    (21-22, 106, 108, 196, 206, 241)
  - sqldim/graph/traversal.py       (60-61, 178-180, 192-193, 203-204,
                                     215-216, 225-226, 242-244, 258-260)
  - sqldim/loaders/array_metric.py  (134-136)
  - sqldim/loaders/bitmask.py       (144-146)
  - sqldim/loaders/cumulative.py    (164, 178, 197-198, 213-216)
  - sqldim/narwhals/adapter.py      (39-41, 46-50)
  - sqldim/narwhals/scd_engine.py   (72)
  - sqldim/narwhals/sk_resolver.py  (138, 149-154, 159-161)
  - sqldim/narwhals/transforms.py   (66, 100-104, 107, 110, 158, 217)
  - sqldim/scd/handler.py           (47, 56, 63, 94-98, 141)
"""
import json
import sys
from datetime import date, datetime
from typing import Optional
from unittest.mock import MagicMock

import narwhals as nw
import pandas as pd
import polars as pl
import pytest
from sqlalchemy import JSON, Column
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select
from sqldim import (
    ArrayMetricLoader,
    BitmaskerLoader,
    CumulativeLoader,
    DimensionModel,
    EdgeModel,
    FactModel,
    Field,
    GraphSchemaGraph,
    NarwhalsHashStrategy,
    SCD2Mixin,
    SCD3Mixin,
    CumulativeMixin,
    DatelistMixin,
    SCDHandler,
    VertexModel,
    col,
    TransformPipeline,
)
from sqldim.graph.schema_graph import _safe_subclass
from sqldim.graph.traversal import DuckDBTraversalEngine, TraversalEngine
from sqldim.processors.adapter import _dicts_to_native
from sqldim.processors.sk_resolver import NarwhalsSKResolver
from sqldim.processors.transforms import _python_type_to_nw, _AppliedTransform


# ===========================================================================
# Section A — core/mixins.py
# ===========================================================================

class TestSCD3MixinLines:
    def test_non_scd3_subclass_returns_early_line52(self):
        """SCD3Mixin.__init_subclass__ returns early when __scd_type__ != 3 (line 52)."""
        class NonSCD3(SCD3Mixin, SQLModel):
            __scd_type__ = 2  # not 3 -> return at line 52
            x: str = ""
        # Just defining the class triggers __init_subclass__; no assertion needed
        assert getattr(NonSCD3, "__scd_type__", None) == 2

    def test_orphan_prev_column_raises_line72(self):
        """SCD3Mixin raises TypeError for prev_* without matching current column (line 72)."""
        with pytest.raises(TypeError, match="prev_ghost"):
            class BadSCD3(SCD3Mixin, SQLModel):
                __scd_type__ = 3
                prev_ghost: Optional[str] = None  # no 'ghost' column = orphan


class TestCumulativeMixinLines:
    def test_current_value_lines85_86(self):
        """CumulativeMixin.current_value() body (lines 85-86)."""
        class CumDim(CumulativeMixin, SQLModel):
            seasons: Optional[list] = None

        obj = CumDim()
        obj.seasons = [{"pts": 10}, {"pts": 20}]
        assert obj.current_value("seasons") == {"pts": 20}
        assert obj.current_value("nonexistent") is None

    def test_first_value_lines90_91(self):
        """CumulativeMixin.first_value() body (lines 90-91)."""
        class CumDim2(CumulativeMixin, SQLModel):
            seasons: Optional[list] = None

        obj = CumDim2()
        obj.seasons = [{"pts": 5}, {"pts": 15}]
        assert obj.first_value("seasons") == {"pts": 5}
        assert obj.first_value("empty_col") is None


class TestDatelistMixinLine109:
    def test_string_dates_converted_line109(self):
        """DatelistMixin.to_bitmask converts ISO string dates (line 109)."""
        class DL(DatelistMixin, SQLModel):
            dates_active: Optional[list] = None

        obj = DL()
        obj.dates_active = ["2024-01-01", "2024-01-03"]  # strings, not date objects
        mask = obj.to_bitmask(date(2024, 1, 5), window=10)
        assert mask > 0  # line 109: date.fromisoformat() was called


# ===========================================================================
# Section B — lazy dimension generation
# ===========================================================================

class _SimpleSink:
    """Minimal sink that writes a DuckDB VIEW to a Table and returns row count."""

    def write(self, con, view_name: str, table_name: str, batch_size: int = 100_000) -> int:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


class TestLazyDimensions:
    def test_date_dimension_generate_lazy(self):
        """DateDimension.generate_lazy() (lines 96-123 of date.py)."""
        import duckdb
        from sqldim.dimensions.date import DateDimension

        con = duckdb.connect()
        sink = _SimpleSink()
        n = DateDimension.generate_lazy(
            "2024-01-01", "2024-01-03", "cov100_date_dim", sink, con=con
        )
        assert n == 3
        con.close()

    def test_time_dimension_generate_lazy(self):
        """TimeDimension.generate_lazy() (lines 68-90 of time.py)."""
        import duckdb
        from sqldim.dimensions.time import TimeDimension

        con = duckdb.connect()
        sink = _SimpleSink()
        n = TimeDimension.generate_lazy("cov100_time_dim", sink, con=con)
        assert n == 1440
        con.close()

    def test_junk_dimension_populate_lazy(self):
        """populate_junk_dimension_lazy() (lines 38-64 of junk.py)."""
        import duckdb
        from sqldim.dimensions.junk import populate_junk_dimension_lazy

        con = duckdb.connect()
        sink = _SimpleSink()
        n = populate_junk_dimension_lazy(
            {"promo": [True, False], "channel": ["web", "store"]},
            "cov100_junk_dim",
            sink,
            con=con,
        )
        assert n == 4
        con.close()


# ===========================================================================
# Section C — graph/schema_graph.py
# ===========================================================================

# --- Models used across tests in this section (unique table names) ---

class Cov100Player(VertexModel, table=True):
    __tablename__ = "cov100_player"
    __natural_key__ = ["code"]
    __vertex_type__ = "cov100_player"
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    is_starter: bool = False  # bool field for line 206


class Cov100Game(VertexModel, table=True):
    __tablename__ = "cov100_game"
    __natural_key__ = ["game_code"]
    __vertex_type__ = "cov100_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_code: str


class Cov100ImplicitFact(FactModel, table=True):
    """Plain FactModel (NOT EdgeModel) with FK dimension metadata -> line 241."""
    __tablename__ = "cov100_implicit_fact"
    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: int = Field(foreign_key="cov100_player.id", dimension=Cov100Player)
    game_id: int = Field(foreign_key="cov100_game.id", dimension=Cov100Game)


class TestSafeSubclass:
    def test_non_type_base_raises_type_error_lines21_22(self):
        """_safe_subclass catches TypeError when base is not a class (lines 21-22)."""
        result = _safe_subclass(int, "not_a_class")
        assert result is False

    def test_non_type_cls_returns_false(self):
        """_safe_subclass returns False when cls is not a type."""
        result = _safe_subclass(None, int)
        assert result is False


class TestSchemaGraphFKAutoDerive:
    def test_graph_schema_auto_derives_subject_object_lines106_108(self):
        """graph_schema() auto-derives subject/object from FK metadata (lines 106, 108)."""
        sg = GraphSchemaGraph([Cov100Player, Cov100Game, Cov100ImplicitFact])
        schema = sg.graph_schema()
        edge_infos = schema.edges
        assert len(edge_infos) == 1
        ei = edge_infos[0]
        assert ei["subject"] is not None   # line 106 auto-derived subject
        assert ei["object"] is not None    # line 108 auto-derived object


class TestSchemaGraphToMermaid:
    def test_duplicate_model_skips_render_line196(self):
        """render_model() skips already-rendered models (line 196)."""
        # Pass Cov100Player twice -> second call hits early return
        sg = GraphSchemaGraph([Cov100Player, Cov100Player, Cov100Game])
        mermaid = sg.to_mermaid()
        # Only one Cov100Player block should appear (duplicate filtered)
        assert mermaid.count("Cov100Player {") == 1

    def test_bool_field_rendered_as_bool_line206(self):
        """Bool-typed fields render as 'bool' in Mermaid (line 206)."""
        sg = GraphSchemaGraph([Cov100Player])
        mermaid = sg.to_mermaid()
        assert "bool is_starter" in mermaid

    def test_implicit_fact_fk_rendered_line241(self):
        """Plain FactModel FK → dimension rendered in else branch (line 241)."""
        sg = GraphSchemaGraph([Cov100Player, Cov100Game, Cov100ImplicitFact])
        mermaid = sg.to_mermaid()
        # Implicit fact should have FK arrows to both player and game
        assert "Cov100ImplicitFact" in mermaid
        assert "Cov100Player" in mermaid


# ===========================================================================
# Section D — graph/traversal.py
# ===========================================================================

# --- Edge models for traversal ---

class Cov100TraversalEdge(EdgeModel, FactModel, table=True):
    __tablename__ = "cov100_traversal_edge"
    __subject__ = Cov100Player
    __object__ = Cov100Game
    __directed__ = True
    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="cov100_player.id")
    object_id: int = Field(foreign_key="cov100_game.id")
    weight: float = 1.0


class Cov100UndirectedEdge(EdgeModel, FactModel, table=True):
    __tablename__ = "cov100_undirected_edge"
    __subject__ = Cov100Player
    __object__ = Cov100Game
    __directed__ = False
    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="cov100_player.id")
    object_id: int = Field(foreign_key="cov100_game.id")
    weight: float = 1.0


@pytest.fixture
def ddb_traversal_con():
    """DuckDB connection with test edge data."""
    import duckdb
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE cov100_traversal_edge "
        "(id INT, subject_id INT, object_id INT, weight DOUBLE)"
    )
    con.execute(
        "INSERT INTO cov100_traversal_edge VALUES "
        "(1,1,2,1.0),(2,1,3,2.0),(3,2,3,1.5)"
    )
    con.execute(
        "CREATE TABLE cov100_undirected_edge "
        "(id INT, subject_id INT, object_id INT, weight DOUBLE)"
    )
    con.execute(
        "INSERT INTO cov100_undirected_edge VALUES "
        "(1,1,2,1.0),(2,2,3,2.0)"
    )
    yield con
    con.close()


class TestTraversalEngineSQL:
    def test_neighbors_sql_undirected_with_filters_lines60_61(self):
        """Undirected neighbors SQL with filters uses UNION (lines 60-61)."""
        engine = TraversalEngine()
        sql = engine.neighbors_sql(
            Cov100UndirectedEdge, 1, direction="both", filters={"weight": 1.0}
        )
        assert "UNION" in sql
        assert "weight = 1.0" in sql

    def test_aggregate_sql_undirected(self, ddb_traversal_con):
        """aggregate_sql with undirected edge uses OR clause."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        sql = engine.aggregate_sql(Cov100UndirectedEdge, 1, "weight", "sum")
        assert "OR object_id" in sql

    def test_degree_sql_lines203_204(self, ddb_traversal_con):
        """degree_sql delegates to aggregate_sql with COUNT (lines 203-204)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        sql = engine.degree_sql(Cov100TraversalEdge, 1, "out")
        assert "COUNT" in sql.upper()


class TestDuckDBTraversalEngine:
    def test_neighbors_lines192_193(self, ddb_traversal_con):
        """DuckDBTraversalEngine.neighbors() executes the query (lines 192-193)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        neighbors = engine.neighbors(Cov100TraversalEdge, 1, direction="out")
        assert isinstance(neighbors, list)
        assert len(neighbors) == 2  # vertex 1 has edges to 2 and 3

    def test_paths_lines178_180(self, ddb_traversal_con):
        """DuckDBTraversalEngine.paths() returns path lists (lines 178-180)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        paths = engine.paths(Cov100TraversalEdge, 1, 3, max_hops=3)
        assert isinstance(paths, list)
        # At least one path from 1 to 3 should exist (1->3 direct or 1->2->3)
        assert len(paths) >= 1

    def test_aggregate_lines215_216(self, ddb_traversal_con):
        """DuckDBTraversalEngine.aggregate() returns scalar (lines 215-216)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        total = engine.aggregate(Cov100TraversalEdge, 1, "weight", "sum")
        assert total == pytest.approx(3.0)  # edges (1,2) + (1,3): 1.0 + 2.0

    def test_degree_lines225_226(self, ddb_traversal_con):
        """DuckDBTraversalEngine.degree() returns edge count (lines 225-226)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        d = engine.degree(Cov100TraversalEdge, 1, "out")
        assert d == 2  # vertex 1 has out-edges to 2 and 3

    def test_register_neighbor_view_lines242_244(self, ddb_traversal_con):
        """register_neighbor_view() creates a DuckDB VIEW (lines 242-244)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        view = engine.register_neighbor_view(Cov100TraversalEdge, 1, "cov100_nbrs")
        assert view == "cov100_nbrs"
        rows = ddb_traversal_con.execute("SELECT * FROM cov100_nbrs").fetchall()
        assert len(rows) >= 1

    def test_register_paths_view_lines258_260(self, ddb_traversal_con):
        """register_paths_view() creates a DuckDB paths VIEW (lines 258-260)."""
        engine = DuckDBTraversalEngine(ddb_traversal_con)
        view = engine.register_paths_view(Cov100TraversalEdge, 1, 3, "cov100_paths")
        assert view == "cov100_paths"
        rows = ddb_traversal_con.execute("SELECT * FROM cov100_paths").fetchall()
        assert len(rows) >= 1


# ===========================================================================
# Section E — loaders (bitmask, array_metric, cumulative) polars branches
# ===========================================================================

class TestBitmaskLoaderPolars:
    def test_polars_branch_lines144_146(self):
        """BitmaskerLoader.process() polars else branch (lines 144-146)."""
        ref = date(2024, 1, 5)
        loader = BitmaskerLoader(
            None, None, None,
            reference_date=ref,
            window_days=10,
        )
        frame = nw.from_native(
            pl.DataFrame({"dates_active": [["2024-01-01", "2024-01-04"]]}),
            eager_only=True,
        )
        result = loader.process(frame)
        assert "datelist_int" in result.columns
        assert result["datelist_int"][0] > 0


class TestArrayMetricLoaderPolars:
    def test_polars_branch_lines134_136(self):
        """ArrayMetricLoader.process() polars else branch (lines 134-136)."""
        loader = ArrayMetricLoader(None, None, "sales_metric", date(2024, 1, 1))
        frame = nw.from_native(
            pl.DataFrame({"value": [5.0, 10.0]}),
            eager_only=True,
        )
        result = loader.process(frame, date(2024, 1, 3))
        assert "metric_array" in result.columns
        assert "metric_name" in result.columns


class TestCumulativeLoaderMissingLines:
    """Covers cumulative.py lines 164, 178, 197-198, 213-216."""

    def test_polars_branch_all_missing_lines(self):
        """Polars path covers lines 164, 178, 197-198, and 213-216.

        - Player 1: in yesterday (JSON string history) AND today → line 178 (json.loads)
          and active path
        - Player 2: in yesterday only, years_since=None → line 164 (_is_present(None)=False)
          and lines 197-198 (TypeError from int(None))
        Polars frame → lines 213-216 (polars branch)
        """
        loader = CumulativeLoader(
            model=None,
            session=None,
            partition_key="player_id",
            cumulative_column="seasons",
        )

        # y'd frame uses polars — null values become Python None in to_dicts()
        yesterday = pl.DataFrame({
            "player_id": [1, 2],
            "seasons": [
                json.dumps([{"pts": 10}]),  # player 1: JSON string -> line 178
                None,                        # player 2: no history
            ],
            "is_active": [True, False],
            "years_since_last_active": [0, None],  # player 2: None -> TypeError -> 197-198
            "current_season": ["2023", None],
        })
        # today only has player 1 — player 2 absent -> pts_today=None -> 164
        today = pl.DataFrame({"player_id": [1], "pts": [25.0]})

        result = loader.process(
            nw.from_native(yesterday, eager_only=True),
            nw.from_native(today, eager_only=True),
            "2024",
        )
        native = nw.to_native(result)
        assert len(native) == 2  # both players present in output


# ===========================================================================
# Section F — narwhals/adapter.py ImportError paths
# ===========================================================================

class TestAdapterImportError:
    def test_dicts_to_native_empty_no_polars_lines39_41(self, monkeypatch):
        """_dicts_to_native([]) falls back to pandas when polars unavailable (lines 39-41)."""
        real_polars = sys.modules.get("polars")
        monkeypatch.setitem(sys.modules, "polars", None)
        result = _dicts_to_native([])
        assert hasattr(result, "to_dict") or hasattr(result, "shape")
        # Restore polars so other tests aren't affected (monkeypatch handles this)

    def test_dicts_to_native_nonempty_no_polars_lines46_50(self, monkeypatch):
        """_dicts_to_native([...]) falls back to pandas when polars unavailable (lines 46-50)."""
        monkeypatch.setitem(sys.modules, "polars", None)
        result = _dicts_to_native([{"x": 1, "y": "hello"}])
        assert hasattr(result, "to_dict") or hasattr(result, "shape")


# ===========================================================================
# Section G — narwhals/scd_engine.py line 72
# ===========================================================================

class TestNarwhalsHashStrategyMissingLine:
    def test_pandas_object_dtype_handling_line72(self):
        """pandas dict-valued column has dtype=object; apply(json.dumps) called (line 72)."""
        strategy = NarwhalsHashStrategy(
            natural_key=["id"],
            track_columns=["id", "data"],
        )
        # dict value -> pandas stores as object dtype
        df = pd.DataFrame({"id": [1, 2], "data": [{"key": "a"}, {"key": "b"}]})
        assert df["data"].dtype == "object", "prerequisite: dict col must be object dtype"

        frame = nw.from_native(df, eager_only=True)
        result = strategy.compute_checksums(frame)
        assert "checksum" in result.columns
        checksums = nw.to_native(result)["checksum"].tolist()
        assert all(len(c) == 32 for c in checksums)  # MD5 hex = 32 chars


# ===========================================================================
# Section H — narwhals/sk_resolver.py
# ===========================================================================

# --- SCD2 dimension model for sk_resolver tests ---

class Cov100SCD2Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd2_dim_resolver"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


@pytest.fixture
def scd2_resolver_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


class TestNarwhalsSKResolverMissingLines:
    def test_scd2_is_current_filter_line138(self, scd2_resolver_session):
        """_load_lookup adds is_current filter when dimension has it and as_of=None (line 138)."""
        # Add a record
        record = Cov100SCD2Dim(code="ALPHA")
        scd2_resolver_session.add(record)
        scd2_resolver_session.commit()

        resolver = NarwhalsSKResolver(scd2_resolver_session)
        frame = nw.from_native(
            pl.DataFrame({"code": ["ALPHA"]}), eager_only=True
        )
        result = resolver.resolve(frame, Cov100SCD2Dim, "code", "dim_id")
        native = nw.to_native(result)
        assert native["dim_id"][0] is not None  # resolved

    def test_empty_records_polars_available_lines149_151(self, scd2_resolver_session):
        """Empty lookup returns polars-backed empty frame (lines 149-151)."""
        resolver = NarwhalsSKResolver(scd2_resolver_session)
        # Call _load_lookup directly to get the empty frame (no join type mismatch)
        result = resolver._load_lookup(Cov100SCD2Dim, None, "code")
        assert "code" in result.columns
        assert "id" in result.columns
        assert len(result) == 0

    def test_empty_records_no_polars_lines152_154(self, scd2_resolver_session, monkeypatch):
        """Empty lookup falls back to pandas when polars unavailable (lines 152-154)."""
        monkeypatch.setitem(sys.modules, "polars", None)
        resolver = NarwhalsSKResolver(scd2_resolver_session)
        # Call _load_lookup directly to get the empty pandas frame
        result = resolver._load_lookup(Cov100SCD2Dim, None, "code")
        assert "code" in result.columns
        assert len(result) == 0

    def test_nonempty_records_no_polars_lines159_161(self, scd2_resolver_session, monkeypatch):
        """Non-empty lookup falls back to pandas when polars unavailable (lines 159-161)."""
        record = Cov100SCD2Dim(code="BETA")
        scd2_resolver_session.add(record)
        scd2_resolver_session.commit()

        monkeypatch.setitem(sys.modules, "polars", None)
        resolver = NarwhalsSKResolver(scd2_resolver_session)
        # Call _load_lookup directly (resolve join would fail with mixed polars/pandas)
        result = resolver._load_lookup(Cov100SCD2Dim, None, "code")
        native = nw.to_native(result)
        assert len(native) >= 1
        assert native["code"][0] == "BETA"


# ===========================================================================
# Section I — narwhals/transforms.py
# ===========================================================================

class TestTransformsMissingLines:
    def test_python_type_to_nw_all_none_args_line66(self):
        """_python_type_to_nw returns None when all Union args are NoneType (line 66)."""
        import typing

        class AllNoneUnion:
            __origin__ = typing.Union
            __args__ = (type(None), type(None))

        result = _python_type_to_nw(AllNoneUnion)
        assert result is None

    def test_applied_transform_cast_known_type_lines100_102(self):
        """_AppliedTransform.cast() with known Python type (lines 100-102)."""
        import narwhals as nw
        t = col("x").str.lowercase()  # returns _AppliedTransform
        t2 = t.cast(str)              # _PYTHON_TO_NW has str -> nw.String
        df = pd.DataFrame({"x": ["hello", "WORLD"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_cast_unknown_type_lines103_104(self):
        """_AppliedTransform.cast() else branch with narwhals type (lines 103-104)."""
        import narwhals as nw
        t = col("x").str.lowercase()   # returns _AppliedTransform
        t2 = t.cast(nw.String)         # nw.String not in _PYTHON_TO_NW -> else branch
        df = pd.DataFrame({"x": ["hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_fill_null_line107(self):
        """_AppliedTransform.fill_null() body (line 107)."""
        import narwhals as nw
        t = col("x").str.lowercase()
        t2 = t.fill_null("default")
        df = pd.DataFrame({"x": [None, "hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_is_null_line110(self):
        """_AppliedTransform.is_null() body (line 110)."""
        import narwhals as nw
        t = col("x").str.lowercase()
        t2 = t.is_null()
        df = pd.DataFrame({"x": [None, "hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_col_transform_cast_narwhals_type_line158(self):
        """ColTransform.cast() else branch when dtype is narwhals type (line 158)."""
        import narwhals as nw
        t = col("x").cast(nw.String)  # nw.String not in _PYTHON_TO_NW -> else (line 158)
        df = pd.DataFrame({"x": [1, 2, 3]})
        frame = nw.from_native(df, eager_only=True)
        result = t.apply(frame)
        assert "x" in result.columns

    def test_validate_schema_skips_unknown_type_line217(self):
        """_validate_schema continues when model type not in _PYTHON_TO_NW (line 217)."""
        from datetime import datetime as dt

        class ModelWithDatetime:
            __annotations__ = {"created_at": dt}  # datetime not in _PYTHON_TO_NW

        pipeline = TransformPipeline(model=ModelWithDatetime)
        df = pd.DataFrame({"created_at": [dt.now()]})
        frame = nw.from_native(df, eager_only=True)
        result = pipeline.apply(frame)  # _validate_schema -> datetime -> None -> continue
        assert "created_at" in result.columns


# ===========================================================================
# Section J — scd/handler.py
# ===========================================================================

# --- Models for handler tests ---

class Cov100SCD1Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd1"
    __natural_key__ = ["code"]
    __scd_type__ = 1
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    city: str = "unknown"


class Cov100SCD6Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd6"
    __natural_key__ = ["code"]
    __scd_type__ = 6
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    city: str = "unknown"
    metadata_diff: Optional[str] = Field(default=None, nullable=True)


@pytest.fixture
def handler_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


class TestSCDHandlerMissingLines:
    def test_get_dim_meta_missing_field_line47(self, handler_session):
        """_get_dim_meta returns {} when field does not exist on model (line 47)."""
        handler = SCDHandler(Cov100SCD1Dim, handler_session, track_columns=["city"])
        result = handler._get_dim_meta("nonexistent_column")
        assert result == {}

    def test_get_dim_meta_no_get_method_line56(self, handler_session):
        """_get_dim_meta handles PydanticUndefined sa_column_kwargs (line 56)."""
        from pydantic_core import PydanticUndefined

        mock_field = MagicMock()
        mock_field.sa_column_kwargs = PydanticUndefined  # no .get() method
        mock_field.json_schema_extra = None

        mock_model = MagicMock()
        mock_model.model_fields = {"test_col": mock_field}

        handler = SCDHandler(mock_model, session=None, track_columns=[])
        result = handler._get_dim_meta("test_col")
        assert result == {}

    def test_get_dim_meta_json_schema_extra_dict_line63(self, handler_session):
        """_get_dim_meta returns json_schema_extra dict when info is empty (line 63)."""
        mock_field = MagicMock()
        mock_field.sa_column_kwargs = {}  # has .get(), returns None for "info"
        mock_field.json_schema_extra = {"dimension": "some_dim"}

        mock_model = MagicMock()
        mock_model.model_fields = {"fk_col": mock_field}

        handler = SCDHandler(mock_model, session=None, track_columns=[])
        result = handler._get_dim_meta("fk_col")
        assert result == {"dimension": "some_dim"}

    async def test_scd_type1_overwrite_lines94_98(self, handler_session):
        """SCD Type 1 overwrites existing row in place (lines 94-98)."""
        handler = SCDHandler(Cov100SCD1Dim, handler_session, track_columns=["city"])

        # First insert
        await handler.process([{"code": "T1", "city": "NYC"}])
        # Second call with changed city -> Type 1 overwrite
        await handler.process([{"code": "T1", "city": "LA"}])

        rows = handler_session.exec(select(Cov100SCD1Dim)).all()
        # Only one row should exist (Type 1 overwrites, no versioning)
        current_rows = [r for r in rows if r.code == "T1"]
        assert len(current_rows) == 1
        assert current_rows[0].city == "LA"

    async def test_scd6_metadata_diff_line141(self, handler_session):
        """SCD Type 6 Type 2 change sets metadata_diff when model has attribute (line 141)."""
        # Use a mock session to intercept the add() calls before DB write,
        # because SQLite can't store a raw dict value.
        added_objects = []
        mock_session = MagicMock()
        mock_session.exec.return_value.first.return_value = None  # first call: no existing
        mock_session.commit.return_value = None

        def capture_add(obj):
            added_objects.append(obj)

        mock_session.add.side_effect = capture_add

        handler = SCDHandler(Cov100SCD6Dim, mock_session, track_columns=["city"])

        # Simulate "first insert" producing an existing row
        existing = Cov100SCD6Dim(
            id=1, code="S6", city="Seattle", is_current=True, checksum="old_hash"
        )

        # Second call: mock session finds the existing record (different checksum)
        mock_session.exec.return_value.first.return_value = existing
        await handler.process([{"code": "S6", "city": "Portland"}])

        # The new row should have metadata_diff set (line 141)
        new_rows = [obj for obj in added_objects if getattr(obj, "code", None) == "S6"
                    and getattr(obj, "city", None) == "Portland"]
        assert len(new_rows) >= 1
        assert new_rows[0].metadata_diff is not None  # line 141 was reached
