import pytest
import pandas as pd
import polars as pl
import narwhals as nw
from datetime import date, datetime, timezone
from sqlmodel import Field, Session, SQLModel, create_engine, select, text
from typing import Optional, List, Any, Union

from sqldim import DimensionModel, FactModel, SCD2Mixin, DatelistMixin
from sqldim.narwhals.sk_resolver import NarwhalsSKResolver
from sqldim.scd.backfill import backfill_scd2
from sqldim.graph.registry import GraphModel
from sqldim.graph.schema_graph import SchemaGraph, _safe_subclass
from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.exceptions import SchemaError, TransformTypeError, LoadError
from sqldim.narwhals.transforms import _python_type_to_nw, TransformPipeline, col, Transform
from sqldim.narwhals.scd_engine import NarwhalsSCDProcessor

# --- Fixtures ---

class CoverageActivityUser(DatelistMixin, SQLModel):
    dates_active: List[date] = []

class CoverageDimMember(DimensionModel, table=True):
    __tablename__ = "cov_dim_member"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    name: str

class CoverageFactSale(FactModel, table=True):
    __tablename__ = "cov_fact_sale"
    id: Optional[int] = Field(default=None, primary_key=True)
    member_code: str = Field(sa_column_kwargs={"info": {"dimension": CoverageDimMember}})

@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

# --- Task: sqldim/core/mixins.py (Bitmask logic) ---

def test_datelist_mixin_coverage():
    user = CoverageActivityUser(dates_active=[date(2024, 1, 1), date(2024, 1, 5)])
    ref = date(2024, 1, 7)
    
    mask = user.to_bitmask(ref, window=10)
    assert bin(mask).count("1") == 2
    
    assert user.l7(ref) == 2
    assert user.l28(ref) == 2
    assert user.activity_in_window(7, ref) is True
    
    empty = CoverageActivityUser(dates_active=[])
    assert empty.to_bitmask(ref) == 0

# --- Task: sqldim/narwhals/sk_resolver.py ---

def test_narwhals_sk_resolver_full(session):
    m1 = CoverageDimMember(code="M1", name="Alice", is_current=True)
    session.add(m1)
    session.commit()
    
    resolver = NarwhalsSKResolver(session)
    df = pl.DataFrame({"code": ["M1", "M2"]})
    frame = nw.from_native(df, eager_only=True)
    
    resolved = resolver.resolve(frame, CoverageDimMember, natural_key_col="code", surrogate_key_col="member_id")
    res_native = nw.to_native(resolved)
    assert res_native["member_id"][0] == m1.id
    assert res_native["member_id"][1] is None
    
    key_map = {"member_id": (CoverageDimMember, "code")}
    resolved_all = resolver.resolve_all(frame, CoverageFactSale, key_map)
    assert "member_id" in resolved_all.columns
    
    resolver.resolve(frame, CoverageDimMember, "code", "member_id")
    assert len(resolver._cache) > 0
    
    resolver.invalidate_cache()
    assert len(resolver._cache) == 0
    
    m2 = CoverageDimMember(code="M3", name="Bob", is_current=False, 
                           valid_from=datetime(2020,1,1), valid_to=datetime(2021,1,1))
    session.add(m2)
    session.commit()
    
    pit_df = pl.DataFrame({"code": ["M3"]})
    pit_frame = nw.from_native(pit_df, eager_only=True)
    resolved_pit = resolver.resolve(pit_frame, CoverageDimMember, "code", "mid", 
                                    as_of=date(2020, 6, 1))
    assert nw.to_native(resolved_pit)["mid"][0] == m2.id

    empty_df = pl.DataFrame({"code": ["NONEXISTENT"]})
    empty_frame = nw.from_native(empty_df, eager_only=True)
    resolved_empty = resolver.resolve(empty_frame, CoverageDimMember, "code", "eid")
    assert nw.to_native(resolved_empty)["eid"][0] is None

# --- Task: sqldim/graph coverage ---

@pytest.mark.asyncio
async def test_graph_registry_missing_branches(session):
    class GVertexUnique(VertexModel, table=True):
        __vertex_type__ = "gvu"
        id: int = Field(primary_key=True)
        
    class GEdgeUnique(EdgeModel, table=True):
        __edge_type__ = "geu"
        __subject__ = GVertexUnique
        __object__ = GVertexUnique
        id: int = Field(primary_key=True)
        subject_id: int = Field(foreign_key="gvertexunique.id")
        object_id: int = Field(foreign_key="gvertexunique.id")

    graph = GraphModel(GVertexUnique, GEdgeUnique, session=session)
    
    with pytest.raises(SchemaError, match="not registered in this GraphModel"):
        graph._assert_edge_registered(CoverageFactSale)

    cls = graph._pick_neighbor_class(GVertexUnique, GVertexUnique, GVertexUnique, "both")
    assert cls == GVertexUnique

def test_schema_graph_safe_subclass_exception():
    assert _safe_subclass(123, VertexModel) is False

# --- Task: sqldim/scd/backfill.py ---

def test_scd_backfill_sql_generation(session):
    session.execute(text("CREATE TABLE snap (code TEXT, name TEXT, dt DATE)"))
    
    sql = backfill_scd2(
        source_table="snap",
        target_model=CoverageDimMember,
        partition_by="code",
        order_by="dt",
        track_columns=["name"],
        session=session,
        dry_run=True
    )
    assert "WITH streak_started" in sql
    assert "INSERT INTO cov_dim_member" in sql

# --- Task: sqldim/narwhals/scd_engine.py missing branches ---

def test_narwhals_scd_processor_no_is_current_column():
    proc = NarwhalsSCDProcessor(["code"], ["name"])
    incoming = nw.from_native(pd.DataFrame({"code": ["A"], "name": ["Alice"]}))
    current = nw.from_native(pd.DataFrame({"code": ["A"], "name": ["Bob"]}))
    
    result = proc.process(incoming, current)
    assert result.versioned == 1

# --- Task: sqldim/narwhals/transforms.py missing branches ---

def test_python_type_to_nw_complex():
    from typing import List, Optional
    assert _python_type_to_nw(List[str]) == nw.String
    assert _python_type_to_nw(Optional[int]) == nw.Int64
    assert _python_type_to_nw(None) is None
    
    class UnkCov: pass
    assert _python_type_to_nw(UnkCov) is None

def test_transform_pipeline_abstract_apply():
    t = Transform()
    with pytest.raises(NotImplementedError):
        t.apply(None)
