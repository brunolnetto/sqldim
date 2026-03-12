import pytest
import pandas as pd
import polars as pl
import narwhals as nw
from datetime import date, datetime, timezone
from sqlmodel import Field, Session, SQLModel, create_engine, select, text
from typing import Optional, List, Any, Union

from sqldim import DimensionModel, FactModel, SCD2Mixin, DatelistMixin
from sqldim.narwhals.sk_resolver import NarwhalsSKResolver
from sqldim.graph.registry import GraphModel
from sqldim.graph.schema_graph import SchemaGraph
from sqldim.graph.traversal import TraversalEngine
from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.exceptions import SchemaError, TransformTypeError, LoadError
from sqldim.narwhals.transforms import col, TransformPipeline
from sqldim.narwhals.adapter import NarwhalsAdapter

# --- Fixtures ---

class GVertex(VertexModel, table=True):
    __vertex_type__ = "gv"
    id: int = Field(primary_key=True)
    name: str = "test"

class HeteroVertex(VertexModel, table=True):
    __vertex_type__ = "hv"
    id: int = Field(primary_key=True)

class GEdge(EdgeModel, table=True):
    __edge_type__ = "ge"
    __subject__ = GVertex
    __object__ = GVertex
    id: int = Field(primary_key=True)
    subject_id: int = Field(foreign_key="gvertex.id")
    object_id: int = Field(foreign_key="gvertex.id")

@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

# --- Graph Coverage (Registry/Traversal/SchemaGraph) ---

@pytest.mark.asyncio
async def test_graph_registry_multi_edge_error(session):
    class ExtraEdge(EdgeModel, table=True):
        __edge_type__ = "ee"
        __subject__ = GVertex
        __object__ = GVertex
        id: int = Field(primary_key=True)

    graph = GraphModel(GVertex, GEdge, ExtraEdge, session=session)
    v = GVertex(id=1)
    
    # Trigger "Multiple edge types connect" error
    with pytest.raises(SchemaError, match="Multiple edge types connect"):
        await graph.neighbors(v)

@pytest.mark.asyncio
async def test_graph_registry_no_edge_error(session):
    graph = GraphModel(GVertex, session=session)
    v = GVertex(id=1)
    with pytest.raises(SchemaError, match="No edge type registered"):
        await graph.neighbors(v)

def test_pick_neighbor_class_hetero():
    # Covers L302-305
    cls = GraphModel._pick_neighbor_class(GVertex, GVertex, HeteroVertex, "both")
    assert cls == HeteroVertex
    cls2 = GraphModel._pick_neighbor_class(HeteroVertex, GVertex, HeteroVertex, "both")
    assert cls2 == GVertex

def test_traversal_engine_undirected_sql():
    class UEdge(EdgeModel, table=True):
        __edge_type__ = "ue"
        __subject__ = GVertex
        __object__ = GVertex
        __directed__ = False
        id: int = Field(primary_key=True)

    engine = TraversalEngine()
    # Covers neighbors_sql undirected branch
    sql = engine.neighbors_sql(UEdge, start_id=1, direction="out")
    assert "UNION" in sql

# --- Narwhals Adapter/Transforms Coverage ---

def test_adapter_errors():
    with pytest.raises(LoadError, match="Unsupported source type"):
        NarwhalsAdapter(object())

def test_transform_pipeline_raw_and_types():
    # Covers TransformPipeline raw_transforms and error paths
    class MockModel:
        __annotations__ = {"val": int}
    
    df = pd.DataFrame({"val": ["not_an_int"]})
    pipeline = TransformPipeline(model=MockModel)
    with pytest.raises(TransformTypeError):
        pipeline.apply(nw.from_native(df))

def test_col_transforms_all_branches():
    df = pl.DataFrame({"s": [" a "], "i": [1], "n": [None]})
    frame = nw.from_native(df)
    
    # Chain multiple to hit builders
    c = col("s").str.strip().str.lowercase()
    res = c.apply(frame)
    assert nw.to_native(res)["s"][0] == "a"
    
    # Null and cast builders
    assert col("n").drop_nulls().apply(frame).is_empty()
    assert col("i").is_null().apply(frame)["i"][0] == False
