import pytest
from sqlalchemy.pool import StaticPool
import pandas as pd
import polars as pl
import narwhals as nw
from typing import Optional, Dict, Any
from sqlmodel import Field, Session, SQLModel, create_engine, JSON, Column, select
from sqldim import DimensionModel, SCDHandler, SCD2Mixin
from sqldim.core.kimball.dimensions.scd.processors.scd_engine import NarwhalsSCDProcessor

# --- Fixtures ---

class HybridDim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "hybrid_dim"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    # Merge info into Column() directly to avoid SQLModel conflict
    properties: Dict[str, Any] = Field(
        default_factory=dict, 
        sa_column=Column(JSON, info={"scd": 2})
    )

@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()

# --- Tests ---

@pytest.mark.asyncio
async def test_scd_handler_jsonb_change(session):
    # track_columns must include 'properties' to trigger SCD2 on JSON changes
    handler = SCDHandler(HybridDim, session, track_columns=["properties"])
    
    # 1. Initial Load
    rec1 = {"code": "H1", "properties": {"color": "red", "size": "L"}}
    await handler.process([rec1])
    
    row = session.exec(select(HybridDim)).first()
    assert row.properties == rec1["properties"]
    assert row.checksum is not None
    
    # 2. Update with same content (reordered keys) -> No change
    rec2 = {"code": "H1", "properties": {"size": "L", "color": "red"}}
    res2 = await handler.process([rec2])
    assert res2.unchanged == 1
    assert res2.versioned == 0
    
    # 3. Update content -> SCD2 Triggered
    rec3 = {"code": "H1", "properties": {"color": "blue", "size": "L"}}
    res3 = await handler.process([rec3])
    assert res3.versioned == 1
    
    # Verify both versions exist
    rows = session.exec(select(HybridDim).order_by(HybridDim.id)).all()
    assert len(rows) == 2
    assert rows[0].is_current is False
    assert rows[1].is_current is True
    assert rows[1].properties["color"] == "blue"

def test_narwhals_scd_jsonb_vectorized():
    proc = NarwhalsSCDProcessor(["code"], ["properties"])
    
    incoming_df = pl.DataFrame({
        "code": ["H1"],
        "properties": [{"meta": "v1"}]
    })
    current_df = pl.DataFrame({
        "code": ["H1"],
        "properties": [{"meta": "v2"}],
        "is_current": [True]
    })
    
    incoming = nw.from_native(incoming_df)
    current = nw.from_native(current_df)
    
    result = proc.process(incoming, current)
    assert result.versioned == 1
    assert len(result.to_insert) == 1

def test_narwhals_jsonb_checksum_determinism():
    from sqldim.core.kimball.dimensions.scd.processors.scd_engine import NarwhalsHashStrategy
    strategy = NarwhalsHashStrategy(["code"], ["properties"])
    
    df1_dict = pl.DataFrame({"code": ["A"], "properties": [{"a": 1, "b": 2}]})
    df2_dict = pl.DataFrame({"code": ["A"], "properties": [{"b": 2, "a": 1}]})
    
    c1 = strategy.compute_checksums(nw.from_native(df1_dict))
    c2 = strategy.compute_checksums(nw.from_native(df2_dict))
    
    assert nw.to_native(c1)["checksum"][0] == nw.to_native(c2)["checksum"][0]
