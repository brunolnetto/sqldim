import pytest
from sqlmodel import Session, create_engine, SQLModel, select
from sqldim import DimensionModel, BridgeModel, Field, SCD2Mixin, SCD3Mixin, SCDHandler

# ── SCD Type 3 Test ───────────────────────────────────────────────────────────

class CustomerSCD3(DimensionModel, SCD2Mixin, SCD3Mixin, table=True):
    __natural_key__ = ["code"]
    __scd_type__ = 3
    id: int = Field(primary_key=True)
    code: str
    city: str = Field(scd=3, previous_column="prev_city")
    prev_city: str = Field(default=None, nullable=True)

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s

@pytest.mark.asyncio
async def test_scd_type3_rotation(session):
    handler = SCDHandler(CustomerSCD3, session, track_columns=["city"])
    
    # 1. Initial Insert
    await handler.process([{"code": "C1", "city": "NYC"}])
    
    # 2. Update (Trigger Rotation)
    await handler.process([{"code": "C1", "city": "LA"}])
    
    row = session.exec(select(CustomerSCD3).where(CustomerSCD3.code == "C1")).one()
    assert row.city == "LA"
    assert row.prev_city == "NYC"

@pytest.mark.asyncio
async def test_scd_type6_hybrid(session):
    from sqldim import SCD2Mixin
    class HybridDim(DimensionModel, SCD2Mixin, table=True):
        __natural_key__ = ["code"]
        __scd_type__ = 6
        id: int = Field(primary_key=True)
        code: str
        email: str = Field(scd=1) # Overwrite
        city: str = Field(scd=2)  # Version

    # Explicitly create table for this dynamic model
    HybridDim.__table__.create(session.bind)
    handler = SCDHandler(HybridDim, session, track_columns=["email", "city"])
    await handler.process([{"code": "H1", "email": "old@b.com", "city": "NYC"}])
    
    # 1. Update Type 1 only -> Overwrite in place
    await handler.process([{"code": "H1", "email": "new@b.com", "city": "NYC"}])
    all_rows = session.exec(select(HybridDim)).all()
    assert len(all_rows) == 1
    assert all_rows[0].email == "new@b.com"
    
    # 2. Update Type 2 -> Create new version
    await handler.process([{"code": "H1", "email": "new@b.com", "city": "LA"}])
    all_rows = session.exec(select(HybridDim)).all()
    assert len(all_rows) == 2

# ── Bridge Weight Test ────────────────────────────────────────────────────────

class SalesRepBridge(BridgeModel, table=True):
    id: int = Field(primary_key=True)
    sale_id: int
    rep_id: int
    # weight field is inherited from BridgeModel

def test_bridge_weight_default():
    bridge = SalesRepBridge(sale_id=1, rep_id=10)
    assert bridge.weight == 1.0

def test_bridge_weight_allocation():
    # Example: 50/50 split between two reps
    b1 = SalesRepBridge(sale_id=2, rep_id=11, weight=0.5)
    b2 = SalesRepBridge(sale_id=2, rep_id=12, weight=0.5)
    assert b1.weight + b2.weight == 1.0
