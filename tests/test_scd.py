import pytest
from sqlmodel import Session, create_engine, SQLModel
from sqldim import DimensionModel, Field, SCD2Mixin
from sqldim.scd.handler import SCDHandler

class UserDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["user_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    user_code: str
    email: str

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@pytest.mark.asyncio
async def test_scd_lifecycle(session):
    handler = SCDHandler(model=UserDim, session=session, track_columns=["email"])
    
    # 1. Initial Insert
    records = [{"user_code": "U1", "email": "a@b.com"}]
    res1 = await handler.process(records)
    assert res1.inserted == 1
    
    # 2. No-op (Same data)
    res2 = await handler.process(records)
    assert res2.unchanged == 1
    
    # 3. Version Change (Email changed)
    updated_records = [{"user_code": "U1", "email": "new@b.com"}]
    res3 = await handler.process(updated_records)
    assert res3.versioned == 1
    
    # Verify DB state
    from sqlmodel import select
    all_versions = session.exec(select(UserDim).where(UserDim.user_code == "U1")).all()
    assert len(all_versions) == 2
    
    current = session.exec(select(UserDim).where(UserDim.user_code == "U1", UserDim.is_current == True)).one()
    assert current.email == "new@b.com"
    assert current.valid_to is None
