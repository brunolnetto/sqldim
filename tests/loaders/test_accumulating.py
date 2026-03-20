import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel, select
from typing import Optional
from sqldim import FactModel, Field
from sqldim.core.loaders.accumulating import AccumulatingLoader

class OrderPipeline(FactModel, table=True):
    __grain__ = "one row per order"
    id: int = Field(primary_key=True)
    order_id: str
    amount: float
    approved_at: Optional[str] = Field(default=None, nullable=True)
    shipped_at: Optional[str] = Field(default=None, nullable=True)
    delivered_at: Optional[str] = Field(default=None, nullable=True)

@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()

def test_accumulating_insert(session):
    loader = AccumulatingLoader(
        fact=OrderPipeline,
        match_column="order_id",
        milestone_columns=["approved_at", "shipped_at", "delivered_at"],
        session=session,
    )
    result = loader.process([{"order_id": "O1", "amount": 100.0}])
    assert result["inserted"] == 1
    assert result["updated"] == 0

def test_accumulating_update_milestone(session):
    loader = AccumulatingLoader(
        fact=OrderPipeline,
        match_column="order_id",
        milestone_columns=["approved_at", "shipped_at", "delivered_at"],
        session=session,
    )
    loader.process([{"order_id": "O2", "amount": 50.0}])
    result = loader.process([{"order_id": "O2", "amount": 50.0, "approved_at": "2024-01-02"}])
    assert result["updated"] == 1

    row = session.exec(select(OrderPipeline).where(OrderPipeline.order_id == "O2")).one()
    assert row.approved_at == "2024-01-02"

def test_accumulating_does_not_overwrite_milestone(session):
    loader = AccumulatingLoader(
        fact=OrderPipeline,
        match_column="order_id",
        milestone_columns=["approved_at", "shipped_at", "delivered_at"],
        session=session,
    )
    loader.process([{"order_id": "O3", "amount": 75.0, "approved_at": "2024-01-01"}])
    # Try to overwrite approved_at with None — should be ignored
    loader.process([{"order_id": "O3", "amount": 75.0, "approved_at": None}])
    row = session.exec(select(OrderPipeline).where(OrderPipeline.order_id == "O3")).one()
    assert row.approved_at == "2024-01-01"

def test_accumulating_non_milestone_update(session):
    loader = AccumulatingLoader(
        fact=OrderPipeline,
        match_column="order_id",
        milestone_columns=["approved_at", "shipped_at", "delivered_at"],
        session=session,
    )
    loader.process([{"order_id": "O4", "amount": 10.0}])
    result = loader.process([{"order_id": "O4", "amount": 20.0}])
    assert result["updated"] == 1
    row = session.exec(select(OrderPipeline).where(OrderPipeline.order_id == "O4")).one()
    assert row.amount == 20.0
