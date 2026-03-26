import pytest
from sqlalchemy.pool import StaticPool
from datetime import date
from sqlmodel import Session, create_engine, SQLModel, select
from sqldim import DimensionModel, FactModel, Field, SCD2Mixin
from sqldim.core.loaders.fact.snapshot import SnapshotLoader


class AccountDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["account_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    account_code: str


class BalanceFact(FactModel, table=True):
    __grain__ = "one row per account per day"
    id: int = Field(primary_key=True)
    account_id: int = Field(foreign_key="accountdim.id")
    balance: float
    snapshot_date: str  # stored as ISO date string for SQLite compatibility


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(
        engine, tables=[AccountDim.__table__, BalanceFact.__table__]
    )
    with Session(engine) as s:
        yield s
    engine.dispose()


def test_snapshot_load_inserts_rows(session):
    loader = SnapshotLoader(
        fact=BalanceFact,
        dimension=AccountDim,
        snapshot_date=date(2024, 6, 1),
        session=session,
    )
    records = [
        {"account_id": 1, "balance": 1000.0},
        {"account_id": 2, "balance": 2500.0},
    ]
    count = loader.load(records)
    assert count == 2

    rows = session.exec(select(BalanceFact)).all()
    assert len(rows) == 2
    assert all(r.snapshot_date == "2024-06-01" for r in rows)


def test_snapshot_date_injected(session):
    snap_date = date(2024, 12, 31)
    loader = SnapshotLoader(
        fact=BalanceFact,
        dimension=AccountDim,
        snapshot_date=snap_date,
        session=session,
    )
    loader.load([{"account_id": 3, "balance": 500.0}])
    row = session.exec(select(BalanceFact)).one()
    assert row.snapshot_date == "2024-12-31"


def test_snapshot_custom_date_field(session):
    # SnapshotLoader supports a custom date_field name
    loader = SnapshotLoader(
        fact=BalanceFact,
        dimension=AccountDim,
        snapshot_date=date(2024, 1, 1),
        session=session,
        date_field="snapshot_date",
    )
    count = loader.load([{"account_id": 4, "balance": 750.0}])
    assert count == 1


def test_snapshot_empty_records(session):
    loader = SnapshotLoader(
        fact=BalanceFact,
        dimension=AccountDim,
        snapshot_date=date(2024, 6, 1),
        session=session,
    )
    count = loader.load([])
    assert count == 0
    assert session.exec(select(BalanceFact)).all() == []
