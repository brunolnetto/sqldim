import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel, select
from typing import Optional
from sqldim import DimensionModel, Field
from sqldim.core.kimball.dimensions.junk import (
    make_junk_dimension,
    populate_junk_dimension,
    _flag_value_sql,
)


# Static model for DB-level tests (make_junk_dimension produces non-table dynamic models)
class SalesFlagsDim(DimensionModel, table=True):
    __natural_key__ = ["is_promo", "channel"]
    id: int = Field(primary_key=True, surrogate_key=True)
    is_promo: Optional[bool] = Field(default=None, nullable=True)
    channel: Optional[str] = Field(default=None, nullable=True)


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


def test_make_junk_dimension_creates_class():
    SalesFlags = make_junk_dimension(
        "SalesFlags",
        {
            "is_promo": [True, False],
            "is_return": [True, False],
        },
    )
    assert SalesFlags.__name__ == "SalesFlags"
    assert "is_promo" in SalesFlags.model_fields
    assert "is_return" in SalesFlags.model_fields


def test_make_junk_dimension_natural_key():
    MyFlags = make_junk_dimension("MyFlags", {"flag_a": [0, 1], "flag_b": ["x", "y"]})
    assert "flag_a" in getattr(MyFlags, "__natural_key__", [])
    assert "flag_b" in getattr(MyFlags, "__natural_key__", [])


def test_populate_junk_dimension(session):
    flags = {"is_promo": [True, False], "channel": ["web", "store"]}
    rows = populate_junk_dimension(SalesFlagsDim, flags, session)
    # 2 * 2 = 4 combinations
    assert len(rows) == 4
    all_rows = session.exec(select(SalesFlagsDim)).all()
    assert len(all_rows) == 4


def test_populate_junk_dimension_idempotent(session):
    flags = {"is_promo": [True, False], "channel": ["web", "store"]}
    rows1 = populate_junk_dimension(SalesFlagsDim, flags, session)
    rows2 = populate_junk_dimension(SalesFlagsDim, flags, session)
    assert len(rows1) == 4
    # Second call inserts nothing — all combos already exist
    assert len(rows2) == 0


def test_flag_value_sql_integer():
    # Line 12: _flag_value_sql returns f"({v})" for non-str, non-bool values
    assert _flag_value_sql(42) == "(42)"
    assert _flag_value_sql(3.14) == "(3.14)"
