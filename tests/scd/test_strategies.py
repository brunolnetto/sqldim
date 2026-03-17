import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel, select
from sqldim import DimensionModel, Field
from sqldim.core.loaders.strategies import BulkInsertStrategy, UpsertStrategy, MergeStrategy

class WidgetDim(DimensionModel, table=True):
    __natural_key__ = ["code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    code: str = Field(index=True, unique=True)
    label: str = Field(default="")

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

def test_bulk_insert(session):
    strat = BulkInsertStrategy()
    records = [{"code": "W1", "label": "Widget One"}, {"code": "W2", "label": "Widget Two"}]
    count = strat.execute(session, WidgetDim, records)
    assert count == 2
    assert len(session.exec(select(WidgetDim)).all()) == 2

def test_upsert_inserts_new(session):
    strat = UpsertStrategy(conflict_column="code")
    records = [{"code": "W3", "label": "Three"}]
    count = strat.execute(session, WidgetDim, records)
    assert count == 1

def test_upsert_updates_existing(session):
    strat = UpsertStrategy(conflict_column="code")
    strat.execute(session, WidgetDim, [{"code": "W4", "label": "Old"}])
    strat.execute(session, WidgetDim, [{"code": "W4", "label": "New"}])
    row = session.exec(select(WidgetDim).where(WidgetDim.code == "W4")).one()
    assert row.label == "New"

def test_upsert_empty_records(session):
    strat = UpsertStrategy(conflict_column="code")
    count = strat.execute(session, WidgetDim, [])
    assert count == 0

def test_upsert_conflict_only_record(session):
    # Record with only the conflict column → on_conflict_do_nothing() branch (line 42)
    strat = UpsertStrategy(conflict_column="code")
    strat.execute(session, WidgetDim, [{"code": "W99", "label": "First"}])
    # Second insert with only the conflict col — update_dict is empty → do_nothing
    count = strat.execute(session, WidgetDim, [{"code": "W99"}])
    assert count == 1

def test_merge_inserts(session):
    strat = MergeStrategy(match_column="code")
    count = strat.execute(session, WidgetDim, [{"code": "W5", "label": "Five"}])
    assert count == 1

def test_merge_updates(session):
    strat = MergeStrategy(match_column="code")
    strat.execute(session, WidgetDim, [{"code": "W6", "label": "Six"}])
    strat.execute(session, WidgetDim, [{"code": "W6", "label": "Updated"}])
    row = session.exec(select(WidgetDim).where(WidgetDim.code == "W6")).one()
    assert row.label == "Updated"
