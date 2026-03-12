import pytest
from datetime import date, datetime, timezone
from sqlmodel import Session, create_engine, SQLModel, select
from sqldim import DimensionModel, FactModel, Field, SCD2Mixin
from sqldim.query.builder import DimensionalQuery, SemanticError

class RegionDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["region_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    region_code: str
    country: str

class RevenueFact(FactModel, table=True):
    __grain__ = "one row per sale"
    id: int = Field(primary_key=True)
    region_id: int = Field(foreign_key="regiondim.id", dimension=RegionDim)
    revenue: float = Field(measure=True, additive=True)
    unit_price: float = Field(measure=True, additive=False)

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        # Setup dimension rows
        r1 = RegionDim(region_code="US", country="United States", is_current=True,
                       valid_from=datetime(2020, 1, 1, tzinfo=timezone.utc))
        s.add(r1)
        s.commit()
        s.refresh(r1)
        # Setup fact row
        f1 = RevenueFact(region_id=r1.id, revenue=100.0, unit_price=10.0)
        s.add(f1)
        s.commit()
        yield s

@pytest.mark.asyncio
async def test_query_sum_by_dimension(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .sum(RevenueFact.revenue)
        .execute(session)
    )
    assert len(results) == 1
    assert results[0][1] == 100.0

@pytest.mark.asyncio
async def test_query_count(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .count()
        .execute(session)
    )
    assert results[0][1] == 1

@pytest.mark.asyncio
async def test_query_avg(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .avg(RevenueFact.revenue)
        .execute(session)
    )
    assert results[0][1] == 100.0

@pytest.mark.asyncio
async def test_query_where_filter(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .sum(RevenueFact.revenue)
        .where(RegionDim.country == "United States")
        .execute(session)
    )
    assert len(results) == 1

@pytest.mark.asyncio
async def test_query_where_filter_no_match(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .sum(RevenueFact.revenue)
        .where(RegionDim.country == "Brazil")
        .execute(session)
    )
    assert len(results) == 0

@pytest.mark.asyncio
async def test_query_as_of(session):
    results = await (
        DimensionalQuery(RevenueFact)
        .by(RegionDim.country)
        .sum(RevenueFact.revenue)
        .as_of(date(2025, 6, 1))
        .execute(session)
    )
    assert len(results) == 1

def test_semantic_error_non_additive_sum():
    with pytest.raises(SemanticError):
        DimensionalQuery(RevenueFact).sum(RevenueFact.unit_price)

def test_semantic_error_empty_query():
    with pytest.raises(SemanticError):
        DimensionalQuery(RevenueFact)._build()

def test_get_column_info_no_info_attr():
    # Cover the AttributeError fallback branch in _get_column_info
    class NoInfoCol:
        pass  # no .info attribute

    q = DimensionalQuery(RevenueFact)
    result = q._get_column_info(NoInfoCol())
    assert result == {}
