import pytest
from sqlalchemy.pool import StaticPool
import duckdb
from datetime import date, datetime, timezone
from sqlmodel import Session, create_engine, SQLModel, select
from sqldim import DimensionModel, FactModel, Field, SCD2Mixin
from sqldim.query.builder import DimensionalQuery, DuckDBDimensionalQuery, SemanticError

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
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
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
    engine.dispose()

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


# ---------------------------------------------------------------------------
# DuckDBDimensionalQuery — SQL-string fluent builder
# ---------------------------------------------------------------------------

class TestDuckDBDimensionalQuery:
    def _con(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE fact_sales (
                id INTEGER, product_id INTEGER, revenue DOUBLE
            )
        """)
        con.execute("""
            CREATE TABLE dim_product (
                id INTEGER, category VARCHAR,
                is_current BOOLEAN, valid_from DATE, valid_to DATE
            )
        """)
        con.execute(
            "INSERT INTO fact_sales VALUES (1, 10, 100.0), (2, 10, 200.0), (3, 20, 50.0)"
        )
        con.execute(
            "INSERT INTO dim_product VALUES "
            "(10, 'Electronics', TRUE, '2020-01-01', NULL), "
            "(20, 'Apparel', TRUE, '2020-01-01', NULL)"
        )
        return con

    def test_init_and_basic_count(self):
        con = self._con()
        rows = DuckDBDimensionalQuery("fact_sales").count().execute(con)
        assert rows[0][0] == 3

    def test_by_and_sum(self):
        con = self._con()
        rows = (
            DuckDBDimensionalQuery("fact_sales")
            .by("f.product_id")
            .sum("f.revenue")
            .execute(con)
        )
        assert len(rows) == 2
        totals = {r[0]: r[1] for r in rows}
        assert totals[10] == 300.0

    def test_avg(self):
        con = self._con()
        rows = DuckDBDimensionalQuery("fact_sales").avg("f.revenue").execute(con)
        assert abs(rows[0][0] - 350.0 / 3) < 0.01

    def test_where(self):
        con = self._con()
        rows = (
            DuckDBDimensionalQuery("fact_sales")
            .where("f.revenue > 100")
            .count()
            .execute(con)
        )
        assert rows[0][0] == 1

    def test_join_dim_is_current(self):
        con = self._con()
        rows = (
            DuckDBDimensionalQuery("fact_sales")
            .join_dim("dim_product", "product_id")
            .by("d_dim_product.category")
            .sum("f.revenue")
            .execute(con)
        )
        categories = {r[0]: r[1] for r in rows}
        assert categories["Electronics"] == 300.0
        assert categories["Apparel"] == 50.0

    def test_join_dim_as_of(self):
        con = self._con()
        rows = (
            DuckDBDimensionalQuery("fact_sales")
            .join_dim("dim_product", "product_id")
            .by("d_dim_product.category")
            .count()
            .as_of("2024-06-01")
            .execute(con)
        )
        assert len(rows) == 2

    def test_as_view(self):
        con = self._con()
        view = (
            DuckDBDimensionalQuery("fact_sales")
            .by("f.product_id")
            .count()
            .as_view(con, "v_revenue_summary")
        )
        assert view == "v_revenue_summary"
        rows = con.execute("SELECT * FROM v_revenue_summary ORDER BY 1").fetchall()
        assert len(rows) == 2

    def test_semantic_error_empty_duckdb_query(self):
        with pytest.raises(SemanticError):
            DuckDBDimensionalQuery("fact_sales").to_sql()


def test_find_fk_col_returns_none_for_unlinked():
    # Line 248: _find_fk_col returns None when no column matches the given dim_model
    class UnrelatedClass:
        pass

    q = DimensionalQuery(RevenueFact)
    assert q._find_fk_col(UnrelatedClass) is None


def test_auto_join_dim_short_circuits_when_already_joined():
    # Line 254: _auto_join_dim returns stmt unchanged when dim already in joined_dims
    from sqlalchemy import select as sa_select
    q = DimensionalQuery(RevenueFact)
    stmt = sa_select(RevenueFact)

    class MockAttr:
        class_ = RegionDim

    joined = {RegionDim}
    result = q._auto_join_dim(stmt, MockAttr(), joined)
    assert result is stmt

