import pytest
from typing import Optional
from unittest.mock import MagicMock
from sqlalchemy.pool import StaticPool
from sqlmodel import Field, Session, SQLModel, create_engine, select, text
from sqldim import DimensionModel, Field, SCD2Mixin
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler
from sqldim.core.kimball.dimensions.scd.backfill import backfill_scd2, backfill_cumulative

class UserDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["user_code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    user_code: str
    email: str

@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine, tables=[UserDim.__table__])
    with Session(engine) as session:
        yield session
    engine.dispose()

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
    
    current = session.exec(select(UserDim).where(UserDim.user_code == "U1", UserDim.is_current)).one()
    assert current.email == "new@b.com"
    assert current.valid_to is None


# ---------------------------------------------------------------------------
# Migrated from test_coverage_100.py Section J
# ---------------------------------------------------------------------------

class Cov100SCD1Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd1"
    __natural_key__ = ["code"]
    __scd_type__ = 1
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    city: str = "unknown"


class Cov100SCD6Dim(DimensionModel, SCD2Mixin, table=True):
    __tablename__ = "cov100_scd6"
    __natural_key__ = ["code"]
    __scd_type__ = 6
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    city: str = "unknown"
    metadata_diff: Optional[str] = Field(default=None, nullable=True)


@pytest.fixture
def handler_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine, tables=[Cov100SCD1Dim.__table__, Cov100SCD6Dim.__table__])
    with Session(engine) as s:
        yield s
    engine.dispose()


class TestSCDHandlerMissingLines:
    def test_get_dim_meta_missing_field(self, handler_session):
        """_get_dim_meta returns {} when field does not exist on model."""
        handler = SCDHandler(Cov100SCD1Dim, handler_session, track_columns=["city"])
        result = handler._get_dim_meta("nonexistent_column")
        assert result == {}

    def test_get_dim_meta_no_get_method(self, handler_session):
        """_get_dim_meta handles PydanticUndefined sa_column_kwargs."""
        from pydantic_core import PydanticUndefined

        mock_field = MagicMock()
        mock_field.sa_column_kwargs = PydanticUndefined
        mock_field.json_schema_extra = None

        mock_model = MagicMock()
        mock_model.model_fields = {"test_col": mock_field}

        handler = SCDHandler(mock_model, session=None, track_columns=[])
        result = handler._get_dim_meta("test_col")
        assert result == {}

    def test_get_dim_meta_json_schema_extra_dict(self, handler_session):
        """_get_dim_meta returns json_schema_extra dict when info is empty."""
        mock_field = MagicMock()
        mock_field.sa_column_kwargs = {}
        mock_field.json_schema_extra = {"dimension": "some_dim"}

        mock_model = MagicMock()
        mock_model.model_fields = {"fk_col": mock_field}

        handler = SCDHandler(mock_model, session=None, track_columns=[])
        result = handler._get_dim_meta("fk_col")
        assert result == {"dimension": "some_dim"}

    @pytest.mark.asyncio
    async def test_scd_type1_overwrite(self, handler_session):
        """SCD Type 1 overwrites existing row in place."""
        handler = SCDHandler(Cov100SCD1Dim, handler_session, track_columns=["city"])
        await handler.process([{"code": "T1", "city": "NYC"}])
        await handler.process([{"code": "T1", "city": "LA"}])
        rows = handler_session.exec(select(Cov100SCD1Dim)).all()
        current_rows = [r for r in rows if r.code == "T1"]
        assert len(current_rows) == 1
        assert current_rows[0].city == "LA"

    @pytest.mark.asyncio
    async def test_scd6_metadata_diff(self, handler_session):
        """SCD Type 6 Type 2 change sets metadata_diff when model has attribute."""
        added_objects = []
        mock_session = MagicMock()
        mock_session.exec.return_value.first.return_value = None
        mock_session.commit.return_value = None
        mock_session.exec.return_value.all.return_value = []

        def capture_add(obj):
            added_objects.append(obj)

        mock_session.add.side_effect = capture_add

        handler = SCDHandler(Cov100SCD6Dim, mock_session, track_columns=["city"])

        existing = Cov100SCD6Dim(
            id=1, code="S6", city="Seattle", is_current=True, checksum="old_hash"
        )
        mock_session.exec.return_value.first.return_value = existing
        mock_session.exec.return_value.all.return_value = [existing]
        await handler.process([{"code": "S6", "city": "Portland"}])

        new_rows = [
            obj for obj in added_objects
            if getattr(obj, "code", None) == "S6"
            and getattr(obj, "city", None) == "Portland"
        ]
        assert len(new_rows) >= 1
        assert new_rows[0].metadata_diff is not None


# ---------------------------------------------------------------------------
# Tests for sqldim/scd/backfill.py
# ---------------------------------------------------------------------------

def test_backfill_scd2_non_dry_run(handler_session):
    """backfill_scd2 with dry_run=False executes SQL and returns rowcount (line 51)."""
    handler_session.execute(text(
        "CREATE TABLE snap_bf (code TEXT, city TEXT, dt TEXT)"
    ))
    handler_session.execute(text(
        "INSERT INTO snap_bf VALUES "
        "('A', 'NY', '2020-01-01'), ('A', 'LA', '2021-01-01')"
    ))
    handler_session.commit()

    result = backfill_scd2(
        source_table="snap_bf",
        target_model=Cov100SCD1Dim,
        partition_by="code",
        order_by="dt",
        track_columns=["city"],
        session=handler_session,
        dry_run=False,
    )
    assert isinstance(result, int)


def test_backfill_scd2_dry_run(handler_session):
    """backfill_scd2 with dry_run=True returns the SQL string (line 51)."""
    handler_session.execute(text(
        "CREATE TABLE snap_bf2 (code TEXT, city TEXT, dt TEXT)"
    ))
    handler_session.commit()

    sql = backfill_scd2(
        source_table="snap_bf2",
        target_model=Cov100SCD1Dim,
        partition_by="code",
        order_by="dt",
        track_columns=["city"],
        session=handler_session,
        dry_run=True,
    )
    assert isinstance(sql, str)
    assert "WITH streak_started" in sql
    assert "INSERT INTO cov100_scd1" in sql


def test_backfill_cumulative_mock():
    """backfill_cumulative() generates and executes INSERT SQL (lines 71-87)."""
    mock_result = MagicMock()
    mock_result.rowcount = 3
    mock_session = MagicMock()
    mock_session.execute.return_value = mock_result

    class CumModel:
        __tablename__ = "player_cumulated"

    result = backfill_cumulative(
        source_table="player_snapshots",
        target_model=CumModel,
        partition_by="player_name",
        order_by="season",
        stats_columns=["pts", "reb"],
        session=mock_session,
    )
    assert result == 3
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# SCDHandler: empty-batch and mixed-batch coverage
# ---------------------------------------------------------------------------


class ScdHandlerCoverageHelper(DimensionModel, SCD2Mixin, table=True):
    """Minimal SCD-2 model used only by the handler coverage tests."""
    __tablename__ = "scd_handler_coverage"
    __natural_key__ = ["code"]
    id: int = Field(primary_key=True, surrogate_key=True)
    code: str
    city: str = "unknown"


@pytest.fixture
def scd_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(
        engine, tables=[ScdHandlerCoverageHelper.__table__]
    )
    with Session(engine) as s:
        yield s
    engine.dispose()


@pytest.mark.asyncio
async def test_process_empty_records_returns_zero(scd_session):
    """process([]) commits immediately and returns zeros (lines 264-265)."""
    handler = SCDHandler(
        model=ScdHandlerCoverageHelper,
        session=scd_session,
        track_columns=["city"],
    )
    result = await handler.process([])
    assert result.inserted == 0
    assert result.unchanged == 0
    assert result.versioned == 0


@pytest.mark.asyncio
async def test_process_mixed_batch_calls_handle_new_record(scd_session):
    """Second batch with a mix of existing + new triggers _handle_new_record (lines 105-107, 300)."""
    handler = SCDHandler(
        model=ScdHandlerCoverageHelper,
        session=scd_session,
        track_columns=["city"],
    )
    # First batch: insert one row via bulk-fast-path
    r1 = await handler.process([{"code": "CX1", "city": "Paris"}])
    assert r1.inserted == 1

    # Second batch: existing key (CX1) + brand-new key (CX2)
    # existing_map will contain CX1 so the fast path is skipped;
    # CX2 is not in existing_map → _handle_new_record is called
    r2 = await handler.process([
        {"code": "CX1", "city": "Paris"},   # unchanged
        {"code": "CX2", "city": "Berlin"},  # new — triggers lines 105-107 and 300
    ])
    assert r2.inserted == 1
    assert r2.unchanged == 1

