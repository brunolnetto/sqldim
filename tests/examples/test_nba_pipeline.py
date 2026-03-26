import pytest
from sqlalchemy.pool import StaticPool
from datetime import date
from unittest.mock import MagicMock
from sqlmodel import Session, SQLModel, create_engine, select
from sqldim.application.examples.real_world.nba_analytics.staging import PlayerSeasons
from sqldim.application.examples.real_world.nba_analytics.models import (
    Player,
    PlayerSCD,
    ScoringClass,
    SeasonStats,
    Game,
    PlaysInEdge,
)
from sqldim.core.kimball.dimensions.scd.backfill import (
    backfill_scd2,
    backfill_cumulative,
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


@pytest.mark.asyncio
async def test_nba_full_pipeline(session):
    # 1. Seed Staging Data (Michael Jordan 1996-1998)
    s1 = PlayerSeasons(
        player_name="Michael Jordan", season=1996, pts=30.4, reb=6.6, ast=4.3, gp=82
    )
    s2 = PlayerSeasons(
        player_name="Michael Jordan", season=1997, pts=29.6, reb=5.9, ast=4.3, gp=82
    )
    s3 = PlayerSeasons(
        player_name="Michael Jordan", season=1998, pts=28.7, reb=5.8, ast=3.5, gp=82
    )
    session.add_all([s1, s2, s3])
    session.commit()

    # 2. Bulk Cumulative Load (reproduces load_players_table_day2.sql)
    # Note: sqlite doesn't have ARRAY_AGG, so we simulate the loader result
    p1 = Player(
        player_name="Michael Jordan",
        current_season=1998,
        scoring_class=ScoringClass.star,
        seasons=[
            SeasonStats(
                season=1996, pts=30.4, ast=4.3, reb=6.6, weight=190
            ).model_dump(),
            SeasonStats(
                season=1997, pts=29.6, ast=4.3, reb=5.9, weight=190
            ).model_dump(),
            SeasonStats(
                season=1998, pts=28.7, ast=3.5, reb=5.8, weight=190
            ).model_dump(),
        ],
    )
    session.add(p1)
    session.commit()

    # 3. SCD2 Generation from History (reproduces scd_generation_query.sql)
    # We create a flat historical table for the backfill to read from
    # and verify the streak detection logic.
    backfill_scd2(
        source_table="player",
        target_model=PlayerSCD,
        partition_by="player_name",
        order_by="current_season",
        track_columns=["scoring_class", "is_active"],
        session=session,
    )

    scd_rows = session.exec(select(PlayerSCD)).all()
    assert len(scd_rows) >= 1
    assert scd_rows[0].player_name == "Michael Jordan"
    assert scd_rows[0].scoring_class == ScoringClass.star

    # 4. Graph Projection (reproduces player_game_edges.sql)
    g1 = Game(
        game_id=1,
        game_date=date(1998, 6, 14),
        home_team_id=1,
        visitor_team_id=2,
        pts_home=87,
        pts_visitor=86,
    )
    session.add(g1)
    session.commit()

    edge = PlaysInEdge(subject_id=p1.id, object_id=g1.id, pts=45.0, start_position="G")
    session.add(edge)
    session.commit()

    # Verify graph link
    from sqldim.core.graph.registry import GraphModel

    graph = GraphModel(Player, Game, PlaysInEdge, session=session)
    jordan = await graph.get_vertex(Player, p1.id)
    neighbors = await graph.neighbors(jordan, edge_type=PlaysInEdge)

    assert len(neighbors) == 1
    assert neighbors[0].game_id == 1


# ---------------------------------------------------------------------------
# backfill_cumulative — mock-based tests (covers lines 71-87)
# ---------------------------------------------------------------------------


class TestBackfillCumulative:
    """
    backfill_cumulative uses PostgreSQL-specific ARRAY_AGG+JSON_BUILD_OBJECT.
    We mock the session to verify SQL generation + result handling without
    requiring a live database.
    """

    def _mock_session(self, rowcount: int = 3):
        mock_result = MagicMock()
        mock_result.rowcount = rowcount
        session = MagicMock()
        session.execute.return_value = mock_result
        return session

    class _FakeModel:
        __tablename__ = "player_cumulative"

    def test_returns_rowcount(self):
        session = self._mock_session(rowcount=5)
        result = backfill_cumulative(
            source_table="player_stats",
            target_model=self._FakeModel,
            partition_by="player_id",
            order_by="season",
            stats_columns=["pts", "ast"],
            session=session,
        )
        assert result == 5

    def test_returns_zero_when_rowcount_is_none(self):
        mock_result = MagicMock()
        mock_result.rowcount = None
        session = MagicMock()
        session.execute.return_value = mock_result
        result = backfill_cumulative(
            source_table="player_stats",
            target_model=self._FakeModel,
            partition_by="player_id",
            order_by="season",
            stats_columns=["pts"],
            session=session,
        )
        assert result == 0

    def test_sql_contains_tablename(self):
        session = self._mock_session()
        backfill_cumulative(
            source_table="player_stats",
            target_model=self._FakeModel,
            partition_by="player_id",
            order_by="season",
            stats_columns=["pts"],
            session=session,
        )
        call_arg = session.execute.call_args[0][0]
        assert "player_cumulative" in str(call_arg)

    def test_sql_contains_array_agg_and_json_build_object(self):
        session = self._mock_session()
        backfill_cumulative(
            source_table="player_stats",
            target_model=self._FakeModel,
            partition_by="player_id",
            order_by="season",
            stats_columns=["pts", "reb"],
            session=session,
        )
        call_arg = str(session.execute.call_args[0][0])
        assert "ARRAY_AGG" in call_arg
        assert "JSON_BUILD_OBJECT" in call_arg
        assert "pts" in call_arg
        assert "reb" in call_arg

    def test_commit_is_called(self):
        session = self._mock_session()
        backfill_cumulative(
            source_table="player_stats",
            target_model=self._FakeModel,
            partition_by="player_id",
            order_by="season",
            stats_columns=["pts"],
            session=session,
        )
        session.commit.assert_called_once()
