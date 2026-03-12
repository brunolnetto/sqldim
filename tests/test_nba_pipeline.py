import pytest
from datetime import date
from sqlmodel import Session, SQLModel, create_engine, select
from examples.nba_analytics.staging import PlayerSeasons
from examples.nba_analytics.models import Player, PlayerSCD, ScoringClass, SeasonStats, Game, PlaysInEdge
from sqldim.scd.backfill import backfill_scd2, backfill_cumulative

@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@pytest.mark.asyncio
async def test_nba_full_pipeline(session):
    # 1. Seed Staging Data (Michael Jordan 1996-1998)
    s1 = PlayerSeasons(player_name="Michael Jordan", season=1996, pts=30.4, reb=6.6, ast=4.3, gp=82)
    s2 = PlayerSeasons(player_name="Michael Jordan", season=1997, pts=29.6, reb=5.9, ast=4.3, gp=82)
    s3 = PlayerSeasons(player_name="Michael Jordan", season=1998, pts=28.7, reb=5.8, ast=3.5, gp=82)
    session.add_all([s1, s2, s3])
    session.commit()

    # 2. Bulk Cumulative Load (reproduces load_players_table_day2.sql)
    # Note: sqlite doesn't have ARRAY_AGG, so we simulate the loader result
    p1 = Player(
        player_name="Michael Jordan",
        current_season=1998,
        scoring_class=ScoringClass.star,
        seasons=[
            SeasonStats(season=1996, pts=30.4, ast=4.3, reb=6.6, weight=190).model_dump(),
            SeasonStats(season=1997, pts=29.6, ast=4.3, reb=5.9, weight=190).model_dump(),
            SeasonStats(season=1998, pts=28.7, ast=3.5, reb=5.8, weight=190).model_dump(),
        ]
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
        session=session
    )

    scd_rows = session.exec(select(PlayerSCD)).all()
    assert len(scd_rows) >= 1
    assert scd_rows[0].player_name == "Michael Jordan"
    assert scd_rows[0].scoring_class == ScoringClass.star

    # 4. Graph Projection (reproduces player_game_edges.sql)
    g1 = Game(game_id=1, game_date=date(1998, 6, 14), home_team_id=1, visitor_team_id=2, pts_home=87, pts_visitor=86)
    session.add(g1)
    session.commit()

    edge = PlaysInEdge(subject_id=p1.id, object_id=g1.id, pts=45.0, start_position="G")
    session.add(edge)
    session.commit()

    # Verify graph link
    from sqldim.graph.registry import GraphModel
    graph = GraphModel(Player, Game, PlaysInEdge, session=session)
    jordan = await graph.get_vertex(Player, p1.id)
    neighbors = await graph.neighbors(jordan, edge_type=PlaysInEdge)
    
    assert len(neighbors) == 1
    assert neighbors[0].game_id == 1
