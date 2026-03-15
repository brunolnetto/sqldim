"""
sqldim Showcase: The "Michael Jordan" Career Pipeline
-----------------------------------------------------
Pillar 1: Cumulative Arrays (Dense History)
Pillar 2: SCD Type 2 (Historical Versioning)
Pillar 3: Semantic Layer (Point-in-Time Queries)
Pillar 4: Graph Projections (Network Analysis)
"""
import asyncio
from datetime import date
from sqlmodel import Session, SQLModel, create_engine, select
from examples.real_world.nba_analytics.models import Player, PlayerSCD, ScoringClass, SeasonStats, Game, PlaysInEdge
from sqldim.scd.backfill import backfill_scd2
from sqldim.graph.registry import GraphModel
from sqldim.query.builder import DimensionalQuery

async def run_showcase():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    session = Session(engine)

    print("🏀 Initializing sqldim Showcase: Michael Jordan Career Data\n")

    # 1. CUMULATIVE PATTERN: Building snapshots across time
    # We'll seed 3 snapshots to trigger SCD2 history
    
    # 1993 Snapshot: The first Three-peat
    j1993 = Player(
        player_name="Michael Jordan",
        current_season=1993,
        scoring_class=ScoringClass.star,
        is_active=True,
        seasons=[
            SeasonStats(season=1991, pts=31.5, ast=5.5, reb=6.0, weight=198).model_dump(),
            SeasonStats(season=1992, pts=30.1, ast=6.1, reb=6.4, weight=198).model_dump(),
            SeasonStats(season=1993, pts=32.6, ast=5.5, reb=6.7, weight=198).model_dump(),
        ]
    )
    
    # 1995 Snapshot: Retired (playing baseball)
    j1995 = Player(
        player_name="Michael Jordan",
        current_season=1995,
        scoring_class=ScoringClass.average, # Out of practice ;)
        is_active=False,
        seasons=j1993.seasons
    )
    
    # 1998 Snapshot: The Last Dance
    j1998 = Player(
        player_name="Michael Jordan",
        current_season=1998,
        scoring_class=ScoringClass.star,
        is_active=True,
        seasons=j1993.seasons + [
            SeasonStats(season=1996, pts=30.4, ast=4.3, reb=6.6, weight=215).model_dump(),
            SeasonStats(season=1997, pts=29.6, ast=4.3, reb=5.9, weight=215).model_dump(),
            SeasonStats(season=1998, pts=28.7, ast=3.5, reb=5.8, weight=215).model_dump(),
        ]
    )
    
    session.add_all([j1993, j1995, j1998])
    session.commit()
    print(f"✅ Pillar 1: Cumulative snapshots built for Michael Jordan")
    print(f"   Stored dense history in 'player' table (1993, 1995, 1998).\n")

    # 2. SCD TYPE 2: Generating Versioned History
    backfill_scd2(
        source_table="player", 
        target_model=PlayerSCD,
        partition_by="player_name",
        order_by="current_season",
        track_columns=["scoring_class", "is_active"],
        session=session
    )
    print(f"✅ Pillar 2: SCD Type 2 timeline generated via backfill_scd2()")
    scd_versions = session.exec(select(PlayerSCD).order_by(PlayerSCD.valid_from)).all()
    print(f"   Found {len(scd_versions)} historical versions in 'player_scd' table:")
    for v in scd_versions:
        status = "Active" if v.is_active else "Retired"
        print(f"     - {v.valid_from} to {v.valid_to or 'Present'}: {v.scoring_class} ({status})")
    print()

    # 3. SEMANTIC LAYER: Point-in-Time "Time Travel"
    # Query for the state in 1995 (The Baseball Years)
    target_year = 1995
    historical_state = session.exec(
        select(PlayerSCD)
        .where(PlayerSCD.player_name == "Michael Jordan")
        .where(PlayerSCD.valid_from <= target_year)
        .where((PlayerSCD.valid_to == None) | (PlayerSCD.valid_to > target_year))
    ).first()
    
    print(f"✅ Pillar 3: Semantic 'AS OF' Query (Time Travel)")
    if historical_state:
        status = "Active" if historical_state.is_active else "Retired"
        print(f"   Status in {target_year}: {status} (Scoring: {historical_state.scoring_class})\n")

    # 4. DUAL-PARADIGM: Graph Traversal
    # Linking Jordan to a specific game
    g1 = Game(game_id=2323, game_date=date(1998, 6, 14), home_team_id=1, visitor_team_id=2, pts_home=87, pts_visitor=86)
    session.add(g1)
    session.commit()

    edge = PlaysInEdge(subject_id=j1998.id, object_id=g1.id, pts=45.0, start_position="G")
    session.add(edge)
    session.commit()

    graph = GraphModel(Player, Game, PlaysInEdge, session=session)
    jordan_v = await graph.get_vertex(Player, j1998.id)
    neighbors = await graph.neighbors(jordan_v, edge_type=PlaysInEdge)
    
    print(f"✅ Pillar 4: Graph Projection (Dual-Paradigm)")
    print(f"   Vertex '{jordan_v.player_name}' played Game ID: {neighbors[0].game_id}")
    print(f"   Path found via Recursive CTE projection.\n")

    print("🚀 Showcase complete. sqldim: Kimball methodology meets Graph power.")

if __name__ == "__main__":
    asyncio.run(run_showcase())
