"""The DGM model the nba_analytics dataset declares (§2.6).

Written from ``sources/``:

* ``teams`` is a dimension keyed by ``team_id``;
* ``games`` is one fact row per game with ``home_team_id`` and
  ``visitor_team_id`` (both "FK → teams.team_id"), so ``Team`` plays two
  roles of the game (``RolePlaying``).  ``team_id_home`` and ``team_id_away``
  repeat those keys and are not mapped;
* ``game_details`` is one fact row per player per game, with ``team_id``
  ("FK → teams.team_id") and ``game_id`` ("FK → games.game_id").  A game is a
  fact type, so the reference to it is an N:1 bridge edge type held by the
  detail row (§2.6).  ``player_id`` names no table of the dataset and is a
  property;
* ``player_seasons`` is one fact row per player per season, with no foreign
  key.

Grain (§2.4): a game and a player's line in a game are events; a player
season is one row per player per season, PERIOD.  ``Team`` holds verb edge
types into ``Game`` and ``GameDetail``, the same teams for both, so it is
Conformed for the two.  A detail row belongs to its game with no order in
time between them, so ``in_game`` is STRUCTURAL (§2.3).
"""

from __future__ import annotations

from sqldim.application.datasets.model_decl import (
    ASSESSED_KINDS,
    DATE,
    INTEGER,
    REAL,
    STRING,
    bridge_fk,
    table_node,
    verb_fk,
)
from sqldim.core.query.dgm.annotations import (
    BridgeSemantics,
    BridgeSemanticsKind,
    Conformed,
    Grain,
    GrainKind,
    RolePlaying,
)
from sqldim.core.query.dgm.model import DGMModel, NodeKind, NodeType, PropType

__all__ = ["build_model"]

_SIDE_STATS: dict[str, PropType] = {
    "pts": REAL,
    "fg_pct": REAL,
    "ft_pct": REAL,
    "fg3_pct": REAL,
    "ast": INTEGER,
    "reb": INTEGER,
}


def _team() -> NodeType:
    return table_node(
        "Team",
        NodeKind.DIM,
        "teams",
        ("team_id",),
        {
            "league_id": INTEGER,
            "min_year": INTEGER,
            "max_year": INTEGER,
            "abbreviation": STRING,
            "nickname": STRING,
            "yearfounded": INTEGER,
            "city": STRING,
            "arena": STRING,
            "arenacapacity": INTEGER,
            "owner": STRING,
            "generalmanager": STRING,
            "headcoach": STRING,
            "dleagueaffiliation": STRING,
        },
    )


def _game() -> NodeType:
    props: dict[str, PropType] = {
        "game_date_est": DATE,
        "game_status_text": STRING,
        "season": INTEGER,
        "home_team_wins": INTEGER,
    }
    for side in ("home", "away"):
        props.update({f"{stat}_{side}": t for stat, t in _SIDE_STATS.items()})
    return table_node(
        "Game", NodeKind.FACT, "games", ("game_id",), props, event_time="game_date_est"
    )


def _game_detail() -> NodeType:
    counts: tuple[str, ...] = (
        "fgm",
        "fga",
        "fg3m",
        "fg3a",
        "ftm",
        "fta",
        "oreb",
        "dreb",
        "reb",
    )
    counts += ("ast", "stl", "blk", "to_", "pf")
    props: dict[str, PropType] = {
        "team_abbreviation": STRING,
        "team_city": STRING,
        "player_id": INTEGER,
        "player_name": STRING,
        "nickname": STRING,
        "start_position": STRING,
        "comment": STRING,
        "min": STRING,  # 'MM:SS'
        **{c: INTEGER for c in counts},
        "fg_pct": REAL,
        "fg3_pct": REAL,
        "ft_pct": REAL,
        "pts": REAL,
        "plus_minus": REAL,
    }
    return table_node(
        "GameDetail", NodeKind.FACT, "game_details", ("game_id", "player_id"), props
    )


def _player_season() -> NodeType:
    rates: tuple[str, ...] = (
        "gp",
        "pts",
        "reb",
        "ast",
        "netrtg",
        "oreb_pct",
        "dreb_pct",
    )
    rates += ("usg_pct", "ts_pct", "ast_pct")
    return table_node(
        "PlayerSeason",
        NodeKind.FACT,
        "player_seasons",
        ("player_name", "season"),
        {
            "player_name": STRING,
            "season": INTEGER,
            "age": INTEGER,
            "height": STRING,
            "weight": INTEGER,
            "college": STRING,
            "country": STRING,
            "draft_year": STRING,
            "draft_round": STRING,
            "draft_number": STRING,
            **{r: REAL for r in rates},
        },
    )


def build_model() -> DGMModel:
    return DGMModel(
        node_types=(_team(), _game(), _game_detail(), _player_season()),
        edge_types=(
            verb_fk("Team", "Game", "home", "home_team_id"),
            verb_fk("Team", "Game", "visitor", "visitor_team_id"),
            verb_fk("Team", "GameDetail", "played_for", "team_id"),
            bridge_fk("GameDetail", "Game", "in_game", "game_id"),
        ),
        sigma=(
            RolePlaying("Team", ["home", "visitor"]),
            Conformed("Team", {"Game", "GameDetail"}),
            Grain("Game", GrainKind.EVENT),
            Grain("GameDetail", GrainKind.EVENT),
            Grain("PlayerSeason", GrainKind.PERIOD),
            BridgeSemantics("in_game", BridgeSemanticsKind.STRUCTURAL),
        ),
        sigma_kinds=ASSESSED_KINDS,
    )
