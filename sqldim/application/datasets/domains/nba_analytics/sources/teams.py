"""TeamsSource — nba_analytics domain source.

Exact schema matches the ``teams`` table from the NBA postgres dump:

    league_id      BIGINT
    team_id        BIGINT   PRIMARY KEY
    min_year       INTEGER
    max_year       INTEGER
    abbreviation   VARCHAR  (3-char)
    nickname       VARCHAR
    yearfounded    INTEGER
    city           VARCHAR
    arena          VARCHAR
    arenacapacity  INTEGER
    owner          VARCHAR
    generalmanager VARCHAR
    headcoach      VARCHAR
    dleagueaffiliation VARCHAR

The event batch simulates mid-season coaching changes (≈10% of teams
get a new head coach), which makes TeamsSource a natural SCD-2 driver.

Exported constant
-----------------
NBA_TEAM_IDS : list[int]
    Sorted list of the 30 team_id values used in this source.  Import
    this in ``GamesSource`` / ``GameDetailsSource`` so all three sources
    share the same FK pool without importing the full source module.
"""

from __future__ import annotations


import duckdb

from sqldim.application.datasets.base import BaseSource, DatasetFactory, SourceProvider

# ── Static fixture data ────────────────────────────────────────────────────────
# Columns: (team_id, abbreviation, nickname, yearfounded, city,
#           arena, arenacapacity, owner, generalmanager, headcoach,
#           dleagueaffiliation)

_TEAMS: list[tuple] = [
    (
        1,
        "ATL",
        "Hawks",
        1949,
        "Atlanta",
        "State Farm Arena",
        18729,
        "Tony Ressler",
        "Travis Schlenk",
        "Nate McMillan",
        "College Park Skyhawks",
    ),
    (
        2,
        "BOS",
        "Celtics",
        1946,
        "Boston",
        "TD Garden",
        19156,
        "Wyc Grousbeck",
        "Brad Stevens",
        "Ime Udoka",
        "Maine Celtics",
    ),
    (
        3,
        "BKN",
        "Nets",
        1967,
        "Brooklyn",
        "Barclays Center",
        17732,
        "Joe Tsai",
        "Sean Marks",
        "Steve Nash",
        "Long Island Nets",
    ),
    (
        4,
        "CHA",
        "Hornets",
        1988,
        "Charlotte",
        "Spectrum Center",
        19077,
        "Michael Jordan",
        "Mitch Kupchak",
        "James Borrego",
        "Greensboro Swarm",
    ),
    (
        5,
        "CHI",
        "Bulls",
        1966,
        "Chicago",
        "United Center",
        20917,
        "Jerry Reinsdorf",
        "Arturas Karnisovas",
        "Billy Donovan",
        "Windy City Bulls",
    ),
    (
        6,
        "CLE",
        "Cavaliers",
        1970,
        "Cleveland",
        "Rocket Mortgage FieldHouse",
        19432,
        "Dan Gilbert",
        "Koby Altman",
        "J.B. Bickerstaff",
        "Cleveland Charge",
    ),
    (
        7,
        "DAL",
        "Mavericks",
        1980,
        "Dallas",
        "American Airlines Center",
        19200,
        "Mark Cuban",
        "Nico Harrison",
        "Jason Kidd",
        "Texas Legends",
    ),
    (
        8,
        "DEN",
        "Nuggets",
        1974,
        "Denver",
        "Ball Arena",
        19520,
        "Stan Kroenke",
        "Calvin Booth",
        "Michael Malone",
        "Grand Rapids Gold",
    ),
    (
        9,
        "DET",
        "Pistons",
        1941,
        "Detroit",
        "Little Caesars Arena",
        20332,
        "Tom Gores",
        "Troy Weaver",
        "Dwane Casey",
        "Motor City Cruise",
    ),
    (
        10,
        "GSW",
        "Warriors",
        1946,
        "San Francisco",
        "Chase Center",
        18064,
        "Joe Lacob",
        "Bob Myers",
        "Steve Kerr",
        "Santa Cruz Warriors",
    ),
    (
        11,
        "HOU",
        "Rockets",
        1967,
        "Houston",
        "Toyota Center",
        18055,
        "Tilman Fertitta",
        "Rafael Stone",
        "Stephen Silas",
        "Rio Grande Valley Vipers",
    ),
    (
        12,
        "IND",
        "Pacers",
        1967,
        "Indianapolis",
        "Gainbridge Fieldhouse",
        17923,
        "Herb Simon",
        "Chad Buchanan",
        "Rick Carlisle",
        "Fort Wayne Mad Ants",
    ),
    (
        13,
        "LAC",
        "Clippers",
        1970,
        "Los Angeles",
        "Crypto.com Arena",
        18997,
        "Steve Ballmer",
        "Lawrence Frank",
        "Tyronn Lue",
        "Agua Caliente Clippers",
    ),
    (
        14,
        "LAL",
        "Lakers",
        1947,
        "Los Angeles",
        "Crypto.com Arena",
        18997,
        "Jeanie Buss",
        "Rob Pelinka",
        "Frank Vogel",
        "South Bay Lakers",
    ),
    (
        15,
        "MEM",
        "Grizzlies",
        1995,
        "Memphis",
        "FedExForum",
        18119,
        "Robert Pera",
        "Zach Kleiman",
        "Taylor Jenkins",
        "Memphis Hustle",
    ),
    (
        16,
        "MIA",
        "Heat",
        1988,
        "Miami",
        "FTX Arena",
        19600,
        "Micky Arison",
        "Pat Riley",
        "Erik Spoelstra",
        "Sioux Falls Skyforce",
    ),
    (
        17,
        "MIL",
        "Bucks",
        1968,
        "Milwaukee",
        "Fiserv Forum",
        17341,
        "Marc Lasry",
        "Jon Horst",
        "Mike Budenholzer",
        "Wisconsin Herd",
    ),
    (
        18,
        "MIN",
        "Timberwolves",
        1989,
        "Minneapolis",
        "Target Center",
        18978,
        "Glen Taylor",
        "Tim Connelly",
        "Chris Finch",
        "Iowa Wolves",
    ),
    (
        19,
        "NOP",
        "Pelicans",
        2002,
        "New Orleans",
        "Smoothie King Center",
        16867,
        "Gayle Benson",
        "David Griffin",
        "Willie Green",
        "Birmingham Squadron",
    ),
    (
        20,
        "NYK",
        "Knicks",
        1946,
        "New York",
        "Madison Square Garden",
        19812,
        "James Dolan",
        "Leon Rose",
        "Tom Thibodeau",
        "Westchester Knicks",
    ),
    (
        21,
        "OKC",
        "Thunder",
        1967,
        "Oklahoma City",
        "Paycom Center",
        18203,
        "Clay Bennett",
        "Sam Presti",
        "Mark Daigneault",
        "Oklahoma City Blue",
    ),
    (
        22,
        "ORL",
        "Magic",
        1989,
        "Orlando",
        "Amway Center",
        18846,
        "Dan DeVos",
        "Jeff Weltman",
        "Jamahl Mosley",
        "Lakeland Magic",
    ),
    (
        23,
        "PHI",
        "76ers",
        1949,
        "Philadelphia",
        "Wells Fargo Center",
        20478,
        "Josh Harris",
        "Daryl Morey",
        "Doc Rivers",
        "Delaware Blue Coats",
    ),
    (
        24,
        "PHX",
        "Suns",
        1968,
        "Phoenix",
        "Footprint Center",
        18422,
        "Robert Sarver",
        "James Jones",
        "Monty Williams",
        "Northern Arizona Suns",
    ),
    (
        25,
        "POR",
        "Trail Blazers",
        1970,
        "Portland",
        "Moda Center",
        19393,
        "Jody Allen",
        "Joe Cronin",
        "Chauncey Billups",
        "Portland Trail Blazers",
    ),
    (
        26,
        "SAC",
        "Kings",
        1945,
        "Sacramento",
        "Golden 1 Center",
        17608,
        "Vivek Ranadive",
        "Monte McNair",
        "Alvin Gentry",
        "Stockton Kings",
    ),
    (
        27,
        "SAS",
        "Spurs",
        1967,
        "San Antonio",
        "AT&T Center",
        18418,
        "Peter Holt",
        "Brian Wright",
        "Gregg Popovich",
        "Austin Spurs",
    ),
    (
        28,
        "TOR",
        "Raptors",
        1995,
        "Toronto",
        "Scotiabank Arena",
        19800,
        "Larry Tanenbaum",
        "Bobby Webster",
        "Nick Nurse",
        "Raptors 905",
    ),
    (
        29,
        "UTA",
        "Jazz",
        1974,
        "Salt Lake City",
        "Vivint Arena",
        18306,
        "Ryan Smith",
        "Danny Ainge",
        "Quin Snyder",
        "Salt Lake City Stars",
    ),
    (
        30,
        "WAS",
        "Wizards",
        1961,
        "Washington",
        "Capital One Arena",
        20356,
        "Ted Leonsis",
        "Tommy Sheppard",
        "Wes Unseld Jr.",
        "Capital City Go-Go",
    ),
]

# Exported: FK pool for GamesSource / GameDetailsSource
NBA_TEAM_IDS: list[int] = [t[0] for t in _TEAMS]

# Abbreviation lookup (team_id → abbrev) for GameDetailsSource
NBA_ABBREV: dict[int, str] = {t[0]: t[1] for t in _TEAMS}
NBA_CITY: dict[int, str] = {t[0]: t[4] for t in _TEAMS}

# Coaching changes for the event batch (≈ 1/3 of teams)
_COACHING_CHANGES: dict[int, str] = {
    2: "Joe Mazzulla",
    3: "Jacque Vaughn",
    6: "Kenny Atkinson",
    9: "Monty Williams",
    11: "Ime Udoka",
    14: "Darvin Ham",
    18: "Ryan Saunders",
    25: "Dave Joerger",
    26: "Mike Brown",
    29: "Will Hardy",
}


# ── DDL ────────────────────────────────────────────────────────────────────────

_TEAMS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    league_id          BIGINT,
    team_id            BIGINT NOT NULL PRIMARY KEY,
    min_year           INTEGER,
    max_year           INTEGER,
    abbreviation       VARCHAR,
    nickname           VARCHAR,
    yearfounded        INTEGER,
    city               VARCHAR,
    arena              VARCHAR,
    arenacapacity      INTEGER,
    owner              VARCHAR,
    generalmanager     VARCHAR,
    headcoach          VARCHAR,
    dleagueaffiliation VARCHAR
)
"""


# ── helpers ────────────────────────────────────────────────────────────────────


def _esc(s: str) -> str:
    return s.replace("'", "''")


def _team_to_row(t: tuple) -> str:
    """Convert a _TEAMS tuple to a single SQL VALUES row string."""
    team_id, abbrev, nick, yearfounded, city, arena, cap, owner, gm, coach, dleague = t
    return (
        f"(0, {team_id}, {yearfounded}, 2022, "
        f"'{_esc(abbrev)}', '{_esc(nick)}', {yearfounded}, "
        f"'{_esc(city)}', '{_esc(arena)}', {cap}, "
        f"'{_esc(owner)}', '{_esc(gm)}', '{_esc(coach)}', '{_esc(dleague)}')"
    )


_COLUMNS = (
    "league_id, team_id, min_year, max_year, abbreviation, nickname, "
    "yearfounded, city, arena, arenacapacity, owner, generalmanager, "
    "headcoach, dleagueaffiliation"
)


# ── Source class ───────────────────────────────────────────────────────────────


@DatasetFactory.register("nba_teams")
class TeamsSource(BaseSource):
    """
    Static NBA team-dimension source — 30 rows matching the ``teams``
    table from the NBA postgres dump.

    The event batch simulates mid-season coaching changes, making this
    source a natural SCD-2 driver for a dim_team table.

    OLTP schema::

        league_id           BIGINT
        team_id             BIGINT   PRIMARY KEY
        min_year / max_year INTEGER
        abbreviation        VARCHAR  (3 chars)
        nickname            VARCHAR
        yearfounded         INTEGER
        city                VARCHAR
        arena               VARCHAR
        arenacapacity       INTEGER
        owner               VARCHAR
        generalmanager      VARCHAR
        headcoach           VARCHAR
        dleagueaffiliation  VARCHAR

    Example
    -------
    ::

        import duckdb
        from sqldim.application.datasets.domains.nba_analytics.sources import TeamsSource

        con = duckdb.connect()
        src = TeamsSource()
        src.setup(con, "teams")
        con.execute("INSERT INTO teams " + src.snapshot().sql)
        print(con.execute("SELECT abbreviation, headcoach FROM teams LIMIT 5").fetchall())
    """

    DIM_DDL = _TEAMS_DDL

    provider = SourceProvider(
        name="NBA Stats API — Teams",
        url="https://www.nba.com/stats",
        description=(
            "Official NBA team registry: 30 franchises with arena, "
            "management and coaching staff data."
        ),
    )

    def snapshot(self):
        from sqldim.sources import SQLSource

        rows = ", ".join(_team_to_row(t) for t in _TEAMS)
        return SQLSource(f"SELECT * FROM (VALUES {rows}) AS t({_COLUMNS})")

    def event_batch(self, n: int = 1):
        """
        Coaching-change batch — returns updated rows for teams that had a
        head-coach change mid-season.  Use this as the ``updated`` batch
        for an SCD-2 dim_team pipeline.
        """
        from sqldim.sources import SQLSource

        rows = []
        for t in _TEAMS:
            team_id = t[0]
            if team_id in _COACHING_CHANGES:
                new_coach = _COACHING_CHANGES[team_id]
                updated = list(t)
                updated[9] = new_coach  # headcoach is index 9
                rows.append(_team_to_row(tuple(updated)))

        sql = f"SELECT * FROM (VALUES {', '.join(rows)}) AS t({_COLUMNS})"
        return SQLSource(sql)

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        con.execute(_TEAMS_DDL.format(table=table))
        con.execute(
            f"INSERT INTO {table} SELECT * FROM ({self.snapshot().as_sql(con)})"
        )

    @property
    def teams(self) -> list[tuple]:
        """Raw team tuples — useful for FK seeding in other sources."""
        return list(_TEAMS)
