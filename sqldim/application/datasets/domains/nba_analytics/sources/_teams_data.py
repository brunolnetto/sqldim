"""Static NBA team fixture data — extracted from TeamsSource to keep the source
module focused on database interaction logic.

This module is an implementation detail of
:mod:`sqldim.application.datasets.domains.nba_analytics.sources.teams`.

Exported symbols
----------------
_TEAMS          : list[tuple]       — 30 NBA teams, raw fixture rows
NBA_TEAM_IDS    : list[int]         — FK pool (team 1–30)
NBA_ABBREV      : dict[int, str]    — team_id → 3-char abbreviation
NBA_CITY        : dict[int, str]    — team_id → city name
_COACHING_CHANGES: dict[int, str]   — mid-season coaching-change patch
_TEAMS_DDL      : str               — CREATE TABLE template
_esc            : (str) -> str      — SQL single-quote escaper
_team_to_row    : (tuple) -> str    — tuple → SQL VALUES row string
_COLUMNS        : str               — ordered column name list
"""

from __future__ import annotations

# ── Static fixture data ────────────────────────────────────────────────────────
# Columns: (team_id, abbreviation, nickname, yearfounded, city,
#           arena, arenacapacity, owner, generalmanager, headcoach,
#           dleagueaffiliation)

_TEAMS: list[tuple] = [
    (1, "ATL", "Hawks", 1949, "Atlanta", "State Farm Arena", 18729,
     "Tony Ressler", "Travis Schlenk", "Nate McMillan", "College Park Skyhawks"),
    (2, "BOS", "Celtics", 1946, "Boston", "TD Garden", 19156,
     "Wyc Grousbeck", "Brad Stevens", "Ime Udoka", "Maine Celtics"),
    (3, "BKN", "Nets", 1967, "Brooklyn", "Barclays Center", 17732,
     "Joe Tsai", "Sean Marks", "Steve Nash", "Long Island Nets"),
    (4, "CHA", "Hornets", 1988, "Charlotte", "Spectrum Center", 19077,
     "Michael Jordan", "Mitch Kupchak", "James Borrego", "Greensboro Swarm"),
    (5, "CHI", "Bulls", 1966, "Chicago", "United Center", 20917,
     "Jerry Reinsdorf", "Arturas Karnisovas", "Billy Donovan", "Windy City Bulls"),
    (6, "CLE", "Cavaliers", 1970, "Cleveland", "Rocket Mortgage FieldHouse", 19432,
     "Dan Gilbert", "Koby Altman", "J.B. Bickerstaff", "Cleveland Charge"),
    (7, "DAL", "Mavericks", 1980, "Dallas", "American Airlines Center", 19200,
     "Mark Cuban", "Nico Harrison", "Jason Kidd", "Texas Legends"),
    (8, "DEN", "Nuggets", 1974, "Denver", "Ball Arena", 19520,
     "Stan Kroenke", "Calvin Booth", "Michael Malone", "Grand Rapids Gold"),
    (9, "DET", "Pistons", 1941, "Detroit", "Little Caesars Arena", 20332,
     "Tom Gores", "Troy Weaver", "Dwane Casey", "Motor City Cruise"),
    (10, "GSW", "Warriors", 1946, "San Francisco", "Chase Center", 18064,
     "Joe Lacob", "Bob Myers", "Steve Kerr", "Santa Cruz Warriors"),
    (11, "HOU", "Rockets", 1967, "Houston", "Toyota Center", 18055,
     "Tilman Fertitta", "Rafael Stone", "Stephen Silas", "Rio Grande Valley Vipers"),
    (12, "IND", "Pacers", 1967, "Indianapolis", "Gainbridge Fieldhouse", 17923,
     "Herb Simon", "Chad Buchanan", "Rick Carlisle", "Fort Wayne Mad Ants"),
    (13, "LAC", "Clippers", 1970, "Los Angeles", "Crypto.com Arena", 18997,
     "Steve Ballmer", "Lawrence Frank", "Tyronn Lue", "Agua Caliente Clippers"),
    (14, "LAL", "Lakers", 1947, "Los Angeles", "Crypto.com Arena", 18997,
     "Jeanie Buss", "Rob Pelinka", "Frank Vogel", "South Bay Lakers"),
    (15, "MEM", "Grizzlies", 1995, "Memphis", "FedExForum", 18119,
     "Robert Pera", "Zach Kleiman", "Taylor Jenkins", "Memphis Hustle"),
    (16, "MIA", "Heat", 1988, "Miami", "FTX Arena", 19600,
     "Micky Arison", "Pat Riley", "Erik Spoelstra", "Sioux Falls Skyforce"),
    (17, "MIL", "Bucks", 1968, "Milwaukee", "Fiserv Forum", 17341,
     "Marc Lasry", "Jon Horst", "Mike Budenholzer", "Wisconsin Herd"),
    (18, "MIN", "Timberwolves", 1989, "Minneapolis", "Target Center", 18978,
     "Glen Taylor", "Tim Connelly", "Chris Finch", "Iowa Wolves"),
    (19, "NOP", "Pelicans", 2002, "New Orleans", "Smoothie King Center", 16867,
     "Gayle Benson", "David Griffin", "Willie Green", "Birmingham Squadron"),
    (20, "NYK", "Knicks", 1946, "New York", "Madison Square Garden", 19812,
     "James Dolan", "Leon Rose", "Tom Thibodeau", "Westchester Knicks"),
    (21, "OKC", "Thunder", 1967, "Oklahoma City", "Paycom Center", 18203,
     "Clay Bennett", "Sam Presti", "Mark Daigneault", "Oklahoma City Blue"),
    (22, "ORL", "Magic", 1989, "Orlando", "Amway Center", 18846,
     "Dan DeVos", "Jeff Weltman", "Jamahl Mosley", "Lakeland Magic"),
    (23, "PHI", "76ers", 1949, "Philadelphia", "Wells Fargo Center", 20478,
     "Josh Harris", "Daryl Morey", "Doc Rivers", "Delaware Blue Coats"),
    (24, "PHX", "Suns", 1968, "Phoenix", "Footprint Center", 18422,
     "Robert Sarver", "James Jones", "Monty Williams", "Northern Arizona Suns"),
    (25, "POR", "Trail Blazers", 1970, "Portland", "Moda Center", 19393,
     "Jody Allen", "Joe Cronin", "Chauncey Billups", "Portland Trail Blazers"),
    (26, "SAC", "Kings", 1945, "Sacramento", "Golden 1 Center", 17608,
     "Vivek Ranadive", "Monte McNair", "Alvin Gentry", "Stockton Kings"),
    (27, "SAS", "Spurs", 1967, "San Antonio", "AT&T Center", 18418,
     "Peter Holt", "Brian Wright", "Gregg Popovich", "Austin Spurs"),
    (28, "TOR", "Raptors", 1995, "Toronto", "Scotiabank Arena", 19800,
     "Larry Tanenbaum", "Bobby Webster", "Nick Nurse", "Raptors 905"),
    (29, "UTA", "Jazz", 1974, "Salt Lake City", "Vivint Arena", 18306,
     "Ryan Smith", "Danny Ainge", "Quin Snyder", "Salt Lake City Stars"),
    (30, "WAS", "Wizards", 1961, "Washington", "Capital One Arena", 20356,
     "Ted Leonsis", "Tommy Sheppard", "Wes Unseld Jr.", "Capital City Go-Go"),
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


# ── Row helpers ────────────────────────────────────────────────────────────────

_COLUMNS = (
    "league_id, team_id, min_year, max_year, abbreviation, nickname, "
    "yearfounded, city, arena, arenacapacity, owner, generalmanager, "
    "headcoach, dleagueaffiliation"
)


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
