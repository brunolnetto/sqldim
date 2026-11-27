"""Export contracts per sink (§6.2 Rules 8 and 10, §6.4) and band B3 windows (§4.1).

The sink cases are run by every sink backend (#138); the write-plan cases by
the planner's Rule 8 / Rule 10.  Values are plain Python: a backend creates
the table with :data:`TYPES` and reads the rows back in column order.
"""

from __future__ import annotations

from datetime import date, datetime

from tests.conformance.cases import DERIVED, Case
from tests.conformance.oracle import REJECTED

SINKS = ("DUCKDB", "POSTGRESQL", "MOTHERDUCK", "PARQUET", "DELTA", "ICEBERG")

#: Column types of the sink fixtures, by column name.
TYPES = {
    "id": "VARCHAR",
    "name": "VARCHAR",
    "qty": "BIGINT",
    "price": "DOUBLE",
    "ts": "TIMESTAMP",
    "is_current": "BOOLEAN",
    "valid_from": "DATE",
    "valid_to": "DATE",
}

RT_COLS = ("id", "name", "qty", "price", "ts", "is_current")


def _r(id_, name, qty, price, ts, cur=True):
    return dict(zip(RT_COLS, (id_, name, qty, price, ts, cur)))


BATCH_1 = (
    _r("a", "Ann", 1, 2.5, datetime(2024, 1, 1, 8, 30)),
    _r("b", "Zoë d'Arc", None, 0.0, None),
    _r("c", None, -3, None, datetime(2024, 2, 29, 23, 59, 59), False),
)
BATCH_2 = (
    _r("d", "Dé", 7, 1e-3, datetime(2024, 3, 1)),
    _r("e", "", 0, -1.25, datetime(1999, 12, 31, 0, 0, 1)),
)

RT = "§6.2 Rule 8 APPEND (INSERT INTO); §2.4 write_mode"

ROUNDTRIP = [
    Case("SINK-RT-01", "sink_roundtrip", RT,
         "rows are read back with the values written, NULLs included", "batch_1",
         {"columns": RT_COLS, "writes": (BATCH_1,)}),
    Case("SINK-RT-02", "sink_roundtrip", RT,
         "a second write appends; the first write's rows remain", "batch_1_2",
         {"columns": RT_COLS, "writes": (BATCH_1, BATCH_2)}),
    Case("SINK-RT-03", "sink_roundtrip", RT,
         "a write of no rows leaves the written rows in place", "batch_1_empty",
         {"columns": RT_COLS, "writes": (BATCH_1, ())}),
]  # fmt: skip

CL_COLS = ("id", "name", "is_current", "valid_from", "valid_to")
D0, D1, CLOSE_AT = date(2023, 1, 1), date(2023, 6, 1), date(2024, 12, 31)


def _v(id_, name, cur, frm, to=None):
    return dict(zip(CL_COLS, (id_, name, cur, frm, to)))


SCD_ROWS = (
    _v("a", "Ann", True, D0),
    _v("b", "Bob-old", False, D0, D1),
    _v("b", "Bob", True, D1),
    _v("c", "Cy", True, D0),
    _v("h", "Hal-old", False, D0, D1),
)

CL = "§2.5 SCD2 versions; §2.6 M2 (a closed version has valid_to set)"


def _cl(cid, rule, close):
    return Case(cid, "sink_close", CL, rule, "scd_rows",
                {"columns": CL_COLS, "rows": SCD_ROWS, "close": close,
                 "valid_to": CLOSE_AT})  # fmt: skip


CLOSE = [
    _cl("SINK-CL-01", "closing one key closes its current version only", ("a",)),
    _cl("SINK-CL-02", "a key's historical version is left as it was", ("b",)),
    _cl(
        "SINK-CL-03",
        "closing every current key keeps the historical rows",
        ("a", "b", "c"),
    ),
    _cl("SINK-CL-04", "a key with no current version changes nothing", ("h",)),
]

SQL = "§6.2 Rule 8, §6.4 sink write semantics: statements executed by DuckDB"

SINK_SQL = [
    Case("SINK-SQL-W", "sink_sql", SQL,
         "write() sends at least one statement and each parses as DuckDB SQL",
         "batch_1", {"operation": "write"}, True),
    Case("SINK-SQL-C", "sink_sql", SQL,
         "close_versions() sends at least one statement and each parses as "
         "DuckDB SQL", "scd_rows", {"operation": "close_versions"}, True),
]  # fmt: skip

R8 = "§6.2 Rule 8 sink-aware write planning; Delta and Iceberg writes"
R10 = "§6.2 Rule 10 with Delta and Iceberg writes (MERGE)"

WRITE_PLAN = (
    [
        Case(f"WP-{s}-target", "write_plan", R8,
             "the write targets a table, never a read-only *_scan function",
             "rule8", {"question": "target", "sink": s}, "WRITABLE")
        for s in SINKS
    ]
    + [
        Case(f"WP-{s}-partition", "write_plan", R8,
             "TemporalAgg: partitioned by timestamp unit iff a file sink",
             "rule8", {"question": "partition", "sink": s, "temporal_agg": True},
             s in ("PARQUET", "DELTA", "ICEBERG"))
        for s in SINKS
    ]
    + [
        Case("WP-DELTA-append", "write_plan", R8, "Q_delta into DELTA appends",
             "rule8", {"question": "append", "sink": "DELTA", "q_delta": True}, True),
        Case("WP-ICEBERG-catalog", "write_plan", R8,
             "an ICEBERG write plan with no attached REST catalog is rejected at "
             "planning time", "rule8",
             {"question": "iceberg_catalog", "sink": "ICEBERG", "catalog": None},
             REJECTED),
        Case("WP-DELTA-merge", "write_plan", R10,
             "a MERGE (REFRESH from Stale) into DELTA is rejected", "rule10",
             {"question": "merge", "sink": "DELTA"}, REJECTED),
        Case("WP-ICEBERG-merge", "write_plan", R10,
             "a MERGE into ICEBERG on DuckDB 1.5.0, a version the capability "
             "table does not cover, is rejected", "rule10",
             {"question": "merge", "sink": "ICEBERG", "engine": "1.5.0",
              "merge_on_read": True}, DERIVED),
    ]
)  # fmt: skip

# ---------------------------------------------------------------------------
# Band B3: window functions and QUALIFY
# ---------------------------------------------------------------------------

SALES = (
    {"id": "s1", "customer": "c1", "revenue": 100, "discount": 0.1},
    {"id": "s2", "customer": "c1", "revenue": 300, "discount": None},
    {"id": "s3", "customer": "c1", "revenue": 300, "discount": 0.2},
    {"id": "s4", "customer": "c2", "revenue": 5, "discount": 0.9},
    {"id": "s5", "customer": "c2", "revenue": 50, "discount": 0.3},
    {"id": "s6", "customer": "c3", "revenue": 20, "discount": None},
)
RN = ("ROW_NUMBER", "customer", (("revenue", "DESC"), ("id", "ASC")))
WIN = "§4.1 Band table (B3: window, QUALIFY); §4.1 NULL rules"

WINDOW = [
    Case("WIN-01", "window", WIN, "QUALIFY rn = 1: the top sale per customer",
         "sales", {"rows": SALES, "window": RN, "qualify": (("w", "=", 1),)}),
    Case("WIN-02", "window", WIN,
         "QUALIFY SUM(discount) OVER (PARTITION BY customer) > 0.25: a "
         "partition of NULLs sums to NULL, and UNKNOWN does not pass", "sales",
         {"rows": SALES, "window": ("SUM", "customer", "discount"),
          "qualify": (("w", ">", 0.25),)}),
    Case("WIN-03", "window", WIN,
         "QUALIFY SUM(revenue) OVER (PARTITION BY customer) > 60", "sales",
         {"rows": SALES, "window": ("SUM", "customer", "revenue"),
          "qualify": (("w", ">", 60),)}),
]  # fmt: skip

CASES = ROUNDTRIP + CLOSE + SINK_SQL + WRITE_PLAN + WINDOW
