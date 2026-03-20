"""Time-based freshness and row-count-delta rules."""

from __future__ import annotations

from sqldim.contracts.report import Severity
from sqldim.contracts.rules import Rule


class Freshness(Rule):
    """Assert that ``MAX(ts_col)`` is within *max_age_hours* of now."""

    severity = Severity.WARNING

    def __init__(self, ts_col: str, max_age_hours: float) -> None:
        self.ts_col = ts_col
        self.max_age_hours = max_age_hours
        self.name = "FRESHNESS"

    def as_sql(self, view: str) -> str:
        col = self.ts_col
        hours = self.max_age_hours
        sev = self.severity.value
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" 1 AS violations,"
            f" 'column={col}, max_age_hours={hours}, latest=' || COALESCE(CAST(MAX({col}) AS VARCHAR), 'NULL') AS detail"
            f" FROM {view}"
            f" HAVING MAX({col}) < (NOW() - INTERVAL '{hours} hours')"
            f"     OR MAX({col}) IS NULL"
        )


class RowCountDelta(Rule):
    """Assert that the row count has not changed by more than *max_pct* vs *baseline*.

    Pass ``baseline`` as the reference row count from a previous run.
    Useful together with benchmark JSON output where prior counts are stored.
    """

    severity = Severity.WARNING

    def __init__(self, baseline: int, max_pct: float) -> None:
        self.baseline = baseline
        self.max_pct = max_pct
        self.name = "ROW_COUNT_DELTA"

    def as_sql(self, view: str) -> str:
        b = self.baseline
        p = self.max_pct
        sev = self.severity.value
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" cnt AS violations,"
            f" 'baseline={b}, actual=' || cnt || ', max_pct={p}' AS detail"
            f" FROM (SELECT COUNT(*) AS cnt FROM {view})"
            f" WHERE ABS(cnt - {b})::DOUBLE / NULLIF({b}, 0) > {p}"
        )
