"""Contract rule ABC and built-in column / table rules.

Every rule implements :meth:`as_sql(view)` which returns a SELECT that
produces **0 rows** when the rule passes and **N rows** when it fails.
The SELECT must alias output as::

    rule      VARCHAR    -- rule name
    severity  VARCHAR    -- 'error' | 'warning' | 'info'
    violations BIGINT   -- row count of violations
    detail    VARCHAR    -- human-readable detail string

:class:`ContractEngine` UNION-ALLs every rule into a single DuckDB
round-trip so validation cost is dominated by one scan per view, not
one connection-round-trip per rule.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from sqldim.contracts.report import Severity


class Rule(ABC):
    """Abstract base for all contract rules."""

    name: str
    severity: Severity

    @abstractmethod
    def as_sql(self, view: str) -> str:
        """Return SQL that yields 0 rows on pass, N rows on fail."""


# ---------------------------------------------------------------------------
# Column rules
# ---------------------------------------------------------------------------


class NotNull(Rule):
    """Assert that *column* contains no NULL values."""

    severity = Severity.ERROR

    def __init__(self, column: str) -> None:
        self.column = column
        self.name = "NOT_NULL"

    def as_sql(self, view: str) -> str:
        col = self.column
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" COUNT(*) AS violations,"
            f" 'column={col}' AS detail"
            f" FROM {view} HAVING COUNT(*) FILTER (WHERE {col} IS NULL) > 0"
        )


class NoDuplicates(Rule):
    """Assert that *column* (or composite key) is unique across all rows."""

    severity = Severity.ERROR

    def __init__(self, column: str) -> None:
        self.column = column
        self.name = "NO_DUPLICATES"

    def as_sql(self, view: str) -> str:
        col = self.column
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" SUM(cnt - 1) AS violations,"
            f" 'column={col}' AS detail"
            f" FROM (SELECT COUNT(*) AS cnt FROM {view} GROUP BY {col} HAVING COUNT(*) > 1)"
            f" HAVING SUM(cnt - 1) > 0"
        )


class NullRate(Rule):
    """Assert that the NULL fraction of *column* does not exceed *max_pct*."""

    severity = Severity.WARNING

    def __init__(self, column: str, max_pct: float) -> None:
        self.column = column
        self.max_pct = max_pct
        self.name = "NULL_RATE"

    def as_sql(self, view: str) -> str:
        col = self.column
        pct = self.max_pct
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" COUNT(*) FILTER (WHERE {col} IS NULL) AS violations,"
            f" 'column={col}, max_pct={pct}' AS detail"
            f" FROM {view}"
            f" HAVING (COUNT(*) FILTER (WHERE {col} IS NULL))::DOUBLE / NULLIF(COUNT(*), 0) > {pct}"
        )


class TypeMatch(Rule):
    """Assert that *column* has exactly *expected_type* in DESCRIBE output."""

    severity = Severity.ERROR

    def __init__(self, column: str, expected_type: str) -> None:
        self.column = column
        self.expected_type = expected_type.upper()
        self.name = "TYPE_MATCH"

    def as_sql(self, view: str) -> str:
        col = self.column
        etype = self.expected_type
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" 1 AS violations,"
            f" 'column={col}, expected={etype}, actual=' || COALESCE(actual_type, 'NOT FOUND') AS detail"
            f" FROM ("
            f"   SELECT column_type AS actual_type"
            f"   FROM (DESCRIBE {view})"
            f"   WHERE column_name = '{col}'"
            f" ) t"
            f" WHERE actual_type IS NULL"
            f"    OR upper(actual_type) NOT LIKE '{etype}%'"
        )


class ColumnExists(Rule):
    """Assert that *column* is present in the view schema."""

    severity = Severity.ERROR

    def __init__(self, column: str) -> None:
        self.column = column
        self.name = "COLUMN_EXISTS"

    def as_sql(self, view: str) -> str:
        col = self.column
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" 1 AS violations,"
            f" 'column={col} missing' AS detail"
            f" FROM (SELECT 1 WHERE NOT EXISTS ("
            f"   SELECT 1 FROM (DESCRIBE {view}) WHERE column_name = '{col}'"
            f" ))"
        )


class RowCount(Rule):
    """Assert that the row count of the view is within [min_rows, max_rows]."""

    severity = Severity.WARNING

    def __init__(
        self,
        min_rows: int | None = None,
        max_rows: int | None = None,
    ) -> None:
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.name = "ROW_COUNT"

    def as_sql(self, view: str) -> str:
        checks = []
        if self.min_rows is not None:
            checks.append(f"cnt < {self.min_rows}")
        if self.max_rows is not None:
            checks.append(f"cnt > {self.max_rows}")
        condition = " OR ".join(checks) if checks else "FALSE"
        bounds = f"min={self.min_rows}, max={self.max_rows}"
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" cnt AS violations,"
            f" 'row_count=' || cnt || ', bounds={bounds}' AS detail"
            f" FROM (SELECT COUNT(*) AS cnt FROM {view})"
            f" WHERE {condition}"
        )


class ValueRange(Rule):
    """Assert that *column* values fall within [min_val, max_val]."""

    severity = Severity.WARNING

    def __init__(
        self,
        column: str,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> None:
        self.column = column
        self.min_val = min_val
        self.max_val = max_val
        self.name = "VALUE_RANGE"

    def as_sql(self, view: str) -> str:
        col = self.column
        conditions = []
        if self.min_val is not None:
            conditions.append(f"{col} < {self.min_val}")
        if self.max_val is not None:
            conditions.append(f"{col} > {self.max_val}")
        condition = " OR ".join(conditions) if conditions else "FALSE"
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" COUNT(*) AS violations,"
            f" 'column={col}' AS detail"
            f" FROM {view} WHERE {condition}"
            f" HAVING COUNT(*) > 0"
        )


class RegexMatch(Rule):
    """Assert that all non-null values in *column* match *pattern*."""

    severity = Severity.WARNING

    def __init__(self, column: str, pattern: str) -> None:
        self.column = column
        self.pattern = pattern
        self.name = "REGEX_MATCH"

    def as_sql(self, view: str) -> str:
        col = self.column
        # Escape single quotes in the pattern
        pat = self.pattern.replace("'", "''")
        return (
            f"SELECT '{self.name}' AS rule, '{self.severity.value}' AS severity,"
            f" COUNT(*) AS violations,"
            f" 'column={col}, pattern={pat}' AS detail"
            f" FROM {view}"
            f" WHERE {col} IS NOT NULL AND NOT regexp_matches({col}, '{pat}')"
            f" HAVING COUNT(*) > 0"
        )
