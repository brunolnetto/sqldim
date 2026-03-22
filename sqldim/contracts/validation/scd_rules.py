"""SCD2-specific structural invariant rules.

These rules encode sqldim's own dimensional modeling invariants.  They
accept *natural_key* as a constructor argument and generate SQL that
checks structural properties across version history.
"""

from __future__ import annotations

from sqldim.contracts.reporting.report import Severity
from sqldim.contracts.validation.rules import Rule


class SCD2Invariants(Rule):
    """Assert multiple SCD2 structural invariants in one rule.

    Checks (any violation → ERROR):
    - At most one ``is_current=TRUE`` per natural key
    - ``valid_from`` not NULL on current rows
    - ``valid_to`` NULL on current, non-null on historical
    - ``valid_from < valid_to`` on historical rows
    - No row with ``is_current=FALSE`` and ``valid_to IS NULL``
    """

    severity = Severity.ERROR

    def __init__(self, natural_key: str) -> None:
        self.natural_key = natural_key
        self.name = "SCD2_INVARIANTS"

    def as_sql(self, view: str) -> str:
        nk = self.natural_key
        sev = self.severity.value
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" SUM(violations) AS violations, STRING_AGG(detail, '; ') AS detail"
            f" FROM ("
            # Check 1: duplicate open versions (> 1 is_current=TRUE per NK)
            f"   SELECT COUNT(*) - COUNT(DISTINCT {nk}) AS violations,"
            f"          'duplicate current versions' AS detail"
            f"   FROM {view} WHERE is_current = TRUE"
            f"   HAVING COUNT(*) > COUNT(DISTINCT {nk})"
            f"   UNION ALL"
            # Check 2: current rows with NULL valid_from
            f"   SELECT COUNT(*) AS violations,"
            f"          'current row with null valid_from' AS detail"
            f"   FROM {view} WHERE is_current = TRUE AND valid_from IS NULL"
            f"   HAVING COUNT(*) > 0"
            f"   UNION ALL"
            # Check 3: current rows with non-NULL valid_to
            f"   SELECT COUNT(*) AS violations,"
            f"          'current row with non-null valid_to' AS detail"
            f"   FROM {view} WHERE is_current = TRUE AND valid_to IS NOT NULL"
            f"   HAVING COUNT(*) > 0"
            f"   UNION ALL"
            # Check 4: historical rows with valid_from >= valid_to
            f"   SELECT COUNT(*) AS violations,"
            f"          'historical row with valid_from >= valid_to' AS detail"
            f"   FROM {view}"
            f"   WHERE is_current = FALSE AND valid_to IS NOT NULL"
            f"     AND valid_from >= valid_to"
            f"   HAVING COUNT(*) > 0"
            f"   UNION ALL"
            # Check 5: historical rows with NULL valid_to
            f"   SELECT COUNT(*) AS violations,"
            f"          'historical row with null valid_to' AS detail"
            f"   FROM {view} WHERE is_current = FALSE AND valid_to IS NULL"
            f"   HAVING COUNT(*) > 0"
            f" ) checks"
            f" HAVING SUM(violations) > 0"
        )


class NoOrphanVersions(Rule):
    """Assert that every historical row has a current successor for its NK."""

    severity = Severity.ERROR

    def __init__(self, natural_key: str) -> None:
        self.natural_key = natural_key
        self.name = "NO_ORPHAN_VERSIONS"

    def as_sql(self, view: str) -> str:
        nk = self.natural_key
        sev = self.severity.value
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" COUNT(DISTINCT h.{nk}) AS violations,"
            f" 'NKs with historical rows but no current version' AS detail"
            f" FROM {view} h"
            f" WHERE h.is_current = FALSE"
            f"   AND NOT EXISTS ("
            f"     SELECT 1 FROM {view} c"
            f"     WHERE c.{nk} = h.{nk} AND c.is_current = TRUE"
            f"   )"
            f" HAVING COUNT(DISTINCT h.{nk}) > 0"
        )


class MonotonicValidFrom(Rule):
    """Assert that ``valid_from`` is strictly increasing across versions per NK."""

    severity = Severity.ERROR

    def __init__(self, natural_key: str) -> None:
        self.natural_key = natural_key
        self.name = "MONOTONIC_VALID_FROM"

    def as_sql(self, view: str) -> str:
        nk = self.natural_key
        sev = self.severity.value
        # A violation exists when a historical row has valid_from AFTER the
        # current (is_current=TRUE) row for the same NK — meaning the current
        # version is not the most-recently effective one.
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" COUNT(DISTINCT h.{nk}) AS violations,"
            f" 'NKs where historical valid_from > current valid_from' AS detail"
            f" FROM {view} h"
            f" JOIN {view} c ON c.{nk} = h.{nk} AND c.is_current = TRUE"
            f" WHERE h.is_current = FALSE AND h.valid_from > c.valid_from"
            f" HAVING COUNT(DISTINCT h.{nk}) > 0"
        )


class NoGapPeriods(Rule):
    """Assert no time gap between consecutive version periods per NK.

    A gap exists when ``valid_to[version N]`` != ``valid_from[version N+1]``.
    Applies only to historical rows (those with non-NULL ``valid_to``).
    """

    severity = Severity.WARNING

    def __init__(self, natural_key: str) -> None:
        self.natural_key = natural_key
        self.name = "NO_GAP_PERIODS"

    def as_sql(self, view: str) -> str:
        nk = self.natural_key
        sev = self.severity.value
        # Compute LEAD over ALL rows (including current whose valid_to IS NULL)
        # so that the last historical row's next_vf correctly sees the current
        # row's valid_from.
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" COUNT(*) AS violations,"
            f" 'NKs with time-gap between consecutive periods' AS detail"
            f" FROM ("
            f"   SELECT {nk}, valid_to AS this_vt,"
            f"     LEAD(valid_from) OVER (PARTITION BY {nk} ORDER BY valid_from) AS next_vf"
            f"   FROM {view}"
            f" ) t"
            f" WHERE this_vt IS NOT NULL AND this_vt <> next_vf AND next_vf IS NOT NULL"
            f" HAVING COUNT(*) > 0"
        )


class HashConsistency(Rule):
    """Assert that stored checksums match recomputed MD5 of tracked columns.

    Parameters
    ----------
    hash_col:
        Name of the stored checksum / row-hash column.
    tracked_cols:
        Columns whose concatenated values the hash should reflect.
        MD5 is computed as ``md5(col1 || '|' || col2 || ...)``.
    """

    severity = Severity.WARNING

    def __init__(self, hash_col: str, tracked_cols: list[str]) -> None:
        self.hash_col = hash_col
        self.tracked_cols = tracked_cols
        self.name = "HASH_CONSISTENCY"

    def as_sql(self, view: str) -> str:
        h = self.hash_col
        sev = self.severity.value
        concat = " || '|' || ".join(
            f"COALESCE(CAST({c} AS VARCHAR), '')" for c in self.tracked_cols
        )
        return (
            f"SELECT '{self.name}' AS rule, '{sev}' AS severity,"
            f" COUNT(*) AS violations,"
            f" 'hash_col={h}, mismatched rows' AS detail"
            f" FROM {view}"
            f" WHERE {h} IS DISTINCT FROM md5({concat})"
            f" HAVING COUNT(*) > 0"
        )
