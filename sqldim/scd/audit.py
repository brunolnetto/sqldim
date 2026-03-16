"""Audit log for SCD version changes.

:class:`AuditEntry` records a single change event (table, natural key,
columns changed, timestamp, operation type) and :class:`AuditLog`
collects them into an in-memory list that can be iterated, filtered,
or bulk-written to an external audit table after a load run.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqldim.scd.detection import ChangeRecord


@dataclass
class AuditEntry:
    """Single audit event for an SCD version change."""
    table: str
    natural_key: Any
    changed_columns: Dict[str, Dict[str, Any]]
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    operation: str = "VERSION"  # INSERT | VERSION | UPDATE (Type 1 overwrite)

    def summary(self) -> str:
        cols = ", ".join(self.changed_columns.keys())
        return f"[{self.occurred_at.isoformat()}] {self.operation} {self.table} nk={self.natural_key!r} changed=({cols})"


class AuditLog:
    """
    In-memory audit log. Collects AuditEntry records during a load run.
    Can be iterated, filtered, or written to an external audit table.
    """

    def __init__(self):
        self._entries: List[AuditEntry] = []

    def record(
        self,
        table: str,
        change: ChangeRecord,
        operation: str = "VERSION",
    ) -> AuditEntry:
        entry = AuditEntry(
            table=table,
            natural_key=change.natural_key,
            changed_columns=change.changed_columns,
            operation=operation,
        )
        self._entries.append(entry)
        return entry

    def entries(self, table: Optional[str] = None) -> List[AuditEntry]:
        if table:
            return [e for e in self._entries if e.table == table]
        return list(self._entries)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)
