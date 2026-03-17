from dataclasses import dataclass, field
from typing import Any, Dict, List
import hashlib

@dataclass
class ChangeRecord:
    """Records exactly which columns changed between two versions of a dimension row."""
    natural_key: Any
    changed_columns: Dict[str, Dict[str, Any]]  # {col: {"old": v, "new": v}}

    def __bool__(self) -> bool:
        return bool(self.changed_columns)


class HashDetection:
    """
    Fast change detection using MD5 checksum over tracked columns.
    Returns True/False — does NOT produce a per-column ChangeRecord.
    """

    def __init__(self, track_columns: List[str], algorithm: str = "md5"):
        self.track_columns = sorted(track_columns)
        self.algorithm = algorithm

    def compute(self, record: Dict[str, Any]) -> str:
        values = [str(record.get(col, "")) for col in self.track_columns]
        combined = "|".join(values)
        h = hashlib.new(self.algorithm)
        h.update(combined.encode("utf-8"))
        return h.hexdigest()

    def has_changed(self, record: Dict[str, Any], stored_checksum: str) -> bool:
        return self.compute(record) != stored_checksum


class ColumnarDetection:
    """
    Slower column-by-column change detection.
    Returns a ChangeRecord with the exact columns that changed.
    Useful for audit trails and SCD Type 6.
    """

    def __init__(self, track_columns: List[str]):
        self.track_columns = track_columns

    def diff(self, incoming: Dict[str, Any], existing: Any, natural_key: Any) -> ChangeRecord:
        changed = {}
        for col in self.track_columns:
            new_val = incoming.get(col)
            old_val = getattr(existing, col, None)
            if new_val != old_val:
                changed[col] = {"old": old_val, "new": new_val}
        return ChangeRecord(natural_key=natural_key, changed_columns=changed)
