"""Row-level SCD handler: MD5-based change detection and versioning.

Provides ``SCDHandler`` (SCD type 1/2/3/6) and ``SCDResult``, which
aggregate per-record outcomes (inserted / versioned / unchanged).
"""
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type, Generic, TypeVar
from sqlmodel import Session, select
from sqldim.core.kimball.models import DimensionModel
from sqldim.core.kimball.mixins import SCD2Mixin

T = TypeVar("T", bound=DimensionModel)

class SCDResult:
    """Accumulates row counts from a single ``SCDHandler.process`` call."""

    def __init__(self):
        self.inserted: int = 0
        self.versioned: int = 0
        self.unchanged: int = 0

class SCDHandler(Generic[T]):
    """Async SCD handler for a single dimension model.

    Computes MD5 checksums from *track_columns*, looks up the current
    dimension row, and routes each incoming record to the correct SCD
    handler (type 1, 2, 3, or 6) depending on ``model.__scd_type__``.

    Parameters
    ----------
    model:
        The SQLModel dimension class to upsert into.
    session:
        Active SQLModel session.
    track_columns:
        Columns included in change detection.
    ignore_columns:
        Columns excluded from change detection (default: none).
    """

    def __init__(
        self,
        model: Type[T],
        session: Session,
        track_columns: List[str],
        ignore_columns: Optional[List[str]] = None,
    ):
        self.model = model
        self.session = session
        self.track_columns = track_columns
        self.ignore_columns = ignore_columns or []

    def compute_checksum(self, record: Dict[str, Any]) -> str:
        """Return a deterministic MD5 hex digest over *track_columns* values.

        Keys are sorted before hashing so column ordering never affects the
        result.  Nested dicts and lists are JSON-serialised for consistency.
        """
        import json
        # Sort keys to ensure deterministic hashing
        values = []
        for col in sorted(self.track_columns):
            val = record.get(col, "")
            # Deterministic JSON serialization for dicts/lists (JSONB support)
            if isinstance(val, (dict, list)):
                val = json.dumps(val, sort_keys=True)
            values.append(str(val))
            
        combined = "|".join(values)
        return hashlib.md5(combined.encode("utf-8")).hexdigest()

    def _get_dim_meta(self, col_name: str) -> Dict[str, Any]:
        """Extract dimensional metadata directly from the field's sa_column_kwargs."""
        field = self.model.model_fields.get(col_name)
        if not field:
            return {}
        
        # In SQLModel/Pydantic, metadata is passed through sa_column_kwargs['info']
        kwargs = getattr(field, "sa_column_kwargs", {})
        
        # Handle PydanticUndefinedType which doesn't have .get()
        if hasattr(kwargs, "get"):
            info = kwargs.get("info")
        else:
            info = None
        if info:
            return info
            
        # Fallback to json_schema_extra if present (modern Pydantic)
        extra = getattr(field, "json_schema_extra", {})
        if isinstance(extra, dict):
            return extra
            
        return {}

    def _handle_new_record(self, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
        """Insert a brand-new dimension row and bump *result.inserted*."""
        new_row = self.model(**record, checksum=checksum, is_current=True)
        self.session.add(new_row)
        result.inserted += 1

    def _handle_type1(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
        """SCD Type 1: overwrite all tracked columns on the existing row."""
        for col, val in record.items():
            setattr(existing, col, val)
        existing.checksum = checksum
        self.session.add(existing)
        result.versioned += 1

    def _handle_type3(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
        """SCD Type 3: shift the current value to the previous column slot.

        For every field marked ``scd=3``, the existing *current* column value is
        moved to the paired *previous* column and the new value is written in its
        place.  Bumps *result.versioned*.
        """
        for col_name in self.model.model_fields:
            meta = self._get_dim_meta(col_name)
            if meta.get("scd") == 3:
                prev_col = meta.get("previous_column")
                if prev_col:
                    setattr(existing, prev_col, getattr(existing, col_name))
                    setattr(existing, col_name, record.get(col_name))
        existing.checksum = checksum
        self.session.add(existing)
        result.versioned += 1

    def _type2_cols(self) -> list[str]:
        """Return the list of column names that carry SCD2 (or default) tracking."""
        return [
            col_name for col_name in self.model.model_fields
            if self._get_dim_meta(col_name).get("scd") in (2, None)
        ]

    def _handle_type6_type1(
        self, existing: Any, record: Dict[str, Any], checksum: str, diff: Any, result: "SCDResult"
    ) -> None:
        """Apply a Type-1-only update for a Type-6 row when no Type-2 column changed.

        Overwrites only the columns that actually changed according to *diff*,
        leaving the version history untouched.  Bumps *result.versioned*.
        """
        for col in diff.changed_columns:
            setattr(existing, col, record.get(col))
        existing.checksum = checksum
        self.session.add(existing)
        result.versioned += 1

    def _handle_type6(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult", nk_value: Any) -> None:
        """SCD Type 6: mixed Type-1 / Type-2 update strategy.

        If only Type-1 columns changed, delegates to ``_handle_type6_type1``.
        If any Type-2 column changed, closes the existing row and inserts a new
        current version, optionally recording which columns differed.
        """
        from sqldim.core.kimball.dimensions.scd.detection import ColumnarDetection
        diff = ColumnarDetection(self.track_columns).diff(record, existing, nk_value)
        type2_cols = self._type2_cols()
        if not any(c in type2_cols for c in diff.changed_columns):
            self._handle_type6_type1(existing, record, checksum, diff, result)
            return
        existing.is_current = False
        existing.valid_to = datetime.now(timezone.utc)
        self.session.add(existing)
        new_data = record.copy()
        if hasattr(self.model, "metadata_diff"):
            new_data["metadata_diff"] = diff.changed_columns
        new_row = self.model(**new_data, checksum=checksum, is_current=True)
        self.session.add(new_row)
        result.versioned += 1

    def _handle_type2(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
        """SCD Type 2: close old row and insert a new current version."""
        existing.is_current = False
        existing.valid_to = datetime.now(timezone.utc)
        self.session.add(existing)
        new_row = self.model(**record.copy(), checksum=checksum, is_current=True)
        self.session.add(new_row)
        result.versioned += 1

    def _handle_changed_record(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult", nk_value: Any) -> None:
        """Dispatch to the correct SCD handler based on the model's ``__scd_type__``.

        Routes to Type-1, Type-2, Type-3, or Type-6 logic.  Defaults to Type-2
        when ``__scd_type__`` is absent or set to any other value.
        """
        scd_type = getattr(self.model, "__scd_type__", 2)
        if scd_type == 1:
            self._handle_type1(existing, record, checksum, result)
        elif scd_type == 3:
            self._handle_type3(existing, record, checksum, result)
        elif scd_type == 6:
            self._handle_type6(existing, record, checksum, result, nk_value)
        else:
            self._handle_type2(existing, record, checksum, result)

    async def _process_one(self, record: Dict[str, Any], result: "SCDResult") -> None:
        nk_field = getattr(self.model, "__natural_key__", ["id"])[0]
        nk_value = record.get(nk_field)
        checksum = self.compute_checksum(record)
        stmt = select(self.model).where(getattr(self.model, nk_field) == nk_value)
        if hasattr(self.model, "is_current"):
            stmt = stmt.where(getattr(self.model, "is_current") == True)
        existing = self.session.exec(stmt).first()
        if not existing:
            self._handle_new_record(record, checksum, result)
        elif existing.checksum != checksum:
            self._handle_changed_record(existing, record, checksum, result, nk_value)
        else:
            result.unchanged += 1

    async def process(self, records: List[Dict[str, Any]]) -> SCDResult:
        """Process *records* in sequence, applying SCD logic per row.

        Commits the session on success and returns an ``SCDResult``
        with inserted / versioned / unchanged counts.
        """
        result = SCDResult()
        for record in records:
            await self._process_one(record, result)
        self.session.commit()
        return result
