import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type, Generic, TypeVar
from sqlmodel import Session, select
from sqldim.core.models import DimensionModel
from sqldim.core.mixins import SCD2Mixin

T = TypeVar("T", bound=DimensionModel)

class SCDResult:
    def __init__(self):
        self.inserted: int = 0
        self.versioned: int = 0
        self.unchanged: int = 0

class SCDHandler(Generic[T]):
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
        new_row = self.model(**record, checksum=checksum, is_current=True)
        self.session.add(new_row)
        result.inserted += 1

    def _handle_type1(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
        for col, val in record.items():
            setattr(existing, col, val)
        existing.checksum = checksum
        self.session.add(existing)
        result.versioned += 1

    def _handle_type3(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult") -> None:
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
        return [
            col_name for col_name in self.model.model_fields
            if self._get_dim_meta(col_name).get("scd") in (2, None)
        ]

    def _handle_type6_type1(
        self, existing: Any, record: Dict[str, Any], checksum: str, diff: Any, result: "SCDResult"
    ) -> None:
        for col in diff.changed_columns:
            setattr(existing, col, record.get(col))
        existing.checksum = checksum
        self.session.add(existing)
        result.versioned += 1

    def _handle_type6(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult", nk_value: Any) -> None:
        from sqldim.scd.detection import ColumnarDetection
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
        existing.is_current = False
        existing.valid_to = datetime.now(timezone.utc)
        self.session.add(existing)
        new_row = self.model(**record.copy(), checksum=checksum, is_current=True)
        self.session.add(new_row)
        result.versioned += 1

    def _handle_changed_record(self, existing: Any, record: Dict[str, Any], checksum: str, result: "SCDResult", nk_value: Any) -> None:
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
        result = SCDResult()
        for record in records:
            await self._process_one(record, result)
        self.session.commit()
        return result
