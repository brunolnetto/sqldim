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
        info = kwargs.get("info")
        if info:
            return info
            
        # Fallback to json_schema_extra if present (modern Pydantic)
        extra = getattr(field, "json_schema_extra", {})
        if isinstance(extra, dict):
            return extra
            
        return {}

    async def process(self, records: List[Dict[str, Any]]) -> SCDResult:
        result = SCDResult()
        scd_type = getattr(self.model, "__scd_type__", 2)
        
        for record in records:
            nk_field = getattr(self.model, "__natural_key__", ["id"])[0]
            nk_value = record.get(nk_field)
            checksum = self.compute_checksum(record)
            
            # 3. Look for existing current record
            stmt = select(self.model).where(getattr(self.model, nk_field) == nk_value)
            
            # Only filter by is_current if the model has that attribute (SCD Type 2/6)
            if hasattr(self.model, "is_current"):
                stmt = stmt.where(getattr(self.model, "is_current") == True)
                
            existing = self.session.exec(stmt).first()
            
            if not existing:
                # NEW RECORD
                new_row = self.model(**record, checksum=checksum, is_current=True)
                self.session.add(new_row)
                result.inserted += 1
            elif existing.checksum != checksum:
                # DATA CHANGED
                if scd_type == 1:
                    # Type 1: Overwrite
                    for col, val in record.items():
                        setattr(existing, col, val)
                    existing.checksum = checksum
                    self.session.add(existing)
                    result.versioned += 1 
                elif scd_type == 3:
                    # Type 3: Rotate
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
                elif scd_type == 6:
                    # Type 6: Hybrid
                    from sqldim.scd.detection import ColumnarDetection
                    det = ColumnarDetection(self.track_columns)
                    diff = det.diff(record, existing, nk_value)
                    
                    type2_cols = []
                    for col_name in self.model.model_fields:
                        meta = self._get_dim_meta(col_name)
                        if meta.get("scd") == 2 or meta.get("scd") is None:
                            type2_cols.append(col_name)
                    
                    changed_type2 = any(c in type2_cols for c in diff.changed_columns)
                    if not changed_type2:
                        # Only Type 1 changed -> Overwrite
                        for col in diff.changed_columns:
                            setattr(existing, col, record.get(col))
                        existing.checksum = checksum
                        self.session.add(existing)
                        result.versioned += 1
                        continue

                    # Fallthrough to Type 2 behavior
                    existing.is_current = False
                    existing.valid_to = datetime.now(timezone.utc)
                    self.session.add(existing)
                    new_data = record.copy()
                    new_row = self.model(**new_data, checksum=checksum, is_current=True)
                    self.session.add(new_row)
                    result.versioned += 1
                else:
                    # Default / Type 2: Versioning
                    existing.is_current = False
                    existing.valid_to = datetime.now(timezone.utc)
                    self.session.add(existing)
                    new_data = record.copy()
                    new_row = self.model(**new_data, checksum=checksum, is_current=True)
                    self.session.add(new_row)
                    result.versioned += 1
            else:
                # UNCHANGED
                result.unchanged += 1
                
        self.session.commit()
        return result
