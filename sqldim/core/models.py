from typing import Optional, List, ClassVar
from sqlmodel import SQLModel
from sqldim.core.fields import Field

class DimensionModel(SQLModel):
    __natural_key__: ClassVar[List[str]] = []
    __scd_type__: ClassVar[int] = 2

class FactModel(SQLModel):
    __grain__: ClassVar[Optional[str]] = None
    __strategy__: ClassVar[Optional[str]] = None  # e.g. "bulk", "upsert", "merge", "accumulating"

class BridgeModel(SQLModel):
    """
    Base class for Kimball Bridge Tables handling many-to-many relationships.
    Includes weighting factors for multi-valued dimensions to prevent double-counting.
    """
    weight: float = Field(default=1.0, description="Allocation factor (0.0 to 1.0)")
