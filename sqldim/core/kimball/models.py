from typing import Optional, List, ClassVar, Sequence
from sqlmodel import SQLModel
from sqlalchemy import Index
from sqldim.core.kimball.fields import Field


def _merge_index_into_table_args(cls, idx: Index) -> None:
    """Append *idx* to *cls.__table_args__*, preserving any trailing options dict."""
    existing = getattr(cls, "__table_args__", ())
    if isinstance(existing, dict):
        raise TypeError(
            f"{cls.__name__}: __table_args__ is a dict; cannot merge indexes. "
            "Use a tuple of (constraints..., {'opts': ...}) instead."
        )
    if existing and isinstance(existing[-1], dict):
        cls.__table_args__ = (*existing[:-1], idx, existing[-1])
    else:
        cls.__table_args__ = (*existing, idx)


class DimensionModel(SQLModel):
    __natural_key__: ClassVar[List[str]] = []
    __scd_type__: ClassVar[int] = 2

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        keys: Sequence[str] = getattr(cls, "__natural_key__", [])
        if not keys:
            return
        table_name: str = getattr(cls, "__tablename__", None) or cls.__name__.lower()
        idx = Index(f"ix_{table_name}_nk", *keys)
        _merge_index_into_table_args(cls, idx)

class FactModel(SQLModel):
    __grain__: ClassVar[Optional[str]] = None
    __strategy__: ClassVar[Optional[str]] = None  # e.g. "bulk", "upsert", "merge", "accumulating"

class BridgeModel(SQLModel):
    """
    Base class for Kimball Bridge Tables handling many-to-many relationships.
    Includes weighting factors for multi-valued dimensions to prevent double-counting.

    Subclasses should declare ``__bridge_keys__`` as a list of column names that
    form the composite unique key (e.g. ``["sale_id", "rep_id"]``).  A composite
    unique index is automatically created on these columns at class-definition time.

    Example
    -------
    class SalesRepBridge(BridgeModel, table=True):
        __bridge_keys__ = ["sale_id", "rep_id"]
        id: int = Field(primary_key=True)
        sale_id: int
        rep_id: int
    """
    __bridge_keys__: ClassVar[List[str]] = []
    weight: float = Field(default=1.0, description="Allocation factor (0.0 to 1.0)")

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        keys: Sequence[str] = getattr(cls, "__bridge_keys__", [])
        if not keys:
            return
        idx = Index(f"uq_{cls.__tablename__}_{'_'.join(keys)}", *keys, unique=True)
        _merge_index_into_table_args(cls, idx)
