from datetime import date, datetime, timezone
from typing import ClassVar, Optional, Any
from sqlmodel import SQLModel
from sqldim.core.kimball.fields import Field


class SCD2Mixin(SQLModel):
    """
    Base mixin for SCD Type 2 tracking.
    Default period columns are datetimes. For non-datetime periods (like integer seasons),
    override valid_from/valid_to in the subclass.
    """

    #: Unique role tag — DimensionModel blocks two mixins with the same role.
    __dim_mixin_role__: ClassVar[str] = "scd_validity_columns"
    #: SCD types that may legally pair with this mixin.
    #: SCDHandler always needs validity/checksum columns for types 1, 2, 3, and 6.
    __dim_mixin_compatible_types__: ClassVar[frozenset] = frozenset({1, 2, 3, 6})

    valid_from: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), index=True
    )
    valid_to: Optional[datetime] = Field(default=None, index=True, nullable=True)
    is_current: bool = Field(default=True, index=True)
    checksum: Optional[str] = Field(default=None, index=True, nullable=True)


_SCD3_SKIP = {"id", "is_current", "valid_from", "valid_to", "checksum"}


def _validate_scd3_fields(cls: type) -> None:
    field_names: set[str] = set(cls.__annotations__)
    orphan_prevs = [
        f for f in field_names if f.startswith("prev_") and f[5:] not in field_names
    ]
    if orphan_prevs:
        raise TypeError(
            f"{cls.__name__}: SCD3Mixin found `prev_*` columns with no matching "
            f"current column: {orphan_prevs}. Either add the current column or "
            f"rename the previous column to match."
        )


class SCD3Mixin(SQLModel):
    """
    Marker mixin for SCD Type 3 (current + one prior value per tracked attribute).

    Convention
    ----------
    For every tracked attribute ``X`` that should retain its previous value,
    declare **both** columns in the subclass:

    .. code-block:: python

        class EmployeeDimension(SCD3Mixin, DimensionModel, table=True):
            __natural_key__  = ["employee_id"]
            __scd_type__     = 3

            employee_id: str
            region:      str                     # current value
            prev_region: Optional[str] = None   # previous value (rotated by SCDHandler)

    The pair ``("region", "prev_region")`` must be registered so the
    handler knows which column to rotate.  Pass it to ``SCDHandler`` via
    ``track_columns`` (the handler discovers ``prev_*`` partners
    automatically) or to ``LazyType3Processor`` via ``column_pairs``.

    Validation
    ----------
    ``__init_subclass__`` enforces that every column without a ``prev_``
    prefix has a matching ``prev_<col>`` sibling when ``__scd_type__ == 3``.
    This catches naming mismatches at class-definition time, not at runtime.
    """

    #: Unique role tag — DimensionModel blocks two mixins with the same role.
    __dim_mixin_role__: ClassVar[str] = "scd_rotation_columns"
    #: SCD types that may legally pair with this mixin.
    #: SCD6 also uses prev_* rotation alongside validity tracking.
    __dim_mixin_compatible_types__: ClassVar[frozenset] = frozenset({3, 6})

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if getattr(cls, "__scd_type__", None) != 3:
            return
        _validate_scd3_fields(cls)


class CumulativeMixin(SQLModel):
    """
    Mixin for Cumulative Dimensions/Facts (array-of-structs pattern).
    Provides helpers for managing history arrays.
    """

    def current_value(self, column: str) -> Any:
        """Returns the most recent element in the cumulative array."""
        arr = getattr(self, column, [])
        return arr[-1] if arr else None

    def first_value(self, column: str) -> Any:
        """Returns the first element in the cumulative array."""
        arr = getattr(self, column, [])
        return arr[0] if arr else None


class DatelistMixin(SQLModel):
    """
    Mixin for tracking activity via dates lists.
    Enables bitmask-based analysis (L7, L28).
    """

    def to_bitmask(self, reference_date: date, window: int = 32) -> int:
        """Converts a list of active dates into an integer bitmask."""
        # Implementation based on the provided theoretical requirement
        mask = 0
        dates = getattr(self, "dates_active", [])
        if not dates:
            return mask

        for active_date in dates:
            # Handle both date objects and ISO strings (common in JSON serialization)
            if isinstance(active_date, str):
                active_date = date.fromisoformat(active_date)
            diff = (reference_date - active_date).days
            if 0 <= diff < window:
                mask |= 1 << diff
        return mask

    def activity_in_window(self, days: int, reference_date: date) -> bool:
        """Returns True if any activity occurred within the last N days."""
        mask = self.to_bitmask(reference_date, window=days)
        return mask > 0

    def l7(self, reference_date: date) -> int:
        """Count of active days in the last 7 days."""
        mask = self.to_bitmask(reference_date, window=7)
        return bin(mask).count("1")

    def l28(self, reference_date: date) -> int:
        """Count of active days in the last 28 days."""
        mask = self.to_bitmask(reference_date, window=28)
        return bin(mask).count("1")
