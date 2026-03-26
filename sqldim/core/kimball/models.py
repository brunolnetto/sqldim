from typing import Any, ClassVar, Sequence
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


def _check_dim_mixin_roles(cls) -> None:
    """Raise TypeError if two mixins share the same ``__dim_mixin_role__``."""
    roles: dict[str, list[str]] = {}
    for b in cls.__mro__[1:]:
        role = getattr(b, "__dim_mixin_role__", None)
        if role is not None:
            roles.setdefault(role, []).append(b.__name__)
    for role, names in roles.items():
        if len(names) > 1:
            raise TypeError(
                f"{cls.__name__} inherits multiple mixins with role '{role}' "
                f"({', '.join(names)}). Only one mixin per column role is allowed."
            )


def _check_dim_mixin_compat(cls) -> None:
    """Raise TypeError if a mixin's compatible SCD types exclude the declared ``__scd_type__``."""
    declared_type: int = getattr(cls, "__scd_type__", 2)
    for b in cls.__mro__:
        if b is cls:
            continue
        compatible = getattr(b, "__dim_mixin_compatible_types__", None)
        if compatible is not None and declared_type not in compatible:
            raise TypeError(
                f"{cls.__name__} uses {b.__name__} "
                f"(compatible with SCD types {sorted(compatible)}) "
                f"but declares __scd_type__ = {declared_type}. "
                f"Use a compatible __scd_type__ or the appropriate mixin."
            )


class DimensionModel(SQLModel):
    __natural_key__: ClassVar[list[str]] = []
    __scd_type__: ClassVar[int] = 2

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        _check_dim_mixin_roles(cls)
        _check_dim_mixin_compat(cls)
        keys: Sequence[str] = getattr(cls, "__natural_key__", [])
        if not keys:
            return
        table_name: str = getattr(cls, "__tablename__", None) or cls.__name__.lower()
        idx = Index(f"ix_{table_name}_nk", *keys)
        _merge_index_into_table_args(cls, idx)

    @classmethod
    def table_name(cls) -> str:
        """The database table name for this model.

        Public shorthand for ``cls.__tablename__`` so callers never need to
        reference an SQLAlchemy/SQLModel dunder directly::

            loader = LazyTransactionLoader(sink)
            loader.load(source, DimensionModel.table_name())
        """
        return getattr(cls, "__tablename__", None) or cls.__name__.lower()

    @classmethod
    def as_loader(cls, session, track_columns=None):
        """Return an :class:`~sqldim.core.kimball.dimensions.scd.handler.SCDHandler`
        for this dimension, with *track_columns* inferred from the model's fields
        when not supplied explicitly.

        Example
        -------
        .. code-block:: python

            handler = CustomerDim.as_loader(session)
            await handler.process([{"customer_id": "C1", "city": "NYC"}])
        """
        from sqldim.core.kimball.dimensions.scd.handler import SCDHandler

        if track_columns is None:
            _skip = {"id", "valid_from", "valid_to", "is_current", "checksum"}
            nk = set(getattr(cls, "__natural_key__", []))
            track_columns = [
                f for f in cls.model_fields if f not in _skip and f not in nk
            ]
        return SCDHandler(cls, session, track_columns=track_columns)


class FactModel(SQLModel):
    """Base class for Kimball fact tables.

    Declare ``__strategy__`` to select the loading strategy, then set the
    corresponding strategy ClassVars so :meth:`as_loader` can build the right
    loader automatically.

    Strategy → required ClassVars
    ──────────────────────────────
    ``"bulk"`` / ``None``    — none
    ``"snapshot"``           — ``__snapshot_date_field__`` (has default)
    ``"accumulating"``       — ``__match_column__`` or ``__natural_key__[0]``;
                               ``__milestones__`` or inferred from datetime | None fields
    ``"cumulative"``         — ``__partition_key__``; ``__cumulative_column__``;
                               ``__metric_columns__``
    ``"bitmask"``            — ``__partition_key__``; ``__dates_column__``
    ``"array_metric"``       — ``__partition_key__``; ``__value_column__``;
                               ``__metric_name__``
    ``"edge_projection"``    — ``__subject_key__``; ``__object_key__``
    """

    __grain__: ClassVar[str | None] = None
    __strategy__: ClassVar[str | None] = None

    # --- snapshot -----------------------------------------------------------
    __snapshot_date_field__: ClassVar[str] = "snapshot_date"

    # --- accumulating -------------------------------------------------------
    __match_column__: ClassVar[str | None] = None
    __milestones__: ClassVar[list[str]] = []

    # --- cumulative ---------------------------------------------------------
    __partition_key__: ClassVar[str | None] = None
    __cumulative_column__: ClassVar[str | None] = None
    __metric_columns__: ClassVar[list[str]] = []
    __current_period_column__: ClassVar[str] = "current_season"

    # --- bitmask ------------------------------------------------------------
    __dates_column__: ClassVar[str | None] = None
    __window_days__: ClassVar[int] = 32

    # --- array_metric -------------------------------------------------------
    __value_column__: ClassVar[str | None] = None
    __metric_name__: ClassVar[str | None] = None

    # --- edge_projection ----------------------------------------------------
    __subject_key__: ClassVar[str | None] = None
    __object_key__: ClassVar[str | None] = None
    __property_map__: ClassVar[dict[str, str | None] | None] = None
    __self_join__: ClassVar[bool] = False
    __self_join_key__: ClassVar[str | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        strategy: str | None = getattr(cls, "__strategy__", None)
        if strategy not in _STRATEGY_REQUIRED:
            return
        required = _STRATEGY_REQUIRED[strategy]
        for attr in required:
            if getattr(cls, attr, None) is None and not getattr(
                cls, "__natural_key__", []
            ):
                raise TypeError(
                    f"{cls.__name__}: strategy '{strategy}' requires "
                    f"'{attr}' to be set (or '__natural_key__' for inference)."
                )

    @classmethod
    def table_name(cls) -> str:
        """The database table name for this model.

        Public shorthand for ``cls.__tablename__`` so callers never need to
        reference an SQLAlchemy/SQLModel dunder directly::

            loader = LazyTransactionLoader(sink)
            loader.load(source, FactModel.table_name())
        """
        return getattr(cls, "__tablename__", None) or cls.__name__.lower()

    @classmethod
    def as_loader(cls, sink, **kwargs):
        """Return the appropriate lazy loader for this model, ready to use.

        Loader type is determined by ``__strategy__``.  Strategy-specific
        params are read from class-level ClassVars; field-type inference fills
        any gaps.  Runtime values (``snapshot_date``, ``reference_date``,
        ``month_start``) are passed as keyword arguments here.

        Example
        -------
        .. code-block:: python

            loader = BalanceFact.as_loader(sink, snapshot_date="2024-03-31")
            loader.load("balances.parquet")

            loader = OrderPipeline.as_loader(sink)
            loader.process("orders.parquet")
        """
        from sqldim.core.loaders._factory import build_lazy_loader

        return build_lazy_loader(cls, sink, **kwargs)


# Required ClassVars per strategy that cannot be meaningfully inferred.
# A missing ClassVar is tolerated only if __natural_key__ provides a fallback.
_STRATEGY_REQUIRED: dict[str, list[str]] = {
    "edge_projection": ["__subject_key__", "__object_key__"],
}


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

    __bridge_keys__: ClassVar[list[str]] = []
    weight: float = Field(default=1.0, description="Allocation factor (0.0 to 1.0)")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        keys: Sequence[str] = getattr(cls, "__bridge_keys__", [])
        if not keys:
            return
        idx = Index(f"uq_{cls.__tablename__}_{'_'.join(keys)}", *keys, unique=True)
        _merge_index_into_table_args(cls, idx)
