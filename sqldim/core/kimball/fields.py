"""Dimensional-aware SQLModel Field factory.

Extends :func:`sqlmodel.Field` with extra kwargs (``surrogate_key``,
``natural_key``, ``measure``, ``dimension``, ``role``, ``scd``, etc.) that
are persisted in the SA column's ``info`` dict for use by the schema graph,
loaders, and SCD handlers.

Design note — FK constraints vs. FK indexes
--------------------------------------------
By default, passing ``foreign_key=`` to this ``Field()`` does **not** create a
database-level ``REFERENCES`` constraint.  Instead it:

1. Stores the FK target in ``column.info["foreign_key_target"]`` so that the
   schema graph and schema-discovery tools can still introspect relationships.
2. Marks the column for automatic indexing (same as ``index=True``), which is
   the primary performance benefit in analytical workloads.

Rationale:
- DB-level FK constraints are unsupported in DuckDB/Parquet and counterproductive
  in OLAP scenarios (enforcement overhead, cross-module definition-order issues).
- All relationship discovery in sqldim goes through ``column.info["dimension"]``
  and ``column.info["foreign_key_target"]``, not through SA FK introspection.

To explicitly opt-in to a database FK constraint (e.g. for PostgreSQL OLTP
side-tables), pass ``constraint=True``:

    order_id: int = Field(foreign_key="order.id", constraint=True)
"""
from typing import Any, Optional, Type
from sqlmodel import Field as SQLModelField

_UNSET = object()

def Field(
    default: Any = ...,
    *,
    default_factory: Optional[Any] = None,
    primary_key: bool = False,
    foreign_key: Optional[str] = None,
    index: Any = _UNSET,
    nullable: Optional[bool] = None,
    # Dimensional metadata
    surrogate_key: bool = False,
    natural_key: bool = False,
    measure: bool = False,
    additive: bool = True,
    dimension: Optional[Type] = None,
    role: Optional[str] = None,
    scd: Optional[int] = None,
    previous_column: Optional[str] = None,
    # FK constraint opt-in (off by default for analytical workloads)
    constraint: bool = False,
    **kwargs: Any,
) -> Any:
    """
    Extends SQLModel's Field with dimensional metadata stored in
    ``sa_column_kwargs["info"]`` for compatibility across SQLModel/SQLAlchemy
    versions.

    Parameters
    ----------
    foreign_key:
        The FK target in ``"table.column"`` format.  Stored in
        ``column.info["foreign_key_target"]`` and auto-indexes this column.
        Does **not** create a DB-level ``REFERENCES`` constraint unless
        ``constraint=True`` is also passed.
    constraint:
        When ``True``, the ``foreign_key`` target is passed through to
        SQLModel/SQLAlchemy, creating a real ``REFERENCES`` DDL constraint.
        Off by default — use only when DB-level referential integrity is
        explicitly required (e.g. PostgreSQL OLTP side-tables).
    """
    # Collect dimensional metadata — FK target stored for schema introspection
    dim_meta = {
        "surrogate_key": surrogate_key,
        "natural_key": natural_key,
        "measure": measure,
        "additive": additive,
        "dimension": dimension,
        "role": role,
        "scd": scd,
        "previous_column": previous_column,
        "foreign_key_target": foreign_key,
    }

    # Auto-index FK-like columns (whether or not a DB constraint is created).
    # Using _UNSET sentinel distinguishes "not specified" from explicit False.
    if index is _UNSET and foreign_key is not None:
        index = True

    # Only propagate the FK to SQLModel when an explicit DB constraint is wanted.
    # By default, analytical models skip the constraint to avoid MetaData
    # resolution errors and unnecessary DB enforcement overhead.
    sa_fk = foreign_key if constraint else None

    # Store in sa_column_kwargs so metadata persists to the SQLAlchemy Column info
    sa_column_kwargs = kwargs.get("sa_column_kwargs", {})
    if "info" not in sa_column_kwargs:
        sa_column_kwargs["info"] = {}
    sa_column_kwargs["info"].update(dim_meta)
    kwargs["sa_column_kwargs"] = sa_column_kwargs

    return SQLModelField(
        default,
        default_factory=default_factory,
        primary_key=primary_key,
        foreign_key=sa_fk,
        index=index,
        nullable=nullable,
        **kwargs,
    )
