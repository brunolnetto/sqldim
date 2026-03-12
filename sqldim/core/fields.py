from typing import Any, Optional, Type
from sqlmodel import Field as SQLModelField

def Field(
    default: Any = ...,
    *,
    default_factory: Optional[Any] = None,
    primary_key: bool = False,
    foreign_key: Optional[str] = None,
    index: bool = False,
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
    **kwargs: Any,
) -> Any:
    """
    Extends SQLModel's Field with dimensional metadata stored in sa_column_kwargs
    for compatibility across SQLModel/SQLAlchemy versions.
    """
    # Collect dimensional metadata
    dim_meta = {
        "surrogate_key": surrogate_key,
        "natural_key": natural_key,
        "measure": measure,
        "additive": additive,
        "dimension": dimension,
        "role": role,
        "scd": scd,
        "previous_column": previous_column,
    }
    
    # Store in sa_column_kwargs so it persists to the SQLAlchemy Column info
    sa_column_kwargs = kwargs.get("sa_column_kwargs", {})
    if "info" not in sa_column_kwargs:
        sa_column_kwargs["info"] = {}
    sa_column_kwargs["info"].update(dim_meta)
    kwargs["sa_column_kwargs"] = sa_column_kwargs

    return SQLModelField(
        default,
        default_factory=default_factory,
        primary_key=primary_key,
        foreign_key=foreign_key,
        index=index,
        nullable=nullable,
        **kwargs,
    )
