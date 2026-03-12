from typing import Any, Dict, List, Optional, Tuple, Type
from itertools import product
from sqlmodel import Session, select
from sqldim import DimensionModel, Field


def make_junk_dimension(
    name: str,
    flags: Dict[str, List[Any]],
) -> Type[DimensionModel]:
    """
    Dynamically creates a JunkDimension model class for a given set of
    low-cardinality flag columns and their possible values.

    Usage:
        SalesFlags = make_junk_dimension("SalesFlags", {
            "is_promo": [True, False],
            "is_return": [True, False],
            "channel": ["web", "store", "phone"],
        })

    The resulting model has one row per unique combination of flag values.
    """
    annotations: Dict[str, Any] = {
        "id": int,
        **{col: Optional[Any] for col in flags},
    }
    fields: Dict[str, Any] = {
        "id": Field(primary_key=True, surrogate_key=True),
        **{col: Field(default=None, nullable=True) for col in flags},
        "__annotations__": annotations,
        "__natural_key__": list(flags.keys()),
    }
    model_cls = type(name, (DimensionModel,), {**fields, "__tablename__": name.lower()})
    return model_cls  # type: ignore


def populate_junk_dimension(
    model: Type[DimensionModel],
    flags: Dict[str, List[Any]],
    session: Session,
) -> List[Any]:
    """
    Populate a junk dimension with all combinations of flag values (idempotent).
    Returns list of inserted rows.
    """
    cols = list(flags.keys())
    combos = list(product(*[flags[c] for c in cols]))
    rows = []
    for combo in combos:
        row_data = dict(zip(cols, combo))
        # Check if already exists
        conditions = [model.__table__.c[col] == val for col, val in row_data.items()]
        existing = session.exec(select(model).where(*conditions)).first()
        if not existing:
            row = model(**row_data)
            session.add(row)
            rows.append(row)
    session.commit()
    return rows
