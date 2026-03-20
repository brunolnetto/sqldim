from typing import Any
from itertools import product
from sqlmodel import Session, select
from sqldim import DimensionModel, Field


def _flag_value_sql(v: Any) -> str:
    if isinstance(v, str):
        return f"('{v}')"
    if isinstance(v, bool):
        return f"({str(v).upper()})"
    return f"({v})"


# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) population — no Python data, no OOM risk
# ---------------------------------------------------------------------------


def populate_junk_dimension_lazy(
    flags: dict[str, list[Any]],
    table_name: str,
    sink,
    batch_size: int = 100_000,
    con=None,
) -> int:
    """
    Populate a junk dimension with all flag combinations using DuckDB
    CROSS JOIN — no Python ``itertools.product`` or row-by-row inserts.

    The Cartesian product is computed entirely inside DuckDB; data flows
    directly from the query engine into the sink without touching Python.

    Parameters
    ----------
    flags : ``{column_name: [value1, value2, ...]}``
    table_name : target table in the sink
    sink : SinkAdapter implementation
    batch_size : write buffer hint
    con : existing DuckDB connection (created if None)

    Returns
    -------
    int — number of rows written
    """
    import duckdb as _duckdb

    _con = con or _duckdb.connect()
    cols = list(flags.keys())

    # Register each flag column as a DuckDB VALUES-based view
    for col in cols:
        vals = flags[col]
        value_rows = ", ".join(_flag_value_sql(v) for v in vals)
        _con.execute(f"""
            CREATE OR REPLACE VIEW _flag_{col} AS
            SELECT {col} FROM (VALUES {value_rows}) AS t({col})
        """)

    # CROSS JOIN all flag views to produce the full Cartesian product
    join_sql = " CROSS JOIN ".join(f"_flag_{col}" for col in cols)
    col_list = ", ".join(cols)
    _con.execute(f"""
        CREATE OR REPLACE VIEW all_combos AS
        SELECT {col_list}
        FROM {join_sql}
    """)

    return sink.write(_con, "all_combos", table_name, batch_size)


def make_junk_dimension(
    name: str,
    flags: dict[str, list[Any]],
) -> type[DimensionModel]:
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
    annotations: dict[str, Any] = {
        "id": int,
        **{col: Any | None for col in flags},
    }
    fields: dict[str, Any] = {
        "id": Field(primary_key=True, surrogate_key=True),
        **{col: Field(default=None, nullable=True) for col in flags},
        "__annotations__": annotations,
        "__natural_key__": list(flags.keys()),
    }
    model_cls = type(name, (DimensionModel,), {**fields, "__tablename__": name.lower()})
    return model_cls  # type: ignore


def populate_junk_dimension(
    model: type[DimensionModel],
    flags: dict[str, list[Any]],
    session: Session,
) -> list[Any]:
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
