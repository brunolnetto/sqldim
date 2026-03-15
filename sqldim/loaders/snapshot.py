from datetime import date
from typing import Any, Dict, List, Type
from sqlmodel import Session
from sqldim.core.models import DimensionModel, FactModel

# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loaders — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyTransactionLoader:
    """
    Append-only transaction fact loader. Speaks only DuckDB SQL.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyTransactionLoader(sink)
            rows = loader.load("events.parquet", "fact_events")
    """

    def __init__(self, sink, batch_size: int = 100_000, con=None):
        import duckdb as _duckdb

        self.sink       = sink
        self.batch_size = batch_size
        self._con       = con or _duckdb.connect()

    def load(self, source, table_name: str) -> int:
        """
        Append all rows from *source* (Parquet path or DuckDB view) into
        *table_name* via the sink.  Returns rows written.
        """
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW pending_facts AS
            SELECT * FROM ({_sql})
        """)
        return self.sink.write(self._con, "pending_facts", table_name, self.batch_size)


class LazySnapshotLoader:
    """
    Periodic snapshot fact loader.  Injects *snapshot_date* as a literal
    column and delegates the write to the sink — no Python row iteration.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazySnapshotLoader(sink, snapshot_date="2024-03-31")
            rows = loader.load("balances.parquet", "fact_account_balance")
    """

    def __init__(
        self,
        sink,
        snapshot_date,
        date_field: str = "snapshot_date",
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.sink          = sink
        self.snapshot_date = snapshot_date
        self.date_field    = date_field
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def load(self, source, table_name: str) -> int:
        """
        Read *source*, inject *snapshot_date*, and write to *table_name*.
        Returns rows written.
        """
        sd = self.snapshot_date
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW snapshot_rows AS
            SELECT *,
                   '{sd}'::DATE AS {self.date_field}
            FROM ({_sql})
        """)
        return self.sink.write(self._con, "snapshot_rows", table_name, self.batch_size)

class SnapshotLoader:
    """
    Loads a Periodic Snapshot Fact Table — one row per entity per time period.

    Usage:
        loader = SnapshotLoader(
            fact=AccountBalanceFact,
            dimension=AccountDimension,
            snapshot_date=date.today(),
            session=session,
        )
        loader.load(records)
    """

    def __init__(
        self,
        fact: Type[FactModel],
        dimension: Type[DimensionModel],
        snapshot_date: date,
        session: Session,
        date_field: str = "snapshot_date",
    ):
        self.fact = fact
        self.dimension = dimension
        self.snapshot_date = snapshot_date
        self.session = session
        self.date_field = date_field

    def load(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert snapshot rows for the given date.
        Each record is a dict of fact column values. The snapshot_date is
        automatically injected into the date_field column.

        Returns the number of rows inserted.
        """
        inserted = 0
        for record in records:
            row_data = {**record, self.date_field: self.snapshot_date.isoformat()}
            row = self.fact(**row_data)
            self.session.add(row)
            inserted += 1
        self.session.commit()
        return inserted
