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

    def load_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
    ):
        """Process a streaming source, appending each micro-batch to *table_name*.

        Parameters
        ----------
        source      : :class:`~sqldim.sources.stream.StreamSourceAdapter`
        table_name  : Target table in the sink.
        batch_size  : Rows per micro-batch (passed to ``source.stream``).
        max_batches : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch    : ``callback(batch_num: int, rows_written: int)`` invoked
                      after each successful write.

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from sqldim.sources.stream import StreamResult

        _log = logging.getLogger(__name__)
        result = StreamResult()

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            try:
                self._con.execute(f"""
                    CREATE OR REPLACE VIEW pending_facts AS
                    SELECT * FROM ({sql_fragment})
                """)
                rows_written = self.sink.write(
                    self._con, "pending_facts", table_name, self.batch_size
                )
                offset = source.checkpoint()
                source.commit(offset)

                result.inserted += rows_written
                result.batches_processed += 1

                if on_batch:
                    on_batch(i, rows_written)

            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed: %s", i, exc)

            finally:
                try:
                    self._con.execute("DROP VIEW IF EXISTS pending_facts")
                except Exception:
                    pass

        return result


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

    def load_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
    ):
        """Process a streaming source, injecting *snapshot_date* into each micro-batch.

        Parameters
        ----------
        source      : :class:`~sqldim.sources.stream.StreamSourceAdapter`
        table_name  : Target table in the sink.
        batch_size  : Rows per micro-batch (passed to ``source.stream``).
        max_batches : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch    : ``callback(batch_num: int, rows_written: int)`` invoked
                      after each successful write.

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from sqldim.sources.stream import StreamResult

        _log = logging.getLogger(__name__)
        sd = self.snapshot_date
        result = StreamResult()

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            try:
                self._con.execute(f"""
                    CREATE OR REPLACE VIEW snapshot_rows AS
                    SELECT *,
                           '{sd}'::DATE AS {self.date_field}
                    FROM ({sql_fragment})
                """)
                rows_written = self.sink.write(
                    self._con, "snapshot_rows", table_name, self.batch_size
                )
                offset = source.checkpoint()
                source.commit(offset)

                result.inserted += rows_written
                result.batches_processed += 1

                if on_batch:
                    on_batch(i, rows_written)

            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed: %s", i, exc)

            finally:
                try:
                    self._con.execute("DROP VIEW IF EXISTS snapshot_rows")
                except Exception:
                    pass

        return result


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
