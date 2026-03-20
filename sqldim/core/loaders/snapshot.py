from __future__ import annotations

import asyncio
from datetime import date
from itertools import islice
from sqlmodel import Session
from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.core.loaders._utils import _resolve_table, _assert_not_dimension

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

        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._model_cls = None  # set by factory when created via as_loader()

    def load(self, source, table: str | type) -> int:
        """
        Append all rows from *source* (Parquet path or DuckDB view) into
        *table* via the sink.  Returns rows written.  *table* may be a
        table-name string or a :class:`~sqldim.core.kimball.models.FactModel`
        subclass (DB table name is derived automatically).
        """
        _assert_not_dimension(table, "LazyTransactionLoader")
        table_name = _resolve_table(table)
        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW pending_facts AS
            SELECT * FROM ({_sql})
        """)
        return self.sink.write(self._con, "pending_facts", table_name, self.batch_size)

    async def aload(self, source, table=None, **kwargs) -> int:
        """Async wrapper around :meth:`load` — runs in a thread pool executor.

        If *table* is ``None`` and the loader was created via
        ``FactModel.as_loader()``, the bound model class is used as the table.
        """
        _table = table if table is not None else self._model_cls
        if _table is None:
            raise TypeError(
                "LazyTransactionLoader.aload(): 'table' is required when the "
                "loader was not created via FactModel.as_loader()."
            )
        return await asyncio.to_thread(self.load, source, _table, **kwargs)

    def _run_batch(
        self, i: int, sql_fragment: str, table_name: str, source, on_batch
    ) -> int:
        """Write one micro-batch; return rows_written.  Caller handles exceptions."""
        try:
            self._con.execute(f"""
                CREATE OR REPLACE VIEW pending_facts AS
                SELECT * FROM ({sql_fragment})
            """)
            rows_written = self.sink.write(
                self._con, "pending_facts", table_name, self.batch_size
            )
            source.commit(source.checkpoint())
            if on_batch:
                on_batch(i, rows_written)
            return rows_written
        finally:
            try:
                self._con.execute("DROP VIEW IF EXISTS pending_facts")
            except Exception:
                pass

    def load_stream(
        self,
        source,
        table: str | type,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
    ):
        """Process a streaming source, appending each micro-batch to *table*.

        Parameters
        ----------
        source      : :class:`~sqldim.sources.stream.StreamSourceAdapter`
        table       : Target table name or FactModel subclass.
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
        _assert_not_dimension(table, "LazyTransactionLoader")
        table_name = _resolve_table(table)
        result = StreamResult()

        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break
            try:
                result.inserted += self._run_batch(
                    i, sql_fragment, table_name, source, on_batch
                )
                result.batches_processed += 1
            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed: %s", i, exc)

        return result


class LazySnapshotLoader:
    """
    Periodic snapshot fact loader.  Injects *snapshot_date* as a literal
    column and delegates the write to the sink — no Python row iteration.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            # Date at construction (one snapshot per loader instance)
            loader = LazySnapshotLoader(sink, snapshot_date="2024-03-31")
            rows = loader.load("balances.parquet", "fact_account_balance")

            # Date at call time (reuse loader across multiple snapshots)
            loader = LazySnapshotLoader(sink)
            rows = loader.load("balances.parquet", "fact_account_balance",
                               snapshot_date="2024-03-31")
    """

    def __init__(
        self,
        sink,
        snapshot_date=None,
        date_field: str = "snapshot_date",
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.sink = sink
        self.snapshot_date = snapshot_date
        self.date_field = date_field
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._model_cls = None  # set by factory when created via as_loader()

    def load(self, source, table: str | type, *, snapshot_date=None) -> int:
        """
        Read *source*, inject *snapshot_date*, and write to *table*.
        Returns rows written.  *table* may be a table-name string or a
        :class:`~sqldim.core.kimball.models.FactModel` subclass.

        *snapshot_date* may be provided here at call time (overrides the
        instance value) or set once at construction for a single-date loader.
        Raises :exc:`TypeError` if neither is present.
        """
        sd = snapshot_date if snapshot_date is not None else self.snapshot_date
        if sd is None:
            raise TypeError(
                "LazySnapshotLoader.load(): 'snapshot_date' must be supplied "
                "either at construction or as a keyword argument to load()."
            )
        _assert_not_dimension(table, "LazySnapshotLoader")
        table_name = _resolve_table(table)
        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW snapshot_rows AS
            SELECT *,
                   '{sd}'::DATE AS {self.date_field}
            FROM ({_sql})
        """)
        return self.sink.write(self._con, "snapshot_rows", table_name, self.batch_size)

    async def aload(self, source, table=None, *, snapshot_date=None) -> int:
        """Async wrapper around :meth:`load` — runs in a thread pool executor.

        If *table* is ``None`` and the loader was created via
        ``FactModel.as_loader()``, the bound model class is used as the table.
        """
        _table = table if table is not None else self._model_cls
        if _table is None:
            raise TypeError(
                "LazySnapshotLoader.aload(): 'table' is required when the "
                "loader was not created via FactModel.as_loader()."
            )
        return await asyncio.to_thread(
            self.load, source, _table, snapshot_date=snapshot_date
        )

    def _resolve_snapshot_date(self, override):
        """Return *override* if given, else fall back to ``self.snapshot_date``."""
        return override if override is not None else self.snapshot_date

    def _run_batch(
        self, i: int, sql_fragment: str, table_name: str, sd, source, on_batch
    ) -> int:
        """Write one snapshot micro-batch; return rows_written.  Caller handles exceptions."""
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
            source.commit(source.checkpoint())
            if on_batch:
                on_batch(i, rows_written)
            return rows_written
        finally:
            try:
                self._con.execute("DROP VIEW IF EXISTS snapshot_rows")
            except Exception:
                pass

    def load_stream(
        self,
        source,
        table: str | type,
        batch_size: int = 10_000,
        max_batches: int | None = None,
        on_batch=None,
        *,
        snapshot_date=None,
    ):
        """Process a streaming source, injecting *snapshot_date* into each micro-batch.

        Parameters
        ----------
        source      : :class:`~sqldim.sources.stream.StreamSourceAdapter`
        table       : Target table name or FactModel subclass.
        batch_size  : Rows per micro-batch (passed to ``source.stream``).
        max_batches : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch    : ``callback(batch_num: int, rows_written: int)`` invoked
                      after each successful write.
        snapshot_date : Override the instance-level *snapshot_date* for this call.

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from sqldim.sources.stream import StreamResult

        _log = logging.getLogger(__name__)
        _assert_not_dimension(table, "LazySnapshotLoader")
        table_name = _resolve_table(table)
        sd = self._resolve_snapshot_date(snapshot_date)
        if sd is None:
            raise TypeError(
                "LazySnapshotLoader.load_stream(): 'snapshot_date' must be "
                "supplied either at construction or as a keyword argument."
            )
        result = StreamResult()
        batches = source.stream(self._con, batch_size)
        if max_batches is not None:
            batches = islice(batches, max_batches)
        for i, sql_fragment in enumerate(batches):
            try:
                result.inserted += self._run_batch(
                    i, sql_fragment, table_name, sd, source, on_batch
                )
                result.batches_processed += 1
            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed: %s", i, exc)

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
        fact: type[FactModel],
        dimension: type[DimensionModel],
        snapshot_date: date,
        session: Session,
        date_field: str = "snapshot_date",
    ):
        self.fact = fact
        self.dimension = dimension
        self.snapshot_date = snapshot_date
        self.session = session
        self.date_field = date_field

    def load(self, records: list[dict[str, object]]) -> int:
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
