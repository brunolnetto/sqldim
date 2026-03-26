from __future__ import annotations

import asyncio
from sqlmodel import Session, select
from sqldim.core.kimball.models import FactModel
from sqldim.core.loaders._utils import _resolve_table, _assert_not_dimension


# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyAccumulatingLoader:
    """
    Accumulating snapshot fact loader. Speaks only DuckDB SQL.

    New rows are inserted via ``sink.write()``.
    Existing rows have their milestone columns patched via
    ``sink.update_milestones()`` — only non-null incoming values are written.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            loader = LazyAccumulatingLoader(
                table_name="fact_order_pipeline",
                match_column="order_id",
                milestone_columns=["approved_at", "shipped_at", "delivered_at"],
                sink=sink,
            )
            result = loader.process("orders_batch.parquet")
    """

    def __init__(
        self,
        table: str | type,
        match_column: str,
        milestone_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        _assert_not_dimension(table, "LazyAccumulatingLoader")
        self.table_name = _resolve_table(table)
        self.match_column = match_column
        self.milestone_columns = milestone_columns
        self.sink = sink
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()
        self._model_cls: type | None = (
            None  # set by factory when created via as_loader()
        )

    def process(self, source) -> dict[str, int]:
        """
        Process incoming records.

        - Rows whose match key is absent from the current state are INSERTed.
        - Rows that already exist have their non-null milestone columns UPDATEd.

        Returns ``{"inserted": n, "updated": n}``.
        """
        mc = self.match_column
        sql = self.sink.current_state_sql(self.table_name)

        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT * FROM ({_sql})
        """)

        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_keys AS
            SELECT {mc} FROM ({sql})
        """)

        # New rows: natural key is not in the current state
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT i.*
            FROM incoming i
            LEFT JOIN current_keys c
                   ON cast(i.{mc} as varchar) = cast(c.{mc} as varchar)
            WHERE c.{mc} IS NULL
        """)

        # Update view: rows that match an existing record
        mc_and_milestones = ", ".join(
            [f"i.{mc}"] + [f"i.{c}" for c in self.milestone_columns]
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW update_rows AS
            SELECT {mc_and_milestones}
            FROM incoming i
            INNER JOIN current_keys c
                    ON cast(i.{mc} as varchar) = cast(c.{mc} as varchar)
        """)

        inserted = self.sink.write(
            self._con, "new_rows", self.table_name, self.batch_size
        )
        updated = self.sink.update_milestones(
            self._con, self.table_name, mc, "update_rows", self.milestone_columns
        )
        self._con.execute("DROP VIEW IF EXISTS incoming")
        self._con.execute("DROP VIEW IF EXISTS new_rows")
        self._con.execute("DROP VIEW IF EXISTS update_rows")
        self._con.execute("DROP TABLE IF EXISTS current_keys")
        return {"inserted": inserted, "updated": updated}

    async def aload(self, source) -> dict[str, int]:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(self.process, source)

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process


class AccumulatingLoader:
    """
    Loads Accumulating Snapshot Fact Tables — one row per business process instance,
    updated in-place as it moves through pipeline stages.

    Classic example: an order progressing through placed → approved → shipped → delivered.
    Each stage has its own date FK. Rows are updated, not inserted, as stages complete.

    Usage:
        loader = AccumulatingLoader(
            fact=OrderPipelineFact,
            match_column="order_id",
            milestone_columns=["approved_at", "shipped_at", "delivered_at"],
            session=session,
        )
        loader.process(records)
    """

    def __init__(
        self,
        fact: type[FactModel],
        match_column: str,
        milestone_columns: list[str],
        session: Session,
    ):
        self.fact = fact
        self.match_column = match_column
        self.milestone_columns = milestone_columns
        self.session = session

    def _update_milestones(
        self, existing: FactModel, record: dict[str, object]
    ) -> bool:
        changed = False
        for col in self.milestone_columns:
            new_val = record.get(col)
            if new_val is not None and getattr(existing, col, None) is None:
                setattr(existing, col, new_val)
                changed = True
        return changed

    def _update_attributes(
        self, existing: FactModel, record: dict[str, object]
    ) -> bool:
        changed = False
        for col, val in record.items():
            if col not in self.milestone_columns and col != self.match_column:
                setattr(existing, col, val)
                changed = True
        return changed

    def _update_existing(self, existing: FactModel, record: dict[str, object]) -> bool:
        """Apply milestone and attribute updates; return True if any field changed."""
        m = self._update_milestones(existing, record)
        a = self._update_attributes(existing, record)
        return m or a

    def process(self, records: list[dict[str, object]]) -> dict[str, int]:
        """
        Process incoming records. For each:
        - If no existing row: INSERT
        - If existing row: UPDATE milestone columns that are now non-null

        Returns a dict with counts: {"inserted": n, "updated": n}
        """
        inserted = 0
        updated = 0
        for record in records:
            existing = self.session.exec(
                select(self.fact).where(
                    getattr(self.fact, self.match_column)
                    == record.get(self.match_column)
                )
            ).first()
            if existing is None:
                self.session.add(self.fact(**record))
                inserted += 1
            elif self._update_existing(existing, record):
                self.session.add(existing)
                updated += 1
        self.session.commit()
        return {"inserted": inserted, "updated": updated}
