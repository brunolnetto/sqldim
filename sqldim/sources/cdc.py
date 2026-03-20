"""
sqldim/sources/cdc.py

DebeziumSource — consumes CDC events from a Debezium-produced Kafka topic.

Debezium wraps every row mutation in a ``{op, before, after, ts_ms}``
envelope.  This source unwraps that envelope:

  - Upserts (``c``, ``u``, ``r``) are yielded as flat SQL fragments.
  - Deletes (``d``) are surfaced as a DuckDB view named ``deleted_nks``
    — the caller decides how to handle soft deletes.

Debezium op codes:
  c = create (INSERT)
  u = update (UPDATE)
  d = delete (DELETE)
  r = read   (initial snapshot)
"""

from __future__ import annotations

from typing import Any, Iterator

import duckdb

from sqldim.sources.kafka import KafkaSource


class DebeziumSource:
    """
    Consume CDC events from a Debezium-produced Kafka topic.

    Parameters
    ----------
    kafka_source:
        A :class:`KafkaSource` connected to the Debezium topic.
    natural_key:
        Natural key column name (used to build the ``deleted_nks`` view).
    """

    def __init__(
        self,
        kafka_source: KafkaSource,
        natural_key: str,
    ) -> None:
        self._kafka = kafka_source
        self._nk = natural_key

    # ------------------------------------------------------------------
    # StreamSourceAdapter interface
    # ------------------------------------------------------------------

    def stream(
        self,
        con: duckdb.DuckDBPyConnection,
        batch_size: int = 10_000,
    ) -> Iterator[str]:
        """Yield upsert SQL fragments, one per Kafka micro-batch.

        As a side effect, registers a ``deleted_nks`` DuckDB view
        containing the natural keys of any deleted rows in that batch.
        """
        for raw_sql in self._kafka.stream(con, batch_size):
            con.execute(f"CREATE OR REPLACE VIEW _cdc_raw AS SELECT * FROM ({raw_sql})")
            con.execute("""
                CREATE OR REPLACE VIEW _cdc_upserts AS
                SELECT after.*
                FROM _cdc_raw
                WHERE op IN ('c', 'u', 'r')
                  AND after IS NOT NULL
            """)
            con.execute(f"""
                CREATE OR REPLACE VIEW deleted_nks AS
                SELECT before.{self._nk}
                FROM _cdc_raw
                WHERE op = 'd'
            """)
            yield "SELECT * FROM _cdc_upserts"

    def commit(self, offset: Any) -> None:
        """Delegate commit to the underlying Kafka source."""
        self._kafka.commit(offset)

    def checkpoint(self) -> Any:
        """Delegate checkpoint to the underlying Kafka source."""
        return self._kafka.checkpoint()
