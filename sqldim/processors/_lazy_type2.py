"""SCD Type 2 and Type 1 lazy processors."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import narwhals as nw

from sqldim.scd.handler import SCDResult

class LazySCDProcessor:
    """
    SCD Type 2 processor. Speaks only DuckDB SQL — no Python data.
    Storage is entirely delegated to a SinkAdapter.

    Usage::

        from sqldim.sinks import DuckDBSink

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            proc = LazySCDProcessor(
                natural_key="product_id",
                track_columns=["name", "price"],
                sink=sink,
            )
            result = proc.process("products.parquet", "dim_product")
    """

    def __init__(
        self,
        natural_key: str,
        track_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key   = natural_key
        self.track_columns = sorted(track_columns)
        self.sink          = sink
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name, now)
        result.versioned = self._write_changed(table_name, now)
        result.unchanged = self._count_unchanged()
        return result

    def _register_source(self, source) -> None:
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({cols}) AS _checksum
            FROM ({_sql})
        """)

    def _register_current_checksums(self, table_name: str) -> None:
        nk  = self.natural_key
        sql = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW current_checksums AS
            SELECT {nk}, checksum AS _checksum
            FROM ({sql})
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN c.{nk}       IS NULL           THEN 'new'
                    WHEN i._checksum != c._checksum      THEN 'changed'
                    ELSE                                      'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_checksums c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

    def _write_new(self, table_name: str, now: str) -> int:
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _write_changed(self, table_name: str, now: str) -> int:
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_nks AS
            SELECT {nk} FROM classified WHERE _scd_class = 'changed'
        """)
        self.sink.close_versions(self._con, table_name, nk, "changed_nks", now)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.write(self._con, "new_versions", table_name, self.batch_size)

    def _count_unchanged(self) -> int:
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# LazyType1Processor  (SCD Type 1 — overwrite in place)
# ---------------------------------------------------------------------------

class LazyType1Processor:
    """
    SCD Type 1 processor. Overwrites tracked attributes in place.
    No versioning — only the latest value is kept.

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        track_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key   = natural_key
        self.track_columns = sorted(track_columns)
        self.sink          = sink
        self.batch_size    = batch_size
        self._con          = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result          = SCDResult()
        result.inserted = self._write_new(table_name)
        result.versioned = self._overwrite_changed(table_name)
        result.unchanged = self._count_unchanged()
        return result

    def _register_source(self, source) -> None:
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({cols}) AS _checksum
            FROM ({_sql})
        """)

    def _register_current_checksums(self, table_name: str) -> None:
        nk = self.natural_key
        sql = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW current_checksums AS
            SELECT {nk}, checksum AS _checksum
            FROM ({sql})
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN c.{nk}       IS NULL           THEN 'new'
                    WHEN i._checksum != c._checksum      THEN 'changed'
                    ELSE                                      'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_checksums c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

    def _write_new(self, table_name: str) -> int:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _overwrite_changed(self, table_name: str) -> int:
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_updates AS
            SELECT {nk}, {", ".join(self.track_columns)}
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.update_attributes(
            self._con, table_name, nk, "changed_updates", self.track_columns
        )

    def _count_unchanged(self) -> int:
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# LazyType3Processor  (SCD Type 3 — current + previous columns)
# ---------------------------------------------------------------------------

