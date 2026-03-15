"""SCD Type 3 and Type 6 lazy processors."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import narwhals as nw

from sqldim.scd.handler import SCDResult

class LazyType3Processor:
    """
    SCD Type 3 processor. Rotates columns: previous_col = current_col,
    current_col = incoming value. Only the current and one prior value
    are preserved.

    ``column_pairs`` is a list of (current_col, previous_col) tuples,
    e.g. [("region", "prev_region"), ("manager", "prev_manager")].

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        column_pairs: list[tuple[str, str]],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key  = natural_key
        self.column_pairs = column_pairs          # [(curr, prev), ...]
        self.track_cols   = [c for c, _ in column_pairs]
        self.sink         = sink
        self.batch_size   = batch_size
        self._con         = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        self._register_source(source)
        self._register_current_checksums(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name)
        result.versioned = self._rotate_changed(table_name)
        result.unchanged = self._count_unchanged()
        return result

    def _register_source(self, source) -> None:
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        cols = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')"
            for c in self.track_cols
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
        # For new rows, previous columns are NULL
        prev_nulls = ", ".join(
            f"NULL::varchar AS {prev}" for _, prev in self.column_pairs
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   {prev_nulls},
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _rotate_changed(self, table_name: str) -> int:
        nk = self.natural_key
        curr_cols = ", ".join(f"{c}" for c, _ in self.column_pairs)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_rotations AS
            SELECT {nk}, {curr_cols}
            FROM classified
            WHERE _scd_class = 'changed'
        """)
        return self.sink.rotate_attributes(
            self._con, table_name, nk, "changed_rotations", self.column_pairs
        )

    def _count_unchanged(self) -> int:
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# LazyType6Processor  (SCD Type 6 — hybrid Type 1 + Type 2)
# ---------------------------------------------------------------------------

class LazyType6Processor:
    """
    SCD Type 6 (hybrid) processor.

    ``type1_columns`` — overwritten in place when changed (no new row).
    ``type2_columns`` — trigger a new versioned row when changed.

    When only type1 columns change: UPDATE existing row.
    When any type2 column changes: close current row, insert new version.
    When both change simultaneously: Type 2 behaviour for the whole row
    (new version carries the updated type1 values too).

    Uses only DuckDB SQL; storage delegated to SinkAdapter.
    """

    def __init__(
        self,
        natural_key: str,
        type1_columns: list[str],
        type2_columns: list[str],
        sink,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key    = natural_key
        self.type1_columns  = sorted(type1_columns)
        self.type2_columns  = sorted(type2_columns)
        self.sink           = sink
        self.batch_size     = batch_size
        self._con           = con or _duckdb.connect()

    def process(self, source, table_name: str) -> SCDResult:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        self._register_source(source)
        self._register_current_state(table_name)
        self._classify()

        result           = SCDResult()
        result.inserted  = self._write_new(table_name, now)
        result.versioned = self._write_type2_changed(table_name, now)
        result.versioned += self._apply_type1_only(table_name)
        result.unchanged = self._count_unchanged()
        return result

    def _register_source(self, source) -> None:
        from sqldim.sources import coerce_source
        _sql = coerce_source(source).as_sql(self._con)
        t1_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type1_columns
        )
        t2_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type2_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT *,
                   md5({t1_hash}) AS _t1_checksum,
                   md5({t2_hash}) AS _t2_checksum
            FROM ({_sql})
        """)

    def _register_current_state(self, table_name: str) -> None:
        nk = self.natural_key
        sql = self.sink.current_state_sql(table_name)
        t1_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type1_columns
        )
        t2_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in self.type2_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW current_state AS
            SELECT {nk},
                   md5({t1_hash}) AS _t1_checksum,
                   md5({t2_hash}) AS _t2_checksum
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
                    WHEN c.{nk} IS NULL                                          THEN 'new'
                    WHEN i._t2_checksum != c._t2_checksum                        THEN 'type2_changed'
                    WHEN i._t2_checksum  = c._t2_checksum
                     AND i._t1_checksum != c._t1_checksum                        THEN 'type1_only'
                    ELSE                                                               'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_state c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

    def _write_new(self, table_name: str, now: str) -> int:
        nk = self.natural_key
        all_cols = self.type1_columns + self.type2_columns
        full_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in all_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _t1_checksum, _t2_checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   md5({full_hash}) AS checksum
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write(self._con, "new_rows", table_name, self.batch_size)

    def _write_type2_changed(self, table_name: str, now: str) -> int:
        nk = self.natural_key
        all_cols = self.type1_columns + self.type2_columns
        full_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in all_cols
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW t2_nks AS
            SELECT {nk} FROM classified WHERE _scd_class = 'type2_changed'
        """)
        self.sink.close_versions(self._con, table_name, nk, "t2_nks", now)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT * EXCLUDE (_scd_class, _t1_checksum, _t2_checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   md5({full_hash}) AS checksum
            FROM classified
            WHERE _scd_class = 'type2_changed'
        """)
        return self.sink.write(self._con, "new_versions", table_name, self.batch_size)

    def _apply_type1_only(self, table_name: str) -> int:
        nk = self.natural_key
        t1_cols = self.type1_columns
        col_list = ", ".join(t1_cols)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW type1_updates AS
            SELECT {nk}, {col_list}
            FROM classified
            WHERE _scd_class = 'type1_only'
        """)
        return self.sink.update_attributes(
            self._con, table_name, nk, "type1_updates", t1_cols
        )

    def _count_unchanged(self) -> int:
        return self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# LazyType4Processor  (SCD Type 4 — mini-dimension split)
# ---------------------------------------------------------------------------

