"""
Narwhals SCD Engine — vectorized change detection and SCD2 processing.

Replaces Python-loop row-by-row comparisons in SCDHandler with
single-join DataFrame operations.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import narwhals as nw

from sqldim.scd.handler import SCDResult


# ---------------------------------------------------------------------------
# NarwhalsHashStrategy
# ---------------------------------------------------------------------------

class NarwhalsHashStrategy:
    """
    Vectorized MD5 checksum computation and change classification.

    Parameters
    ----------
    natural_key : list[str]
        Column(s) that uniquely identify a dimension member.
    track_columns : list[str]
        Columns whose changes drive SCD versioning.
    ignore_columns : list[str]
        Columns excluded from change detection (default: none).
    """

    def __init__(
        self,
        natural_key: list[str],
        track_columns: list[str],
        ignore_columns: list[str] | None = None,
    ) -> None:
        self.natural_key = natural_key
        self.track_columns = track_columns
        self.ignore_columns = ignore_columns or []

    def compute_checksums(self, frame: nw.DataFrame) -> nw.DataFrame:
        """
        Add a ``checksum`` column computed from ``track_columns``.

        Columns are cast to string and concatenated with ``|``, then
        the resulting string is hashed via narwhals' ``hash()``.
        """
        cols_to_hash = sorted(self.track_columns)

        # Pre-process dictionary columns to be deterministic strings
        # because cast(nw.String) behavior is backend-dependent.
        native_prep = nw.to_native(frame)
        import json
        module = type(native_prep).__module__.split(".")[0]
        
        for c in cols_to_hash:
            # Check if column contains complex types
            if module == "polars":
                import polars as pl
                if native_prep[c].dtype in (pl.Struct, pl.List, pl.Object):
                    native_prep = native_prep.with_columns(
                        native_prep[c].map_elements(lambda x: json.dumps(x, sort_keys=True) if x is not None else None, return_dtype=pl.Utf8)
                    )
            else:
                # pandas
                if native_prep[c].dtype == "object":
                    native_prep[c] = native_prep[c].apply(lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x)
        
        frame = nw.from_native(native_prep, eager_only=True)

        # Build a single concatenated string column, then hash it
        concat_expr = nw.concat_str(
            [nw.col(c).cast(nw.String).fill_null("") for c in cols_to_hash],
            separator="|",
        )
        # Add concatenated string column
        frame_with_input = frame.with_columns(concat_expr.alias("_hash_input"))

        # Compute MD5 checksums on the native series (avoids map_batches API differences)
        native = nw.to_native(frame_with_input)
        
        import json
        def deterministic_hash(val: Any) -> str:
            if val is None: return ""
            # If the row was already a string representation of JSON, we use it,
            # otherwise ensure deterministic string for hashing.
            return hashlib.md5(str(val).encode("utf-8")).hexdigest()

        module = type(native).__module__.split(".")[0]
        if module == "polars":
            import polars as pl
            checksums_list = []
            for v in native["_hash_input"].to_list():
                # JSONB columns in Polars might come in as Struct/Dict
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, sort_keys=True)
                checksums_list.append(hashlib.md5(str(v).encode("utf-8")).hexdigest())
                
            checksums_series = pl.Series("checksum", checksums_list)
            native = native.with_columns(checksums_series).drop("_hash_input")
        else:
            # pandas / modin
            def pd_hash(v):
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, sort_keys=True)
                return hashlib.md5(str(v).encode("utf-8")).hexdigest()
                
            native["checksum"] = native["_hash_input"].apply(pd_hash)
            native = native.drop(columns=["_hash_input"])

        return nw.from_native(native, eager_only=True)

    def find_changes(
        self,
        incoming: nw.DataFrame,
        current: nw.DataFrame,
    ) -> tuple[nw.DataFrame, nw.DataFrame, nw.DataFrame]:
        """
        Classify incoming records into (new, changed, unchanged) via a
        single left-join against the current open rows.

        Both DataFrames must already have a ``checksum`` column
        (call ``compute_checksums`` first).

        Returns
        -------
        new : rows whose natural key is absent from *current*
        changed : rows present in *current* but with a different checksum
        unchanged : rows present in *current* with an identical checksum
        """
        current_slim = current.select(self.natural_key + ["checksum"]).rename(
            {"checksum": "checksum_current"}
        )

        merged = incoming.join(current_slim, on=self.natural_key, how="left")

        new = merged.filter(nw.col("checksum_current").is_null()).drop("checksum_current")

        changed = merged.filter(
            ~nw.col("checksum_current").is_null()
            & (nw.col("checksum") != nw.col("checksum_current"))
        ).drop("checksum_current")

        unchanged = merged.filter(
            ~nw.col("checksum_current").is_null()
            & (nw.col("checksum") == nw.col("checksum_current"))
        ).drop("checksum_current")

        return new, changed, unchanged


# ---------------------------------------------------------------------------
# NarwhalsSCDProcessor
# ---------------------------------------------------------------------------

class NarwhalsSCDProcessor:
    """
    In-process SCD2 processor.

    Accepts a full incoming batch and the current SCD table as narwhals
    DataFrames.  Returns an ``SCDResult`` with ``to_close`` and
    ``to_insert`` as native DataFrames (same type as *incoming*).

    Parameters
    ----------
    natural_key : list[str]
    track_columns : list[str]
    ignore_columns : list[str]
    """

    def __init__(
        self,
        natural_key: list[str],
        track_columns: list[str],
        ignore_columns: list[str] | None = None,
    ) -> None:
        self.natural_key = natural_key
        self.track_columns = track_columns
        self.ignore_columns = ignore_columns or []
        self._strategy = NarwhalsHashStrategy(natural_key, track_columns, ignore_columns)

    def process(
        self,
        incoming: nw.DataFrame,
        current_scd: nw.DataFrame,
        as_of: datetime | None = None,
    ) -> SCDResult:
        """
        Classify incoming records and build close/insert sets.

        Parameters
        ----------
        incoming : narwhals DataFrame of source records (no checksum yet)
        current_scd : narwhals DataFrame of the full current SCD table
        as_of : timestamp for valid_from / valid_to (default: now UTC)

        Returns
        -------
        SCDResult
            .to_close   — native DataFrame of rows to mark is_current=False
            .to_insert  — native DataFrame of new rows to insert
            .unchanged  — count of unchanged rows
        """
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        # 1. Compute checksums on incoming
        incoming = self._strategy.compute_checksums(incoming)

        # 2. Filter current to open rows only, add checksums
        if "is_current" in current_scd.columns:
            current_open = current_scd.filter(nw.col("is_current") == True)
        else:
            current_open = current_scd

        current_open = self._strategy.compute_checksums(current_open)

        # 3. Classify
        new, changed, unchanged = self._strategy.find_changes(incoming, current_open)

        # 4. Build "to close" — current open rows that changed
        if len(changed) > 0:
            closed = (
                current_open
                .join(changed.select(self.natural_key), on=self.natural_key, how="inner")
                .with_columns([
                    nw.lit(as_of.isoformat()).alias("valid_to"),
                    nw.lit(False).alias("is_current"),
                ])
            )
        else:
            # Empty frame: filter to zero rows via a column-based predicate
            first_col = current_open.columns[0] if current_open.columns else None
            if first_col:
                closed = current_open.filter(nw.col(first_col).is_null() & ~nw.col(first_col).is_null())
            else:
                closed = current_open

        # 5. Build "to insert" — new versions for changed + brand-new rows
        ts = as_of.isoformat()
        new_versions = changed.with_columns([
            nw.lit(ts).alias("valid_from"),
            nw.lit(None).cast(nw.String).alias("valid_to"),
            nw.lit(True).alias("is_current"),
        ])
        new_rows = new.with_columns([
            nw.lit(ts).alias("valid_from"),
            nw.lit(None).cast(nw.String).alias("valid_to"),
            nw.lit(True).alias("is_current"),
        ])

        to_insert = nw.concat([new_versions, new_rows]) if len(new_versions) + len(new_rows) > 0 else new_versions

        result = SCDResult()
        result.inserted = len(new_rows)
        result.versioned = len(changed)
        result.unchanged = len(unchanged)
        result.to_close = nw.to_native(closed)
        result.to_insert = nw.to_native(to_insert)

        return result


# ---------------------------------------------------------------------------
# LazySCDProcessor  (SCD Type 2)
# ---------------------------------------------------------------------------

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
            CREATE OR REPLACE VIEW classified AS
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
            CREATE OR REPLACE VIEW classified AS
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
            CREATE OR REPLACE VIEW classified AS
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
            CREATE OR REPLACE VIEW classified AS
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
