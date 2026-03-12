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
