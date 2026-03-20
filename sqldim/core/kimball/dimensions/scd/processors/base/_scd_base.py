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

from sqldim.core.kimball.dimensions.scd.handler import SCDResult

import json as _json


def _prep_complex_cols_polars(native: Any, cols: list[str]) -> Any:
    import polars as pl

    for c in cols:
        if native[c].dtype in (pl.Struct, pl.List, pl.Object):
            native = native.with_columns(
                native[c].map_elements(
                    lambda x: _json.dumps(x, sort_keys=True) if x is not None else None,
                    return_dtype=pl.Utf8,
                )
            )
    return native


def _prep_complex_cols_pandas(native: Any, cols: list[str]) -> Any:
    for c in cols:
        if native[c].dtype == "object":
            native[c] = native[c].apply(
                lambda x: (
                    _json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
                )
            )
    return native


def _hash_polars_frame(native: Any) -> Any:
    import polars as pl

    checksums = [
        hashlib.md5(
            (
                _json.dumps(v, sort_keys=True)
                if isinstance(v, (dict, list))
                else str(v)
            ).encode("utf-8")
        ).hexdigest()
        for v in native["_hash_input"].to_list()
    ]
    return native.with_columns(pl.Series("checksum", checksums)).drop("_hash_input")


def _hash_pandas_frame(native: Any) -> Any:
    def _pd_hash(v: Any) -> str:
        if isinstance(v, (dict, list)):
            v = _json.dumps(v, sort_keys=True)  # pragma: no cover
        return hashlib.md5(str(v).encode("utf-8")).hexdigest()

    native["checksum"] = native["_hash_input"].apply(_pd_hash)
    return native.drop(columns=["_hash_input"])


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
        """
        cols_to_hash = sorted(self.track_columns)
        native_prep = nw.to_native(frame)
        module = type(native_prep).__module__.split(".")[0]
        if module == "polars":
            native_prep = _prep_complex_cols_polars(native_prep, cols_to_hash)
        else:
            native_prep = _prep_complex_cols_pandas(native_prep, cols_to_hash)
        frame = nw.from_native(native_prep, eager_only=True)
        concat_expr = nw.concat_str(
            [nw.col(c).cast(nw.String).fill_null("") for c in cols_to_hash],
            separator="|",
        )
        frame_with_input = frame.with_columns(concat_expr.alias("_hash_input"))
        native = nw.to_native(frame_with_input)
        if type(native).__module__.split(".")[0] == "polars":
            native = _hash_polars_frame(native)
        else:
            native = _hash_pandas_frame(native)
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

        new = merged.filter(nw.col("checksum_current").is_null()).drop(
            "checksum_current"
        )

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
        self._strategy = NarwhalsHashStrategy(
            natural_key, track_columns, ignore_columns
        )

    def _open_rows(self, current_scd: nw.DataFrame) -> nw.DataFrame:
        if "is_current" in current_scd.columns:
            return current_scd.filter(nw.col("is_current"))
        return current_scd

    def _build_closed_frame(
        self,
        current_open: nw.DataFrame,
        changed: nw.DataFrame,
        as_of: datetime,
    ) -> nw.DataFrame:
        if len(changed) > 0:
            return current_open.join(
                changed.select(self.natural_key), on=self.natural_key, how="inner"
            ).with_columns(
                [
                    nw.lit(as_of.isoformat()).alias("valid_to"),
                    nw.lit(False).alias("is_current"),
                ]
            )
        first_col = current_open.columns[0] if current_open.columns else None
        if first_col:
            return current_open.filter(
                nw.col(first_col).is_null() & ~nw.col(first_col).is_null()
            )
        return current_open  # pragma: no cover

    def process(
        self,
        incoming: nw.DataFrame,
        current_scd: nw.DataFrame,
        as_of: datetime | None = None,
    ) -> SCDResult:
        """
        Classify incoming records and build close/insert sets.
        """
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        incoming = self._strategy.compute_checksums(incoming)
        current_open = self._strategy.compute_checksums(self._open_rows(current_scd))
        new, changed, unchanged = self._strategy.find_changes(incoming, current_open)
        closed = self._build_closed_frame(current_open, changed, as_of)
        ts = as_of.isoformat()
        scd_cols = [
            nw.lit(ts).alias("valid_from"),
            nw.lit(None).cast(nw.String).alias("valid_to"),
            nw.lit(True).alias("is_current"),
        ]
        new_versions = changed.with_columns(scd_cols)
        new_rows = new.with_columns(scd_cols)
        to_insert = (
            nw.concat([new_versions, new_rows])
            if len(new_versions) + len(new_rows) > 0
            else new_versions
        )

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
