"""Tests for TransformPipeline and col() DSL — Task 7.4."""
import pytest
import polars as pl
import pandas as pd
import narwhals as nw

from sqldim.processors.transforms import (
    col, TransformPipeline, Transform,
    _types_compatible, _python_type_to_nw, _AppliedTransform,
)
from sqldim.exceptions import TransformTypeError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _pl_frame():
    return nw.from_native(pl.DataFrame({
        "name":    ["  Alice  ", "BOB"],
        "code":    ["A001",     "B002"],
        "amount":  ["10.5",     "20.0"],
        "date_str":["2024-01-15", "2024-03-22"],
        "active":  [1,           0],
        "nullable":["x",         None],
    }), eager_only=True)


def _pd_frame():
    return nw.from_native(pd.DataFrame({
        "name":  ["  Alice  ", "BOB"],
        "code":  ["A001",     "B002"],
        "amount":["10.5",     "20.0"],
    }), eager_only=True)


# ---------------------------------------------------------------------------
# String transforms
# ---------------------------------------------------------------------------

def test_str_lowercase():
    frame = _pl_frame()
    result = col("name").str.lowercase().apply(frame)
    vals = result["name"].to_list()
    assert vals[1] == "bob"


def test_str_uppercase():
    frame = _pl_frame()
    result = col("code").str.uppercase().apply(frame)
    vals = result["code"].to_list()
    assert vals[0] == "A001"
    assert vals[1] == "B002"


def test_str_strip():
    frame = _pl_frame()
    result = col("name").str.strip().apply(frame)
    assert result["name"].to_list()[0] == "Alice"


def test_str_replace():
    frame = _pl_frame()
    result = col("code").str.replace("A", "X").apply(frame)
    assert result["code"].to_list()[0] == "X001"


def test_str_slice():
    frame = _pl_frame()
    result = col("code").str.slice(0, 1).apply(frame)
    assert result["code"].to_list()[0] == "A"
    assert result["code"].to_list()[1] == "B"


# ---------------------------------------------------------------------------
# Cast transforms
# ---------------------------------------------------------------------------

def test_cast_to_float():
    frame = _pl_frame()
    result = col("amount").cast(float).apply(frame)
    schema = result.schema
    import narwhals as nw
    assert schema["amount"] in (nw.Float32, nw.Float64)


def test_cast_to_int():
    frame = _pl_frame()
    result = col("active").cast(int).apply(frame)
    schema = result.schema
    import narwhals as nw
    assert schema["active"] in (nw.Int8, nw.Int16, nw.Int32, nw.Int64)


def test_cast_to_str():
    frame = _pl_frame()
    result = col("active").cast(str).apply(frame)
    import narwhals as nw
    assert result.schema["active"] == nw.String


def test_cast_pandas_frame():
    frame = _pd_frame()
    result = col("amount").cast(float).apply(frame)
    import narwhals as nw
    assert result.schema["amount"] in (nw.Float32, nw.Float64)


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

def test_fill_null():
    frame = _pl_frame()
    result = col("nullable").fill_null("DEFAULT").apply(frame)
    vals = result["nullable"].to_list()
    assert "DEFAULT" in vals
    assert None not in vals


def test_drop_nulls():
    frame = _pl_frame()
    result = col("nullable").drop_nulls().apply(frame)
    assert len(result) == 1  # only row with non-null nullable


def test_is_null_produces_boolean():
    frame = _pl_frame()
    result = col("nullable").is_null().apply(frame)
    import narwhals as nw
    assert result.schema["nullable"] == nw.Boolean


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def test_to_date():
    frame = _pl_frame()
    result = col("date_str").to_date("%Y-%m-%d").apply(frame)
    import narwhals as nw
    assert result.schema["date_str"] == nw.Date


def test_to_datetime():
    frame = nw.from_native(pl.DataFrame({
        "ts": ["2024-01-15 12:00:00", "2024-03-22 08:30:00"],
    }), eager_only=True)
    result = col("ts").to_datetime("%Y-%m-%d %H:%M:%S").apply(frame)
    # Datetime dtype may carry time unit — check the base class
    assert type(result.schema["ts"]).__name__ == "Datetime" or result.schema["ts"] == nw.Datetime


# ---------------------------------------------------------------------------
# TransformPipeline
# ---------------------------------------------------------------------------

def test_pipeline_applies_in_order():
    frame = _pl_frame()
    pipeline = TransformPipeline(transforms=[
        col("name").str.strip(),
        col("name").str.lowercase(),
    ])
    result = pipeline.apply(frame)
    assert result["name"].to_list()[0] == "alice"


def test_pipeline_raw_transforms():
    frame = _pl_frame()
    pipeline = TransformPipeline(
        raw_transforms=[nw.col("amount").cast(nw.Float64).alias("amount")]
    )
    result = pipeline.apply(frame)
    assert result.schema["amount"] in (nw.Float32, nw.Float64)


def test_pipeline_empty_is_noop():
    frame = _pl_frame()
    pipeline = TransformPipeline()
    result = pipeline.apply(frame)
    assert result.columns == frame.columns


def test_pipeline_schema_validation_passes():
    # Model with str annotation — should not raise
    class FakeModel:
        __annotations__ = {"name": str}

    frame = _pl_frame()
    pipeline = TransformPipeline(
        transforms=[col("name").str.strip()],
        model=FakeModel,
    )
    result = pipeline.apply(frame)  # no error
    assert result is not None


def test_pipeline_schema_validation_fails_type_mismatch():
    # Model declares amount: int, but we're passing a string column
    class FakeModel:
        __annotations__ = {"amount": int}

    frame = _pl_frame()
    # amount is currently a string (not cast) — should fail int check
    pipeline = TransformPipeline(model=FakeModel)
    with pytest.raises(TransformTypeError) as exc:
        pipeline.apply(frame)
    assert exc.value.column == "amount"


# ---------------------------------------------------------------------------
# _types_compatible
# ---------------------------------------------------------------------------

def test_types_compatible_identical():
    assert _types_compatible(nw.Int64, nw.Int64) is True


def test_types_compatible_int_family():
    assert _types_compatible(nw.Int32, nw.Int64) is True


def test_types_compatible_float_family():
    assert _types_compatible(nw.Float32, nw.Float64) is True


def test_types_compatible_string_not_compatible_with_int():
    # String is NOT compatible with Int64 — strict type checking
    assert _types_compatible(nw.String, nw.Int64) is False


def test_types_compatible_string_with_string():
    assert _types_compatible(nw.String, nw.String) is True


def test_types_compatible_incompatible():
    assert _types_compatible(nw.Boolean, nw.Float64) is False


# ---------------------------------------------------------------------------
# _python_type_to_nw
# ---------------------------------------------------------------------------

def test_python_type_to_nw_int():
    assert _python_type_to_nw(int) == nw.Int64


def test_python_type_to_nw_float():
    assert _python_type_to_nw(float) == nw.Float64


def test_python_type_to_nw_str():
    assert _python_type_to_nw(str) == nw.String


def test_python_type_to_nw_bool():
    assert _python_type_to_nw(bool) == nw.Boolean


def test_python_type_to_nw_unknown_returns_none():
    class CustomType:
        pass
    assert _python_type_to_nw(CustomType) is None


def test_python_type_to_nw_optional():
    from typing import Optional
    result = _python_type_to_nw(Optional[str])
    assert result == nw.String


# ---------------------------------------------------------------------------
# Migrated from test_coverage_100.py, test_coverage_gap.py, test_coverage_gap_v2.py
# ---------------------------------------------------------------------------

class TestTransformsMissingLines:
    def test_python_type_to_nw_all_none_args(self):
        """_python_type_to_nw returns None when all Union args are NoneType."""
        import typing

        class AllNoneUnion:
            __origin__ = typing.Union
            __args__ = (type(None), type(None))

        result = _python_type_to_nw(AllNoneUnion)
        assert result is None

    def test_applied_transform_cast_known_type(self):
        """_AppliedTransform.cast() with known Python type."""
        t = col("x").str.lowercase()
        t2 = t.cast(str)
        df = pd.DataFrame({"x": ["hello", "WORLD"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_cast_unknown_type(self):
        """_AppliedTransform.cast() else branch with narwhals type."""
        t = col("x").str.lowercase()
        t2 = t.cast(nw.String)
        df = pd.DataFrame({"x": ["hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_fill_null(self):
        """_AppliedTransform.fill_null() body."""
        t = col("x").str.lowercase()
        t2 = t.fill_null("default")
        df = pd.DataFrame({"x": [None, "hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_applied_transform_is_null(self):
        """_AppliedTransform.is_null() body."""
        t = col("x").str.lowercase()
        t2 = t.is_null()
        df = pd.DataFrame({"x": [None, "hello"]})
        frame = nw.from_native(df, eager_only=True)
        result = t2.apply(frame)
        assert "x" in result.columns

    def test_col_transform_cast_narwhals_type(self):
        """ColTransform.cast() else branch when dtype is narwhals type."""
        t = col("x").cast(nw.String)
        df = pd.DataFrame({"x": [1, 2, 3]})
        frame = nw.from_native(df, eager_only=True)
        result = t.apply(frame)
        assert "x" in result.columns

    def test_validate_schema_skips_unknown_type(self):
        """_validate_schema continues when model type not in _PYTHON_TO_NW."""
        from datetime import datetime as dt

        class ModelWithDatetime:
            __annotations__ = {"created_at": dt}

        pipeline = TransformPipeline(model=ModelWithDatetime)
        df = pd.DataFrame({"created_at": [dt.now()]})
        frame = nw.from_native(df, eager_only=True)
        result = pipeline.apply(frame)
        assert "created_at" in result.columns


def test_python_type_to_nw_list():
    from typing import List
    assert _python_type_to_nw(List[str]) == nw.String


def test_python_type_to_nw_none_type():
    assert _python_type_to_nw(None) is None


def test_python_type_to_nw_unknown_class():
    class UnkCov:
        pass
    assert _python_type_to_nw(UnkCov) is None


def test_transform_abstract_apply_raises():
    """Transform.apply() raises NotImplementedError."""
    t = Transform()
    with pytest.raises(NotImplementedError):
        t.apply(None)


def test_transform_pipeline_raw_and_type_error():
    """TransformPipeline raises TransformTypeError on incompatible types."""
    class MockModel:
        __annotations__ = {"val": int}

    df = pd.DataFrame({"val": ["not_an_int"]})
    pipeline = TransformPipeline(model=MockModel)
    with pytest.raises(TransformTypeError):
        pipeline.apply(nw.from_native(df))


def test_col_transforms_all_branches():
    """Chain multiple col transforms including strip, lowercase, drop_nulls, is_null."""
    df = pl.DataFrame({"s": [" a "], "i": [1], "n": [None]})
    frame = nw.from_native(df)

    c = col("s").str.strip().str.lowercase()
    res = c.apply(frame)
    assert nw.to_native(res)["s"][0] == "a"

    assert col("n").drop_nulls().apply(frame).is_empty()
    assert col("i").is_null().apply(frame)["i"][0] == False
