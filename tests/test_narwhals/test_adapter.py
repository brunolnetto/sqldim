"""Tests for NarwhalsAdapter — Task 7.1."""
import sys
import pytest
import pandas as pd
import polars as pl

from sqldim.processors.adapter import NarwhalsAdapter, _is_dataframe, _dicts_to_native
from sqldim.exceptions import LoadError


# ---------------------------------------------------------------------------
# _is_dataframe helper
# ---------------------------------------------------------------------------

def test_is_dataframe_pandas():
    df = pd.DataFrame({"a": [1]})
    assert _is_dataframe(df) is True


def test_is_dataframe_polars():
    df = pl.DataFrame({"a": [1]})
    assert _is_dataframe(df) is True


def test_is_dataframe_rejects_list():
    assert _is_dataframe([{"a": 1}]) is False


def test_is_dataframe_rejects_dict():
    assert _is_dataframe({"a": 1}) is False


def test_is_dataframe_rejects_string():
    assert _is_dataframe("hello") is False


# ---------------------------------------------------------------------------
# _dicts_to_native helper
# ---------------------------------------------------------------------------

def test_dicts_to_native_nonempty():
    native = _dicts_to_native([{"x": 1, "y": "a"}])
    assert native is not None


def test_dicts_to_native_empty_returns_frame():
    native = _dicts_to_native([])
    assert native is not None


# ---------------------------------------------------------------------------
# NarwhalsAdapter construction
# ---------------------------------------------------------------------------

def test_adapter_wraps_pandas():
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    adapter = NarwhalsAdapter(df)
    assert adapter.mode == "dataframe"
    assert len(adapter) == 2


def test_adapter_wraps_polars():
    df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    adapter = NarwhalsAdapter(df)
    assert adapter.mode == "dataframe"
    assert len(adapter) == 2


def test_adapter_wraps_list_of_dicts():
    records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    adapter = NarwhalsAdapter(records)
    assert adapter.mode == "list"
    assert len(adapter) == 2


def test_adapter_rejects_unsupported_type():
    with pytest.raises(LoadError, match="Unsupported source type"):
        NarwhalsAdapter(42)


def test_adapter_rejects_none():
    with pytest.raises(LoadError):
        NarwhalsAdapter(None)


# ---------------------------------------------------------------------------
# NarwhalsAdapter accessors
# ---------------------------------------------------------------------------

def test_adapter_to_dicts_pandas():
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    adapter = NarwhalsAdapter(df)
    dicts = adapter.to_dicts()
    assert dicts == [{"id": 1, "name": "Alice"}]


def test_adapter_to_dicts_polars():
    df = pl.DataFrame({"id": [1], "name": ["Alice"]})
    adapter = NarwhalsAdapter(df)
    dicts = adapter.to_dicts()
    assert dicts == [{"id": 1, "name": "Alice"}]


def test_adapter_to_dicts_list():
    records = [{"id": 1, "name": "Alice"}]
    adapter = NarwhalsAdapter(records)
    dicts = adapter.to_dicts()
    assert dicts[0]["id"] == 1
    assert dicts[0]["name"] == "Alice"


def test_adapter_to_native_pandas_returns_pandas():
    df = pd.DataFrame({"id": [1]})
    adapter = NarwhalsAdapter(df)
    native = adapter.to_native()
    assert isinstance(native, pd.DataFrame)


def test_adapter_to_native_polars_returns_polars():
    df = pl.DataFrame({"id": [1]})
    adapter = NarwhalsAdapter(df)
    native = adapter.to_native()
    assert isinstance(native, pl.DataFrame)


def test_adapter_schema_returns_dict():
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    adapter = NarwhalsAdapter(df)
    schema = adapter.schema()
    assert isinstance(schema, dict)
    assert "id" in schema
    assert "name" in schema


def test_adapter_frame_returns_narwhals():
    import narwhals as nw
    df = pl.DataFrame({"id": [1]})
    adapter = NarwhalsAdapter(df)
    assert isinstance(adapter.frame(), nw.DataFrame)


def test_adapter_repr_contains_mode():
    df = pl.DataFrame({"id": [1]})
    adapter = NarwhalsAdapter(df)
    r = repr(adapter)
    assert "dataframe" in r


def test_adapter_list_to_dicts_roundtrip():
    records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    adapter = NarwhalsAdapter(records)
    result = adapter.to_dicts()
    assert len(result) == 2
    assert result[0]["a"] == 1
    assert result[1]["b"] == "y"


# ---------------------------------------------------------------------------
# Migrated from test_coverage_100.py and test_coverage_final.py
# ---------------------------------------------------------------------------

def test_adapter_empty_list():
    """NarwhalsAdapter([]) has length 0."""
    adapter = NarwhalsAdapter([])
    assert len(adapter) == 0


def test_adapter_errors_unsupported_object():
    """NarwhalsAdapter(object()) raises LoadError."""
    with pytest.raises(LoadError, match="Unsupported source type"):
        NarwhalsAdapter(object())


class TestAdapterImportError:
    def test_dicts_to_native_empty_no_polars(self, monkeypatch):
        """_dicts_to_native([]) falls back to pandas when polars unavailable."""
        monkeypatch.setitem(sys.modules, "polars", None)
        result = _dicts_to_native([])
        assert hasattr(result, "to_dict") or hasattr(result, "shape")

    def test_dicts_to_native_nonempty_no_polars(self, monkeypatch):
        """_dicts_to_native([...]) falls back to pandas when polars unavailable."""
        monkeypatch.setitem(sys.modules, "polars", None)
        result = _dicts_to_native([{"x": 1, "y": "hello"}])
        assert hasattr(result, "to_dict") or hasattr(result, "shape")
