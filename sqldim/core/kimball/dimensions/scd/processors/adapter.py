"""
NarwhalsAdapter — unified entry point for DataFrame + list[dict] sources.

Detects the native type and wraps in a narwhals DataFrame automatically.
Users never call narwhals directly.
"""
from __future__ import annotations

from typing import Any

import narwhals as nw

from sqldim.exceptions import LoadError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_dataframe(obj: Any) -> bool:
    """
    Detect pandas / polars / modin / PyArrow / cuDF DataFrames without
    importing any of those libraries.
    """
    module = type(obj).__module__.split(".")[0]
    return module in {"pandas", "polars", "modin", "pyarrow", "cudf"}


def _dicts_to_native(records: list[dict]) -> Any:
    """
    Convert list[dict] to a native DataFrame.

    Prefers polars when installed (faster); falls back to pandas.
    """
    if not records:
        try:
            import polars as pl
            return pl.DataFrame()
        except ImportError:
            import pandas as pd
            return pd.DataFrame()

    try:
        import polars as pl
        return pl.from_dicts(records)
    except ImportError:
        pass

    import pandas as pd
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# NarwhalsAdapter
# ---------------------------------------------------------------------------

class NarwhalsAdapter:
    """
    Wraps any supported source (DataFrame or list[dict]) in a narwhals
    DataFrame for uniform downstream processing.

    Parameters
    ----------
    source:
        A pandas / polars / modin / PyArrow / cuDF DataFrame, or list[dict].

    Raises
    ------
    LoadError
        When the source type is not recognised.
    """

    def __init__(self, source: Any) -> None:
        if _is_dataframe(source):
            self._frame: nw.DataFrame = nw.from_native(source, eager_only=True)
            self._mode = "dataframe"
        elif isinstance(source, list):
            native = _dicts_to_native(source)
            self._frame = nw.from_native(native, eager_only=True)
            self._mode = "list"
        else:
            raise LoadError(
                f"Unsupported source type: {type(source).__name__}. "
                f"Expected a DataFrame (pandas/polars/modin/pyarrow/cuDF) "
                f"or list[dict]."
            )

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def mode(self) -> str:
        """'dataframe' or 'list'."""
        return self._mode

    def frame(self) -> nw.DataFrame:
        """Return the underlying narwhals DataFrame."""
        return self._frame

    def to_native(self) -> Any:
        """Return the original native type (pandas → pandas, polars → polars)."""
        return nw.to_native(self._frame)

    def to_dicts(self) -> list[dict]:
        """Convert to list[dict] — fallback for components that still need it."""
        native = nw.to_native(self._frame)
        module = type(native).__module__.split(".")[0]
        if module == "pandas":
            return native.to_dict(orient="records")
        # polars / other narwhals-native types
        return native.to_dicts()

    def schema(self) -> dict[str, Any]:
        """Return {column_name: narwhals dtype} mapping."""
        return dict(self._frame.schema)

    def __len__(self) -> int:
        return len(self._frame)

    def __repr__(self) -> str:
        return f"NarwhalsAdapter(mode={self._mode!r}, rows={len(self)}, cols={list(self.schema())})"
