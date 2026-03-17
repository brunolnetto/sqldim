"""
Pre-load Transform Pipeline — declarative DSL for DataFrame transformations.

Users express transforms in sqldim's own API; Narwhals handles the execution.
Power users can drop through via transforms_raw with raw narwhals expressions.

DSL scope (locked):
  String ops  : .str.lowercase(), .str.uppercase(), .str.strip(), .str.replace(), .str.slice()
  Casting     : .cast(int), .cast(float), .cast(str), .cast(bool)
  Null handling: .fill_null(value), .drop_nulls(), .is_null()
  Date parsing : .to_date(format), .to_datetime(format)
"""
from __future__ import annotations

from typing import Any, TYPE_CHECKING

import narwhals as nw

from sqldim.exceptions import LoadError, TransformTypeError

if TYPE_CHECKING:
    from sqldim.core.kimball.models import DimensionModel, FactModel


# ---------------------------------------------------------------------------
# Type compatibility helper
# ---------------------------------------------------------------------------

_PYTHON_TO_NW: dict[type, Any] = {
    int: nw.Int64,
    float: nw.Float64,
    str: nw.String,
    bool: nw.Boolean,
}

_COMPATIBLE_GROUPS: list[set] = [
    {nw.Int8, nw.Int16, nw.Int32, nw.Int64, nw.UInt8, nw.UInt16, nw.UInt32, nw.UInt64},
    {nw.Float32, nw.Float64},
    {nw.String, nw.Categorical},
    {nw.Boolean},
    {nw.Date, nw.Datetime},
]


def _types_compatible(nw_dtype: Any, model_dtype: Any) -> bool:
    """Return True when *nw_dtype* is a valid match for *model_dtype*.

    Considers both exact equality and membership in the same compatibility
    group (e.g. any integer width is compatible with any other integer width).
    """
    if nw_dtype == model_dtype:
        return True
    for group in _COMPATIBLE_GROUPS:
        if nw_dtype in group and model_dtype in group:
            return True
    return False


def _python_type_to_nw(annotation: Any) -> Any | None:
    """Convert a Python type annotation to a narwhals dtype, or None.

    Handles ``Optional[T]`` by unwrapping the inner type, and returns
    ``None`` for annotations with no narwhals equivalent.
    """
    if annotation is None:
        return None
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        # e.g. Optional[str] → unwrap
        args = getattr(annotation, "__args__", ())
        for a in args:
            if a is not type(None):
                return _python_type_to_nw(a)
        return None
    return _PYTHON_TO_NW.get(annotation)


# ---------------------------------------------------------------------------
# Transform base
# ---------------------------------------------------------------------------

class Transform:
    """Base class for a single column transformation.

    Sub-classes implement :meth:`apply` to return a modified :class:`narwhals.DataFrame`.
    """

    def apply(self, frame: nw.DataFrame) -> nw.DataFrame:
        """Apply this transform to *frame* and return the modified DataFrame."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Applied expressions (internal)
# ---------------------------------------------------------------------------

class _AppliedTransform(Transform):
    """A Transform that carries a concrete narwhals expression and supports chaining."""

    def __init__(self, column: str, expr: Any) -> None:
        self._column = column
        self._expr = expr

    def apply(self, frame: nw.DataFrame) -> nw.DataFrame:
        return frame.with_columns(self._expr.alias(self._column))

    @property
    def str(self) -> StringTransforms:
        return StringTransforms(self._column, self._expr)

    def cast(self, dtype: type | Any) -> _AppliedTransform:
        if dtype in _PYTHON_TO_NW:
            nw_dtype = _PYTHON_TO_NW[dtype]
        else:
            nw_dtype = dtype
        return _AppliedTransform(self._column, self._expr.cast(nw_dtype))

    def fill_null(self, value: Any) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.fill_null(value))

    def is_null(self) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.is_null().cast(nw.Boolean))


# ---------------------------------------------------------------------------
# String transforms
# ---------------------------------------------------------------------------

class StringTransforms:
    """Chainable string operation builder."""

    def __init__(self, column: str, expr: Any | None = None) -> None:
        self._column = column
        self._expr = expr if expr is not None else nw.col(column)

    def lowercase(self) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.str.to_lowercase())

    def uppercase(self) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.str.to_uppercase())

    def strip(self) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.str.strip_chars())

    def replace(self, old: str, new: str) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.str.replace_all(old, new))

    def slice(self, start: int, length: int | None = None) -> _AppliedTransform:
        return _AppliedTransform(self._column, self._expr.str.slice(start, length))


# ---------------------------------------------------------------------------
# ColTransform — entry point returned by col()
# ---------------------------------------------------------------------------

class ColTransform:
    """Builder for a single-column transform."""

    def __init__(self, column: str) -> None:
        self._column = column

    @property
    def str(self) -> StringTransforms:
        return StringTransforms(self._column)

    def cast(self, dtype: type | Any) -> _AppliedTransform:
        if dtype in _PYTHON_TO_NW:
            nw_dtype = _PYTHON_TO_NW[dtype]
        else:
            nw_dtype = dtype
        return _AppliedTransform(self._column, nw.col(self._column).cast(nw_dtype))

    def fill_null(self, value: Any) -> _AppliedTransform:
        return _AppliedTransform(self._column, nw.col(self._column).fill_null(value))

    def drop_nulls(self) -> _DropNullsTransform:
        return _DropNullsTransform(self._column)

    def is_null(self) -> _AppliedTransform:
        return _AppliedTransform(self._column, nw.col(self._column).is_null().cast(nw.Boolean))

    def to_date(self, format: str) -> _AppliedTransform:
        return _AppliedTransform(self._column, nw.col(self._column).str.to_date(format=format))

    def to_datetime(self, format: str) -> _AppliedTransform:
        return _AppliedTransform(self._column, nw.col(self._column).str.to_datetime(format=format))


class _DropNullsTransform(Transform):
    def __init__(self, column: str) -> None:
        self._column = column

    def apply(self, frame: nw.DataFrame) -> nw.DataFrame:
        return frame.filter(~nw.col(self._column).is_null())


def col(name: str) -> ColTransform:
    return ColTransform(name)


class TransformPipeline:
    def __init__(
        self,
        transforms: list[Transform] | None = None,
        raw_transforms: list[Any] | None = None,
        model: type | None = None,
    ) -> None:
        self.transforms = transforms or []
        self.raw_transforms = raw_transforms or []
        self._model = model

    def apply(self, frame: nw.DataFrame) -> nw.DataFrame:
        for transform in self.transforms:
            frame = transform.apply(frame)
        if self.raw_transforms:
            frame = frame.with_columns(self.raw_transforms)
        if self._model is not None:
            self._validate_schema(frame)
        return frame

    def _validate_schema(self, frame: nw.DataFrame) -> None:
        schema = frame.schema
        for col_name, nw_dtype in schema.items():
            annotation = self._model.__annotations__.get(col_name)
            if annotation is None:
                continue
            model_nw_dtype = _python_type_to_nw(annotation)
            if model_nw_dtype is None:
                continue
            if not _types_compatible(nw_dtype, model_nw_dtype):
                raise TransformTypeError(
                    column=col_name,
                    expected=model_nw_dtype,
                    got=nw_dtype,
                    hint=f"Use col('{col_name}').cast({annotation.__name__}) before this step",
                )
