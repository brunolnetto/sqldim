"""Shared helpers for all lazy loaders."""

from __future__ import annotations

from typing import Any, Union, get_args, get_origin

TableRef = Union[str, type]

# Fields that are always SCD infrastructure / bridge allocation — never inferred
# as strategy params.
_SKIP_FIELDS = frozenset(
    {"id", "valid_from", "valid_to", "is_current", "checksum", "weight"}
)


def _resolve_table(table: TableRef) -> str:
    """Return the DB table name for *table* — a string or a SQLModel class."""
    if isinstance(table, str):
        return table
    return getattr(table, "__tablename__", table.__name__.lower())


def _unwrap_optional(annotation: Any) -> Any:
    """Return the inner type if *annotation* is ``X | None``; else return ``None``."""
    if get_origin(annotation) is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return None


def _assert_not_dimension(table: TableRef, loader_name: str) -> None:
    """Raise TypeError if *table* is a DimensionModel subclass.

    SCD dimensions must be loaded via :class:`~sqldim.scd.handler.SCDHandler`
    so that surrogate-key assignment, validity windows, and checksums are
    handled correctly.
    """
    if not isinstance(table, type):
        return
    from sqldim.core.kimball.models import DimensionModel

    if issubclass(table, DimensionModel):
        raise TypeError(
            f"{loader_name} is a fact loader — "
            f"dimension model {table.__name__!r} must be loaded via SCDHandler."
        )


def _is_optional_datetime(annotation: Any) -> bool:
    """True for ``datetime | None``, ``date | None``, bare ``datetime``/``date``."""
    from datetime import date, datetime

    _dt = (datetime, date)
    return annotation in _dt or _unwrap_optional(annotation) in _dt


def _is_list_annotation(annotation: Any) -> bool:
    """True for ``list[X]``, ``list[X]``, bare ``list``."""
    origin = get_origin(annotation)
    return origin in (list,) or annotation is list


def _is_numeric(annotation: Any) -> bool:
    """True for int, float, int | None, float | None."""
    _num = (int, float)
    return annotation in _num or _unwrap_optional(annotation) in _num


def _classify_field(annotation: Any) -> str | None:
    """Return ``'datetime'``, ``'list'``, ``'numeric'``, or ``None``."""
    if _is_optional_datetime(annotation):
        return "datetime"
    if _is_list_annotation(annotation):
        return "list"
    if _is_numeric(annotation):
        return "numeric"
    return None


def _bucket_fields(fields: dict[str, Any], nk_set: set) -> dict[str, list[str]]:
    """Classify *fields* into ``datetime``, ``list``, and ``numeric`` buckets."""
    buckets: dict[str, list[str]] = {"datetime": [], "list": [], "numeric": []}
    skip = _SKIP_FIELDS | nk_set
    for name, field_info in fields.items():
        if name in skip:
            continue
        kind = _classify_field(field_info.annotation)
        if kind:
            buckets[kind].append(name)
    return buckets


def _infer_loader_params(model_cls: type) -> dict[str, Any]:
    """Return inferred loader params from *model_cls* field annotations.

    ClassVar values on the model take precedence; this function provides
    fallbacks for any that are ``None``/empty.

    Inferred values
    ---------------
    ``match_column``     ← ``__natural_key__[0]`` (if available)
    ``partition_key``    ← ``__natural_key__[0]`` (if available)
    ``milestones``       ← all ``datetime|date | None`` non-skip fields
    ``cumulative_column``← first ``list[Any]`` non-skip field
    ``metric_columns``   ← remaining non-key numeric fields (non-list, non-datetime)
    ``dates_column``     ← first ``list[str|date]`` non-skip field (used by bitmask)
    ``value_column``     ← first numeric non-key non-skip field
    ``metric_name``      ← same as ``value_column``
    ``subject_key``      ← never inferred (must be explicit)
    ``object_key``       ← never inferred (must be explicit)
    """

    fields: dict[str, Any] = getattr(model_cls, "model_fields", {})
    nk: list[str] = list(getattr(model_cls, "__natural_key__", ()))
    nk_set = set(nk)

    fallback_key: str | None = next(iter(nk), None)
    buckets = _bucket_fields(fields, nk_set)

    inferred: dict[str, Any] = {"metric_columns": buckets["numeric"]}
    if fallback_key:
        inferred["match_column"] = fallback_key
        inferred["partition_key"] = fallback_key
    if buckets["datetime"]:
        inferred["milestones"] = buckets["datetime"]
    if buckets["list"]:
        inferred["cumulative_column"] = buckets["list"][0]
        inferred["dates_column"] = buckets["list"][0]
    if buckets["numeric"]:
        inferred["value_column"] = buckets["numeric"][0]
        inferred["metric_name"] = buckets["numeric"][0]

    return inferred
