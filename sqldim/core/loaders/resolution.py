"""Surrogate-key resolver with an in-process cache for fast batch dimension lookups.

The :class:`SKResolver` caches every ``(model, column, value) → sk`` lookup
so repeated resolutions within the same loader run never hit the database
twice.

Late-arriving dimensions
------------------------
Set ``infer_missing=True`` to enable inferred (placeholder) member generation.
When the natural key is absent from the dimension table, a placeholder row is
inserted with default attribute values and ``is_inferred = True``.  The real
dimension data can arrive later — the SCD pipeline detects the inferred version
and replaces it transparently on reconnect.

Requirements for ``infer_missing=True``:
1. The model must have an ``is_inferred`` column.
2. All non-key columns must have default values (or be provided in
   *inferred_defaults*).
"""

from typing import Any
from datetime import datetime, timezone

from sqlmodel import Session, select
from sqldim.core.kimball.models import DimensionModel
from sqldim.exceptions import SKResolutionError


class ConfigurationError(Exception):
    """Raised when SKResolver is misconfigured for inferred member mode."""


def _default_from_finfo(finfo: Any, _is_undefined: Any) -> Any:
    """Return the field default if defined, else None."""
    if finfo.default_factory is not None:  # type: ignore[attr-defined]
        return finfo.default_factory()  # type: ignore[misc]
    if (
        finfo.default is not None
        and finfo.default is not ...
        and not _is_undefined(finfo.default)
    ):
        return finfo.default
    return None


def _field_value(
    fname: str,
    finfo: Any,
    row_kwargs: dict,
    inferred_defaults: dict,
    _is_undefined: Any,
) -> Any:
    """Return the value to use for *fname* when building an inferred member row."""
    if fname in inferred_defaults:
        return inferred_defaults[fname]
    return _default_from_finfo(finfo, _is_undefined)


class SKResolver:
    """
    Resolves natural keys to surrogate keys for dimension tables.
    Supports single and multi-column natural keys with an in-process cache.

    Parameters
    ----------
    session:
        An active SQLModel/SQLAlchemy session.
    raise_on_missing:
        When ``True`` and ``infer_missing=False``, raises
        :exc:`~sqldim.exceptions.SKResolutionError` instead of returning
        ``None`` for unknown natural keys.
    infer_missing:
        When ``True``, insert a placeholder dimension row when the natural
        key is absent.  The dimension model must have an ``is_inferred``
        column; raises :exc:`ConfigurationError` otherwise.
    inferred_defaults:
        Optional mapping of column name → default value used when building
        the placeholder row.  Columns not listed here fall back to the
        model's own column defaults.
    lineage_emitter:
        Optional callable invoked with an
        :class:`~sqldim.lineage.events.InferredMemberEvent` after each
        inferred member is created.  Use this to attach audit trail.
    """

    def __init__(
        self,
        session: Session,
        raise_on_missing: bool = False,
        infer_missing: bool = False,
        inferred_defaults: dict[str, object] | None = None,
        lineage_emitter: Any | None = None,
    ):
        self.session = session
        self.raise_on_missing = raise_on_missing
        self.infer_missing = infer_missing
        self.inferred_defaults = inferred_defaults or {}
        self._lineage_emitter = lineage_emitter
        self._cache: dict[tuple, object] = {}

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _require_is_inferred_column(self, model: type[DimensionModel]) -> None:
        """Raise ConfigurationError when the model lacks an ``is_inferred`` column."""
        if "is_inferred" not in model.model_fields:
            raise ConfigurationError(
                f"{model.__name__} does not have an 'is_inferred' column. "
                "Add 'is_inferred: bool = False' to use infer_missing=True."
            )

    # ------------------------------------------------------------------
    # Inferred member creation
    # ------------------------------------------------------------------

    def _create_inferred_member(
        self,
        model: type[DimensionModel],
        natural_key_name: str,
        value: Any,
        fact_table: str = "",
    ) -> Any:
        """Insert a placeholder dimension row and return its surrogate key."""
        self._require_is_inferred_column(model)

        row_kwargs: dict[str, object] = {natural_key_name: value, "is_inferred": True}

        try:
            from pydantic_core import PydanticUndefinedType

            def _is_undefined(v):
                return isinstance(v, PydanticUndefinedType)
        except ImportError:  # pragma: no cover

            def _is_undefined(v):
                return False

        for fname, finfo in model.model_fields.items():
            if fname in row_kwargs:
                continue
            val = _field_value(
                fname, finfo, row_kwargs, self.inferred_defaults, _is_undefined
            )
            if val is not None:
                row_kwargs[fname] = val

        self._apply_scd_defaults(row_kwargs, model)

        instance = model(**row_kwargs)
        self.session.add(instance)
        self.session.flush()
        sk = getattr(instance, "id")
        self._emit_inferred_event(model, value, fact_table)
        return sk

    def _apply_scd_defaults(self, row_kwargs: dict, model: Any) -> None:
        """Populate SCD2 temporal fields if present and not already set."""
        now = datetime.now(timezone.utc)
        for col, default in (
            ("valid_from", now),
            ("valid_to", None),
            ("is_current", True),
        ):
            if col in model.model_fields and col not in row_kwargs:
                row_kwargs[col] = default

    def _emit_inferred_event(self, model: Any, value: Any, fact_table: str) -> None:
        """Emit a lineage event for the newly created inferred member."""
        if self._lineage_emitter is not None:
            from sqldim.lineage.events import (
                InferredMemberEvent,
                InferredMemberEventType,
            )

            self._lineage_emitter(
                InferredMemberEvent(
                    event_type=InferredMemberEventType.CREATED,
                    dimension=model.__tablename__,  # type: ignore[attr-defined]
                    natural_key=str(value),
                    fact_table=fact_table,
                )
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(
        self,
        model: type[DimensionModel],
        natural_key_name: str,
        value: Any,
        fact_table: str = "",
    ) -> object:
        """Resolve a single natural key column to its surrogate key (id).

        Checks the in-process cache first; falls back to a DB query and caches
        the result.  Returns ``None`` (or raises) when the key is absent and
        ``infer_missing=False``.

        When ``infer_missing=True`` and the key is absent, a placeholder row is
        inserted and its surrogate key is returned.
        """
        cache_key = (model, natural_key_name, value)
        if cache_key in self._cache:
            return self._cache[cache_key]

        stmt = select(model.id).where(  # type: ignore[attr-defined]
            getattr(model, natural_key_name) == value,
            getattr(model, "is_current"),
        )
        sk = self.session.exec(stmt).first()

        if sk is not None:
            self._cache[cache_key] = sk
        elif self.infer_missing:
            sk = self._create_inferred_member(
                model, natural_key_name, value, fact_table
            )
            self._cache[cache_key] = sk
        elif self.raise_on_missing:
            raise SKResolutionError(
                f"Natural key '{natural_key_name}={value}' not found in {model.__name__}"
            )
        return sk

    def _resolve_multi_miss(
        self,
        model: type[DimensionModel],
        key_values: dict[str, object],
        cache_key: tuple,
        fact_table: str,
    ) -> object:
        """Handle a cache miss for resolve_multi: query, infer, or raise."""
        stmt = select(model.id).where(  # type: ignore[attr-defined]
            getattr(model, "is_current"),
            *[getattr(model, col) == val for col, val in key_values.items()],
        )
        sk = self.session.exec(stmt).first()
        if sk is not None:
            self._cache[cache_key] = sk
        elif self.infer_missing:
            sk = self._infer_composite(model, key_values, fact_table)
            self._cache[cache_key] = sk
        elif self.raise_on_missing:
            raise SKResolutionError(
                f"Composite natural key {key_values} not found in {model.__name__}"
            )
        return sk

    def resolve_multi(
        self,
        model: type[DimensionModel],
        key_values: dict[str, object],
        fact_table: str = "",
    ) -> object:
        """Resolve a composite (multi-column) natural key to surrogate key."""
        cache_key = (model, tuple(sorted(key_values.items())))
        if cache_key in self._cache:
            return self._cache[cache_key]
        return self._resolve_multi_miss(model, key_values, cache_key, fact_table)

    def _infer_composite(
        self,
        model: type[DimensionModel],
        key_values: dict[str, object],
        fact_table: str,
    ) -> Any:
        """Create an inferred member from a composite key, preserving all key values."""
        first_nk, first_val = next(iter(key_values.items()))
        saved_defaults = dict(self.inferred_defaults)
        self.inferred_defaults.update(key_values)
        sk = self._create_inferred_member(model, first_nk, first_val, fact_table)
        self.inferred_defaults = saved_defaults
        return sk

    def warm(self, model: type[DimensionModel], natural_key_name: str) -> int:
        """
        Pre-load the entire current dimension into cache for batch resolution.
        Returns the number of rows cached.
        """
        stmt = select(getattr(model, natural_key_name), model.id).where(  # type: ignore[attr-defined]
            getattr(model, "is_current")
        )
        rows = self.session.exec(stmt).all()
        for nk_val, sk_val in rows:
            self._cache[(model, natural_key_name, nk_val)] = sk_val
        return len(rows)

    def clear(self) -> None:
        self._cache.clear()
