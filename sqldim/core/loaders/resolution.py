"""Surrogate-key resolver with an in-process cache for fast batch dimension lookups.

The :class:`SKResolver` caches every ``(model, column, value) → sk`` lookup
so repeated resolutions within the same loader run never hit the database
twice.
"""
from typing import Any, Dict, List, Optional, Tuple, Type
from sqlmodel import Session, select
from sqldim.core.kimball.models import DimensionModel
from sqldim.exceptions import SKResolutionError


class SKResolver:
    """
    Resolves natural keys to surrogate keys for dimension tables.
    Supports single and multi-column natural keys with an in-process cache.
    """

    def __init__(self, session: Session, raise_on_missing: bool = False):
        self.session = session
        self.raise_on_missing = raise_on_missing
        self._cache: Dict[Tuple, Any] = {}

    def resolve(
        self,
        model: Type[DimensionModel],
        natural_key_name: str,
        value: Any,
    ) -> Optional[Any]:
        """Resolve a single natural key column to its surrogate key (id).

        Checks the in-process cache first; falls back to a DB query and caches
        the result.  Returns ``None`` (or raises) when the key is absent.
        """
        cache_key = (model, natural_key_name, value)
        if cache_key in self._cache:
            return self._cache[cache_key]

        stmt = select(model.id).where(
            getattr(model, natural_key_name) == value,
            getattr(model, "is_current") == True,
        )
        sk = self.session.exec(stmt).first()

        if sk is not None:
            self._cache[cache_key] = sk
        elif self.raise_on_missing:
            raise SKResolutionError(
                f"Natural key '{natural_key_name}={value}' not found in {model.__name__}"
            )
        return sk

    def resolve_multi(
        self,
        model: Type[DimensionModel],
        key_values: Dict[str, Any],
    ) -> Optional[Any]:
        """Resolve a composite (multi-column) natural key to surrogate key.

        Builds a compound WHERE clause from all items in *key_values*, checks
        the cache, then falls back to a DB query.  Returns ``None`` or raises
        ``SKResolutionError`` according to *raise_on_missing*.
        """
        cache_key = (model, tuple(sorted(key_values.items())))
        if cache_key in self._cache:
            return self._cache[cache_key]

        stmt = select(model.id).where(
            getattr(model, "is_current") == True,
            *[getattr(model, col) == val for col, val in key_values.items()],
        )
        sk = self.session.exec(stmt).first()

        if sk is not None:
            self._cache[cache_key] = sk
        elif self.raise_on_missing:
            raise SKResolutionError(
                f"Composite natural key {key_values} not found in {model.__name__}"
            )
        return sk

    def warm(self, model: Type[DimensionModel], natural_key_name: str) -> int:
        """
        Pre-load the entire current dimension into cache for batch resolution.
        Returns the number of rows cached.
        """
        stmt = select(getattr(model, natural_key_name), model.id).where(
            getattr(model, "is_current") == True
        )
        rows = self.session.exec(stmt).all()
        for nk_val, sk_val in rows:
            self._cache[(model, natural_key_name, nk_val)] = sk_val
        return len(rows)

    def clear(self) -> None:
        self._cache.clear()
