"""
Narwhals SK Resolver — batch natural-key → surrogate-key resolution
via a single join instead of N individual dict lookups.
"""
from __future__ import annotations

from datetime import date
from typing import Any, TYPE_CHECKING

import narwhals as nw

if TYPE_CHECKING:
    from sqldim.core.models import DimensionModel, FactModel


class NarwhalsSKResolver:
    """
    Resolves natural keys to surrogate keys for an entire batch at once.

    Parameters
    ----------
    session : SQLAlchemy Session (sync or async wrapper).

    Usage
    -----
    .. code-block:: python

        resolver = NarwhalsSKResolver(session)
        frame = resolver.resolve(
            frame,
            dimension=CustomerDim,
            natural_key_col="customer_code",
            surrogate_key_col="customer_id",
        )
    """

    def __init__(self, session: Any) -> None:
        self._session = session
        # Cache: (DimensionModel, as_of) → nw.DataFrame with NK + SK columns
        self._cache: dict[tuple, nw.DataFrame] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(
        self,
        frame: nw.DataFrame,
        dimension: type["DimensionModel"],
        natural_key_col: str,
        surrogate_key_col: str,
        as_of: date | None = None,
    ) -> nw.DataFrame:
        """
        Left-join *frame* with the dimension's NK→SK lookup table.

        Rows whose natural key is absent from the dimension receive a
        null surrogate key (not an error — caller decides how to handle).

        Parameters
        ----------
        frame : incoming narwhals DataFrame
        dimension : DimensionModel class
        natural_key_col : column in *frame* that holds the natural key value
        surrogate_key_col : name to give the resolved SK column in output
        as_of : if set, resolve to the SCD2 version active on this date
        """
        lookup = self._get_lookup(dimension, as_of, natural_key_col)

        # lookup has columns: [natural_key_col, "id"]
        return (
            frame
            .join(
                lookup.rename({"id": surrogate_key_col}),
                on=natural_key_col,
                how="left",
            )
        )

    def resolve_all(
        self,
        frame: nw.DataFrame,
        fact_model: type["FactModel"],
        key_map: dict[str, tuple[type["DimensionModel"], str]],
    ) -> nw.DataFrame:
        """
        Resolve all FK columns declared in *key_map* in one pass.

        Parameters
        ----------
        key_map : {fk_col: (DimensionModel, natural_key_col)}
        """
        for fk_col, (dimension, nk_col) in key_map.items():
            frame = self.resolve(
                frame,
                dimension=dimension,
                natural_key_col=nk_col,
                surrogate_key_col=fk_col,
            )
        return frame

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _get_lookup(
        self,
        dimension: type["DimensionModel"],
        as_of: date | None,
        natural_key_col: str,
    ) -> nw.DataFrame:
        cache_key = (dimension, as_of, natural_key_col)
        if cache_key in self._cache:
            return self._cache[cache_key]

        lookup = self._load_lookup(dimension, as_of, natural_key_col)
        self._cache[cache_key] = lookup
        return lookup

    def _load_lookup(
        self,
        dimension: type["DimensionModel"],
        as_of: date | None,
        natural_key_col: str,
    ) -> nw.DataFrame:
        """Query the dimension table and return a narwhals DataFrame with
        [natural_key_col, "id"]."""
        from sqlmodel import select as sa_select

        stmt = sa_select(
            getattr(dimension, natural_key_col),
            dimension.id,  # type: ignore[attr-defined]
        )

        # SCD2 filtering
        if hasattr(dimension, "is_current"):
            if as_of is None:
                stmt = stmt.where(dimension.is_current == True)  # type: ignore[attr-defined]
            else:
                stmt = stmt.where(
                    dimension.valid_from <= as_of,  # type: ignore[attr-defined]
                    (dimension.valid_to == None) | (dimension.valid_to > as_of),  # type: ignore[attr-defined]
                )

        rows = self._session.exec(stmt).all()
        records = [{natural_key_col: r[0], "id": r[1]} for r in rows]

        if not records:
            try:
                import polars as pl
                return nw.from_native(pl.DataFrame({natural_key_col: [], "id": []}), eager_only=True)
            except ImportError:
                import pandas as pd
                return nw.from_native(pd.DataFrame({natural_key_col: [], "id": []}), eager_only=True)

        try:
            import polars as pl
            return nw.from_native(pl.from_dicts(records), eager_only=True)
        except ImportError:
            import pandas as pd
            return nw.from_native(pd.DataFrame(records), eager_only=True)

    def invalidate_cache(self) -> None:
        """Clear the lookup cache (call between loader runs)."""
        self._cache.clear()
