"""
Narwhals SK Resolver — batch natural-key → surrogate-key resolution
via a single join instead of N individual dict lookups.
"""

from __future__ import annotations

from datetime import date
from typing import Any, TYPE_CHECKING

import narwhals as nw

if TYPE_CHECKING:
    from sqldim.core.kimball.models import DimensionModel, FactModel


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
        return frame.join(
            lookup.rename({"id": surrogate_key_col}),
            on=natural_key_col,
            how="left",
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
                stmt = stmt.where(dimension.is_current)  # type: ignore[attr-defined]
            else:
                stmt = stmt.where(
                    dimension.valid_from <= as_of,  # type: ignore[attr-defined]
                    dimension.valid_to.is_(None) | (dimension.valid_to > as_of),  # type: ignore[attr-defined]
                )

        rows = self._session.exec(stmt).all()
        records = [{natural_key_col: r[0], "id": r[1]} for r in rows]
        return self._df_from_records(records, natural_key_col)

    @staticmethod
    def _df_from_records(records: list[dict], natural_key_col: str) -> nw.DataFrame:
        """Convert a list of {nk, id} dicts to a narwhals DataFrame."""
        if not records:
            try:
                import polars as pl

                return nw.from_native(
                    pl.DataFrame({natural_key_col: [], "id": []}), eager_only=True
                )
            except ImportError:
                import pandas as pd  # type: ignore[import-untyped]

                return nw.from_native(
                    pd.DataFrame({natural_key_col: [], "id": []}), eager_only=True
                )
        try:
            import polars as pl

            return nw.from_native(pl.from_dicts(records), eager_only=True)
        except ImportError:
            import pandas as pd  # type: ignore[import-untyped]

            return nw.from_native(pd.DataFrame(records), eager_only=True)

    def invalidate_cache(self) -> None:
        """Clear the lookup cache (call between loader runs)."""
        self._cache.clear()


# ---------------------------------------------------------------------------
# LazySKResolver — DuckDB SQL LEFT JOIN, no Python data
# ---------------------------------------------------------------------------


class LazySKResolver:
    """
    Lazy natural-key → surrogate-key resolver via DuckDB LEFT JOIN.

    Replaces the session-backed lookup with a single DuckDB SQL join
    against ``sink.current_state_sql(dim_table)``.  The result is a
    DuckDB VIEW — no rows ever enter Python memory.

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            resolver = LazySKResolver(sink, con)
            # Register a fact view first, then resolve one FK:
            resolved_view = resolver.resolve(
                fact_view="pending_facts",
                dim_table_name="dim_customer",
                natural_key_col="customer_code",
                surrogate_key_col="customer_id",
                output_view="facts_with_customer_sk",
            )
            # Or resolve all FKs in one pass:
            final_view = resolver.resolve_all(
                fact_view="pending_facts",
                key_map={
                    "customer_id": ("dim_customer", "customer_code", "customer_id"),
                    "product_id":  ("dim_product",  "product_code",  "product_id"),
                },
            )
    """

    def __init__(self, sink, con=None):
        import duckdb as _duckdb

        self.sink = sink
        self._con = con or _duckdb.connect()

    def resolve(
        self,
        fact_view: str,
        dim_table_name: str,
        natural_key_col: str,
        surrogate_key_col: str,
        output_view: str = "fact_with_sk",
    ) -> str:
        """
        Left-join *fact_view* with the current dimension state to resolve
        one natural key column to a surrogate key.

        Rows without a matching dimension member receive ``NULL`` for the
        surrogate key — the caller decides how to handle unresolved keys.

        Returns the name of the output DuckDB view.
        """
        dim_sql = self.sink.current_state_sql(dim_table_name)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT f.*,
                   d.id AS {surrogate_key_col}
            FROM {fact_view} f
            LEFT JOIN ({dim_sql}) d
                   ON cast(f.{natural_key_col} as varchar) = cast(d.{natural_key_col} as varchar)
                  AND d.is_current = TRUE
        """)
        return output_view

    def resolve_all(
        self,
        fact_view: str,
        key_map: dict[str, tuple[str, str, str]],
        output_view: str = "fact_with_all_sk",
    ) -> str:
        """
        Resolve all FK columns in one chained pass.

        Parameters
        ----------
        key_map : ``{fk_col: (dim_table_name, natural_key_col, surrogate_key_alias)}``

        Returns the name of the final output DuckDB view.
        """
        current_view = fact_view
        for i, (fk_col, (dim_table, nk_col, sk_alias)) in enumerate(key_map.items()):
            intermediate = f"_sk_stage_{i}"
            self.resolve(current_view, dim_table, nk_col, sk_alias, intermediate)
            current_view = intermediate

        self._con.execute(f"""
            CREATE OR REPLACE VIEW {output_view} AS
            SELECT * FROM {current_view}
        """)
        return output_view
