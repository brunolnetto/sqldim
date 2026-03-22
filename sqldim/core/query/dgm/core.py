"""DGM — Dimensional Graph Model query algebra (DGM §3).

Three-band model Q = B1 ∘ B2? ∘ B3?.  Accepts SQLModel classes directly;
FK join conditions are inferred from SQLAlchemy metadata.
"""

from __future__ import annotations

from sqldim.exceptions import SemanticError
from sqldim.core.query.dgm.refs import PropRef, AggRef, WinRef, SignatureRef
from sqldim.core.query.dgm.preds import (
    ScalarPred, AND, OR, NOT, RawPred, PathPred, SignaturePred, _HopBase,
)
from sqldim.core.query.dgm.temporal import TemporalContext
from sqldim.core.query.dgm._query_helpers import (
    _collect_fk_matches, _infer_on,
    _check_where_pred, _check_having_pred, _check_qualify_pred,
    _validate_bands, _build_select_list, _build_from_joins, _semi_additive_expr,
)


class DGMQuery:
    """
    Three-band dimensional graph query builder.

    Implements Q = B1 ∘ B2? ∘ B3? generating DuckDB-compatible SQL.
    """

    def __init__(self, fact_table: "str | None" = None) -> None:
        self._anchor_table: str | None = None
        self._anchor_alias: str | None = None
        self._joins: list[tuple[str, str, str]] = []
        self._scd2_joins: list[tuple[str, str, str, str]] = []
        self._as_of: str | None = None
        self._where_pred: object = None
        self._group_by_cols: list[str] = []
        self._agg_exprs: dict[str, str] = {}
        self._having_pred: object = None
        self._window_exprs: dict[str, str] = {}
        self._qualify_pred: object = None
        self._temporal_context: "TemporalContext | None" = None
        # Maps alias → model class for FK inference in model-first path_join
        self._alias_registry: dict[str, type] = {}
        if fact_table is not None:
            self._anchor_table = fact_table
            self._anchor_alias = "f"

    # -- Temporal Context (DGM §5.1) ----------------------------------------

    def temporal_context(
        self,
        default_as_of: str,
        *,
        node_resolution: str = "LAX",
        edge_resolution: str = "LAX",
    ) -> "DGMQuery":
        """Set the snapshot temporal context (TemporalContext, DGM §5.1)."""
        self._temporal_context = TemporalContext(
            default_as_of, node_resolution, edge_resolution
        )
        return self

    # -- B1 Context --------------------------------------------------------

    def anchor(
        self,
        table_or_model: "str | type",
        alias: str | None = None,
    ) -> "DGMQuery":
        """Set the anchor (FROM) table for B1.

        Accepts either a plain table-name string or a SQLModel class.  When a
        class is supplied the table name is read from ``cls.table_name()`` and
        the alias (or table name when alias is omitted) is registered in the
        internal alias registry so that subsequent :meth:`path_join` calls can
        infer FK join conditions automatically.
        """
        if isinstance(table_or_model, type):
            table = table_or_model.table_name()
            key = alias if alias is not None else table
            self._alias_registry[key] = table_or_model
        else:
            table = table_or_model
        self._anchor_table = table
        self._anchor_alias = alias
        return self

    # Spec-vocabulary alias (§7 — B1 is called "Context")
    def context(
        self,
        table_or_model: "str | type",
        alias: str | None = None,
    ) -> "DGMQuery":
        """Alias for :meth:`anchor` — matches the DGM spec vocabulary (Band 1 = Context)."""
        return self.anchor(table_or_model, alias)

    def path_join(
        self,
        hop_or_model: "_HopBase | type",
        *,
        from_alias: str | None = None,
        to_alias: str | None = None,
        label: str = "",
    ) -> "DGMQuery":
        """Add a LEFT JOIN to the query context.

        Two calling forms are supported:

        **Hop form** (backward-compatible)::

            .path_join(VerbHop("c", "placed", "s",
                               table="fact_order", on="c.id = s.customer_id"))

        **Model-first form**::

            .path_join(OrderFact, from_alias="c", to_alias="s")

        In the model-first form the table name is taken from
        ``OrderFact.table_name()`` and the ON clause is derived automatically
        from ``OrderFact``'s FK metadata pointing at the registered
        *from_alias* model.
        """
        if isinstance(hop_or_model, type):
            table, resolved_alias, on = self._resolve_model_join(
                hop_or_model, from_alias, to_alias
            )
        else:
            table, resolved_alias, on = self._resolve_hop_join(hop_or_model)
        self._joins.append((table, resolved_alias, on))
        return self

    def _resolve_model_join(
        self,
        model: type,
        from_alias: "str | None",
        to_alias: "str | None",
    ) -> "tuple[str, str, str]":
        if from_alias is None or to_alias is None:
            raise SemanticError(
                "path_join(ModelClass) requires from_alias= and to_alias= "
                "keyword arguments."
            )
        from_model = self._alias_registry.get(from_alias)
        if from_model is None:
            raise SemanticError(
                f"from_alias {from_alias!r} is not registered. "
                "Call anchor() / context() with that alias first."
            )
        on = _infer_on(from_alias, to_alias, model, from_model.table_name())
        self._alias_registry[to_alias] = model
        return model.table_name(), to_alias, on

    def _resolve_hop_join(
        self,
        hop: "_HopBase",
    ) -> "tuple[str, str, str]":
        table = hop.table or hop.model.table_name()
        on = hop.on
        if on is None:
            from_model = self._alias_registry[hop.from_alias]
            on = _infer_on(
                hop.from_alias, hop.to_alias, hop.model, from_model.table_name()
            )
        if hop.model is not None:
            self._alias_registry[hop.to_alias] = hop.model
        return table, hop.to_alias, on

    def temporal_join(self, as_of: str) -> "DGMQuery":
        self._as_of = as_of
        return self

    def as_of(self, point_in_time: str) -> "DGMQuery":
        """Point-in-time SCD2 filter (alias for :meth:`temporal_join`)."""
        return self.temporal_join(point_in_time)

    def where(self, pred: object) -> "DGMQuery":
        """Apply a B1 filter (PropRef, PathPred, or raw SQL string).

        Raises :class:`~sqldim.exceptions.SemanticError` at construction time
        if any ``AggRef`` or ``WinRef`` is found inside the predicate tree.
        Multiple calls accumulate conditions with AND.
        """
        if isinstance(pred, str):
            pred = RawPred(pred)
        _check_where_pred(pred)
        if self._where_pred is None:
            self._where_pred = pred
        else:
            self._where_pred = AND(self._where_pred, pred)
        return self

    # -- B2 Aggregation ----------------------------------------------------

    def group_by(self, *cols: str) -> "DGMQuery":
        self._group_by_cols.extend(cols)
        return self

    def agg(self, **named_exprs: str) -> "DGMQuery":
        self._agg_exprs.update(named_exprs)
        return self

    def having(self, pred: object) -> "DGMQuery":
        """Apply a B2 filter (AggRef only; PathPred not allowed).

        Raises :class:`~sqldim.exceptions.SemanticError` at construction time
        if any ``PropRef``, ``WinRef``, or ``PathPred`` is found.
        """
        _check_having_pred(pred)
        self._having_pred = pred
        return self

    def aggregate(
        self,
        *group_by_cols: str,
        having: object = None,
        **agg_exprs: str,
    ) -> "DGMQuery":
        """Combined B2 setup — matches the DGM spec vocabulary (Band 2 = Aggregation).

        Equivalent to chaining ``.group_by(*cols).agg(**exprs).having(pred)``::

            .aggregate("c.region", total_rev="SUM(s.revenue)",
                       having=ScalarPred(AggRef("total_rev"), ">", 5000))
        """
        self.group_by(*group_by_cols).agg(**agg_exprs)
        if having is not None:
            self.having(having)
        return self

    # -- B3 Ranking --------------------------------------------------------

    def window(self, **named_exprs: str) -> "DGMQuery":
        self._window_exprs.update(named_exprs)
        return self

    def qualify(self, pred: object) -> "DGMQuery":
        """Apply a B3 filter (WinRef only; PathPred not allowed).

        Raises :class:`~sqldim.exceptions.SemanticError` at construction time
        if any ``PropRef``, ``AggRef``, or ``PathPred`` is found.
        """
        _check_qualify_pred(pred)
        self._qualify_pred = pred
        return self

    def rank(self, *, qualify: object = None, **window_exprs: str) -> "DGMQuery":
        """Combined B3 setup — matches the DGM spec vocabulary (Band 3 = Ranking).

        Equivalent to chaining ``.window(**exprs).qualify(pred)``::

            .rank(rn="ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY s.revenue DESC)",
                  qualify=ScalarPred(WinRef("rn"), "<=", 2))
        """
        self.window(**window_exprs)
        if qualify is not None:
            self.qualify(qualify)
        return self

    # -- Output ------------------------------------------------------------

    def to_sql(self) -> str:
        """Render the three-band query as a DuckDB SQL string."""
        _validate_bands(self)
        select_parts = _build_select_list(self)
        lines = [
            "SELECT " + ", ".join(select_parts),
            _build_from_joins(self),
        ]
        if self._where_pred is not None:
            lines.append("WHERE " + self._where_pred.to_sql())
        if self._group_by_cols:
            lines.append("GROUP BY " + ", ".join(self._group_by_cols))
        if self._having_pred is not None:
            lines.append("HAVING " + self._having_pred.to_sql())
        if self._qualify_pred is not None:
            lines.append("QUALIFY " + self._qualify_pred.to_sql())
        return "\n".join(lines)

    def execute(self, con: object) -> list:
        """Execute the query and return all rows."""
        return con.execute(self.to_sql()).fetchall()

    # -- Legacy / DuckDB-compat API ----------------------------------------
    # These methods support the original DuckDBDimensionalQuery calling style.

    def by(self, *attributes: str) -> "DGMQuery":
        """Add GROUP BY attributes (alias for :meth:`group_by`)."""
        return self.group_by(*attributes)

    def sum(self, measure: str) -> "DGMQuery":
        """Append ``SUM({measure}) AS sum_{col}`` to the aggregation."""
        col = measure.rsplit(".", 1)[-1]
        self._agg_exprs[f"sum_{col}"] = f"SUM({measure})"
        return self

    def avg(self, measure: str) -> "DGMQuery":
        """Append ``AVG({measure}) AS avg_{col}`` to the aggregation."""
        col = measure.rsplit(".", 1)[-1]
        self._agg_exprs[f"avg_{col}"] = f"AVG({measure})"
        return self

    def count(self) -> "DGMQuery":
        """Append ``COUNT(*) AS count`` to the aggregation."""
        self._agg_exprs["count"] = "COUNT(*)"
        return self

    def join_dim(
        self,
        dim_table: str,
        fact_fk: str,
        dim_pk: str = "id",
    ) -> "DGMQuery":
        """Declare a SCD2-aware dimension join.

        Generates ``LEFT JOIN {dim_table} d_{dim_table} ON f.{fact_fk} = d_{dim_table}.{dim_pk}
        AND d_{dim_table}.is_current = TRUE`` (or the date-range variant when
        :meth:`as_of` / :meth:`temporal_join` is set).
        """
        alias = f"d_{dim_table}"
        self._scd2_joins.append((dim_table, alias, fact_fk, dim_pk))
        return self

    def ntile_bucket(
        self,
        measure: str,
        bucket_count: int,
        alias: "str | None" = None,
    ) -> "DGMQuery":
        """Add ``NTILE(n) OVER (ORDER BY measure)`` as a window expression."""
        col = measure.rsplit(".", 1)[-1]
        out_alias = alias or f"{col}_ntile_{bucket_count}"
        self._window_exprs[out_alias] = (
            f"NTILE({bucket_count}) OVER (ORDER BY {measure})"
        )
        return self

    def width_bucket(
        self,
        measure: str,
        bounds: tuple,
        alias: "str | None" = None,
    ) -> "DGMQuery":
        """Add ``WIDTH_BUCKET(measure, ARRAY[...])`` as a window expression."""
        col = measure.rsplit(".", 1)[-1]
        out_alias = alias or f"{col}_bucket"
        bounds_sql = "ARRAY[" + ", ".join(str(b) for b in bounds) + "]"
        self._window_exprs[out_alias] = f"WIDTH_BUCKET({measure}, {bounds_sql})"
        return self

    def date_trunc_by(
        self,
        date_col: str,
        grain: str,
        alias: "str | None" = None,
    ) -> "DGMQuery":
        """Add ``DATE_TRUNC(grain, date_col)`` as a GROUP BY + SELECT expression."""
        col = date_col.rsplit(".", 1)[-1]
        out_alias = alias or f"{col}_{grain}"
        self._group_by_cols.append(f"DATE_TRUNC('{grain}', {date_col}) AS {out_alias}")
        return self

    def semi_additive_sum(
        self,
        measure: str,
        fallback: str = "last",
        forbidden_dimensions: "list[str] | None" = None,
        alias: "str | None" = None,
    ) -> "DGMQuery":
        """Add a semi-additive measure that switches aggregation strategy at runtime.

        When a *forbidden_dimension* is present in the GROUP BY, the
        aggregation falls back to ``LAST_VALUE``, ``AVG``, or ``MAX`` instead
        of ``SUM``.
        """
        col = measure.rsplit(".", 1)[-1]
        out_alias = alias or f"sum_{col}"
        self._agg_exprs[out_alias] = _semi_additive_expr(
            measure,
            fallback,
            set(forbidden_dimensions or []),
            set(self._group_by_cols),
        )
        return self

    def as_view(self, con: object, view_name: str) -> str:
        """Register the compiled SQL as a DuckDB VIEW and return the view name."""
        con.execute(f"CREATE OR REPLACE VIEW {view_name} AS\n{self.to_sql()}")
        return view_name


# Convenience alias
Query = DGMQuery
