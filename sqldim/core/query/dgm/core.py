"""
DGM — Dimensional Graph Model query algebra.

Implements the three-band query model Q = B1 ∘ B2? ∘ B3? from DGM §3:
  B1  Context     — anchor/context, path_join, temporal_join, where
  B2  Aggregation — group_by, agg, having / aggregate (combined alias)
  B3  Ranking     — window, qualify / rank (combined alias)

Model-first API
---------------
Pass SQLModel classes directly instead of table-name strings::

    DGMQuery()
        .context(Customer, alias="c")
        .path_join(OrderFact, from_alias="c", to_alias="s")
        .aggregate("c.region", total_rev="SUM(s.revenue)",
                   having=ScalarPred(AggRef("total_rev"), ">", 5000))

FK join conditions are inferred automatically from SQLAlchemy column metadata.
"""

from __future__ import annotations

from typing import Iterator

from sqldim.exceptions import SemanticError

# -- Re-export from private submodules (public API unchanged) ----------------
from sqldim.core.query.dgm.refs import (  # noqa: F401
    PropRef,
    AggRef,
    WinRef,
    _format_value,
    _paren_if_compound,
    _ConstExpr,
    ArithExpr,
    SignatureRef,
)
from sqldim.core.query.dgm.preds import (  # noqa: F401
    ScalarPred,
    AND,
    OR,
    NOT,
    RawPred,
    Strategy,
    ALL,
    SHORTEST,
    K_SHORTEST,
    MIN_WEIGHT,
    Quantifier,
    _HopBase,
    VerbHop,
    BridgeHop,
    Compose,
    _flatten_path,
    _path_pred_sql,
    PathPred,
    PathAgg,
    SignaturePred,
)
# ---------------------------------------------------------------------------
# Model-first FK inference
# ---------------------------------------------------------------------------


def _collect_fk_matches(to_model: type, from_table_name: str) -> list[str]:
    """Return FK column names on *to_model* that point at *from_table_name*."""
    if not hasattr(to_model, "__table__"):
        raise SemanticError(
            f"{to_model.__name__} has no SQLAlchemy table metadata. "
            "Is it a SQLModel class with table=True?"
        )
    matches: list[str] = []
    for col_name, col in to_model.__table__.columns.items():
        for fk in col.foreign_keys:
            if fk.target_fullname.rsplit(".", 1)[0] == from_table_name:
                matches.append(col_name)
    return matches


def _infer_on(
    from_alias: str,
    to_alias: str,
    to_model: type,
    from_table_name: str,
) -> str:
    """Derive an ON clause '{from_alias}.id = {to_alias}.fk' via FK metadata.

    Raises :class:`~sqldim.exceptions.SemanticError` when no FK is found or
    more than one FK column targets the same table (role-playing ambiguity —
    pass *on=* explicitly in that case).
    """
    matches = _collect_fk_matches(to_model, from_table_name)
    if not matches:
        raise SemanticError(
            f"No FK from {to_model.__name__!r} → {from_table_name!r} found. "
            "Specify on= explicitly."
        )
    if len(matches) > 1:
        raise SemanticError(
            f"Ambiguous FK: {to_model.__name__!r} has {len(matches)} FK columns "
            f"pointing at {from_table_name!r} ({', '.join(matches)}). "
            "Specify on= explicitly."
        )
    return f"{from_alias}.id = {to_alias}.{matches[0]}"


# ---------------------------------------------------------------------------
# Ref-kind validation helpers
# ---------------------------------------------------------------------------


def _iter_scalar_refs(pred: object) -> Iterator[object]:
    """Recursively yield every Ref object found inside *pred*'s ScalarPred leaves."""
    if isinstance(pred, ScalarPred):
        yield pred.ref
    elif isinstance(pred, (AND, OR)):
        for sub in pred.preds:
            yield from _iter_scalar_refs(sub)
    elif isinstance(pred, NOT):
        yield from _iter_scalar_refs(pred.pred)
    # PathPred has no Ref inside; its sub_filter refs would be band-scoped separately


def _has_path_pred(pred: object) -> bool:
    """Return True if *pred* contains any PathPred node."""
    if isinstance(pred, PathPred):
        return True
    if isinstance(pred, (AND, OR)):
        return any(_has_path_pred(sub) for sub in pred.preds)
    if isinstance(pred, NOT):
        return _has_path_pred(pred.pred)
    return False


def _has_signature_pred(pred: object) -> bool:
    """Return True if *pred* subtree contains any SignaturePred node."""
    if isinstance(pred, SignaturePred):
        return True
    if isinstance(pred, (AND, OR)):
        return any(_has_signature_pred(sub) for sub in pred.preds)
    if isinstance(pred, NOT):
        return _has_signature_pred(pred.pred)
    return False


def _check_where_pred(pred: object) -> None:
    if isinstance(pred, (str, RawPred)):
        return  # raw SQL strings bypass Ref-kind checking
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (AggRef, WinRef)):
            raise SemanticError(
                f"B1 Where only admits PropRef; found {type(ref).__name__}. "
                "Use Having (B2) for AggRef or Qualify (B3) for WinRef."
            )


def _reject_band_pred(pred: object, band_label: str) -> None:
    """Raise SemanticError if *pred* contains PathPred or SignaturePred."""
    if _has_path_pred(pred):
        raise SemanticError(f"PathPred is not allowed in {band_label}.")
    if _has_signature_pred(pred):
        raise SemanticError(
            f"SignaturePred is not allowed in {band_label}; use Where (B1)."
        )


def _check_having_pred(pred: object) -> None:
    _reject_band_pred(pred, "Having (B2)")
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (PropRef, WinRef)):
            raise SemanticError(
                f"B2 Having only admits AggRef; found {type(ref).__name__}. "
                "Use Where (B1) for PropRef or Qualify (B3) for WinRef."
            )
        if isinstance(ref, SignatureRef):
            raise SemanticError(
                "SignatureRef is B1-only; not allowed in Having (B2)."
            )


def _check_qualify_pred(pred: object) -> None:
    _reject_band_pred(pred, "Qualify (B3)")
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (PropRef, AggRef)):
            raise SemanticError(
                f"B3 Qualify only admits WinRef; found {type(ref).__name__}. "
                "Use Where (B1) for PropRef or Having (B2) for AggRef."
            )
        if isinstance(ref, SignatureRef):
            raise SemanticError(
                "SignatureRef is B1-only; not allowed in Qualify (B3)."
            )


# ---------------------------------------------------------------------------
# DGMQuery helpers
# ---------------------------------------------------------------------------


def _validate_b2(q: "DGMQuery") -> None:
    if q._having_pred is not None and not q._agg_exprs:
        raise SemanticError("B2: .having() requires .agg() expressions")


def _validate_b3(q: "DGMQuery") -> None:
    if q._qualify_pred is not None and not q._window_exprs:
        raise SemanticError("B3: .qualify() requires .window() expressions")


def _validate_bands(q: "DGMQuery") -> None:
    if not q._anchor_table:
        raise SemanticError("DGMQuery requires anchor() (B1 mandatory)")
    _validate_b2(q)
    _validate_b3(q)


def _build_select_list(q: "DGMQuery") -> list[str]:
    if q._group_by_cols or q._agg_exprs:
        parts: list[str] = list(q._group_by_cols)
        parts += [f"{expr} AS {name}" for name, expr in q._agg_exprs.items()]
    else:
        parts = ["*"]
    parts += [f"{expr} AS {name}" for name, expr in q._window_exprs.items()]
    return parts


def _augment_on_with_scd2(on: str, alias: str, as_of: str) -> str:
    return (
        f"{on} AND {alias}.valid_from <= '{as_of}'"
        f" AND ({alias}.valid_to IS NULL OR {alias}.valid_to > '{as_of}')"
    )


def _semi_additive_expr(
    measure: str,
    fallback: str,
    forbidden: set,
    active: set,
) -> str:
    """Return the SQL expression for a semi-additive measure."""
    if not (forbidden & active):
        return f"SUM({measure})"
    if fallback == "last":
        order_col = next(iter(forbidden & active))
        return (
            f"LAST_VALUE({measure}) OVER "
            f"(PARTITION BY {order_col} ORDER BY {order_col} "
            f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        )
    if fallback == "avg":
        return f"AVG({measure})"
    if fallback == "max":
        return f"MAX({measure})"
    return f"SUM({measure})"


def _dim_join_clause(
    anchor_alias: str,
    dim_table: str,
    alias: str,
    fact_fk: str,
    dim_pk: str,
    as_of: "str | None",
) -> str:
    """Build a LEFT JOIN clause for a SCD2-aware dimension join."""
    base = f"{anchor_alias}.{fact_fk} = {alias}.{dim_pk}"
    if as_of:
        return (
            f"LEFT JOIN {dim_table} {alias} ON {base}"
            f" AND {alias}.valid_from::DATE <= '{as_of}'::DATE"
            f" AND ({alias}.valid_to IS NULL"
            f" OR {alias}.valid_to::DATE > '{as_of}'::DATE)"
        )
    return f"LEFT JOIN {dim_table} {alias} ON {base} AND {alias}.is_current = TRUE"


def _build_from_joins(q: "DGMQuery") -> str:
    anchor_alias = q._anchor_alias or q._anchor_table
    lines = [f"FROM {q._anchor_table} {anchor_alias}"]
    for table, alias, on in q._joins:
        on_full = _augment_on_with_scd2(on, alias, q._as_of) if q._as_of else on
        lines.append(f"LEFT JOIN {table} {alias} ON {on_full}")
    for dim_table, alias, fact_fk, dim_pk in q._scd2_joins:
        lines.append(
            _dim_join_clause(anchor_alias, dim_table, alias, fact_fk, dim_pk, q._as_of)
        )
    return "\n".join(lines)


# -- Re-export: Graph algorithm hierarchy (DGM §16.4, §11) ------------------
from sqldim.core.query.dgm.graph import (  # noqa: F401
    GraphAlgorithm,
    NodeAlg,
    PairAlg,
    SubgraphAlg,
    CommAlg,
    LOUVAIN,
    LABEL_PROPAGATION,
    CONNECTED_COMPONENTS,
    TARJAN_SCC,
    PAGE_RANK,
    BETWEENNESS_CENTRALITY,
    CLOSENESS_CENTRALITY,
    DEGREE,
    COMMUNITY_LABEL,
    # TrailExpr NodeAlg
    OUTGOING_SIGNATURES,
    INCOMING_SIGNATURES,
    DOMINANT_OUTGOING_SIGNATURE,
    DOMINANT_INCOMING_SIGNATURE,
    SIGNATURE_DIVERSITY,
    SHORTEST_PATH_LENGTH,
    MIN_WEIGHT_PATH_LENGTH,
    REACHABLE,
    # TrailExpr PairAlg
    DISTINCT_SIGNATURES,
    DOMINANT_SIGNATURE,
    SIGNATURE_SIMILARITY,
    MAX_FLOW,
    DENSITY,
    DIAMETER,
    # TrailExpr SubgraphAlg
    GLOBAL_SIGNATURE_COUNT,
    GLOBAL_DOMINANT_SIGNATURE,
    SIGNATURE_ENTROPY,
    NodeExpr,
    PairExpr,
    SubgraphExpr,
    GraphExpr,
    # RelationshipSubgraph
    Endpoint,
    Bound,
    Free,
    FREE,
    RelationshipSubgraph,
    # GraphStatistics
    GraphStatistics,
    # TrimJoin / TrimCriterion (DGM §10.1, §18.8)
    TrimCriterion,
    REACHABLE_BETWEEN,
    REACHABLE_FROM,
    REACHABLE_TO,
    MIN_DEGREE,
    SINK_FREE,
    SOURCE_FREE,
    TrimJoin,
)

# -- Re-export: Refs (DGM §4.1) --------------------------------------------
from sqldim.core.query.dgm.refs import SignatureRef  # noqa: F401

# -- Re-export: Preds (DGM §4.1) -------------------------------------------
from sqldim.core.query.dgm.preds import (  # noqa: F401
    VerbHopInverse,
    SAFETY,
    LIVENESS,
    RESPONSE,
    PERSISTENCE,
    RECURRENCE,
    SequenceMatch,
    SignaturePred,
)

# -- Re-export: Temporal types (DGM §3.2, §4.1, §4.2, §5.1) ---------------
from sqldim.core.query.dgm.annotations import (  # noqa: F401
    GrainKind,
    SCDKind,
    WeightConstraintKind,
    BridgeSemanticsKind,
    RAGGED,
    SchemaAnnotation,
    Conformed,
    Grain,
    SCDType,
    Degenerate,
    RolePlaying,
    ProjectsFrom,
    FactlessFact,
    DerivedFact,
    WeightConstraint,
    BridgeSemantics,
    Hierarchy,
    annotation_kind,
    AnnotationSigma,
)
from sqldim.core.query.dgm.temporal import (  # noqa: F401
    TemporalMode,
    EVENTUALLY,
    GLOBALLY,
    NEXT,
    ONCE,
    PREVIOUSLY,
    UntilMode,
    SinceMode,
    TemporalOrdering,
    BEFORE,
    AFTER,
    CONCURRENT,
    MEETS,
    MET_BY,
    OVERLAPS,
    OVERLAPPED_BY,
    STARTS,
    STARTED_BY,
    DURING,
    CONTAINS,
    FINISHES,
    FINISHED_BY,
    EQUALS,
    UntilOrdering,
    SinceOrdering,
    TemporalWindow,
    ROLLING,
    TRAILING,
    PERIOD,
    YTD,
    QTD,
    MTD,
    TemporalAgg,
    DeltaSpec,
    ADDED_NODES,
    REMOVED_NODES,
    ADDED_EDGES,
    REMOVED_EDGES,
    CHANGED_PROPERTY,
    ROLE_DRIFT,
    DeltaQuery,
)

# -- Re-export: Recommender (DGM §7) ----------------------------------------
from sqldim.core.query.dgm.recommender import (  # noqa: F401
    SuggestionKind,
    Suggestion,
    Stage1Result,
    Stage2Result,
    DGMRecommender,
    ENTROPY_THRESHOLD,
)

# -- Re-export: Planner (DGM §6.2) ------------------------------------------
from sqldim.core.query.dgm.planner import (  # noqa: F401
    QueryTarget,
    SinkTarget,
    PreComputation,
    CostEstimate,
    ExportPlan,
    DGMPlanner,
    SMALL,
    CLOSURE_THRESHOLD,
    SMALL_GRAPH_THRESHOLD,
    DENSE,
)

# -- Re-export: Exporters (DGM §6.3) ----------------------------------------
from sqldim.core.query.dgm.exporters import (  # noqa: F401
    CypherExporter,
    SPARQLExporter,
    DGMJSONExporter,
    DGMYAMLExporter,
)


# ---------------------------------------------------------------------------
# TemporalContext  (DGM §5.1)
# ---------------------------------------------------------------------------


class TemporalContext:
    """Snapshot-query temporal context — sets the default as-of point and
    version-resolution modes for all joins in a query.

    Parameters
    ----------
    default_as_of:
        ISO-8601 timestamp applied as the implicit SCD2 snapshot point.
    node_resolution:
        ``"STRICT"`` — reject tuples with missing versions;
        ``"LAX"`` (default) — silently drop them.
    edge_resolution:
        ``"STRICT"`` / ``"LAX"`` (default) applied to edge validity windows.
    """

    def __init__(
        self,
        default_as_of: str,
        node_resolution: str = "LAX",
        edge_resolution: str = "LAX",
    ) -> None:
        self.default_as_of = default_as_of
        self.node_resolution = node_resolution
        self.edge_resolution = edge_resolution


# ---------------------------------------------------------------------------
# DGMQuery
# ---------------------------------------------------------------------------


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
        """Set the snapshot temporal context for all joins in the query.

        This is the ``TemporalContext`` operator from DGM §5.1. It applies
        ``default_as_of`` as the default SCD2 snapshot point and configures
        version-resolution modes for nodes and edges.

        Explicit :meth:`temporal_join` / :meth:`as_of` calls on individual
        joins continue to override the context for those sub-joins.
        """
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
