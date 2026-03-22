"""DGM temporal DSL types — TemporalWindow, TemporalAgg, DeltaSpec, DeltaQuery,
TemporalContext (DGM v0.16 §3.2, §4.1, §4.2, §5.1).

TemporalMode and its subclasses live in :mod:`temporal_modes`.
TemporalOrdering and Allen's interval relations live in :mod:`temporal_ordering`.
Both are re-exported here so that ``from sqldim.core.query.dgm.temporal import X``
continues to work unchanged.
"""

from __future__ import annotations

# Re-export mode and ordering types for backward compatibility
from sqldim.core.query.dgm.temporal._modes import (  # noqa: F401
    TemporalMode,
    EVENTUALLY,
    GLOBALLY,
    NEXT,
    ONCE,
    PREVIOUSLY,
    UntilMode,
    SinceMode,
)
from sqldim.core.query.dgm.temporal._ordering import (  # noqa: F401
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
)

__all__ = [
    # TemporalContext (moved here from core.py)
    "TemporalContext",
    # TemporalMode (re-exported from temporal_modes)
    "TemporalMode",
    "EVENTUALLY",
    "GLOBALLY",
    "NEXT",
    "ONCE",
    "PREVIOUSLY",
    "UntilMode",
    "SinceMode",
    # TemporalOrdering (re-exported from temporal_ordering)
    "TemporalOrdering",
    "BEFORE",
    "AFTER",
    "CONCURRENT",
    "MEETS",
    "MET_BY",
    "OVERLAPS",
    "OVERLAPPED_BY",
    "STARTS",
    "STARTED_BY",
    "DURING",
    "CONTAINS",
    "FINISHES",
    "FINISHED_BY",
    "EQUALS",
    "UntilOrdering",
    "SinceOrdering",
    # TemporalWindow
    "TemporalWindow",
    "ROLLING",
    "TRAILING",
    "PERIOD",
    "YTD",
    "QTD",
    "MTD",
    # TemporalAgg
    "TemporalAgg",
    # DeltaSpec / DeltaQuery
    "DeltaSpec",
    "ADDED_NODES",
    "REMOVED_NODES",
    "ADDED_EDGES",
    "REMOVED_EDGES",
    "CHANGED_PROPERTY",
    "ROLE_DRIFT",
    "DeltaQuery",
]


# ---------------------------------------------------------------------------
# TemporalContext  (DGM §5.1 — snapshot-query temporal context)
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
# TemporalWindow  (DGM §4.2)
# ---------------------------------------------------------------------------


class TemporalWindow:
    """Abstract base for all temporal aggregation window specifications."""

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


class _SingletonWindow(TemporalWindow):
    """Base for parameterless windows — subclasses are used as the class itself."""
    sql_operator: str = ""

    def to_sql(self) -> str:
        return self.sql_operator


class YTD(_SingletonWindow):
    """Year-to-date window."""
    sql_operator = "YTD"


class QTD(_SingletonWindow):
    """Quarter-to-date window."""
    sql_operator = "QTD"


class MTD(_SingletonWindow):
    """Month-to-date window."""
    sql_operator = "MTD"


class ROLLING(TemporalWindow):
    """Rolling window of fixed duration (e.g. '30 DAYS')."""

    def __init__(self, duration: str) -> None:
        self.duration = duration

    def to_sql(self) -> str:
        return f"ROLLING({self.duration})"


class TRAILING(TemporalWindow):
    """Trailing window of *n* temporal units (e.g. TRAILING(7, 'DAY'))."""

    def __init__(self, n: int, unit: str) -> None:
        self.n = n
        self.unit = unit

    def to_sql(self) -> str:
        return f"TRAILING({self.n}, '{self.unit}')"


class PERIOD(TemporalWindow):
    """Fixed calendar period [start, end]."""

    def __init__(self, start: str, end: str) -> None:
        self.start = start
        self.end = end

    def to_sql(self) -> str:
        return f"PERIOD('{self.start}', '{self.end}')"


# ---------------------------------------------------------------------------
# TemporalAgg  (DGM §4.2)
# ---------------------------------------------------------------------------


class TemporalAgg:
    """Temporal aggregation: fn(ref) over a time-ordered window frame.

    Parameters
    ----------
    fn:
        Aggregate function name (SUM, COUNT, AVG, MIN, MAX, PROD).
    ref:
        Measure reference (PropRef).
    timestamp:
        Timestamp column reference (PropRef) used for partitioning/ordering.
    window:
        TemporalWindow instance (or singleton class).
    """

    def __init__(
        self,
        fn: str,
        ref: object,
        timestamp: object,
        window: "type | TemporalWindow",
    ) -> None:
        self.fn = fn
        self.ref = ref
        self.timestamp = timestamp
        self.window = window

    def _window_sql(self) -> str:
        """Return the window SQL string whether *window* is a class or instance."""
        if isinstance(self.window, type) and issubclass(self.window, _SingletonWindow):
            return self.window.sql_operator
        if isinstance(self.window, _SingletonWindow):
            return self.window.to_sql()
        if isinstance(self.window, TemporalWindow):
            return self.window.to_sql()
        return str(self.window)  # pragma: no cover

    def to_sql(self) -> str:
        win_sql = self._window_sql()
        ts_sql = self.timestamp.to_sql()
        ref_sql = self.ref.to_sql()
        return (
            f"{self.fn}({ref_sql}) OVER ("
            f"ORDER BY {ts_sql} "
            f"{win_sql})"
        )


# ---------------------------------------------------------------------------
# DeltaSpec  (DGM §5.1)
# ---------------------------------------------------------------------------


class DeltaSpec:
    """Abstract base for all Q_delta specification variants."""


class ADDED_NODES(DeltaSpec):
    """Rows whose node type τ_N was added between t₁ and t₂."""

    def __init__(self, node_type: str) -> None:
        self.node_type = node_type


class REMOVED_NODES(DeltaSpec):
    """Rows whose node type τ_N was removed between t₁ and t₂."""

    def __init__(self, node_type: str) -> None:
        self.node_type = node_type


class ADDED_EDGES(DeltaSpec):
    """Edges of type τ_E added between t₁ and t₂."""

    def __init__(self, edge_type: str) -> None:
        self.edge_type = edge_type


class REMOVED_EDGES(DeltaSpec):
    """Edges of type τ_E removed between t₁ and t₂."""

    def __init__(self, edge_type: str) -> None:
        self.edge_type = edge_type


class CHANGED_PROPERTY(DeltaSpec):
    """Rows where *alias.prop* changed in a given direction between t₁ and t₂."""

    def __init__(self, alias: str, prop: str, comparator: str) -> None:
        self.alias = alias
        self.prop = prop
        self.comparator = comparator


class ROLE_DRIFT(DeltaSpec):
    """Rows that drifted from *from_type* to *to_type* between t₁ and t₂."""

    def __init__(self, alias: str, from_type: str, to_type: str) -> None:
        self.alias = alias
        self.from_type = from_type
        self.to_type = to_type


# ---------------------------------------------------------------------------
# DeltaQuery  (DGM §5.1)
# ---------------------------------------------------------------------------


class DeltaQuery:
    """Delta query: compare graph state at t₁ vs t₂ according to *spec*.

    Operates outside the three-band structure.

    Parameters
    ----------
    t1, t2:
        ISO-8601 timestamp strings bounding the comparison window.
    spec:
        :class:`DeltaSpec` instance describing what kind of change to detect.
    filter:
        Optional predicate (T_W) applied during delta detection.
    """

    def __init__(
        self,
        t1: str,
        t2: str,
        spec: "DeltaSpec",
        *,
        filter: "object | None" = None,
    ) -> None:
        self.t1 = t1
        self.t2 = t2
        self.spec = spec
        self.filter = filter

    def to_sql(self) -> str:
        """Return a placeholder delta SQL expression."""
        spec_name = type(self.spec).__name__
        filter_sql = ""
        if self.filter is not None:
            filter_sql = f" WHERE {self.filter.to_sql()}"
        return (
            f"-- Q_delta({spec_name}, t1='{self.t1}', t2='{self.t2}'){filter_sql}"
        )
