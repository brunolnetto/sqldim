"""DGM temporal DSL types — TemporalMode, TemporalOrdering, TemporalWindow,
TemporalAgg, DeltaSpec, DeltaQuery (DGM v0.16 §3.2, §4.1, §4.2, §5.1)."""

from __future__ import annotations

__all__ = [
    # TemporalMode
    "TemporalMode",
    "EVENTUALLY",
    "GLOBALLY",
    "NEXT",
    "ONCE",
    "PREVIOUSLY",
    "UntilMode",
    "SinceMode",
    # TemporalOrdering
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
# TemporalMode  (DGM §4.1 — used inside PathPred.temporal_mode)
# ---------------------------------------------------------------------------


class TemporalMode:
    """Abstract base for all CTL temporal modes used in PathPred."""


class _SingletonMode(TemporalMode):
    """Base for modes without a payload — subclasses are used as the class itself."""
    sql_operator: str = ""


class EVENTUALLY(_SingletonMode):
    """EF / AF — the property holds at some future hop."""
    sql_operator = "EVENTUALLY"


class GLOBALLY(_SingletonMode):
    """EG / AG — the property holds at every hop."""
    sql_operator = "GLOBALLY"


class NEXT(_SingletonMode):
    """EX / AX — the property holds at the immediately next hop."""
    sql_operator = "NEXT"


class ONCE(_SingletonMode):
    """Past dual of NEXT — holds at the immediately previous hop."""
    sql_operator = "ONCE"


class PREVIOUSLY(_SingletonMode):
    """Past dual of GLOBALLY — holds at every prior hop."""
    sql_operator = "PREVIOUSLY"


class UntilMode(TemporalMode):
    """E(φ U ψ) / A(φ U ψ) — φ holds until ψ becomes true (CTL operator)."""

    def __init__(self, pred: object) -> None:
        self.pred = pred


class SinceMode(TemporalMode):
    """Past dual of UNTIL — φ has held since ψ (LTL past operator)."""

    def __init__(self, pred: object) -> None:
        self.pred = pred


# ---------------------------------------------------------------------------
# TemporalOrdering  (DGM §3.2 — used in Compose.temporal)
# ---------------------------------------------------------------------------


class TemporalOrdering:
    """Abstract base for all ordering constraints on composed path hops."""

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


# ── Point-based orderings ────────────────────────────────────────────────────


class BEFORE(TemporalOrdering):
    """left_ts < right_ts (strict temporal precedence)."""

    def __init__(self, left_ts: str, right_ts: str) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts

    def to_sql(self) -> str:
        return f"{self.left_ts} < {self.right_ts}"


class AFTER(TemporalOrdering):
    """left_ts > right_ts (strict temporal succession)."""

    def __init__(self, left_ts: str, right_ts: str) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts

    def to_sql(self) -> str:
        return f"{self.left_ts} > {self.right_ts}"


class CONCURRENT(TemporalOrdering):
    """left_ts ≈ right_ts — equal or within optional tolerance."""

    def __init__(
        self,
        left_ts: str,
        right_ts: str,
        tolerance: "object | None" = None,
    ) -> None:
        self.left_ts = left_ts
        self.right_ts = right_ts
        self.tolerance = tolerance

    def to_sql(self) -> str:
        if self.tolerance is None:
            return f"{self.left_ts} = {self.right_ts}"
        return f"ABS({self.left_ts} - {self.right_ts}) <= {self.tolerance}"


# ── Allen's 13 interval relations ────────────────────────────────────────────
# Each takes l=(l_start, l_end) and r=(r_start, r_end) tuples.


class _AllenBase(TemporalOrdering):
    def __init__(self, left: tuple, right: tuple) -> None:
        self.left = left
        self.right = right

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


class MEETS(_AllenBase):
    """l ends exactly when r starts: l_end = r_start."""

    def to_sql(self) -> str:
        return f"{self.left[1]} = {self.right[0]}"


class MET_BY(_AllenBase):
    """r ends exactly when l starts: r_end = l_start."""

    def to_sql(self) -> str:
        return f"{self.right[1]} = {self.left[0]}"


class OVERLAPS(_AllenBase):
    """l starts before r, ends after r starts but before r ends."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} < {rs} AND {le} > {rs} AND {le} < {re}"


class OVERLAPPED_BY(_AllenBase):
    """r starts before l, ends after l starts but before l ends."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{rs} < {ls} AND {re} > {ls} AND {re} < {le}"


class STARTS(_AllenBase):
    """l and r share a start; l ends before r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} < {re}"


class STARTED_BY(_AllenBase):
    """l and r share a start; l ends after r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} > {re}"


class DURING(_AllenBase):
    """l is entirely within r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} > {rs} AND {le} < {re}"


class CONTAINS(_AllenBase):
    """r is entirely within l."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} < {rs} AND {le} > {re}"


class FINISHES(_AllenBase):
    """l and r share an end; l starts after r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{le} = {re} AND {ls} > {rs}"


class FINISHED_BY(_AllenBase):
    """l and r share an end; l starts before r."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{le} = {re} AND {ls} < {rs}"


class EQUALS(_AllenBase):
    """l and r have the same start and end."""

    def to_sql(self) -> str:
        ls, le = self.left
        rs, re = self.right
        return f"{ls} = {rs} AND {le} = {re}"


# ── LTL path-level operators ─────────────────────────────────────────────────


class UntilOrdering(TemporalOrdering):
    """φ holds at every intermediate hop until ψ first becomes true."""

    def __init__(self, hold_pred: object, trigger_pred: object) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return (
            f"path_until({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"
        )


class SinceOrdering(TemporalOrdering):
    """Past dual of UNTIL: φ has held at every hop since ψ last became true."""

    def __init__(self, hold_pred: object, trigger_pred: object) -> None:
        self.hold_pred = hold_pred
        self.trigger_pred = trigger_pred

    def to_sql(self) -> str:
        return (
            f"path_since({self.hold_pred.to_sql()}, {self.trigger_pred.to_sql()})"
        )


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
