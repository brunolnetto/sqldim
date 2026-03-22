"""DGM graph expression wrappers, topology, and trim types (DGM §16.4, §11).

Algorithm descriptor classes (GraphAlgorithm, NodeAlg, PairAlg, SubgraphAlg,
CommAlg, etc.) live in :mod:`graph_algs` and are re-exported from here so
that ``from sqldim.core.query.dgm.graph import X`` continues to work unchanged.
"""

from __future__ import annotations

# Re-export algorithm classes for backward compatibility
from sqldim.core.query.dgm.graph._algs import (  # noqa: F401
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
    OUTGOING_SIGNATURES,
    INCOMING_SIGNATURES,
    DOMINANT_OUTGOING_SIGNATURE,
    DOMINANT_INCOMING_SIGNATURE,
    SIGNATURE_DIVERSITY,
    SHORTEST_PATH_LENGTH,
    MIN_WEIGHT_PATH_LENGTH,
    REACHABLE,
    DISTINCT_SIGNATURES,
    DOMINANT_SIGNATURE,
    SIGNATURE_SIMILARITY,
    MAX_FLOW,
    DENSITY,
    DIAMETER,
    GLOBAL_SIGNATURE_COUNT,
    GLOBAL_DOMINANT_SIGNATURE,
    SIGNATURE_ENTROPY,
)

# ── Graph expression wrappers  (DGM §11) ────────────────────────────────────


class NodeExpr:
    """Wraps a :class:`NodeAlg` together with the context alias it grounds to."""

    def __init__(self, algorithm: NodeAlg, alias: str) -> None:
        self.algorithm = algorithm
        self.alias = alias

    def to_sql(self) -> str:
        return f"{self.algorithm.to_sql()} /* alias={self.alias} */"


class PairExpr:
    """Wraps a :class:`PairAlg` together with src/tgt alias pair."""

    def __init__(self, algorithm: PairAlg, src: str, tgt: str) -> None:
        self.algorithm = algorithm
        self.src = src
        self.tgt = tgt

    def to_sql(self) -> str:
        return f"{self.algorithm.to_sql()} /* src={self.src}, tgt={self.tgt} */"


class SubgraphExpr:
    """Wraps a :class:`SubgraphAlg` over the current subgraph context.

    Parameters
    ----------
    algorithm:
        The subgraph algorithm to evaluate.
    partition:
        Optional list of :class:`~sqldim.core.query._dgm_refs.PropRef` values.
        When non-empty the algorithm is evaluated once per partition group and
        the result is broadcast to each tuple in that group (DGM §18.9).
    scope:
        Optional :class:`RelationshipSubgraph` restricting the subgraph (§3.4).
    """

    def __init__(
        self,
        algorithm: SubgraphAlg,
        partition: "list[object | None]" = None,
        scope: "RelationshipSubgraph | None" = None,
    ) -> None:
        self.algorithm = algorithm
        self.partition: list[object | None] = partition
        self.scope = scope

    def to_sql(self) -> str:
        base = self.algorithm.to_sql()
        if self.partition:
            cols = ", ".join(p.to_sql() for p in self.partition)
            return f"{base} /* partition=[{cols}] */"
        return base


class GraphExpr:
    """
    Dimensionally-grounded graph algorithm expression (DGM §18.12).

    Wraps a :class:`NodeExpr`, :class:`PairExpr`, or :class:`SubgraphExpr`
    and exposes a unified ``to_sql()`` interface.
    """

    def __init__(self, inner: "NodeExpr | PairExpr | SubgraphExpr") -> None:
        self.inner = inner

    def to_sql(self) -> str:
        return self.inner.to_sql()


# ---------------------------------------------------------------------------
# RelationshipSubgraph  (DGM §3.4)
# ---------------------------------------------------------------------------


class Endpoint:
    """Abstract base for RelationshipSubgraph endpoint descriptors (§3.4.1)."""


class Bound(Endpoint):
    """Fixed endpoint — a specific aliased node resolved in the query context."""

    def __init__(self, alias: str) -> None:
        self.alias = alias

    def __repr__(self) -> str:
        return f"Bound(alias={self.alias!r})"


class _Free(Endpoint):
    """Singleton Free endpoint — all nodes satisfying the traversal criterion."""

    _instance: "_Free | None" = None

    def __new__(cls) -> "_Free":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "Free"


# Canonical singleton for free endpoint
Free = _Free
FREE: _Free = _Free()

# Endpoint-case → granularity tier mapping
_ENDPOINT_TIER: dict[str, str] = {
    "BB": "PairExpr",
    "BF": "NodeExpr",
    "FB": "NodeExpr",
    "FF": "SubgraphExpr",
}


class RelationshipSubgraph:
    """Represents a scope-scoped subgraph with typed endpoints (DGM §3.4).

    Parameters
    ----------
    source:
        Source endpoint: :class:`Bound` (fixed alias) or :data:`FREE`.
    target:
        Target endpoint: :class:`Bound` (fixed alias) or :data:`FREE`.
    strategy:
        Optional path strategy governing traversal (e.g. ``ALL_PATHS()``).
    """

    def __init__(
        self,
        source: Endpoint,
        target: Endpoint,
        strategy: object = None,
    ) -> None:
        self.source = source
        self.target = target
        self.strategy = strategy

    @property
    def endpoint_case(self) -> str:
        """Two-character code: ``"BB"``, ``"BF"``, ``"FB"``, or ``"FF"``."""
        src = "B" if isinstance(self.source, Bound) else "F"
        tgt = "B" if isinstance(self.target, Bound) else "F"
        return src + tgt

    @property
    def granularity_tier(self) -> str:
        """DGM §3.4.3 granularity tier name for this endpoint configuration."""
        return _ENDPOINT_TIER[self.endpoint_case]


# ---------------------------------------------------------------------------
# GraphStatistics  (DGM §7.1)
# ---------------------------------------------------------------------------


class GraphStatistics:
    """Runtime graph statistics consumed by the planner (DGM §7.1).

    Parameters
    ----------
    node_count:
        Total node count in the graph.
    edge_count:
        Total edge count in the graph.
    degree_dist:
        Optional degree distribution summary (e.g. ``{"mean": 5.0}``).
    path_cardinality:
        Optional mapping of :class:`~sqldim.core.query._dgm_preds.BoundPath`
        identifiers to estimated row counts.
    property_dist:
        Optional mapping of PropRef → distribution summary.
    temporal_density:
        Optional mapping of (PropRef, Duration) → density scalar.
    scc_sizes:
        Optional mapping of SCC identifier → component size (from Tarjan).
    transposed_adj:
        G^T adjacency list: node_id → list of predecessor node_ids.
        Built at graph load time.
    """

    def __init__(
        self,
        node_count: int,
        edge_count: int,
        degree_dist: dict | None = None,
        path_cardinality: dict | None = None,
        property_dist: dict | None = None,
        temporal_density: dict | None = None,
        scc_sizes: dict | None = None,
        transposed_adj: dict | None = None,
    ) -> None:
        self.node_count = node_count
        self.edge_count = edge_count
        self.degree_dist = degree_dist
        self.path_cardinality = path_cardinality
        self.property_dist = property_dist
        self.temporal_density = temporal_density
        self.scc_sizes = scc_sizes
        self.transposed_adj: dict = transposed_adj if transposed_adj is not None else {}


# ---------------------------------------------------------------------------
# TrimCriterion + TrimJoin  (DGM §10.1, §18.8)
# ---------------------------------------------------------------------------


class TrimCriterion:
    """Abstract base for structural trim criteria used with :class:`TrimJoin`."""

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


class REACHABLE_BETWEEN(TrimCriterion):
    """Retain only nodes that lie on at least one directed path from *source* to *target*.

    This criterion cannot be expressed as a single ``PathPred`` existence check
    because it requires global knowledge of all paths in the subgraph.
    """

    def __init__(self, source: str, target: str) -> None:
        self.source = source
        self.target = target

    def to_sql(self) -> str:
        return f"REACHABLE_BETWEEN(source={self.source!r}, target={self.target!r})"


class REACHABLE_FROM(TrimCriterion):
    """Restrict context to the forward cone G_A* via BFS on G (Bound→Free).

    Retains only nodes reachable from *source* via forward traversal on the
    schema graph.  Computed as BFS/DFS on G starting from *source*.
    """

    def __init__(self, source: str) -> None:
        self.source = source

    def to_sql(self) -> str:
        return f"REACHABLE_FROM(source={self.source!r})"


class REACHABLE_TO(TrimCriterion):
    """Restrict context to the backward cone G_*B via BFS on G^T (Free→Bound).

    Retains only nodes from which *target* is reachable — computed via BFS on
    the transposed graph G^T.  Enables ``INCOMING_SIGNATURES`` and SINCE/ONCE
    temporal modes that require reverse traversal.
    """

    def __init__(self, target: str) -> None:
        self.target = target

    def to_sql(self) -> str:
        return f"REACHABLE_TO(target={self.target!r})"


class MIN_DEGREE(TrimCriterion):
    """Retain only nodes whose incident edge count meets a minimum threshold.

    Parameters
    ----------
    n:
        Minimum degree (inclusive).
    direction:
        ``"IN"`` / ``"OUT"`` / ``"BOTH"`` (default).
    """

    def __init__(self, n: int, direction: str = "BOTH") -> None:
        self.n = n
        self.direction = direction

    def to_sql(self) -> str:
        return f"MIN_DEGREE(n={self.n}, direction={self.direction!r})"


class SINK_FREE(TrimCriterion):
    """Retain only nodes that have at least one outgoing edge (no dead-end sinks)."""

    def to_sql(self) -> str:
        return "SINK_FREE()"


class SOURCE_FREE(TrimCriterion):
    """Retain only nodes that have at least one incoming edge (no dangling sources)."""

    def to_sql(self) -> str:
        return "SOURCE_FREE()"


class TrimJoin:
    """Wraps any ``Join`` expression and restricts the resulting node set by a
    structural criterion, evaluated before further path expansion (DGM §10.1).

    ``TrimJoin`` modifiers may be nested; they are applied inside-out during
    Phase 1 of query evaluation.

    Parameters
    ----------
    join:
        Any join expression (``Anchor``, ``PathJoin``, ``TrimJoin``, etc.).
    criterion:
        The :class:`TrimCriterion` applied to the node set produced by *join*.
    """

    def __init__(self, join: object, criterion: TrimCriterion) -> None:
        self.join = join
        self.criterion = criterion

    def to_sql(self) -> str:
        criterion_sql = self.criterion.to_sql()
        return f"TrimJoin(<join>, {criterion_sql})"
