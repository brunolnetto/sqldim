"""DGM graph algorithm hierarchy and expression wrapper types (DGM §16.4, §11)."""

from __future__ import annotations


__all__ = [
    "GraphAlgorithm",
    "NodeAlg",
    "PairAlg",
    "SubgraphAlg",
    "CommAlg",
    "LOUVAIN",
    "LABEL_PROPAGATION",
    "CONNECTED_COMPONENTS",
    "TARJAN_SCC",
    "PAGE_RANK",
    "BETWEENNESS_CENTRALITY",
    "CLOSENESS_CENTRALITY",
    "DEGREE",
    "COMMUNITY_LABEL",
    # TrailExpr NodeAlg
    "OUTGOING_SIGNATURES",
    "INCOMING_SIGNATURES",
    "DOMINANT_OUTGOING_SIGNATURE",
    "DOMINANT_INCOMING_SIGNATURE",
    "SIGNATURE_DIVERSITY",
    "SHORTEST_PATH_LENGTH",
    "MIN_WEIGHT_PATH_LENGTH",
    "REACHABLE",
    # TrailExpr PairAlg
    "DISTINCT_SIGNATURES",
    "DOMINANT_SIGNATURE",
    "SIGNATURE_SIMILARITY",
    "MAX_FLOW",
    "DENSITY",
    "DIAMETER",
    # TrailExpr SubgraphAlg
    "GLOBAL_SIGNATURE_COUNT",
    "GLOBAL_DOMINANT_SIGNATURE",
    "SIGNATURE_ENTROPY",
    "NodeExpr",
    "PairExpr",
    "SubgraphExpr",
    "GraphExpr",
    # RelationshipSubgraph
    "Endpoint",
    "Bound",
    "Free",
    "FREE",
    "RelationshipSubgraph",
    # GraphStatistics
    "GraphStatistics",
    # TrimJoin / TrimCriterion (DGM §10.1, §18.8)
    "TrimCriterion",
    "REACHABLE_BETWEEN",
    "REACHABLE_FROM",
    "REACHABLE_TO",
    "MIN_DEGREE",
    "SINK_FREE",
    "SOURCE_FREE",
    "TrimJoin",
]

# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


class GraphAlgorithm:
    """Abstract base for all graph algorithms."""

    def to_sql(self) -> str:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__}.to_sql()")


class NodeAlg(GraphAlgorithm):
    """Algorithm that produces a scalar value *per node* in the graph."""


class PairAlg(GraphAlgorithm):
    """Algorithm that produces a scalar value *per ordered node pair*."""


class SubgraphAlg(GraphAlgorithm):
    """Algorithm that produces a single scalar over the entire subgraph."""


# ── Community-detection algorithms (CommAlg ⊆ NodeAlg) ─────────────────────


class CommAlg(NodeAlg):
    """Community-detection algorithm (subtype of :class:`NodeAlg`)."""


class LOUVAIN(CommAlg):
    """Louvain modularity-maximisation community detection."""

    def to_sql(self) -> str:
        return "louvain_community()"


class LABEL_PROPAGATION(CommAlg):
    """Label-propagation community detection."""

    def to_sql(self) -> str:
        return "label_propagation_community()"


class CONNECTED_COMPONENTS(CommAlg):
    """Connected-components partitioning (undirected / weak connectivity)."""

    def to_sql(self) -> str:
        return "connected_components()"


class TARJAN_SCC(CommAlg):
    """Tarjan's strongly-connected-components algorithm (directed SCC, DGM §18.7).

    Assigns a component label identifying the maximal SCC each node belongs to.
    Unlike :class:`CONNECTED_COMPONENTS`, respects the direction of verb and
    bridge edges — enabling detection of directed event cycles in DGM fact graphs.
    """

    def to_sql(self) -> str:
        return "tarjan_scc()"


# ── Node algorithms ──────────────────────────────────────────────────────────


class PAGE_RANK(NodeAlg):
    """PageRank centrality with configurable damping and iteration count."""

    def __init__(self, damping: float = 0.85, iterations: int = 20) -> None:
        self.damping = damping
        self.iterations = iterations

    def to_sql(self) -> str:
        return f"page_rank(damping={self.damping}, iterations={self.iterations})"


class BETWEENNESS_CENTRALITY(NodeAlg):
    """Betweenness centrality."""

    def to_sql(self) -> str:
        return "betweenness_centrality()"


class CLOSENESS_CENTRALITY(NodeAlg):
    """Closeness centrality."""

    def to_sql(self) -> str:
        return "closeness_centrality()"


class DEGREE(NodeAlg):
    """Node degree, optionally restricted to a single direction (DGM §8.3).

    Parameters
    ----------
    direction:
        ``"IN"`` — incoming edges only;
        ``"OUT"`` — outgoing edges only;
        ``"BOTH"`` (default) — combined in+out degree.
    """

    def __init__(self, direction: str = "BOTH") -> None:
        self.direction = direction

    def to_sql(self) -> str:
        return f"degree(direction='{self.direction}')"


class COMMUNITY_LABEL(NodeAlg):
    """Community label per node, backed by a configurable :class:`CommAlg`."""

    def __init__(self, algorithm: "CommAlg | None" = None) -> None:
        self.algorithm: CommAlg = algorithm if algorithm is not None else LOUVAIN()

    def to_sql(self) -> str:
        return f"community_label({self.algorithm.to_sql()})"


# ── TrailExpr NodeAlg types ──────────────────────────────────────────────────


class OUTGOING_SIGNATURES(NodeAlg):
    """Count of distinct label sequences on all simple paths starting at A.

    TrailExpr single-relaxation (Bound→Free).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"OUTGOING_SIGNATURES(max_depth={self.max_depth})"
        return "OUTGOING_SIGNATURES()"


class INCOMING_SIGNATURES(NodeAlg):
    """Count of distinct label sequences on all simple paths ending at B.

    Computed via BFS on G^T.  TrailExpr single-relaxation (Free→Bound).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"INCOMING_SIGNATURES(max_depth={self.max_depth})"
        return "INCOMING_SIGNATURES()"


class DOMINANT_OUTGOING_SIGNATURE(NodeAlg):
    """Most frequent label sequence in the forward cone of A."""

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"DOMINANT_OUTGOING_SIGNATURE(max_depth={self.max_depth})"
        return "DOMINANT_OUTGOING_SIGNATURE()"


class DOMINANT_INCOMING_SIGNATURE(NodeAlg):
    """Most frequent label sequence in the backward cone of B."""

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"DOMINANT_INCOMING_SIGNATURE(max_depth={self.max_depth})"
        return "DOMINANT_INCOMING_SIGNATURE()"


class SIGNATURE_DIVERSITY(NodeAlg):
    """Normalised variety of path signatures in the applicable cone.

    Scalar in [0, 1].  TrailExpr single-relaxation (Bound→Free or Free→Bound).
    """

    def to_sql(self) -> str:
        return "SIGNATURE_DIVERSITY()"


# ── Pair algorithms ──────────────────────────────────────────────────────────


class SHORTEST_PATH_LENGTH(PairAlg):
    """Length of the shortest (hop-count-minimal) path between a node pair."""

    def to_sql(self) -> str:
        return "shortest_path_length()"


class MIN_WEIGHT_PATH_LENGTH(PairAlg):
    """Length of the minimum-weight path between a node pair."""

    def to_sql(self) -> str:
        return "min_weight_path_length()"


class REACHABLE(PairAlg):
    """Boolean reachability between a node pair."""

    def to_sql(self) -> str:
        return "reachable()"


class DISTINCT_SIGNATURES(PairAlg):
    """Count of distinct label sequences connecting A to B.

    TrailExpr fixed-endpoint (Bound→Bound).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"DISTINCT_SIGNATURES(max_depth={self.max_depth})"
        return "DISTINCT_SIGNATURES()"


class DOMINANT_SIGNATURE(PairAlg):
    """Most frequent label sequence connecting A to B.

    TrailExpr fixed-endpoint (Bound→Bound).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"DOMINANT_SIGNATURE(max_depth={self.max_depth})"
        return "DOMINANT_SIGNATURE()"


class SIGNATURE_SIMILARITY(PairAlg):
    """Fraction of A→B paths whose label sequence matches *reference*.

    TrailExpr fixed-endpoint (Bound→Bound).
    """

    def __init__(self, reference: list[str]) -> None:
        self.reference = reference

    def to_sql(self) -> str:
        return f"SIGNATURE_SIMILARITY(reference={self.reference!r})"


# ── Subgraph algorithms ──────────────────────────────────────────────────────


class MAX_FLOW(SubgraphAlg):
    """Maximum flow between a designated source and sink/target node.

    The ``target`` parameter is the canonical name (DGM v0.16 §4.2); ``sink``
    is accepted as a backward-compatible alias.
    """

    def __init__(
        self,
        source: str,
        target: str | None = None,
        capacity: str | None = None,
        *,
        sink: str | None = None,
    ) -> None:
        self.source = source
        # Accept either 'target' or 'sink' (backward-compat alias)
        if target is not None:
            self._target = target
        elif sink is not None:
            self._target = sink
        else:
            raise TypeError("MAX_FLOW() requires 'target' (or 'sink') argument")
        self.capacity = capacity

    @property
    def target(self) -> str:
        return self._target

    @property
    def sink(self) -> str:
        """Backward-compatible alias for :attr:`target`."""
        return self._target

    def to_sql(self) -> str:
        if self.capacity is not None:
            return (
                f"max_flow(source={self.source!r}, target={self._target!r},"
                f" capacity={self.capacity!r})"
            )
        return f"max_flow(source={self.source!r}, target={self._target!r})"


class DENSITY(SubgraphAlg):
    """Graph density (edges / possible edges)."""

    def to_sql(self) -> str:
        return "density()"


class DIAMETER(SubgraphAlg):
    """Graph diameter (longest shortest path)."""

    def to_sql(self) -> str:
        return "diameter()"


class GLOBAL_SIGNATURE_COUNT(SubgraphAlg):
    """Count of distinct label sequences across the full scope.

    TrailExpr fully-free (Free→Free).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"GLOBAL_SIGNATURE_COUNT(max_depth={self.max_depth})"
        return "GLOBAL_SIGNATURE_COUNT()"


class GLOBAL_DOMINANT_SIGNATURE(SubgraphAlg):
    """Most frequent label sequence globally.

    TrailExpr fully-free (Free→Free).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"GLOBAL_DOMINANT_SIGNATURE(max_depth={self.max_depth})"
        return "GLOBAL_DOMINANT_SIGNATURE()"


class SIGNATURE_ENTROPY(SubgraphAlg):
    """Shannon entropy H = -Σ p(s) log₂ p(s) over path signature distribution.

    H=0: all paths same type (maximally focused).
    H=log₂(k): k equally frequent types (maximally diverse).
    Range [0, log₂(|distinct signatures|)].

    TrailExpr fully-free (Free→Free).
    """

    def __init__(self, max_depth: int | None = None) -> None:
        self.max_depth = max_depth

    def to_sql(self) -> str:
        if self.max_depth is not None:
            return f"SIGNATURE_ENTROPY(max_depth={self.max_depth})"
        return "SIGNATURE_ENTROPY()"


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
