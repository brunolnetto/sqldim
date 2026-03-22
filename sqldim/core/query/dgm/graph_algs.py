"""DGM graph algorithm descriptor classes (DGM §16.4, §11).

All concrete algorithm types (community, node, pair, subgraph) live here.
Expression wrappers (NodeExpr, PairExpr, SubgraphExpr, GraphExpr), topology
types (Endpoint, RelationshipSubgraph, GraphStatistics), and trim criteria
(TrimCriterion, TrimJoin) live in :mod:`graph`.
"""

from __future__ import annotations


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
