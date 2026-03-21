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
    "SHORTEST_PATH_LENGTH",
    "MIN_WEIGHT_PATH_LENGTH",
    "REACHABLE",
    "MAX_FLOW",
    "DENSITY",
    "DIAMETER",
    "NodeExpr",
    "PairExpr",
    "SubgraphExpr",
    "GraphExpr",
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


# ── Subgraph algorithms ──────────────────────────────────────────────────────


class MAX_FLOW(SubgraphAlg):
    """Maximum flow between a designated source and sink node."""

    def __init__(self, source: str, sink: str) -> None:
        self.source = source
        self.sink = sink

    def to_sql(self) -> str:
        return f"max_flow(source={self.source!r}, sink={self.sink!r})"


class DENSITY(SubgraphAlg):
    """Graph density (edges / possible edges)."""

    def to_sql(self) -> str:
        return "density()"


class DIAMETER(SubgraphAlg):
    """Graph diameter (longest shortest path)."""

    def to_sql(self) -> str:
        return "diameter()"


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
    """

    def __init__(
        self,
        algorithm: SubgraphAlg,
        partition: "list[object | None]" = None,
    ) -> None:
        self.algorithm = algorithm
        self.partition: list[object | None] = partition

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
