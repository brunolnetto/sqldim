"""Schema graph registry and DuckDB-backed traversal engine.

Exports :class:`GraphModel` for vertex/edge registration and
:class:`TraversalEngine` / :class:`DuckDBTraversalEngine` for queries.
Also exports graph model base classes for convenience.
"""
from sqldim.graph.registry import GraphModel
from sqldim.graph.traversal import TraversalEngine, DuckDBTraversalEngine
from sqldim.graph.models import VertexModel, EdgeModel, Vertex

__all__ = ["GraphModel", "TraversalEngine", "DuckDBTraversalEngine",
           "VertexModel", "EdgeModel", "Vertex"]
