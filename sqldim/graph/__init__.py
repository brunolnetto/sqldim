"""Schema graph registry and DuckDB-backed traversal engine.

Exports :class:`GraphModel` for vertex/edge registration and
:class:`TraversalEngine` / :class:`DuckDBTraversalEngine` for queries.
"""
from sqldim.graph.registry import GraphModel
from sqldim.graph.traversal import TraversalEngine, DuckDBTraversalEngine

__all__ = ["GraphModel", "TraversalEngine", "DuckDBTraversalEngine"]
