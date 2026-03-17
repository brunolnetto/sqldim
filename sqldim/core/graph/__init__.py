"""Schema graph registry and DuckDB-backed traversal engine.

Exports :class:`GraphModel` for vertex/edge registration and
:class:`TraversalEngine` / :class:`DuckDBTraversalEngine` for queries.
Also exports graph model base classes and the extended SchemaGraph for
convenience.
"""
from sqldim.core.graph.registry import GraphModel
from sqldim.core.graph.traversal import TraversalEngine, DuckDBTraversalEngine
from sqldim.core.graph.models import VertexModel, EdgeModel, Vertex
from sqldim.core.graph.schema_graph import SchemaGraph, GraphSchema
from sqldim.core.kimball.schema_graph import (
    RolePlayingRef,
    _col_dimension,
    _role_ref_from_col,
)

__all__ = [
    "GraphModel", "TraversalEngine", "DuckDBTraversalEngine",
    "VertexModel", "EdgeModel", "Vertex",
    "SchemaGraph", "GraphSchema",
    "RolePlayingRef", "_col_dimension", "_role_ref_from_col",
]
