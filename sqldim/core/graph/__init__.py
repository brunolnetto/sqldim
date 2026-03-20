"""Schema graph registry and DuckDB-backed traversal engine.

Exports :class:`GraphModel` for vertex/edge registration and
:class:`TraversalEngine` / :class:`DuckDBTraversalEngine` for queries.
Also exports graph model base classes and the extended SchemaGraph for
convenience.

Tier 4 additions:
- :class:`UnifiedGraph` — fuses SchemaGraph (ERD) and GraphModel (runtime)
- :class:`SchemaDiff` / :class:`ColumnDiff` — schema evolution diffing
"""

from sqldim.core.graph.registry import GraphModel, UnifiedGraph
from sqldim.core.graph.traversal import TraversalEngine, DuckDBTraversalEngine
from sqldim.core.graph.models import VertexModel, EdgeModel, Vertex
from sqldim.core.graph.schema_graph import (
    SchemaGraph,
    GraphSchema,
    SchemaDiff,
    ColumnDiff,
)
from sqldim.core.kimball.schema_graph import (
    RolePlayingRef,
    _col_dimension,
    _role_ref_from_col,
)

__all__ = [
    "GraphModel",
    "UnifiedGraph",
    "TraversalEngine",
    "DuckDBTraversalEngine",
    "VertexModel",
    "EdgeModel",
    "Vertex",
    "SchemaGraph",
    "GraphSchema",
    "SchemaDiff",
    "ColumnDiff",
    "RolePlayingRef",
    "_col_dimension",
    "_role_ref_from_col",
]
