"""Graph-oriented model base classes: VertexModel and EdgeModel.

Both classes extend SQLModel and register their schema with the global
:class:`GraphSchemaGraph` on first instantiation.
"""
from sqldim.models.graph import VertexModel, EdgeModel

__all__ = ["VertexModel", "EdgeModel"]
