"""Backward-compatible shim — canonical location is sqldim.graph.models."""
from sqldim.graph.models import VertexModel, EdgeModel, Vertex  # noqa: F401

__all__ = ["VertexModel", "EdgeModel", "Vertex"]
