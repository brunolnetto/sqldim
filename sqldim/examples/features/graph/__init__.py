"""Showcase: graph-traversal analytics over a dimensional schema.

Uses :class:`TraversalEngine` to walk the registered ``VertexModel`` /
``EdgeModel`` graph and return adjacency paths as DataFrames.
"""

from sqldim.examples.features.graph.showcase import run_showcase

__all__ = ["run_showcase"]
