# Pattern: The Dual-Paradigm Model

The most common architectural friction in data engineering is the choice between a **Star Schema** (for BI/aggregations) and a **Graph Schema** (for network analysis). `sqldim` eliminates this choice by projecting your Dimensions as Graph Vertices.

## The Pain Point: The "Join-Depth" Tax
In traditional SQL, finding "Friends of Friends" or "Shared Supply Chain Paths" requires recursive queries that are notoriously difficult to write and maintain. Many teams export data to Neo4j just to solve this, introducing sync lag and infrastructure overhead.

## The Solution: Seamless Projection
With `sqldim`, your existing `DimensionModel` is already a graph vertex. By adding an `EdgeModel`, you can traverse relationships using a native Graph API without leaving your SQL database.

```python
from sqldim import VertexModel, EdgeModel

# 1. Any Dimension is a Vertex
class Player(VertexModel, table=True):
    __vertex_type__ = "player"
    name: str

# 2. Any Fact is an Edge
class TeammateEdge(EdgeModel, table=True):
    __edge_type__ = "is_teammate_with"
    __subject__ = Player
    __object__ = Player
    __directed__ = False # Undirected graph support
```

## Traversal in 3 Lines
```python
graph = GraphModel(Player, TeammateEdge, session=session)
jordan = await graph.get_vertex(Player, id=23)

# Recursive CTE generated under the hood
teammates = await graph.neighbors(jordan, edge_type=TeammateEdge)
```

## Why it matters
- **No Data Movement**: Your graph queries run directly on your relational data.
- **Transactional Consistency**: If your dimension updates via SCD2, your graph reflects the latest state immediately.
- **Hybrid Power**: Use SQL for "Total Points Scored" and Graph for "Shortest Path between Teammates" in the same script.
