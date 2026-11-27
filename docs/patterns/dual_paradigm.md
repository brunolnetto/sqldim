# Pattern: The Dual-Paradigm Model

A star schema serves aggregation; a graph serves questions about
relationships ("friends of friends", "shared supply-chain paths"). sqldim
lets one relational schema serve both: a `VertexModel` is a `DimensionModel`
and an `EdgeModel` is a `FactModel`, so the same tables are queried with SQL
and traversed with `GraphModel`.

## Vertices and edges

```python
import asyncio
from typing import Optional

from sqlmodel import Session, SQLModel, create_engine

from sqldim import EdgeModel, Field, GraphModel, VertexModel


class Player(VertexModel, table=True):
    __tablename__ = "player"
    __vertex_type__ = "player"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class TeammateEdge(EdgeModel, table=True):
    __tablename__ = "teammate"
    __edge_type__ = "is_teammate_with"
    __subject__ = Player
    __object__ = Player
    __directed__ = False

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="player.id")
    object_id: int = Field(foreign_key="player.id")


engine = create_engine("sqlite://")
SQLModel.metadata.create_all(engine, tables=[Player.__table__, TeammateEdge.__table__])
with Session(engine) as session:
    session.add_all([Player(id=23, name="Jordan"), Player(id=33, name="Pippen"),
                     Player(id=91, name="Rodman")])
    session.add_all([TeammateEdge(subject_id=23, object_id=33),
                     TeammateEdge(subject_id=91, object_id=23)])
    session.commit()

    graph = GraphModel(Player, TeammateEdge, session=session)
    jordan = asyncio.run(graph.get_vertex(Player, 23))
    teammates = asyncio.run(graph.neighbors(jordan, edge_type=TeammateEdge))
    print(sorted(p.name for p in teammates))  # ['Pippen', 'Rodman']
```

`GraphModel` generates the SQL: `neighbors()` is one hop (`direction="out"`,
`"in"` or `"both"`, the default), and `paths(source, target, max_hops=3)` is a
recursive CTE bounded by `max_hops`, written for DuckDB and PostgreSQL (SQLite has no arrays for it). `paths()` awaits `session.execute`, so it
needs an `AsyncSession`; `get_vertex()` and `neighbors()` accept either kind.

## What it does and does not give you

- **No data movement.** The traversal SQL runs on the tables your loaders
  write; there is no separate graph store.
- **SCD2 versions are rows.** A `VertexModel` with `SCD2Mixin` has one row per
  version, and the traversal SQL does not filter on `is_current`; filter the
  versions you mean.
- **One script, two paradigms.** Aggregate with SQL (or `DimensionalQuery`)
  and traverse with `GraphModel` over the same session.
