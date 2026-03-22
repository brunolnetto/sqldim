# ADR: Recursive Hierarchy Dimensions

**Date**: 2026-03-18
**POC**: @pingu
**TL;DR**: Introduce native parent-child dimension support alongside the existing graph traversal module. Provide three physical strategies (adjacency list, materialized path, closure table) behind a `HierarchyStrategy` protocol, with recursive CTE queries for runtime traversal. Bridges the gap between flat Kimball dimensions and real-world hierarchical data (org charts, BOM, category trees).

## Status

Proposed

## Context

sqldim can model hierarchies indirectly via the graph module — `EdgeModel` with `is_self_referential=True` plus `TraversalEngine`'s recursive CTEs. The SaaS growth example demonstrates referral tree traversal this way.

However, this approach has limitations for Kimball-style dimensional modeling:

| Issue | Graph Approach | Native Hierarchy Approach |
|-------|---------------|--------------------------|
| Dimension attributes | Lost — vertices are generic `Vertex` dataclasses | Preserved — parent and child are the same `DimensionModel` |
| SCD versioning | Not integrated — graph edges don't track SCD state | Integrated — hierarchy changes can trigger SCD events |
| Aggregate rollups | Manual — user writes SQL against graph CTE | Declarative — `rollup(level=2)` on hierarchy metadata |
| Fact joins | Indirect — fact → dimension → graph → dimension | Direct — fact joins dimension, hierarchy is a dimension property |
| Bridge tables | No concept of bridge weights in graph | Hierarchy bridges can carry allocation weights |

### Common Hierarchical Use Cases in Data Warehousing

- **Org charts**: Employee → Manager → Department → Division (variable depth)
- **Bill of Materials**: Component → Sub-assembly → Product (multi-parent)
- **Location trees**: City → State → Country → Region (fixed depth, role-playing)
- **Category trees**: SKU → Category → Department → Division (multi-parent, retail)
- **Account charts**: Account → Sub-account → Cost Center (financial reporting)

## Decision

Introduce hierarchy as a dimension-level concern, not a graph-level concern. Three physical strategies behind a protocol, with the adjacency list as default.

### HierarchyDimension Mixin

```python
class HierarchyDimension(DimensionModel, table=True):
    """
    Mixin for parent-child dimension tables.
    """
    parent_id: Optional[int] = Field(
        foreign_key="self.id",  # Self-referential FK
        hierarchy_parent=True,   # Metadata flag for schema graph
    )
    hierarchy_level: int = Field(default=0)  # Depth from root (0 = root)
    hierarchy_path: Optional[str] = None     # Materialized path (strategy-dependent)
```

### HierarchyStrategy Protocol

```python
class HierarchyStrategy(Protocol):
    """Protocol for hierarchy physical implementations."""

    def build_sql(self, table: str, parent_col: str, id_col: str) -> str:
        """SQL to build/maintain the hierarchy structure."""
        ...

    def ancestors_sql(self, table: str, id_col: str, node_id: str, max_depth: int = -1) -> str:
        """SQL to find all ancestors of a node."""
        ...

    def descendants_sql(self, table: str, id_col: str, node_id: str, max_depth: int = -1) -> str:
        """SQL to find all descendants of a node."""
        ...

    def rollup_sql(self, fact_table: str, dim_table: str, measure: str,
                   id_col: str, parent_col: str, level: int) -> str:
        """SQL to aggregate a measure up to a specific hierarchy level."""
        ...
```

### Strategy Implementations

#### 1. Adjacency List (Default)

Simplest — stores only `parent_id`. All queries use `WITH RECURSIVE` CTEs (already supported by `TraversalEngine`).

```sql
-- Ancestors of node 42
WITH RECURSIVE hierarchy AS (
    SELECT id, parent_id, 1 AS depth
    FROM org_dim WHERE id = 42
    UNION ALL
    SELECT d.id, d.parent_id, h.depth + 1
    FROM org_dim d JOIN hierarchy h ON d.id = h.parent_id
    WHERE h.depth < :max_depth
)
SELECT * FROM hierarchy WHERE id != 42;
```

**Pros**: Minimal storage, simple inserts, natural fit for DuckDB's recursive CTEs.
**Cons**: Ancestor/descendant queries are O(depth) per query. No pre-computed rollups.

#### 2. Materialized Path

Stores the full path as a string (e.g., `"/1/5/12/42/"`) in `hierarchy_path`. Enables substring-based ancestor queries without recursion.

```sql
-- All descendants of node 5
SELECT * FROM org_dim WHERE hierarchy_path LIKE '/1/5/%';
-- All ancestors of node 42
SELECT * FROM org_dim WHERE '/1/5/12/42/' LIKE hierarchy_path || '%';
```

**Pros**: O(1) ancestor/descendant lookup, simple substring queries, works in all SQL dialects.
**Cons**: Path updates on subtree moves are O(subtree_size), path length limits in some DBs.

#### 3. Closure Table

Separate bridge table storing all ancestor-descendant pairs with depth:

```python
class OrgClosure(BridgeModel, table=True):
    __bridge_keys__ = ["ancestor_id", "descendant_id"]
    ancestor_id: int
    descendant_id: int
    depth: int = 0  # 0 = self, 1 = parent, 2 = grandparent, ...
```

```sql
-- All ancestors of node 42
SELECT d.* FROM org_dim d
JOIN org_closure c ON d.id = c.ancestor_id
WHERE c.descendant_id = 42 AND c.depth > 0;
```

**Pros**: O(1) any-direction traversal, supports multi-parent, depth-filtered queries.
**Cons**: O(n²) storage for deep/wide hierarchies, requires maintenance on inserts/moves.

### Strategy Selection

```python
class OrgDimension(HierarchyDimension, SCD2Mixin, table=True):
    __natural_key__ = ["employee_code"]
    __hierarchy_strategy__ = "adjacency"  # "adjacency" | "materialized_path" | "closure"
    id: int = Field(primary_key=True)
    employee_code: str = Field(natural_key=True)
    parent_id: Optional[int] = Field(foreign_key="self.id", hierarchy_parent=True)
    name: str
```

Default is `"adjacency"` — simplest, works with existing recursive CTE infrastructure. Users opt into materialized path or closure table when query performance demands it.

### Rollup Queries

A key use case is aggregating measures up the hierarchy:

```python
# "Total sales rolled up to region level (depth 2)"
HierarchyRoller.rollup(
    fact=SalesFact,
    dimension=OrgDimension,
    measure="revenue",
    level=2,
    hierarchy_column="parent_id",
)
```

Emits SQL using the hierarchy strategy's `rollup_sql` — for adjacency, this is a recursive CTE that aggregates at each level. For closure tables, it's a simple JOIN + GROUP BY.

### Integration with Existing Graph Module

The graph module is **not replaced** — it remains for cross-entity relationship traversal (customer → product → category). Hierarchy dimensions handle **intra-entity** parent-child relationships where both parent and child are the same dimension model.

The schema graph bridges both:
- `Field(hierarchy_parent=True)` → schema graph records the self-referential relationship
- `EdgeModel.is_self_referential` → schema graph detects graph edges on the same vertex type
- Both can coexist: a dimension can have a parent-child hierarchy AND participate in graph edges

### SCD Interaction

Hierarchies change over time (reorgs, category restructuring). With SCD2:

- **Adjacency list**: `parent_id` is a tracked column — changes create new versions. Point-in-time hierarchy queries use `WHERE valid_from <= :as_of AND valid_to > :as_of`.
- **Materialized path**: `hierarchy_path` must be recomputed on SCD2 version creation (the path references surrogate keys, which change).
- **Closure table**: Must be rebuilt when SCD2 creates new versions (ancestor-descendant pairs reference specific SK versions).

Adjacency list is the most SCD-compatible strategy — closure table requires the most maintenance.

## Consequences

**Positive**
- Hierarchical dimensions become first-class Kimball citizens — not just graph afterthoughts
- Rollup queries are declarative — no raw recursive SQL from users
- Strategy protocol allows performance tuning without model changes
- SCD2 integration preserves historical hierarchy state
- Existing graph module continues to handle cross-entity relationships

**Neutral**
- `HierarchyDimension` is a mixin — opt-in, doesn't affect flat dimensions
- Default adjacency strategy reuses existing recursive CTE infrastructure
- `hierarchy_path` and closure table columns are only added when those strategies are selected

**Negative**
- Adds complexity to SCD2 processing — hierarchy columns must be tracked and maintained
- Closure table maintenance is O(n²) for large hierarchies — needs compaction strategy
- Materialized path subtree moves are expensive — not suitable for frequently-reorged hierarchies
- Multi-parent hierarchies (e.g., SKU in multiple categories) only work with closure tables
- Three strategies mean three code paths to test and maintain

## Future Work

| Item | Description |
|------|-------------|
| Ragged/skip-level support | Handle hierarchies where levels are skipped (employee → division, no department) |
| Multi-parent hierarchy | First-class support for nodes with multiple parents (closure table only) |
| Hierarchy-aware fact grain | Facts at different hierarchy levels (daily at leaf, monthly at root) |
| Slowly changing closure table | Versioned closure entries for point-in-time hierarchy queries |
| Hierarchy visualization | Tree rendering in schema graph output |

## Key Files (Planned)

| File | Change |
|------|--------|
| `sqldim/core/kimball/dimensions/hierarchy.py` | New `HierarchyDimension` mixin + strategy protocol + implementations |
| `sqldim/core/kimball/fields.py` | Add `hierarchy_parent` metadata flag |
| `sqldim/core/kimball/schema_graph.py` | Detect self-referential hierarchy relationships |
| `sqldim/core/query/hierarchy.py` | New `HierarchyRoller` for rollup query generation |

## See Also

- [SCD Engine Design](./scd-engine-design.md) — SCD2 interaction with hierarchy columns
- [Graph Analytics Roadmap](./graph-analytics-roadmap.md) — cross-entity graph vs. intra-entity hierarchy
- [Analytical Indexing Strategy](./analytical-indexing-strategy.md) — FK auto-index covers parent_id
