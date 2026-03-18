# ADR: Graph Analytics Roadmap — Metadata-Driven Property Graph

- **Date**: 2026-03-17
- **Status**: Proposed
- **Deciders**: sqldim maintainers

---

## Context

sqldim's `core/graph/` module provides a SQL-native property graph layer:

- **`VertexModel`** (extends `DimensionModel`) — vertices with optional `__vertex_type__`, `__vertex_properties__`.
- **`EdgeModel`** (extends `FactModel`) — directed/undirected edges with `__subject__`, `__object__`, `__edge_type__`.
- **`GraphModel`** — registry + traversal coordinator over existing SQL tables (neighbors, paths, aggregation, degree).
- **`TraversalEngine`** — recursive CTE SQL generation for PostgreSQL and DuckDB.
- **`DuckDBTraversalEngine`** — executes traversal SQL directly, with VIEW registration for composable lazy-loader pipelines.
- **`SchemaGraph`** (`core/graph/schema_graph.py`) — projects any Kimball star schema as a property graph, auto-inferring vertex/edge types from FK metadata.

The graph layer also has a companion in `core/kimball/schema_graph.py` (ERD-level star schema discovery — Mermaid diagrams, role-playing refs, star-schema introspection). The two are related but distinct: the Kimball version answers *"what joins to what?"*, while the graph version answers *"how do entities relate at runtime?"*.

### The Problem: Rich Metadata, Shallow Graph

The Kimball `Field()` factory embeds **semantic intent** into every column via `column.info`:

| Metadata | Lives On | Meaning |
|---|---|---|
| `surrogate_key` | DimensionModel columns | Technical PK — not business identity |
| `natural_key` | DimensionModel columns | Business identity for cross-system matching |
| `__natural_key__` | DimensionModel class | Which columns form the natural key |
| `__scd_type__` | DimensionModel class | History tracking strategy (1, 2, 3, 4, 6) |
| `measure` | FactModel columns | This column is a numeric metric |
| `additive` | FactModel columns | Whether `SUM` is semantically valid |
| `__grain__` | FactModel class | Business process grain declaration |
| `__strategy__` | FactModel class | Loading behavior (bulk, upsert, merge, accumulating) |
| `dimension` | FactModel FK columns | Which DimensionModel this FK points to |
| `role` | FactModel FK columns | Logical role for role-playing dimensions |
| `weight` | BridgeModel | Allocation factor to prevent double-counting |
| `__directed__` | EdgeModel class | Directed vs undirected edge |
| `scd` / `previous_column` | DimensionModel columns | Type 3 SCD previous-value tracking |

The graph layer currently uses **none** of this. It treats vertices and edges as opaque tables with `subject_id`/`object_id` integer columns. Traversal has no awareness of temporal versions, additive semantics, grain compatibility, role-playing, or bridge weights.

This is an architectural blind spot: the Kimball metadata exists precisely to make graph operations *correct*, not just *possible*.

## Decision

Layer graph analytics improvements into four tiers, each exploiting specific dimensional metadata that the graph layer currently ignores.

### Tier 1: Metadata-Aware Lookup (Low Effort, Immediate Correctness)

The simplest wins — use metadata that already exists but is bypassed.

| Capability | Metadata Source | Change |
|---|---|---|
| **Natural key lookup** | `__natural_key__` on VertexModel | Add `get_vertex_by_key(vertex_type, **key_values)` to `GraphModel`. Resolves business identity without knowing surrogate `id`. Essential for cross-system graph queries. |
| **Measure-safe aggregation** | `additive` in `column.info` | `neighbor_aggregation()` should inspect `additive`. If `additive=False`, only `count`, `max`, `min` are valid — `sum` and `avg` should raise `SchemaError` or warn. Currently any aggregation on any column is allowed. |
| **Grain-aware join validation** | `__grain__` on FactModel | Before generating multi-hop SQL through shared dimensions, validate that joined facts declare compatible grains. Prevent semantically meaningless fact-to-fact joins. |

### Tier 2: Temporal Graph Traversal (Medium Effort, High Value)

This is the largest single gap. Type 2 SCD dimensions create **versioned vertices** — the same business entity exists as multiple rows differentiated by `effective_from`/`effective_to` and `is_current`.

| Capability | Metadata Source | Change |
|---|---|---|
| **Point-in-time traversal** | `__scd_type__ == 2`, `is_current` column | `neighbors()` and `paths()` gain a `as_of: datetime` parameter. Recursive CTE adds `WHERE is_current = true` or `WHERE effective_from <= :as_of` for temporal correctness. |
| **Version-aware path finding** | Same | `paths()` returns vertex IDs that may have multiple versions. Add `temporal=True` mode that deduplicates to one row per natural key, using the version active at the query timestamp. |
| **SCD type dispatch** | `__scd_type__` on VertexModel | Type 1 dims need no temporal filter. Type 3 dims have `previous_column` pairs. Type 4 dims have separate current/historical tables. The engine should branch on `__scd_type__` rather than treating all vertices identically. |

### Tier 3: Role-Playing & Bridge-Aware Traversal (Medium Effort, Unique Value)

Role-playing dimensions and bridge tables are Kimball-specific graph structures that generic graph engines don't handle.

| Capability | Metadata Source | Change |
|---|---|---|
| **Role-aware edge resolution** | `role` in `column.info`, `RolePlayingRef` | Auto-discover edges from `SchemaGraph._fk_dimensions()`. A fact with `order_date_id` (role=`"ordered_on"`) and `ship_date_id` (role=`"shipped_on"`) both pointing to `DateDim` should produce two *logical* edge types: `ordered_on` and `shipped_on` — not one ambiguous edge. |
| **Weighted bridge traversal** | `weight` on BridgeModel | When a path traverses through a `BridgeModel`, multiply edge measures by `weight`. Prevents double-counting in multi-valued dimension analysis (e.g., "account holders" where a customer has weight 0.5 across two accounts). |
| **Strategy-guided freshness** | `__strategy__` on FactModel | Add `freshness(vertex_type) -> datetime | None` to `GraphModel`. For `bulk` strategies, check the edge table's latest timestamp. For `upsert`/`merge`, the data is always current. For `accumulating`, check the high-water mark. |

### Tier 4: Schema-to-Graph Projection & Fusion (Advanced)

These capabilities bridge the `core/kimball/schema_graph.py` (ERD) and `core/graph/` (runtime property graph) into a unified analytical surface.

| Capability | Approach |
|---|---|
| **Auto-edge discovery from FK metadata** | Use `SchemaGraph._fk_dimensions()` to generate implicit `EdgeModel` registrations in `GraphModel` without requiring users to declare `__subject__`/`__object__`. Any fact with FK metadata becomes traversable. |
| **Medallion × Schema fusion graph** | Merge the property graph with medallion lineage into a heterogeneous graph — two edge types (`schema_join` and `data_flow`) on the same node set. Enables: *"If bronze source X is late, which gold facts are blocked?"* |
| **Data mart clustering** | Community detection (Louvain) on the schema graph to auto-discover subject areas from FK topology alone. |
| **Schema evolution diffing** | Graph edit distance between two `SchemaGraph` snapshots — *"what changed between schema v2 and v3?"* |

## Consequences

### Positive

- Tier 1 is pure validation logic — no new dependencies, no SQL changes, just guards on existing APIs
- Tier 2 makes the graph layer correct for the primary Kimball use case (Type 2 dimensions) rather than accidentally correct
- Tier 3 exploits metadata that only exists in Kimball toolkits — no generic graph library can do this
- Tier 4 bridges the two `schema_graph.py` files into a coherent architecture, eliminating the current "two disconnected graphs" problem

### Negative

- Temporal traversal (Tier 2) increases CTE complexity — may need query hints or materialized views for large Type 2 tables
- Role-aware edge resolution (Tier 3) changes the implicit edge cardinality — existing users who rely on "one fact = one edge" may see different behavior
- Auto-edge discovery (Tier 4) blurs the line between `SchemaGraph` (ERD) and `GraphModel` (runtime) — needs clear API boundaries to avoid confusion

### Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Temporal CTE performance degradation | Default `as_of=None` (no filter) preserves current behavior. Temporal mode is opt-in per call. |
| Backward compatibility on `neighbor_aggregation` | Add `validate_additive=True` parameter; default `False` preserves current permissive behavior. |
| Two `SchemaGraph` classes causing confusion | Tier 4 should produce a single `UnifiedGraph` that wraps both, deprecating direct use of either for graph analytics. |
| Bridge weight propagation complexity | Keep weight multiplication as an opt-in `weighted=True` parameter on `aggregate()` — not default behavior. |

## Implementation Priority

```
Immediate (correctness fixes, low effort):
  1. Natural key lookup on GraphModel              → get_vertex_by_key()
  2. Additive guard on neighbor_aggregation        → validate_additive param
  3. Grain validation on multi-fact joins          → __grain__ check

Near-term (temporal correctness, medium effort):
  4. Point-in-time neighbor/path traversal         → as_of parameter
  5. SCD type dispatch in TraversalEngine          → branch on __scd_type__
  6. Role-aware edge auto-discovery                → RolePlayingRef integration

Later (fusion, advanced):
  7. Weighted bridge traversal                     → weight propagation
  8. Auto-edge discovery from FK metadata          → SchemaGraph → GraphModel bridge
  9. Medallion × Schema fusion graph               → heterogeneous graph
  10. Schema evolution diffing                      → graph edit distance
```
