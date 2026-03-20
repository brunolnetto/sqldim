## Origins & Purpose

Dimensional modeling was developed by **Ralph Kimball** in the 1990s (codified in *The Data Warehouse Toolkit*, first published 1996). It's a design technique for **analytical databases (OLAP)**, optimized for query performance and business understandability — as opposed to normalized (3NF) designs optimized for transactional integrity (OLTP).

The core philosophy: *organize data the way business users think about it*, not the way it's stored in source systems.

---

## The Two Core Table Types

### Fact Tables
Store **measurements, metrics, and events** — the things you want to analyze.

- Each row represents a **business event** at a specific grain (e.g., one row per order line, one row per daily account balance)
- Columns are either **foreign keys** to dimensions or **measures** (numeric, additive values)
- Typically narrow but very tall (millions/billions of rows)
- Three types of measures:
  - **Additive** — can be summed across all dimensions (e.g., revenue, quantity)
  - **Semi-additive** — can be summed across some dimensions but not all (e.g., account balance: summing across customers is fine, summing across time is not)
  - **Non-additive** — can never be summed meaningfully (e.g., ratios, percentages, unit prices)

### Dimension Tables
Store **descriptive context** about the business entities involved in events.

- Each row describes one entity (a customer, a product, a date)
- Columns are **attributes** used for filtering, grouping, and labeling
- Typically wide (many columns) but shorter (thousands/millions of rows)
- Always have a **surrogate key** (system-generated, meaningless integer/UUID) as the primary key, separate from the **natural key** (the business identifier from the source system)

---

## Schema Patterns

### Star Schema
The simplest and most common layout. One central fact table surrounded by denormalized dimension tables. Optimized for query performance — joins are simple and few.

```
         [Date Dim]
              |
[Customer Dim] — [Sales Fact] — [Product Dim]
              |
         [Store Dim]
```

### Snowflake Schema
Dimension tables are **normalized** into sub-dimensions (e.g., Product → Category → Department). Saves storage, but adds joins and complexity. Generally discouraged by Kimball unless storage is a hard constraint.

### Galaxy Schema (Fact Constellation)
Multiple fact tables sharing **conformed dimensions**. This is the real-world enterprise pattern — a Sales Fact and an Inventory Fact both reference the same Date and Product dimensions.

---

## Key Concepts

### The Grain
The single most important design decision. The grain defines **exactly what one row in the fact table represents**. Everything else flows from it.

- "One row per individual line item on a sales transaction" — atomic grain
- "One row per daily snapshot of account balance" — periodic snapshot grain
- You must declare it, commit to it, and never mix grains in one table

### Slowly Changing Dimensions (SCD)
Dimensions change over time (a customer moves, a product is re-categorized). SCDs define the strategy for handling this:

| Type | Strategy | Tradeoff |
|------|----------|----------|
| **Type 0** | Ignore changes, keep original | No history at all |
| **Type 1** | Overwrite old value | Loses history, simple |
| **Type 2** | Add a new row with validity dates | Full history, most common |
| **Type 3** | Add a "previous value" column | Limited history, awkward |
| **Type 4** | Mini-dimension for rapidly changing attributes | Complex but performant |
| **Type 5** | Type 1 + Type 4 | Hybrid, but complex |
| **Type 6** | Hybrid of 1+2+3 | Full history + current value on all rows |

SCD Type 2 is by far the most important to implement well. It requires:
- A surrogate key per version
- `valid_from` / `valid_to` date columns
- An `is_current` flag (denormalized for query convenience)
- Logic to "close" the prior row and "open" a new one on change

### Conformed Dimensions
A dimension is **conformed** when it has the same structure and meaning across multiple fact tables and subject areas. A `DateDimension` used by both Sales and HR facts is conformed. This is what enables cross-process analysis ("drill across") and is a cornerstone of the **Kimball Bus Architecture**.

### Degenerate Dimensions
A dimension with no attributes beyond its key — so it lives as a column directly in the fact table rather than in its own table. A transaction ID or order number is the classic example.

### Junk Dimensions
Low-cardinality flags and indicators (e.g., `is_promo`, `is_return`, `channel`) that don't belong to any natural dimension. Rather than cluttering the fact table, they're combined into a single **junk dimension** table.

### Role-Playing Dimensions
A single physical dimension table playing multiple logical roles in the same fact table. A Date dimension might be referenced three times in a flight fact as `departure_date_id`, `arrival_date_id`, and `booking_date_id`. Typically handled via **views** or **aliases**.

### Bridge Tables
Handle many-to-many relationships between facts and dimensions (e.g., a sales transaction attributed to multiple salespeople). A bridge table sits between the fact and dimension with weighting factors.

---

## Fact Table Patterns

### Transaction Fact Table
One row per discrete event. The most common type. Naturally additive. Examples: sales, web clicks, payments.

### Periodic Snapshot Fact Table
One row per entity per time period. Captures state at regular intervals. Used when you need to see balances, inventory levels, etc. over time. Semi-additive measures are common here.

### Accumulating Snapshot Fact Table
One row per business process instance, updated as it progresses through stages. Classic example: an order that moves from *placed → approved → shipped → delivered*. Has multiple date foreign keys, one per milestone. Rows are **updated in place**, which is unusual in dimensional modeling.

---

## Kimball vs. Inmon

The two major schools of thought in data warehousing:

| | **Kimball** | **Inmon** |
|---|---|---|
| Approach | Bottom-up (build data marts first) | Top-down (enterprise DW first) |
| Storage | Denormalized star schemas | Normalized 3NF enterprise DW |
| Speed to value | Faster | Slower |
| Consistency | Via conformed dimensions | Via central DW |
| Complexity | Lower per mart | Higher overall |

Modern practice often blends both — a normalized staging layer feeding denormalized dimensional marts.

---

## Modern Context

Dimensional modeling has seen a revival in the **dbt + cloud data warehouse** era:

- **DuckDB, BigQuery, Snowflake, Redshift** all perform extremely well on star schemas
- **dbt** encourages dimensional thinking through its staging/marts layer convention
- The **medallion architecture** (Bronze → Silver → Gold) maps loosely to: raw → conformed → dimensional
- **One Big Table (OBT)** is sometimes used as a shortcut but loses the flexibility of proper dimensional models
- Tools like **dbt-metrics**, **MetricFlow**, and **Cube.js** are building semantic layers on top of dimensional models

---

## Dimensional Graph Model (DGM)

sqldim extends the classical Kimball model with a formal graph algebra — the
**Dimensional Graph Model (DGM)** — which unifies dimensional analysis with graph
modelling in a single typed framework.

### Why a Graph Extension?

Classical Kimball models answer *"aggregate measures per dimension group."*
Graph models answer *"which entities are related and how?"*  The two questions
arise from the same data but require different query patterns:

| Classical Kimball | Graph extension |
|---|---|
| `SUM(revenue) GROUP BY customer.region` | "Which customers are reachable within 2 hops via bridge edges?" |
| `JOIN` along FK foreign keys | Recursive CTE traversal with cycle detection |
| Aggregation stops at group boundaries | Path finding is unbounded in depth |

DGM makes both questions first-class — using the same schema definitions.

### Graph Primitives

Classical Kimball objects map directly to DGM graph primitives:

| Kimball object | DGM primitive | Carries |
|---|---|---|
| `DimensionModel` | Dimension node (`dim`) | Attributes (filterable, groupable) |
| `FactModel` | Fact node (`fact`) | Measures (aggregable) |
| FK from fact → dimension | Verb edge (`dim → fact`) | Business verb label |
| `BridgeModel` | Bridge edge (`dim → dim`) | Optional allocation weight |

A key departure from classical Kimball: **facts are first-class nodes**, not merely
intersection tables. This solves the *reification problem* — an event that involves
three or more dimensions (a delivery assigned to a customer, a product, and a courier)
is represented cleanly without forcing an artificial binary subject/object split.

### Three-Band Query Algebra

DGM structures every query as `Q = B1 ∘ B2? ∘ B3?`:

| Band | SQL role | Maps to |
|---|---|---|
| **B1 — Context** | `FROM … JOIN … WHERE` | Define the working subgraph |
| **B2 — Aggregation** | `GROUP BY … HAVING` | Collapse and summarise |
| **B3 — Ranking** | Window functions + `QUALIFY` | Rank tuples and filter |

The bands correspond exactly to the semantic phases of SQL evaluation:
row-level filtering, set-level aggregation, then tuple-level ranking.
Making them explicit and named:

- eliminates the "where vs. having confusion" problem (wrong band → type error at construction time)
- enables the `B1 ∘ B3` form (QUALIFY without GROUP BY) which is otherwise awkward
  to express in most query builders
- provides a clean model for cross-band reference validation (`PropRef` / `AggRef` / `WinRef`)

### Unified Predicate Language

All three bands share a single boolean tree grammar (`AND`, `OR`, `NOT`, `ScalarPred`).
Band safety is enforced via the `Ref` kind of the leaf, not via a structural grammar difference:

```
ScalarPred(PropRef("c.segment"), "=", "retail")  → valid in B1 Where
ScalarPred(AggRef("total_rev"), ">", 5000)        → valid in B2 Having
ScalarPred(WinRef("rnk"), "<=", 2)                → valid in B3 Qualify
```

`PathPred` is the only predicate restricted to B1 — it fires an `EXISTS` subquery
over a multi-hop path traversal, which is meaningful only while the graph context
is live (before aggregation collapses it).

### DGM vs. Property Graph Engines (Neo4j, Gremlin)

| Dimension | Neo4j / Gremlin | DGM |
|---|---|---|
| Storage | Dedicated graph store | Existing SQL tables |
| Schema | Schema-optional | Typed via `DimensionModel` / `FactModel` |
| Aggregation | Limited | Full SQL GROUP BY + HAVING |
| Temporal (SCD2) | No native support | First-class `TemporalJoin` |
| Bridge weights | Ad-hoc property | First-class `P(b).weight` in Band 2 |
| Backend | JVM / specialized | DuckDB; any SQL engine |

DGM does not aim to replace Neo4j for deeply recursive graph workloads (e.g., social
graph community detection). Its sweet spot is analytical OLAP workloads where
dimensional structure is known, SCD2 history matters, and the graph relationships
are the star schema FK topology.

### Further Reading

- [DGM Feature Reference](../features/dimensional_graph_model.md) — API reference with
  code examples for all four query forms
- [Graph Analytics Roadmap ADR](./adr/graph-analytics-roadmap.md) — four-tier
  implementation plan for closing the remaining gaps

---

