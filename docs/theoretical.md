Dimensional modeling is a well-established discipline — let me give you a thorough rundown of what we know.

---

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

