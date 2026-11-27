# Feature: Dimensional Graph Model (DGM)

The **Dimensional Graph Model** is sqldim's formal query algebra for dimensional data. It extends the classical Kimball star schema with a typed directed graph layer and a three-band query model that maps directly onto the SQL evaluation pipeline: 

`WHERE → GROUP BY/HAVING → QUALIFY`.

---

## Core Ideas

| Concept | One-liner |
|---|---|
| **Dimension node** | A domain entity (customer, product, date). Carries attributes. May be SCD2-versioned. |
| **Fact node** | A measurable event. Carries numeric measures. First-class graph citizen. |
| **Verb edge** | Directed `dim → fact` link, labeled with a business verb (`placed`, `occurred on`). |
| **Bridge edge** | Directed `dim → dim` link, optionally weighted. Encodes structural relationships. |
| **Three-band query** | `B1 (context) ∘ B2? (aggregation) ∘ B3? (ranking)` |

---

## The Three-Band Query Model

A `DGMQuery` is composed of up to three named bands, each with a distinct SQL role.

```
Q = B1  ∘  B2?  ∘  B3?
```

| Band | Clauses | SQL equivalent | Required |
|---|---|---|---|
| **B1 — Context** | `anchor`, `path_join`, `temporal_join`, `where` | `FROM … JOIN … WHERE` | Always |
| **B2 — Aggregation** | `group_by`, `agg`, `having` | `GROUP BY … HAVING` | Optional |
| **B3 — Ranking** | `window`, `qualify` | Window functions + `QUALIFY` | Optional |

This produces exactly four valid query shapes:

| Shape | When to use |
|---|---|
| `B1` | Filter and return the context subgraph |
| `B1 ∘ B2` | Aggregate and summarise groups |
| `B1 ∘ B3` | Rank rows without aggregation (top-N per partition, deduplication) |
| `B1 ∘ B2 ∘ B3` | Aggregate then rank (top-N per aggregated group) |

**Within-band co-dependency**: B2 clauses (`group_by`, `agg`, `having`) must all be
present or all absent. B3 clauses (`window`, `qualify`) must both be present or both absent.
`DGMQuery.to_sql()` raises `SemanticError` if this rule is violated.

---

## Quick Start

```python
from sqldim.core.query.dgm import (
    DGMQuery,
    PropRef, AggRef, WinRef,
    ScalarPred, AND, NOT,
    VerbHop, BridgeHop, PathPred,
)

# B1 only — filter on a joined dimension attribute
q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim", on="c.customer_id = s.customer_id"))
    .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
)
print(q.to_sql())
```

### B1 ∘ B2 — Aggregation with HAVING

```python
q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim", on="c.customer_id = s.customer_id"))
    .where(ScalarPred(PropRef("s", "year"), "=", 2024))
    .group_by("c.region")
    .agg(total_rev="SUM(s.revenue)", order_count="COUNT(*)")
    .having(ScalarPred(AggRef("order_count"), ">=", 3))
)
```

### B1 ∘ B3 — Row deduplication without aggregation

```python
q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim", on="c.customer_id = s.customer_id"))
    .where(ScalarPred(PropRef("s", "year"), "=", 2024))
    .window(latest="ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY s.occurred_on DESC)")
    .qualify(ScalarPred(WinRef("latest"), "=", 1))
)
```

### B1 ∘ B2 ∘ B3 — Top-N per aggregated group

```python
q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim", on="c.customer_id = s.customer_id"))
    .path_join(BridgeHop("c", "promoted_in", "st",
                          table="store_dim", on="st.store_id = s.store_id"))
    .where(AND(
        ScalarPred(PropRef("c", "segment"), "=", "retail"),
        ScalarPred(PropRef("s", "year"), "=", 2024),
    ))
    .group_by("c.customer_id", "c.region")
    # an allocation weight is per path instance: ask for PATHS explicitly
    .agg(weighted_rev="SUM(s.revenue * st.weight)", order_count="COUNT(*)",
         over="PATHS")
    .having(ScalarPred(AggRef("weighted_rev"), ">", 5000))
    .window(rnk="RANK() OVER (PARTITION BY c.region ORDER BY weighted_rev DESC)")
    .qualify(ScalarPred(WinRef("rnk"), "<=", 2))
)
```

### What an aggregate counts (`over`)

A `path_join` puts one tuple in the context per path instance, so a sale
reached through two campaigns appears twice. `agg(..., over=...)` fixes the unit
of aggregation (§4.2):

| `over` | Meaning |
|---|---|
| `"ENTITIES"` (default) | each distinct instance of the aliases an argument references contributes once per group: `SUM(s.revenue)` counts each sale once, `COUNT(s.id)` counts distinct sales |
| `"PATHS"` | every tuple contributes, once per path instance; must be written explicitly |

With a `path_join`, `to_sql()` rewrites each `SUM`, `AVG` and `COUNT` whose
argument references an alias so that every instance is read once. The instance
of a `join_dim` alias is its entity (its `dim_pk`), so the SCD versions of one
customer are one instance; any other alias is identified by its whole row. A
`join_dim` alias is rewritten the same way without a `path_join`, since one
customer is reached by many facts: `AVG(d_customer.age)` reads each customer once.
If two versions of one entity give the argument different values in a group,
execution raises `EntityValueConflict` (a `SemanticError`) rather than pick one. `MIN`/`MAX`, `COUNT(*)`, a `DISTINCT`
argument and an argument that references no alias are left as written; an
aggregate with `FILTER` or `OVER` raises `UnsupportedConstruct` unless
`over="PATHS"`. A query with neither a `path_join` nor a `join_dim` is unchanged.

---

## Reference Types

DGM enforces cross-band reference rules through three typed `Ref` classes.

| Class | Constructed from | Valid in |
|---|---|---|
| `PropRef(alias, prop)` | Raw context column | B1 `where` |
| `AggRef(name)` | Named `agg()` attribute | B2 `having` |
| `WinRef(name)` | Named `window()` attribute | B3 `qualify` |

No band may reference the output of a later band.

---

## Predicate Language

All three bands share an identical boolean tree grammar. The only variation is which
`Ref` kind is permitted inside `ScalarPred` leaves.

```python
# Scalar comparison
ScalarPred(ref, op, value)    # ref.to_sql() op value
ScalarPred(ref, "IS", None)   # the NULL test: NULL is not a literal, so
                              # ScalarPred(ref, "=", None) is rejected (§4.1)

# Boolean tree
AND(*preds)                    # preds joined with AND (auto short-circuits single pred)
OR(*preds)                     # preds joined with OR
NOT(pred)                      # negation; NOT(NOT(T)) ≡ T (folded at construction time)
```

### PathPred (B1 only)

`PathPred` turns a path traversal into an `EXISTS` subquery — useful for "has at least one X that satisfies Y" without adding a join alias to the main context.

```python
# "Customers who have at least one sale of a premium product"
PathPred(
    anchor="c",
    path=Compose(
        VerbHop("c", "placed", "s", table="sale_fact", on="s.customer_id = c.customer_id"),
        VerbHop("s", "included_in", "p", table="product_dim", on="p.product_id = s.product_id"),
    ),
    sub_filter=ScalarPred(PropRef("p", "category"), "=", "premium"),
)
```

`to_sql()` renders `EXISTS` and `FORALL` under strategy `ALL` only. It is
fail-closed, like the plan IR: a `temporal_mode`, a strategy other than `ALL`
and a `TemporalContext` raise `UnsupportedConstruct` instead of a SQL text that
would drop them. The construction rules of the specification are checked when
the `PathPred` is built: `promote=True` needs `SHORTEST` or `MIN_WEIGHT`,
`FORALL` with `ONCE` or `SINCE` is rejected, and `ONCE`, `PREVIOUSLY` and `SINCE`
are rejected over a verb hop (never time-ordered). A path alias equal to an
alias of the join context is rejected by `to_sql()` (C ∩ L ≠ ∅).

---

## Temporal Joins (SCD2)

`temporal_join(as_of)` (alias `as_of`) states the reference instant τ of the
query. Every `path_join` edge is read valid at τ; windows are half-open and a
NULL bound is open (§2.6). The generated SQL appends:

```sql
AND (alias.valid_from IS NULL OR alias.valid_from <= '<as_of>')
AND (alias.valid_to IS NULL OR alias.valid_to > '<as_of>')
```

The instant is compared as it is, never cast to a date, so a version that
starts later on the same day is not read.

`join_dim(dim_table, fact_fk, dim_pk="id", *, mode="AS_WAS", event_time=None,
version_key=None)` joins the anchor fact to a versioned dimension and reads the
version that the SCD resolution mode selects (§2.5):

| Fact stores | `mode="AS_WAS"` (default) | `mode="AS_IS"` |
|---|---|---|
| the durable key (`fact_fk` = `dim_pk`) | the version valid at the fact's `event_time` column, which is then required | the version valid at τ, or the current one without τ |
| a version surrogate (`version_key=`) | the stored version | the version of the same entity (`dim_pk`) valid at τ, or the current one |

```python
q = (
    DGMQuery("fact_sale")
    .join_dim("dim_customer", "customer_key", event_time="occurred_on")  # AS_WAS
    .by("d_dim_customer.region")
    .sum("f.revenue")
)
```

```python
q = (
    DGMQuery()
    .anchor("sale_fact", "s")
    .temporal_join("2024-06-30")        # applied to all subsequent path_joins
    .path_join(VerbHop("s", "placed", "c",
                       table="customer_dim", on="c.customer_id = s.customer_id"))
    .where(ScalarPred(PropRef("s", "year"), "=", 2024))
)
```

---

## Drill-across (constellations)

Two facts reached through a conformed dimension, such as sales and returns per
customer, are not joined row to row: each fact branch is aggregated on its own
and the results are aligned on the GroupBy keys (§5.2.2). `DGMQuery` reads no
model, so it cannot find the fact branches itself. Declare them with
`drill_across`, one set of aliases per branch. Every other alias is a conformed
dimension shared by all branches.

```python
q = (
    DGMQuery()
    .anchor("customer", "c")
    .path_join(VerbHop("c", "placed", "s", table="sale", on="s.customer_id = c.id"))
    .path_join(VerbHop("c", "returned", "r", table="return_ev", on="r.customer_id = c.id"))
    .drill_across(["s"], ["r"])
    .group_by("c.id")
    .agg(n_sales="COUNT(s.id)", n_returns="COUNT(r.id)",
         rate="COUNT(r.id) / COUNT(s.id)")
)
```

Each branch is grouped and aggregated in its own subquery, and the subqueries
are aligned by `FULL OUTER JOIN` on the keys. The named expressions are then
evaluated on the aligned rows, followed by `having`. The rules:

- DA1: each aggregate call references one branch.
- DA2: a GroupBy key references shared aliases only.
- DA3: a Where conjunct references at most one branch.

`to_sql()` raises `SemanticError` for a violation. A COUNT of a branch absent
for a key is 0 and every other aggregate is NULL (DA4). A division by zero in a
named expression is NULL. The plan IR (`DGMPlanner.plan_query`) checks DA1–DA3
and then raises `UnsupportedConstruct`, because it has no node for the aligned
aggregation.

---

## Executing a Query

```python
import duckdb

con = duckdb.connect()
# ... create/populate tables ...

results = q.execute(con)          # returns list of tuples
# or
sql = q.to_sql()                   # inspect the generated SQL
df  = con.execute(sql).fetchdf()   # Pandas DataFrame
```

---

## Predicate Summary Table

| Predicate | Band | Purpose |
|---|---|---|
| `ScalarPred(PropRef, op, v)` | B1 | Attribute filter on joined context |
| `PathPred(anchor, path, sub_filter)` | B1 | Existence check via path traversal |
| `ScalarPred(AggRef, op, v)` | B2 | Post-aggregation group filter |
| `ScalarPred(WinRef, op, v)` | B3 | Post-window tuple filter |
| `AND`, `OR`, `NOT` | All | Boolean composition |

---

## Alignment with sqldim Models

| DGM Concept | sqldim class |
|---|---|
| Dimension node | `DimensionModel` |
| Fact node | `FactModel` |
| Verb edge | `FactModel` FK to two `DimensionModel`s |
| Bridge edge | `BridgeModel` with optional `weight` |
| SCD2 temporal join | `SCD2Mixin` (`valid_from`, `valid_to`, `is_current`) |
| Path traversal | `TraversalEngine` (recursive CTE, multi-hop) |

---

## Question Algebra (Multi-CTE Composition)

A single `DGMQuery` produces one SQL CTE.  The **question algebra** is the formal
closure of that CTE space under five CTE composition operators:

```
Q_algebra(A, B) = closure of Q_valid(A, B)
                  under { JOIN, UNION, INTERSECT, EXCEPT, WITH }
```

`QuestionAlgebra` is an ordered registry that accumulates CTEs, tracks
compositions, and emits a single `WITH … SELECT` statement in topological order.

### Building an algebra

```python
from sqldim.core.query.dgm import (
    DGMQuery, QuestionAlgebra, ComposeOp,
    PropRef, ScalarPred, AND,
)

alg = QuestionAlgebra()

# Leaf CTEs — plain DGMQuery instances
alg.add("retail", DGMQuery().anchor("sale_fact", "s")
        .where(ScalarPred(PropRef("s", "channel"), "=", "retail")))
alg.add("premium", DGMQuery().anchor("sale_fact", "s")
        .where(ScalarPred(PropRef("s", "tier"), "=", "premium")))

# Compose with UNION — generates a new CTE that is UNION ALL of both
alg.compose("retail", ComposeOp.UNION, "premium", name="combined")

print(alg.to_sql(final="combined"))
# WITH retail AS (...),
#      premium AS (...),
#      combined AS (SELECT * FROM retail  UNION ALL  SELECT * FROM premium)
# SELECT * FROM combined
```

### ComposeOp reference

| Operator | SQL emitted | Use when |
|---|---|---|
| `UNION` | `UNION ALL` | Disjunctive queries — merge independent answer sets |
| `INTERSECT` | `INTERSECT` | Conjunctive queries — rows in both answer sets |
| `EXCEPT` | `EXCEPT` | Set difference — rows in left but not right |
| `WITH` | _right_ CTE uses left as its scope | Dependent chain — `right` references `left` |
| `JOIN` | `JOIN … ON …` | Cross-question correlation — requires `on=` clause |

### Semiring structure

`(Q_algebra, UNION, INTERSECT, ∅, Q_top)` is a semiring where:

- **`∅`** (`QuestionAlgebra.EMPTY_Q`) is the additive identity — a CTE that
  returns no rows.
- **`Q_top`** (`QuestionAlgebra.TOP_Q`) is the multiplicative identity — a CTE
  that returns all rows with no filter.

This algebraic structure underpins the Query DAG Minimisation optimiser.

---

## Common Sub-expression Elimination (CSE)

When multiple CTEs share an **identical `WHERE` predicate** — detected via
canonical BDD node ID equality — `apply_cse` extracts the shared predicate into a
dedicated `__cse_<id>` CTE inserted before the group.  Each sharing CTE then
queries the pre-filtered result, avoiding repeated table scans.

```python
from sqldim.core.query.dgm import (
    apply_cse, find_shared_predicates,
    BDDManager, DGMPredicateBDD,
)

bdd = DGMPredicateBDD(BDDManager())

# Detect sharing (O(|CTEs|))
groups = find_shared_predicates(alg, bdd)
# → {bdd_id: ["retail", "premium"]} for identical predicates

# Apply CSE — returns new algebra, original untouched
optimised = apply_cse(alg, bdd)
```

The `__cse_*` CTE contains `SELECT * FROM <anchor_table> WHERE <pred_sql>`.
Downstream CTEs reference it without re-evaluating the predicate.  Both
`find_shared_predicates` and `apply_cse` are `O(|CTEs|)`.

---

## Query DAG Minimisation (Rule 11 Extended)

After composing an algebra, `apply_semiring_minimisation` (or
`QuestionAlgebra.minimize(bdd)`) eliminates redundant CTEs using the semiring
laws of the question algebra:

| Law | Condition | Result |
|---|---|---|
| Union idempotence | `Q ∪ Q` | `Q` |
| Union identity | `Q ∪ ∅` | `Q` |
| Containment absorption | `Q₁ ⊆ Q₂` → `Q₁ ∪ Q₂` | `Q₂` |
| Intersection idempotence | `Q ∩ Q` | `Q` |
| Intersection identity | `Q ∩ Q_top` | `Q` |
| Containment selection | `Q₁ ⊆ Q₂` → `Q₁ ∩ Q₂` | `Q₁` |

Containment (`Q₁ ⊆ Q₂`) is checked via `bdd.implies(id₁, id₂)` — the BDD
canonicalises predicates so that structurally equivalent filters share integer IDs.

```python
from sqldim.core.query.dgm import (
    apply_semiring_minimisation, BDDManager, DGMPredicateBDD,
)

bdd = DGMPredicateBDD(BDDManager())

# Idempotent union — Q ∪ Q = Q
alg = QuestionAlgebra()
alg.add("q", DGMQuery().anchor("orders"))
alg.compose("q", ComposeOp.UNION, "q", name="q_dup")
assert len(alg) == 2

minimal = alg.minimize(bdd)
assert len(minimal) == 1  # q_dup eliminated
```

The minimum CTE count equals the number of nodes in the canonical DAG — this is a
**tight lower bound**: no semantically equivalent expression with fewer CTEs exists.

### QueryDAGManager — intern-table for the DAG

`QueryDAGManager` maps each unique `(op, left_id, right_id)` triple to a
single node ID — the same structural-sharing principle as BDD `make()`.
Idempotence and identity laws are applied inline at `make()` time without
requiring a BDD for the structural cases.

```python
from sqldim.core.query.dgm import QueryDAGManager, ComposeOp

mgr = QueryDAGManager()
q1 = mgr.leaf("q1")
q2 = mgr.leaf("q2")
union_id = mgr.make(ComposeOp.UNION, q1, q2)
same_id  = mgr.make(ComposeOp.UNION, q1, q2)   # shared — same node
assert union_id == same_id
```

---

## Optimisation Pipeline (Recommended Order)

For maximum SQL efficiency, apply the three optimisers in order:

```python
bdd = DGMPredicateBDD(BDDManager())

# Step 1 — eliminate duplicate CTEs (semiring laws, BDD containment)
step1 = alg.minimize(bdd)

# Step 2 — extract shared predicates into __cse_* CTEs
step2 = apply_cse(step1, bdd)

# Step 3 — emit final SQL
sql = step2.to_sql(final="my_final_cte")
```

---

## Related

- [Graph Analytics Roadmap ADR](../development/adr/graph-analytics-roadmap.md) — metadata-driven property graph tiers
- [Benchmark Suite ADR: Group O](../architecture/benchmark-suite.md#group-o-dgm-query-builder--throughput-semantics) — verified throughput and three-band measurement semantics
- [Semantic Layer Guide](../guides/semantic_layer.md) — higher-level query builder built on the same DuckDB backend
- [Full API Surface](../reference/api-surface.md#question-algebra-multi-cte-composition) — complete symbol reference
