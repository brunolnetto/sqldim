# Dimensional Graph Model — Formal Specification
**Version 0.11 — Draft**

---

## Contents

1. [Overview](#1-overview)
2. [Graph Structure](#2-graph-structure)
3. [Node Types](#3-node-types)
4. [Edge Types](#4-edge-types)
5. [Traversal Paths](#5-traversal-paths)
6. [Path Strategy](#6-path-strategy)
7. [Predicate Language](#7-predicate-language)
8. [Expression Language](#8-expression-language)
9. [Query Structure](#9-query-structure)
10. [Band 1 — Context Layer](#10-band-1--context-layer)
11. [Band 2 — Aggregation Layer](#11-band-2--aggregation-layer)
12. [Band 3 — Ranking Layer](#12-band-3--ranking-layer)
13. [Evaluation Order](#13-evaluation-order)
14. [Worked Examples](#14-worked-examples)
15. [Summary](#15-summary)
16. [Relationship to sqldim](#16-relationship-to-sqldim)
17. [Related Work](#17-related-work)
18. [Design Decisions](#18-design-decisions)
19. [References](#19-references)

---

## 1  Overview

The **Dimensional Graph Model (DGM)** is a typed directed graph framework that unifies dimensional analysis with graph modelling. It provides a formal basis for representing domain entities, their relationships, measurable events, and structural connections — together with a query language for filtering, joining, aggregating, and qualifying over the resulting graph.

The framework rests on nine core ideas:

- **Dimensions and facts are both first-class nodes**, distinguished by the kind of data they carry.
- **Relationships are typed directed edges**: verb edges bind dimensions to facts (`V ⊆ D×F×L`); bridge edges are general structural connectors between any two nodes (`B ⊆ N×N×L`).
- **Joins are path traversals** that expand the query context. Temporal resolution (SCD2) and structural trimming are first-class `Join` modifiers.
- **Path strategy is explicit**: every path-based operation declares how paths are selected and, in predicates, what quantifier scopes the truth condition. Multiplicity is never implicit.
- **Path-local bindings** allow hops in a `BoundPath` to introduce named aliases for intermediate nodes and edges, evaluated over `C ∪ L`. For single-instance strategies, path-local bindings may be promoted into `C`.
- **A unified predicate language** governs all filter clauses via two constructors (`ScalarPred`, `PathPred`) and a typed `Ref` system.
- **A unified expression language** governs all computed values. `GraphExpr` is typed by **granularity** — node, pair, or subgraph — and placement rules follow directly from granularity.
- **Graph algorithms are dimensionally grounded**: every `GraphExpr` references aliases already in context `C`. Classical graph algorithms (Floyd-Warshall, Tarjan SCC, trim) are accommodated either as execution strategies, `CommAlg` variants, or `Join` modifiers — each at the layer that matches their dimensional semantics.
- **Structural trimming** at the `Join` layer restricts the anchor node set to those satisfying a structural criterion before path expansion, keeping the expression language purely scalar.

---

## 2  Graph Structure

A DGM graph is a 5-tuple:

```
G = (N, E, τ_N, τ_E, P)
```

| Symbol | Definition |
|---|---|
| `N` | Finite set of nodes. |
| `E ⊆ N×N×L` | Set of directed labeled edges. `L` is the set of verb labels. |
| `τ_N : N → {dim, fact}` | Node type function. Partitions `N` into `D` (dimension) and `F` (fact). |
| `τ_E : E → {verb, bridge}` | Edge type function. Partitions `E` into `V` (verb) and `B` (bridge). |
| `P : N∪E → (K→V)` | Property function. Maps every node and edge to a key-value property bag. |

**Structural invariants:**

```
D ∩ F = ∅,   D ∪ F = N
V ∩ B = ∅,   V ∪ B = E
V ⊆ D × F × L     -- verb edges: dim → fact only
B ⊆ N × N × L     -- bridge edges: any node → any node
```

---

## 3  Node Types

### 3.1  Dimension nodes

`d ∈ D` — domain entity. `P(d)` carries **attributes** for filtering and grouping. May be SCD2-versioned (§10.1). Notation: `d.attr`.

### 3.2  Fact nodes

`f ∈ F` — measurable event. `P(f)` carries **measures** — numeric values for aggregation. Notation: `f.measure`.

Fact nodes connect to any number of dimension nodes via verb edges, eliminating the reification problem for N-ary events. Fact nodes may also be endpoints of bridge edges, enabling structural relationships between events (causal chains, temporal succession, supersession) without implying a new event.

---

## 4  Edge Types

### 4.1  Verb edges

`v = (d, f, label) ∈ V`. Directed from participating dimension toward the event.

```
V ⊆ D × F × L
```

Examples: `placed`, `included in`, `occurred on`, `fulfilled by`.

### 4.2  Bridge edges

`b = (n₁, n₂, label) ∈ B`. Connects any two nodes with no event semantics.

```
B ⊆ N × N × L

P(b).weight ∈ ℝ⁺   (optional; defaults to 1 when absent)
```

**Endpoint taxonomy:**

| Source | Target | Typical use |
|---|---|---|
| `dim` | `dim` | Taxonomic, geographic, organisational hierarchy |
| `fact` | `fact` | Causal chain, temporal succession, supersession |
| `fact` | `dim` | Event-to-entity structural link |
| `dim` | `fact` | Structural reference (not verb-role participation) |

Weight is a first-class property. Applied only when explicitly referenced in an expression — never implicitly (§18.5).

---

## 5  Traversal Paths

### 5.1  Unbound paths

Structural skeleton of a traversal — no named aliases:

```
Path ::= NodeRef(n: N)
       | VerbHop(d: D, label: L, f: F)
       | BridgeHop(n₁: N, label: L, n₂: N)
       | Compose(Path, Path)
```

Used in `Join` and `Window` partition arguments.

### 5.2  Bound paths

Strict superset of `Path`. Each hop may introduce named aliases:

```
BoundPath ::= NodeRef(n: N)
            | VerbHop(d: D, label: L, f: F,
                      node_alias?: A,
                      edge_alias?: A)
            | BridgeHop(n₁: N, label: L, n₂: N,
                        node_alias?: A,
                        edge_alias?: A)
            | Compose(BoundPath, BoundPath)
```

Every `Path` is a valid `BoundPath` with all aliases omitted. Used in `PathPred` and `PathAgg`.

**Alias uniqueness.** All declared aliases within a `BoundPath` must be distinct. `C ∩ L ≠ ∅` is rejected at construction time.

### 5.3  Evaluation contexts

```
C  -- outer join context: aliases from Join (extended by L-promotion)
L  -- path-local context: aliases from BoundPath hop bindings (per path instance)
```

`PropRef(alias.prop)` inside `sub_filter` or `PathAgg` resolves against `C ∪ L`. `AggRef` and `WinRef` are not valid in `C ∪ L`.

### 5.4  L-promotion for single-instance strategies

By default `L` is ephemeral — discarded after `sub_filter` or `PathAgg` evaluates.

When `Strategy ∈ {SHORTEST, MIN_WEIGHT}`, a `PathPred` or `PathAgg` may declare `promote: true`. The path-local bindings from `L` are then lifted into `C`, available to all downstream clauses and bands. Promotion is one-way and irreversible within a query.

`promote: true` is rejected at construction time for `ALL` and `K_SHORTEST` — those strategies may produce multiple path instances per tuple, and promoting `L` from them would silently fan-out `C`.

---

## 6  Path Strategy

```
Strategy ::= ALL                  -- every matching path; explicit fan-out
           | SHORTEST             -- single path minimising hop count
           | K_SHORTEST(k: ℕ)    -- k paths minimising hop count, k ≥ 1
           | MIN_WEIGHT           -- single path minimising sum of P(e).weight
```

**Single-instance strategies** (`SHORTEST`, `MIN_WEIGHT`): select exactly one path per outer tuple. Compatible with `promote: true`.

**Multi-instance strategies** (`ALL`, `K_SHORTEST`): select a multiset. Fan-out is explicit.

**Execution note — Floyd-Warshall.** When a query involves `PairExpr(SHORTEST_PATH_LENGTH, ...)` or `PairExpr(MIN_WEIGHT_PATH_LENGTH, ...)` over many source-target pairs in `C`, the query planner may execute the underlying shortest-path computation using the Floyd-Warshall all-pairs algorithm and look up results from the materialised distance matrix. This is a **planner-level optimisation**, transparent to the query author. The grammar does not expose Floyd-Warshall as a named algorithm — the semantic intent is always expressed as `PairExpr` with a specific pair of context aliases. See §18.6.

**`MIN_WEIGHT` path weight.** Sum of `P(e).weight` along the path, defaulting to 1 for edges without a weight property.

---

## 7  Predicate Language

A single unified predicate language governs all filter clauses. Band validity is a typing rule on the `Ref` kind.

### 7.1  Reference types

```
Ref ::= PropRef(alias.prop)    -- raw context property      (B₁; C∪L inside PathPred)
      | AggRef(aᵢ)             -- named Agg attribute        (B₂)
      | WinRef(wᵢ)             -- named Window attribute     (B₃)
```

| Band | Filter clause | Valid `Ref` kinds | `PathPred` allowed |
|---|---|---|---|
| B₁ | `Where` | `PropRef` from `C` | Yes (evaluates over `C∪L`) |
| B₂ | `Having` | `AggRef` | No |
| B₃ | `Qualify` | `WinRef` | No |

### 7.2  Predicate constructors

```
Pred ::= ScalarPred(ref: Ref, pred: V → Bool)
       | PathPred(alias: A, path: BoundPath,
                  quantifier: Quantifier,
                  strategy: Strategy,
                  promote?: Bool,
                  sub_filter: Pred)

Quantifier ::= EXISTS | FORALL
```

`promote: true` valid only for `SHORTEST` and `MIN_WEIGHT`.

**Trim as PathPred.** Degree-based trimming — removing nodes with no incident edges of a given type — is expressible directly as a `PathPred` existence check without any new primitive:

```
-- Keep only nodes that have at least one outgoing bridge edge
PathPred(n, BridgeHop(n, ANY, m), EXISTS, ALL,
         ScalarPred(PropRef(m.id), IS_NOT_NULL))
```

This covers `MIN_DEGREE(1, OUT)` and similar degree filters. Structural trim (on-path reduction) requires `TrimJoin` — see §10.1.

### 7.3  Boolean filter tree

```
T ::= AND(T₁, ..., Tₙ) | OR(T₁, ..., Tₙ) | NOT(T) | Pred
```

`NOT` is tuple-level. `NOT(NOT(T)) ≡ T`.

---

## 8  Expression Language

Unified expression language for all computed values in `Agg` (B₂) and `Window` (B₃).

```
Expr ::= AggFn(fn: AggFn, ref: PropRef)
       | PathAgg(alias: A, path: BoundPath,
                 strategy: Strategy,
                 promote?: Bool,
                 fn: AggFn, ref: Ref_L)
       | GraphExpr(algorithm: GraphAlgorithm)
       | ArithExpr(Expr, op: {+, -, *, /}, Expr)
       | Const(c: ℝ)

AggFn  ::= SUM | COUNT | AVG | MIN | MAX | PROD
Ref_L  ::= PropRef(alias.prop)   where alias ∈ C ∪ L
```

### 8.1  AggFn

Plain aggregation over a `PropRef` in outer context `C'`.

### 8.2  PathAgg

Aggregation over values collected along path instances selected by `strategy`. Uses `BoundPath`; `ref` resolves against `C ∪ L`. Weight is applied only when explicitly referenced in `ref` — no implicit weighting.

`promote: true` valid for `SHORTEST` and `MIN_WEIGHT` only. Promoted `L` bindings enter the group context after `Agg` evaluates, available to `Having`, `Window`, and `Qualify`.

### 8.3  GraphExpr

Graph-algorithmic expressions typed by **granularity**. The general placement rule: *a `GraphExpr` in `Window` must produce one value per tuple*.

```
GraphAlgorithm ::= NodeExpr(algorithm: NodeAlg, alias: A)
                 | PairExpr(algorithm: PairAlg, source: A, target: A)
                 | SubgraphExpr(algorithm: SubgraphAlg,
                                partition?: [PropRef])

NodeAlg     ::= PAGE_RANK(damping: ℝ, iterations: ℕ)
              | BETWEENNESS_CENTRALITY
              | CLOSENESS_CENTRALITY
              | DEGREE(direction: {IN, OUT, BOTH})
              | COMMUNITY_LABEL(algorithm: CommAlg)

CommAlg     ::= LOUVAIN
              | LABEL_PROPAGATION
              | CONNECTED_COMPONENTS        -- undirected / weak connectivity
              | TARJAN_SCC                  -- directed strong connectivity

PairAlg     ::= SHORTEST_PATH_LENGTH        -- Floyd-Warshall eligible (§6)
              | MIN_WEIGHT_PATH_LENGTH      -- Floyd-Warshall eligible (§6)
              | REACHABLE

SubgraphAlg ::= MAX_FLOW(source: A, target: A, capacity: PropRef)
              | DENSITY
              | DIAMETER
```

**`TARJAN_SCC`.** Finds strongly connected components — maximal subsets of nodes where every node is reachable from every other via directed edges. Unlike `CONNECTED_COMPONENTS` (which ignores edge direction) and `LOUVAIN` (which optimises modularity on undirected structure), `TARJAN_SCC` respects the directed topology of verb and bridge edges. In DGM this has direct analytical meaning: an SCC in fact-to-fact bridges identifies a cycle of events (e.g. a chain of orders and returns that loops back to itself). Each node receives a component label identifying its SCC. See §18.7.

**Floyd-Warshall note.** `SHORTEST_PATH_LENGTH` and `MIN_WEIGHT_PATH_LENGTH` under `PairAlg` express the semantic intent. The planner may execute them using Floyd-Warshall when many pairs are involved. The grammar does not expose this choice.

**Granularity and placement:**

| Granularity | Kind | In `Agg` | In `Window` |
|---|---|---|---|
| Node | `NodeExpr` | ✅ | ✅ always |
| Pair | `PairExpr` | ✅ | ✅ always |
| Subgraph | `SubgraphExpr` | ✅ | ✅ with non-empty `partition` only |

`SubgraphExpr` with `partition` computes one value per partition group and broadcasts to each tuple in that group.

**Structure-producing algorithms.** Algorithms that produce new nodes or edges are outside DGM scope. The query language produces result sets; it does not mutate `G`. Derived scalar results (e.g. SCC labels) may be persisted as `P(n).attr` via an ETL step outside the query.

---

## 9  Query Structure

```
Q = B₁  ∘  B₂?  ∘  B₃?
```

| Band | Name | Role | Clauses | Required |
|---|---|---|---|---|
| B₁ | Context | Define working subgraph | `Join`, `Where` | Always |
| B₂ | Aggregation | Collapse and summarise | `GroupBy`, `Agg`, `Having` | Optional |
| B₃ | Ranking | Assign per-tuple values; filter | `Window`, `Qualify` | Optional |

**Valid query forms:**

```
Q = B₁   ·   Q = B₁ ∘ B₂   ·   Q = B₁ ∘ B₃   ·   Q = B₁ ∘ B₂ ∘ B₃
```

**Within-band co-dependency.** B₂ clauses appear together; B₃ clauses appear together.

---

## 10  Band 1 — Context Layer

```
B₁ = (Join, Where?)
```

It answers: *what nodes and tuples am I working with?*

### 10.1  Join

```
Join ::= Anchor(node_type: τ_N, alias: A)
       | PathJoin(alias: A, path: Path, alias': A')
       | CrossJoin(Join, Join)
       | TemporalJoin(join: Join, as_of: Timestamp)
       | TrimJoin(join: Join, criterion: TrimCriterion)

TrimCriterion ::= REACHABLE_BETWEEN(source: A, target: A)
                | MIN_DEGREE(n: ℕ, direction: {IN, OUT, BOTH})
                | SINK_FREE
                | SOURCE_FREE
```

**`TrimJoin`.** Wraps any `Join` expression and restricts the resulting node set to those satisfying a structural criterion, evaluated before any subsequent path expansion. `TrimJoin` modifiers may be nested and combined:

```
TrimJoin(
  TrimJoin(
    Anchor(dim, c),
    MIN_DEGREE(1, OUT)           -- remove isolated nodes
  ),
  REACHABLE_BETWEEN(source=c, target=st)  -- keep only nodes on c→st paths
)
```

**`TrimCriterion` semantics:**

| Criterion | Retains |
|---|---|
| `REACHABLE_BETWEEN(source, target)` | Nodes lying on at least one directed path from `source` to `target` (backbone / on-path trim) |
| `MIN_DEGREE(n, direction)` | Nodes with at least `n` incident edges in the given direction |
| `SINK_FREE` | Nodes with at least one outgoing edge (no dead-end sinks) |
| `SOURCE_FREE` | Nodes with at least one incoming edge (no dangling sources) |

**Relationship to `PathPred`.** `MIN_DEGREE`, `SINK_FREE`, and `SOURCE_FREE` are structurally expressible as `PathPred` existence checks in `Where`. `TrimJoin` is provided as a declarative shorthand and, critically, as a planner hint: trimming at join time avoids expanding paths from nodes that will be discarded, whereas a `PathPred` in `Where` expands first and filters second. `REACHABLE_BETWEEN` is not expressible as a single `PathPred` because it requires global knowledge of which nodes lie on any source-to-target path — it is the structural trim that genuinely requires `TrimJoin`.

**`TemporalJoin`.** Resolves SCD2-versioned dimension nodes at expansion time:

```
effective_from <= as_of  AND  (effective_to > as_of  OR  effective_to IS NULL)
```

**`CrossJoin`.** Cartesian product; aliases must be disjoint across branches (§18.8).

### 10.2  Where

```
Where: T [ Ref ∈ {PropRef from C}, PathPred(BoundPath) allowed ]
```

`PathPred` evaluates `sub_filter` over `C ∪ L`. If `promote: true` and `strategy ∈ {SHORTEST, MIN_WEIGHT}`, `L` bindings are lifted into `C` after evaluation. `NOT` is tuple-level.

---

## 11  Band 2 — Aggregation Layer

```
B₂ = (GroupBy, Agg, Having)
```

### 11.1  GroupBy

Ordered list of `PropRef` values from `C'`, including promoted `L` bindings and edge properties.

### 11.2  Agg

```
Agg = { aᵢ := Exprᵢ  |  i = 1..k }
```

`Exprᵢ` drawn from the unified expression language (§8).

### 11.3  Having

```
Having: T [ Ref ∈ {AggRef}, PathPred not allowed ]
```

---

## 12  Band 3 — Ranking Layer

```
B₃ = (Window, Qualify)
```

Operates on B₂ output when present, B₁ output otherwise.

### 12.1  Window

```
Window = { wᵢ := WinExprᵢ  |  i = 1..m }

WinExpr ::= RankFn(fn: RankFn, partition: [PropRef], order: [PropRef × {ASC,DESC}])
          | RunningAgg(fn: AggFn, ref: PropRef,
                       partition: [PropRef], order: [PropRef × {ASC,DESC}])
          | StatFn(fn: StatFn, ref: PropRef,
                   partition: [PropRef], order: [PropRef × {ASC,DESC}]?)
          | GraphExpr(algorithm: GraphAlgorithm)

RankFn  ::= ROW_NUMBER | RANK | DENSE_RANK | NTILE(n: ℕ)
          | LEAD(ref: PropRef, offset: ℕ) | LAG(ref: PropRef, offset: ℕ)

StatFn  ::= PERCENT_RANK | CUME_DIST
          | PERCENTILE_CONT(p: [0,1]) | PERCENTILE_DISC(p: [0,1])
```

`GraphExpr` in `Window`: `NodeExpr` and `PairExpr` always valid; `SubgraphExpr` requires non-empty `partition`.

### 12.2  Qualify

```
Qualify: T [ Ref ∈ {WinRef}, PathPred not allowed ]
```

---

## 13  Evaluation Order

| Band | Phase | Clause | Action | Input | Output |
|---|---|---|---|---|---|
| B₁ | 1 | `Join` | Traverse paths; apply `TrimJoin` criteria; apply `TemporalJoin`; build `C`. | `G` | `C` |
| B₁ | 2 | `Where` | Apply `T[PropRef]`; `PathPred` over `C∪L`; L-promotion updates `C` if `promote:true`. | `C` | `C'` |
| B₂ | 3 | `GroupBy` | Partition `C'` by `PropRef` list (incl. promoted bindings). | `C'` | groups |
| B₂ | 4 | `Agg` | Evaluate `Expr` per group; `PathAgg` L-promotion updates group context if `promote:true`. | groups | agg |
| B₂ | 5 | `Having` | Apply `T[AggRef]`; retain groups. | agg | `G'` |
| B₃ | 6 | `Window` | Evaluate `WinExpr` per tuple; `SubgraphExpr` per partition when `partition` non-empty. | `G'` or `C'` | `W'` |
| B₃ | 7 | `Qualify` | Apply `T[WinRef]`; retain tuples. | `W'` | result |

**`TrimJoin` timing.** Trim criteria are evaluated during Phase 1, after path expansion for the wrapped `Join` but before any subsequent `PathJoin` steps use the trimmed set as a base. Multiple nested `TrimJoin` modifiers are applied inside-out.

**L-promotion timing.** Promotion in Phase 2 makes bindings available from Phase 3 onward. Promotion in Phase 4 makes bindings available from Phase 5 onward.

---

## 14  Worked Examples

### 14.1  TrimJoin — backbone subgraph then aggregate

Restrict the graph to customers and stores that lie on any directed path in the promotional network, then aggregate weighted revenue per region.

```
-- Band 1: Context
Join:
  TrimJoin(
    TemporalJoin(
      Anchor(dim, c)
      PathJoin(c, VerbHop(c, 'placed', s), s)
      PathJoin(s, VerbHop(d, 'included in', s), d)
      PathJoin(d, BridgeHop(d, 'promoted in', st), st)
      as_of = '2024-06-30'
    ),
    REACHABLE_BETWEEN(source=c, target=st)
    -- retain only (c, s, d, st) tuples where c lies on
    -- a directed path to st in the promotional subgraph
  )

Where:
  AND(
    ScalarPred(PropRef(c.segment),          (= "retail")),
    ScalarPred(PropRef(s.occurred_on.year), (= 2024))
  )

-- Band 2: Aggregation
GroupBy: [ c.region ]

Agg:
  weighted_rev := PathAgg(
    d,
    BridgeHop(d, 'promoted in', st, edge_alias=promo),
    ALL, fn=SUM,
    ArithExpr(PropRef(s.revenue), *, PropRef(promo.weight))
  )

Having:
  ScalarPred(AggRef(weighted_rev), (> 5000))
```

### 14.2  TARJAN_SCC — detect event cycles

Find all sales that participate in a directed cycle of `succeeded` fact-to-fact bridges (indicating a circular chain of events), ranked by cycle size.

```
-- Band 1: Context
Join:
  TrimJoin(
    Anchor(fact, s),
    MIN_DEGREE(1, OUT)      -- discard isolated sale facts
  )

-- Band 3: Ranking
Window:
  scc_label := GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC), s))
  scc_size  := GraphExpr(SubgraphExpr(DENSITY,
                                      partition=[scc_label]))
  rank_in_scc := RANK() OVER (PARTITION BY scc_label
                               ORDER BY s.revenue DESC)

Qualify:
  AND(
    ScalarPred(WinRef(scc_size), (> 1)),    -- only non-trivial SCCs (actual cycles)
    ScalarPred(WinRef(rank_in_scc), (<= 3)) -- top-3 sales per cycle by revenue
  )
```

`TARJAN_SCC` assigns each sale a strongly connected component label. `scc_size > 1` retains only nodes that are part of a genuine directed cycle — a trivial SCC (a single node with no self-loop) has size 1.

### 14.3  Floyd-Warshall transparent use — average distance per region

Compute the average shortest-path length from each customer to their nearest store, grouped by region. The planner executes Floyd-Warshall internally for the `PairExpr`.

```
-- Band 1: Context
Join:
  Anchor(dim, c)
  PathJoin(c, VerbHop(c, 'placed', s), s)
  PathJoin(d, BridgeHop(d, 'promoted in', st), st)

Where:
  ScalarPred(PropRef(s.occurred_on.year), (= 2024))

-- Band 2: Aggregation
GroupBy: [ c.region ]

Agg:
  avg_dist := AVG(GraphExpr(PairExpr(SHORTEST_PATH_LENGTH, c, st)))

Having:
  ScalarPred(AggRef(avg_dist), (< 5))
```

The query author writes `PairExpr(SHORTEST_PATH_LENGTH, c, st)`. If `C` contains many `(c, st)` pairs, the planner may execute the full Floyd-Warshall matrix once and serve all lookups from it — transparent to the query.

### 14.4  Nested TrimJoin — composing criteria

Restrict to nodes reachable between customer and store, then further restrict to those with at least one outgoing edge (removing dead-end sinks introduced by the backbone trim).

```
Join:
  TrimJoin(
    TrimJoin(
      Anchor(dim, c)
      PathJoin(c, BridgeHop(c, 'near', st), st),
      REACHABLE_BETWEEN(source=c, target=st)
    ),
    SINK_FREE     -- remove any sinks introduced after backbone trim
  )
```

`TrimJoin` modifiers are applied inside-out: `REACHABLE_BETWEEN` first, then `SINK_FREE` over the result.

### 14.5  B₁ ∘ B₂ ∘ B₃ — full pipeline with fact-to-fact bridge and L-promotion

Retail customers whose top-2 sales by revenue in 2024 each succeeded a prior sale, with total weighted revenue exceeding 5000.

```
-- Band 1: Context
Join:
  TemporalJoin(
    Anchor(dim, c)
    PathJoin(c, VerbHop(c, 'placed', s), s)
    PathJoin(s, VerbHop(d, 'included in', s), d)
    as_of = '2024-06-30'
  )

Where:
  AND(
    ScalarPred(PropRef(c.segment),          (= "retail")),
    ScalarPred(PropRef(s.occurred_on.year), (= 2024)),
    PathPred(
      s,
      BridgeHop(s, 'succeeded', s_prior, node_alias=prior_sale),
      EXISTS, SHORTEST, promote: true,
      ScalarPred(PropRef(prior_sale.occurred_on),
                 (< PropRef(s.occurred_on)))
    )
  )

-- Band 2: Aggregation
GroupBy: [ c.id, c.region ]

Agg:
  weighted_rev := PathAgg(
    d,
    BridgeHop(d, 'promoted in', st, edge_alias=promo),
    ALL, fn=SUM,
    ArithExpr(PropRef(s.revenue), *, PropRef(promo.weight))
  )
  order_count := COUNT(s)

Having:
  AND(
    ScalarPred(AggRef(weighted_rev), (> 5000)),
    ScalarPred(AggRef(order_count),  (>= 3))
  )

-- Band 3: Ranking
Window:
  rank_by_rev := RANK() OVER (PARTITION BY c.id ORDER BY s.revenue DESC)

Qualify:
  ScalarPred(WinRef(rank_by_rev), (<= 2))
```

---

## 15  Summary

### Graph elements

| Element | Kind | Connects | Carries |
|---|---|---|---|
| Dimension node | `dim` | — | Attributes; optionally SCD2-versioned |
| Fact node | `fact` | — | Measures (aggregable) |
| Verb edge | `verb` | `dim → fact` | Verb label |
| Bridge edge | `bridge` | `N → N` (any node types) | Verb label + optional weight |

### Join modifiers

| Modifier | Wraps | Effect |
|---|---|---|
| `TemporalJoin(join, as_of)` | Any `Join` | Resolves SCD2-versioned nodes at expansion time |
| `TrimJoin(join, criterion)` | Any `Join` | Restricts node set by structural criterion before further expansion |

### TrimCriterion

| Criterion | Retains | PathPred equivalent? |
|---|---|---|
| `REACHABLE_BETWEEN(source, target)` | On-path nodes | No — requires global path knowledge |
| `MIN_DEGREE(n, direction)` | Nodes with ≥ n edges | Yes — `PathPred` existence check |
| `SINK_FREE` | Nodes with ≥ 1 outgoing edge | Yes — `PathPred` existence check |
| `SOURCE_FREE` | Nodes with ≥ 1 incoming edge | Yes — `PathPred` existence check |

### Classical algorithms

| Algorithm | DGM placement | Grammar element |
|---|---|---|
| Floyd-Warshall | Planner strategy for `PairAlg` | Transparent; no grammar exposure |
| Tarjan SCC | Directed community detection | `CommAlg :: TARJAN_SCC` |
| Trim (degree-based) | `PathPred` existence check | `PathPred` in `Where` |
| Trim (on-path / structural) | Context restriction at join time | `TrimJoin(criterion: REACHABLE_BETWEEN)` |

### GraphExpr granularity and placement

| Granularity | Kind | In `Agg` | In `Window` |
|---|---|---|---|
| Node | `NodeExpr` | ✅ | ✅ always |
| Pair | `PairExpr` | ✅ | ✅ always |
| Subgraph | `SubgraphExpr` | ✅ | ✅ with non-empty `partition` |

### Path grammar

| Grammar | Aliases | Used in |
|---|---|---|
| `Path` | None | `Join`, `Window` partition |
| `BoundPath` | Optional per hop | `PathPred`, `PathAgg` |

### Evaluation contexts

| Symbol | Content | Scope |
|---|---|---|
| `C` | Aliases from `Join`; promoted `L` bindings | All phases |
| `L` | Aliases from `BoundPath` bindings | Per path instance, within `PathPred`/`PathAgg` |
| `C ∪ L` | Combined | Inside `sub_filter` and `PathAgg` ref |

### L-promotion

| Strategy | `promote: true` | Effect |
|---|---|---|
| `SHORTEST` | Allowed | Lifts `L` into `C` |
| `MIN_WEIGHT` | Allowed | Lifts `L` into `C` |
| `ALL` | Rejected | `L` always ephemeral |
| `K_SHORTEST(k)` | Rejected | `L` always ephemeral |

### Query bands

| Band | Clauses | Role | Required | Admitted predicates |
|---|---|---|---|---|
| B₁ Context | `Join`, `Where` | Define working subgraph | Always | `ScalarPred[PropRef∈C]`, `PathPred[C∪L]` |
| B₂ Aggregation | `GroupBy`, `Agg`, `Having` | Collapse and summarise | Optional | `ScalarPred[AggRef]` |
| B₃ Ranking | `Window`, `Qualify` | Rank tuples and filter | Optional | `ScalarPred[WinRef]` |

---

## 16  Relationship to sqldim

### 16.1  Node and edge alignment

| DGM Concept | sqldim | Status |
|---|---|---|
| Dimension nodes (`D`) | `DimensionModel` → implicit vertex | ✅ Aligned |
| Fact nodes (`F`) | `FactModel` → binary edge | ⚠️ Partial |
| Verb edges (`V ⊆ D×F×L`) | FK relationships | ⚠️ Implicit |
| Bridge edges (`B ⊆ N×N×L`) | dim→dim hierarchy only | ⚠️ Partial |
| Fact-to-fact bridge edges | Not implemented | ❌ Missing |
| `τ_E : E → {verb, bridge}` | Not present | ❌ Missing |

### 16.2  Query algebra alignment

| DGM | sqldim | Status |
|---|---|---|
| B₁ `TrimJoin` | Not implemented | ❌ Missing |
| B₁ `PathPred` with `BoundPath`, quantifier, strategy, `promote` | Not implemented | ❌ Missing |
| B₂ `PathAgg` with strategy, `promote`, `C∪L` | `aggregate_sql(weighted=True)` (implicit ALL) | ⚠️ Partial |
| B₂/B₃ `GraphExpr` — `NodeExpr`, `PairExpr`, `SubgraphExpr` | Not implemented | ❌ Missing |
| `TARJAN_SCC` | Not implemented | ❌ Missing |
| Floyd-Warshall planner optimisation | Not implemented | ❌ Missing |
| B₁ `TemporalJoin` | `.as_of(timestamp)` | ✅ Aligned |
| B₂ `Having` | Not implemented | ❌ Missing |
| B₃ `Qualify` | Internal SCD processors only | ⚠️ Not exposed |

### 16.3  Implementation roadmap

**Phase 1 — Schema layer (2–3 days)**
- Extend `GraphSchema` to `B ⊆ N×N×L` (fact-to-fact and fact-to-dim bridges).
- Add `τ_E : E → {verb, bridge}` classification.
- Extend `to_graph()` for fact-as-node (N-ary events).

**Phase 2 — Query DSL (1 week)**
- Implement `TrimJoin` with all four `TrimCriterion` variants.
- Implement `BoundPath` with `node_alias`/`edge_alias` and `promote` flag.
- Implement `PathPred` with `Quantifier`, `Strategy`, `promote`, `C∪L` scope.
- Implement `PathAgg` with `Strategy`, `promote`, cross-context `ArithExpr`.
- Implement L-promotion mechanics (single-instance strategies only).
- Promote `Qualify` to public API.

**Phase 3 — Filter tree typing (3–4 days)**
- `Ref` kind enforcement across all bands.
- Reject `promote: true` with `ALL`/`K_SHORTEST` at construction.
- Cross-band reference rule enforcement.

**Phase 4 — Graph algorithms (ongoing)**
- `NodeExpr`: `PAGE_RANK`, `BETWEENNESS_CENTRALITY`, `CLOSENESS_CENTRALITY`, `DEGREE`, `CONNECTED_COMPONENTS`, `TARJAN_SCC`, `LOUVAIN`, `LABEL_PROPAGATION`.
- `PairExpr`: `SHORTEST_PATH_LENGTH`, `MIN_WEIGHT_PATH_LENGTH`, `REACHABLE`; Floyd-Warshall planner for multi-pair queries.
- `SubgraphExpr`: `MAX_FLOW`, `DENSITY`, `DIAMETER`; partition broadcast in `Window`.
- Dimensional grounding constraint at construction time.

---

## 17  Related Work

### 17.1  Graph OLAP

Chen et al. (2008, 2009) [1, 2] introduced informational and topological OLAP. DGM extends the aggregation layer with path strategies, path-local context, L-promotion, `TrimJoin`, and `GraphExpr` typed by granularity.

### 17.2  Graphoids

Gómez, Kuijpers, and Vaisman (2017, 2019) [3, 4] formalised OLAP over directed multi-hypergraphs in Neo4j. DGM adds the `dim`/`fact` split, `B ⊆ N×N×L`, `BoundPath` with L-promotion, `TrimJoin`, and `GraphExpr`.

### 17.3  Multi-dimensional event data on graphs

Esser and Fahland (2021) [5] arrived at reified event-nodes from process mining. DGM extends their model with generalised bridge edges (including fact-to-fact), `TrimJoin`, `TemporalJoin`, quantified `PathPred` with L-promotion, `PathAgg`, and `GraphExpr` with directed algorithms including `TARJAN_SCC`.

### 17.4  Knowledge Graph OLAP

Schuetz et al. (2021) [6] introduced KG-OLAP over RDF context dimensions. DGM targets numeric aggregation with graph-algorithmic enrichment. Complementary frameworks.

### 17.5  Novel contributions of DGM

| Aspect | Status in literature |
|---|---|
| Typed `dim`/`fact` split as schema constraint | Not present |
| Verb edge constraint (`V ⊆ D×F×L`) | Implicit in [5]; not formalised |
| Generalised bridge edge `B ⊆ N×N×L` (incl. fact-to-fact) | Novel |
| `TemporalJoin` as first-class SCD2 modifier | Novel |
| `TrimJoin` as structural context restriction at join time | Novel |
| `BoundPath` with path-local bindings and L-promotion | Novel |
| Explicit path strategy with `EXISTS`/`FORALL` quantifier | Novel |
| `PathAgg` with cross-context `ArithExpr` (`C∪L`) | Novel |
| `GraphExpr` typed by granularity with unified placement rule | Novel |
| `TARJAN_SCC` for directed SCC in dimensional context | Novel |
| Floyd-Warshall as transparent planner strategy for `PairExpr` | Novel |
| `SubgraphExpr` in `Window` with partition broadcast | Novel |
| Unified predicate and expression languages across three bands | Novel |

---

## 18  Design Decisions

### 18.1  Bridge edges relaxed to B ⊆ N×N×L

**Decision.** Bridge edges connect any two nodes. Verb edge constraint `V ⊆ D×F×L` unchanged.

**Rationale.** Fact-to-fact structural relationships (causal chains, succession, supersession) require bridge edges between fact nodes. The semantic distinction between fact and dimension is preserved at the node type level; the bridge adds no event semantics.

### 18.2  NOT: tuple level

**Decision.** `NOT` is tuple-level. `NOT(NOT(T)) ≡ T`.

**Rationale.** Preserves compositionality. Graph-level exclusion would be order-dependent.

### 18.3  PathPred: explicit quantifier and strategy

**Decision.** `PathPred` requires explicit `Quantifier` and `Strategy`. No defaults.

**Rationale.** Implicit quantifier or fan-out produces topology-dependent results silently.

### 18.4  L-promotion: single-instance strategies only

**Decision.** `promote: true` valid only for `SHORTEST` and `MIN_WEIGHT`.

**Rationale.** Single-instance strategies guarantee cardinality 1 per outer tuple — promotion produces no fan-out. Multi-instance strategies may yield multiple instances; promoting `L` from them would silently fan-out `C`. If fan-out is intended, `PathJoin` expresses it explicitly.

### 18.5  No implicit weighting

**Decision.** Weight applied only when explicitly referenced. Absence means "ignore weight" — no error raised.

**Rationale.** The earlier "raises error" formulation was over-specified. Not referencing weight is a valid and common intent.

### 18.6  Floyd-Warshall as planner strategy

**Decision.** Floyd-Warshall is not exposed in the grammar. It is a planner-level execution strategy for `PairExpr(SHORTEST_PATH_LENGTH, ...)` and `PairExpr(MIN_WEIGHT_PATH_LENGTH, ...)` when many pairs are present in `C`.

**Rationale.** Floyd-Warshall computes the same semantic result as per-pair shortest-path queries — the all-pairs distance matrix — but more efficiently when the full matrix is needed. The query author expresses intent via `PairExpr`; the planner selects the algorithm. Exposing Floyd-Warshall as a named `PairAlg` would conflate semantic intent with execution strategy.

### 18.7  Tarjan SCC as CommAlg variant

**Decision.** `TARJAN_SCC` is added to `CommAlg` as a directed strongly-connected-components algorithm.

**Rationale.** `CONNECTED_COMPONENTS` and `LOUVAIN` operate on undirected or weakly connected structure. DGM's verb and bridge edges are directed — `TARJAN_SCC` is semantically distinct because it respects edge direction. In DGM, an SCC in fact-to-fact bridges identifies a directed cycle of events, which `CONNECTED_COMPONENTS` cannot detect. It belongs in `CommAlg` because it assigns a component label to each node, fitting the existing `NodeExpr` interface.

### 18.8  TrimJoin as Join modifier

**Decision.** Structural graph trimming is a `TrimJoin(join, criterion)` modifier in the `Join` grammar. Four criteria: `REACHABLE_BETWEEN`, `MIN_DEGREE`, `SINK_FREE`, `SOURCE_FREE`.

**Rationale.** Three of the four criteria (`MIN_DEGREE`, `SINK_FREE`, `SOURCE_FREE`) are expressible as `PathPred` existence checks. `TrimJoin` is provided as a declarative shorthand and a planner hint — trimming at join time avoids expanding paths from nodes that will be discarded. `REACHABLE_BETWEEN` is not expressible as a single `PathPred` because it requires global knowledge of which nodes lie on any directed path between two endpoints; it genuinely requires join-time evaluation over the full graph.

### 18.9  GraphExpr typed by granularity; SubgraphExpr in Window with partition

**Decision.** Unified placement rule: a `GraphExpr` in `Window` must produce one value per tuple. `SubgraphExpr` satisfies this with a non-empty `partition` via broadcast semantics.

**Rationale.** `SubgraphExpr` is `GraphExpr` at coarser granularity, not a structurally different kind. A single unified rule replaces the prior special-case restriction.

### 18.10  Unified predicate language

**Decision.** `ScalarPred` and `PathPred` with typed `Ref`. Band validity via `Ref` kind.

### 18.11  Unified expression language

**Decision.** `AggFn`, `PathAgg`, `GraphExpr` share a common `Expr` interface.

### 18.12  L context: ephemeral by default; explicit promotion

**Decision.** `L` ephemeral by default. `promote: true` on single-instance strategies lifts `L` into `C`. Irreversible within a query.

### 18.13  Three-band structure

**Decision.** `Q = B₁ ∘ B₂? ∘ B₃?`. Four valid forms. B₁ defines *what*, B₂ *how to collapse*, B₃ *which tuples to keep*.

---

## 19  References

[1] Chen, C., Yan, X., Zhu, F., Han, J., Yu, P. S. (2008). Graph OLAP: Towards Online Analytical Processing on Graphs. *Proceedings of the 8th IEEE International Conference on Data Mining (ICDM 2008)*, 103–112. https://doi.org/10.1109/ICDM.2008.45

[2] Chen, C., Yan, X., Zhu, F., Han, J., Yu, P. S. (2009). Graph OLAP: A Multi-Dimensional Framework for Graph Data Analysis. *Knowledge and Information Systems*, 21(1), 41–63. https://doi.org/10.1007/s10115-009-0228-8

[3] Gómez, L. I., Kuijpers, B., Vaisman, A. A. (2017). Performing OLAP over Graph Data: Query Language, Implementation, and a Case Study. *Proceedings of the International Workshop on Real-Time Business Intelligence and Analytics (BIRTE 2017)*, 6:1–6:8.

[4] Gómez, L. I., Kuijpers, B., Vaisman, A. A. (2019). Online Analytical Processing on Graph Data. *Intelligent Data Analysis*, 24(3), 515–541. arXiv:1909.01216. https://doi.org/10.3233/IDA-194576

[5] Esser, S., Fahland, D. (2021). Multi-Dimensional Event Data in Graph Databases. *Journal on Data Semantics*, 10(1–2), 109–141. arXiv:2005.14552. https://doi.org/10.1007/s13740-021-00122-1

[6] Schuetz, C. G., Bozzato, L., Neumayr, B., Schrefl, M., Serafini, L. (2021). Knowledge Graph OLAP: A Multidimensional Model and Query Operations for Contextualized Knowledge Graphs. *Semantic Web*, 12(4), 649–683. https://doi.org/10.3233/SW-200419

---

*— end of specification —*