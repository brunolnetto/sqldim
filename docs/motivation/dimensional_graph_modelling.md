# Dimensional Graph Model — Formal Specification

---

## Contents

1. [Overview](#1-overview)
2. [Graph Model](#2-graph-model)
   - 2.1 Structure and invariants
   - 2.2 Node types
   - 2.3 Edge types
   - 2.4 Schema annotation layer (Σ)
   - 2.5 Temporal properties and snapshots
3. [Traversal Paths](#3-traversal-paths)
   - 3.1 Unbound paths
   - 3.2 Bound paths and TemporalOrdering
   - 3.3 Path strategy and lattice
   - 3.4 RelationshipSubgraph
   - 3.5 Evaluation contexts and L-promotion
4. [Query Language](#4-query-language)
   - 4.1 Predicate language
   - 4.2 Expression language
5. [Query Structure](#5-query-structure)
   - 5.1 Query forms
   - 5.2 Bands
     - 5.2.1 Band 1 — Context layer
     - 5.2.2 Band 2 — Aggregation layer
     - 5.2.3 Band 3 — Ranking layer
   - 5.3 Evaluation order
6. [Execution Architecture](#6-execution-architecture)
   - 6.1 BDD canonical predicate representation
   - 6.2 Query planner
   - 6.3 Query exporter
7. [Recommender Architecture](#7-recommender-architecture)
8. [Theoretical Foundations](#8-theoretical-foundations)
9. [Implementation Guide](#9-implementation-guide)
   - 9.1 Alignment with sqldim
   - 9.2 Implementation roadmap
10. [Reference](#10-reference)
    - 10.1 Worked examples
    - 10.2 Summary tables
    - 10.3 Related work
    - 10.4 Design decisions
    - 10.5 References

---

## 1  Overview

The **Dimensional Graph Model (DGM)** is a typed directed graph framework that unifies dimensional analysis, graph modelling, and temporal logic. It provides a formal basis for representing domain entities, their relationships, measurable events, structural connections, and their evolution over time.

The framework rests on fifteen core ideas:

- **Dimensions and facts are both first-class nodes**, distinguished by the kind of data they carry.
- **Relationships are typed directed edges**: verb edges bind dimensions to facts (V ⊆ D×F×L); bridge edges are general structural connectors (B ⊆ N×N×L), optionally bounded by validity windows.
- **A schema annotation layer Σ** carries semantic metadata — grain, SCD type, weight constraints, bridge semantics, hierarchy structure, and fact/dimension taxonomy — that drives decisions in the planner, recommender, and exporter without changing graph topology.
- **Joins are path traversals** expanding the query context. Temporal resolution, edge validity, and structural trimming are first-class Join modifiers.
- **Path strategy is explicit** — every path-based operation declares how paths are selected and what quantifier scopes the truth condition.
- **Reverse verb hops** are permitted in BoundPath, enabling traversal from fact to dimension against the natural verb direction.
- **Temporal path ordering** supports point-based ordering, Allen's full 13-relation interval algebra, and LTL UNTIL/SINCE operators.
- **Path-local bindings** allow hops in a BoundPath to introduce named aliases evaluated over C ∪ L.
- **A unified predicate language** governs all filter clauses via three constructors (ScalarPred, PathPred, SignaturePred). Functionally complete; realises full CTL via TemporalMode.
- **A unified expression language** governs all computed values. TrailExpr algorithms operate over RelationshipSubgraph scopes with relaxed or fixed endpoints — enabling forward-cone, backward-cone, and full traversal-space analysis.
- **DGM is a Kripke structure** — nodes are states, edges are transitions, property bags are labels. The transposed graph G^T enables backward-cone computation and past-temporal model checking.
- **Temporal queries** come in two forms: snapshot queries (Q) and delta queries (Q_delta).
- **A query planner** makes DGM-specific physical execution decisions informed by schema annotations, including cone containment optimisation (Rule 9) for relaxed-endpoint traversals.
- **A query exporter** translates physical plans to multiple query and sink targets.
- **A recommender** surfaces analytically useful next steps via a two-stage trail exploration flow grounded in endpoint relaxation.

---

## 2  Graph Model

### 2.1  Structure and invariants

A DGM graph is a 6-tuple:

```
G = (N, E, τ_N, τ_E, P, Σ)
```

| Symbol | Definition |
|---|---|
| N | Finite set of nodes. |
| E ⊆ N×N×L | Set of directed labeled edges. L is the set of verb labels. |
| τ_N : N → {dim, fact} | Node type function. Partitions N into D (dimension) and F (fact). |
| τ_E : E → {verb, bridge} | Edge type function. Partitions E into V (verb) and B (bridge). |
| P : N∪E → (K→V) | Property function. Maps every node and edge to a key-value property bag. |
| Σ | Schema annotation set. See §2.4. |

Structural invariants:

```
D ∩ F = ∅,   D ∪ F = N
V ∩ B = ∅,   V ∪ B = E
V ⊆ D × F × L     -- verb edges: dim → fact only (schema direction)
B ⊆ N × N × L     -- bridge edges: any node → any node
```

**Transposed graph.** G^T = (N, E^R, τ_N, τ_E, P, Σ) where E^R = { (b,a,l) | (a,b,l) ∈ E }. Reverses all edge directions. Produced by adjacency-list reversal at graph load time (O(|E|)); shared across all queries. Used by the planner for Free→Bound backward-cone computation and by SINCE/ONCE/PREVIOUSLY TemporalMode SQL generation.

**Kripke equivalence.** G is formally a Kripke structure K = (S, R, L) under S=N, R=E, L=P. G^T is the time-reversal of K — past-temporal model checking runs on G^T. See §8.1.

### 2.2  Node types

**Dimension nodes** d ∈ D — domain entities. P(d) carries attributes for filtering and grouping. May be SCD-versioned (SCDType annotation, §2.4). Notation: d.attr.

**Fact nodes** f ∈ F — measurable events or states. P(f) carries measures and a timestamp or interval depending on grain annotation (§2.4). Notation: f.measure, f.occurred_on.

Fact nodes may not be the source of verb edges (§10.4, D6).

### 2.3  Edge types

**Verb edges** v = (d, f, label) ∈ V. Schema direction: dim → fact. Query-time reverse traversal permitted via VerbHop⁻¹ (§3.2).

```
V ⊆ D × F × L
```

**Bridge edges** b = (n₁, n₂, label) ∈ B.

```
B ⊆ N × N × L
P(b).weight ∈ ℝ⁺ (optional; defaults to 1)
P(b).valid_from/to : Timestamp (optional)
```

| Source | Target | Typical use | Bridge semantics |
|---|---|---|---|
| dim | dim | Hierarchy, geography, organisation | STRUCTURAL |
| fact | fact | Causal chain, succession, supersession | CAUSAL/TEMPORAL/SUPERSESSION |
| fact | dim | Event-to-entity structural link | STRUCTURAL |
| dim | fact | Structural reference | STRUCTURAL |

### 2.4  Schema annotation layer (Σ)

Σ carries semantic metadata. Annotations do not change graph topology or property bags.

```
Σ ⊆ SchemaAnnotation*

SchemaAnnotation ::=
    Conformed(d: D, fact_types: set[τ_N])
  | Grain(f: F, grain: EVENT | PERIOD | ACCUMULATING)
  | SCDType(d: D, scd: SCD1|SCD2|SCD3|SCD6,
             versioned_attrs?: set[K], overwrite_attrs?: set[K], prev_attrs?: set[K])
  | Degenerate(d: D)
  | RolePlaying(d: D, roles: list[L])
  | ProjectsFrom(d_mini: D, d_full: D)
  | FactlessFact(f: F)
  | DerivedFact(f: F, sources: list[F], expr: Expr)
  | WeightConstraint(b: B, constraint: ALLOCATIVE | UNCONSTRAINED)
  | BridgeSemantics(b: B, sem: CAUSAL|TEMPORAL|SUPERSESSION|STRUCTURAL)
  | Hierarchy(root: D, depth: ℕ | RAGGED)
```

Key annotation semantics:

**Conformed(d, fact_types)** — d reachable via verb edges from each fact type. Enables constellation paths. Bound→Free TrailExpr can discover candidates empirically.

**Grain(f, grain)** — EVENT: additive; PERIOD: state (SUM invalid); ACCUMULATING: lifecycle row.

**SCDType** — SCD1: strip temporal resolution. SCD2: effective_from/to. SCD3: current/previous props. SCD6: per-attribute policy.

**BridgeSemantics(CAUSAL)** — DAG; drop cycle guard; use DAG BFS on both G and G^T.

**Σ is an operational decision table**: every annotation drives specified planner, recommender, and exporter behaviour.

### 2.5  Temporal properties and snapshots

```
P(n).effective_from/to : Timestamp (NULL = open)
P(e).valid_from/to     : Timestamp (NULL = open)
```

Snapshot G(t): nodes/edges valid at t. Delta ΔG(t₁, t₂): N_added, N_removed, E_added, E_removed, P_changed.

---

## 3  Traversal Paths

### 3.1  Unbound paths

```
Path ::= NodeRef(n: N)
       | VerbHop(d: D, label: L, f: F)
       | VerbHop⁻¹(f: F, label: L, d: D)    -- reverse: fact → dim (query-time)
       | BridgeHop(n₁: N, label: L, n₂: N)
       | Compose(Path, Path)
```

### 3.2  Bound paths and TemporalOrdering

```
BoundPath ::= NodeRef(n: N)
            | VerbHop(d,label,f, node_alias?,edge_alias?)
            | VerbHop⁻¹(f,label,d, node_alias?,edge_alias?)
            | BridgeHop(n₁,label,n₂, node_alias?,edge_alias?)
            | Compose(BoundPath, BoundPath, temporal?: TemporalOrdering)

TemporalOrdering ::=
  -- Point-based:
    BEFORE(left_ts,right_ts) | AFTER(left_ts,right_ts)
  | CONCURRENT(left_ts,right_ts,tolerance?)
  -- Allen's 13 interval relations:
  | MEETS(l,r) | MET_BY(l,r) | OVERLAPS(l,r) | OVERLAPPED_BY(l,r)
  | STARTS(l,r) | STARTED_BY(l,r) | DURING(l,r) | CONTAINS(l,r)
  | FINISHES(l,r) | FINISHED_BY(l,r) | EQUALS(l,r)
  -- LTL operators:
  | UNTIL(hold_pred: Pred, trigger_pred: Pred)
  | SINCE(hold_pred: Pred, trigger_pred: Pred)

Interval ::= (start: PropRef, end: PropRef)
```

Allen's relations apply to PERIOD/ACCUMULATING grain facts. UNTIL(φ,ψ): φ holds at every intermediate hop until first ψ. SINCE is the past dual.

**Constellation path** — two-hop via conformed dimension:

```
Compose(VerbHop⁻¹(f₁,verb₁,dim,node_alias=shared_dim),
        VerbHop(shared_dim,verb₂,f₂,node_alias=fact2))
-- valid when Conformed(dim,{type(f₁),type(f₂)}) ∈ Σ
```

Alias uniqueness enforced at construction: C ∩ L ≠ ∅ rejected.

### 3.3  Path strategy and lattice

```
Strategy ::= ALL | SHORTEST | K_SHORTEST(k: ℕ) | MIN_WEIGHT

SHORTEST ⊆ K_SHORTEST(1) ⊆ K_SHORTEST(k) ⊆ ALL
MIN_WEIGHT ⊆ ALL
```

Single-instance (SHORTEST, MIN_WEIGHT): promote: true compatible.
Multi-instance (ALL, K_SHORTEST): explicit fan-out.

### 3.4  RelationshipSubgraph

#### 3.4.1  Endpoint type

```
Endpoint ::= Bound(alias: A)   -- fixed node, resolved in C
           | Free               -- all nodes satisfying traversal criterion
```

#### 3.4.2  Extended definition

```
RelationshipSubgraph(source: Endpoint, target: Endpoint, strategy: Strategy)
  → G_src_tgt = (N_src_tgt, E_src_tgt, τ_N, τ_E, P, Σ)
```

**Case 1 — Bound(A) → Bound(B)** (fixed endpoints; existing):

```
N_AB = { n ∈ N | n lies on ≥1 simple path A →* B under strategy }
E_AB = { e ∈ E | e lies on ≥1 such path }
```

Computed via TrimJoin(REACHABLE_BETWEEN(A,B)).

**Case 2 — Bound(A) → Free** (forward cone):

```
N_A* = { n ∈ N | ∃ simple path A →* n under strategy }
E_A* = { e ∈ E | e lies on ≥1 simple path starting at A }
```

Computed via forward BFS/DFS from A on G. TrimJoin(REACHABLE_FROM(A)).

**Case 3 — Free → Bound(B)** (backward cone):

```
N_*B = { n ∈ N | ∃ simple path n →* B under strategy }
E_*B = { e ∈ E | e lies on ≥1 simple path terminating at B }
```

Computed via forward BFS/DFS from B on G^T. TrimJoin(REACHABLE_TO(B)).

**Case 4 — Free → Free** (full traversal space):

```
N_** = N  (weakly connected component of C when inside a query)
E_** = E
```

Only permitted in Agg or Window — not in Where PathPred scope (§10.4, D17).

**Containment invariant:**

```
G_AB ⊆ G_A*  and  G_AB ⊆ G_*B  and  G_A* ⊆ G_**  and  G_*B ⊆ G_**
```

Cone intersection identity: G_A* ∩ G_*B = G_AB (basis for Rule 9, §6.2).

#### 3.4.3  Granularity tier by endpoint case

| Source | Target | Tier | Produces |
|---|---|---|---|
| Bound(A) | Bound(B) | PairExpr | One value per (A,B) pair in C |
| Bound(A) | Free | NodeExpr | One value per A node in C |
| Free | Bound(B) | NodeExpr | One value per B node in C |
| Free | Free | SubgraphExpr | One graph-level scalar |

Tier assignment preserves the granularity placement rule (§4.2) without modification.

#### 3.4.4  Semantic interpretation

| Case | Algorithm | Meaning |
|---|---|---|
| Bound→Bound | DENSITY | Richness of connection between pair |
| Bound→Bound | MAX_FLOW | Total analytical bandwidth |
| Bound→Free | OUTGOING_SIGNATURES | Relationship type inventory from A |
| Bound→Free | SIGNATURE_DIVERSITY | Normalised variety of outgoing paths |
| Free→Bound | INCOMING_SIGNATURES | Provenance type inventory into B |
| Free→Bound | SIGNATURE_DIVERSITY | Normalised variety of incoming paths |
| Free→Free | SIGNATURE_ENTROPY | Graph-scope relationship diversity |

### 3.5  Evaluation contexts and L-promotion

```
C  -- outer join context (extended by L-promotion)
L  -- path-local context (per path instance; ephemeral by default)
```

PropRef inside sub_filter or PathAgg resolves against C ∪ L. alias ∈ C ∩ L rejected at construction.

L-promotion: promote: true valid only for SHORTEST and MIN_WEIGHT. Lifts L into C; one-way. Rejected for ALL and K_SHORTEST.

---

## 4  Query Language

### 4.1  Predicate language

```
Ref ::= PropRef(alias.prop)      -- B₁; C∪L inside PathPred
      | AggRef(aᵢ)               -- B₂
      | WinRef(wᵢ)               -- B₃
      | SignatureRef(path_alias) -- label sequence of BoundPath instance (B₁ only)
```

| Band | Filter | Valid Ref | PathPred | SignaturePred |
|---|---|---|---|---|
| B₁ | Where | PropRef from C | Yes (C∪L) | Yes |
| B₂ | Having | AggRef | No | No |
| B₃ | Qualify | WinRef | No | No |

```
Pred ::= ScalarPred(ref: Ref, pred: V → Bool)
       | PathPred(alias: A, path: BoundPath,
                  quantifier: Quantifier,
                  strategy: Strategy,
                  temporal_mode?: TemporalMode,
                  promote?: Bool,
                  sub_filter: Pred)
       | SignaturePred(path_alias: A,
                       sequence: list[L | WILDCARD],
                       match: EXACT|PREFIX|CONTAINS|REGEX)

Quantifier   ::= EXISTS | FORALL

TemporalMode ::= EVENTUALLY | GLOBALLY | NEXT
               | UNTIL(φ: Pred) | SINCE(φ: Pred)
               | ONCE | PREVIOUSLY
```

FORALL ≡ NOT PathPred(EXISTS, ..., NOT(sub_filter)) — De Morgan at construction.

**CTL operator map:**

| Quantifier | Mode | CTL |
|---|---|---|
| FORALL | GLOBALLY | AG φ — safety |
| EXISTS | GLOBALLY | EG φ |
| FORALL | EVENTUALLY | AF φ — liveness |
| EXISTS | EVENTUALLY | EF φ |
| FORALL | NEXT | AX φ |
| EXISTS | NEXT | EX φ |
| FORALL | UNTIL(ψ) | A(φ U ψ) |
| EXISTS | UNTIL(ψ) | E(φ U ψ) |

**Temporal property classes** (syntactic sugar over PathPred compositions):

```
TemporalProperty ::= SAFETY(bad)          -- AG(¬bad)
                   | LIVENESS(good)        -- AF(good)
                   | RESPONSE(trig,resp)   -- AG(trig → AF(resp))
                   | PERSISTENCE(good)     -- AF(AG(good))
                   | RECURRENCE(good,win)  -- good in every window period
```

**Minterm/maxterm duality:**

```
PathPred(EXISTS, ALL, φ) ≅ disjunction over path instances ≅ minterms
PathPred(FORALL, ALL, φ) ≅ conjunction over path instances ≅ maxterms
```

Boolean filter tree: T ::= AND(...) | OR(...) | NOT(T) | Pred. NOT is tuple-level.

### 4.2  Expression language

```
Expr ::= AggFn(fn: AggFn, ref: PropRef)
       | PathAgg(alias, path: BoundPath, strategy, promote?, fn: AggFn, ref: Ref_L)
       | TemporalAgg(fn: AggFn, ref: PropRef, timestamp: PropRef, window: TemporalWindow)
       | GraphExpr(algorithm: GraphAlgorithm)
       | ArithExpr(Expr, op, Expr) | Const(c: ℝ)

AggFn          ::= SUM|COUNT|AVG|MIN|MAX|PROD
TemporalWindow ::= ROLLING(Duration)|TRAILING(n,unit)|PERIOD(start,end)|YTD|QTD|MTD
```

GraphExpr algorithms distributed across tiers by endpoint case:

```
GraphAlgorithm ::= NodeExpr(algorithm: NodeAlg, alias: A)
                 | PairExpr(algorithm: PairAlg, source: A, target: A)
                 | SubgraphExpr(algorithm: SubgraphAlg,
                                partition?: [PropRef],
                                scope?: RelationshipSubgraph)

NodeAlg ::= PAGE_RANK(damping,iterations)
          | BETWEENNESS_CENTRALITY | CLOSENESS_CENTRALITY
          | DEGREE(direction: {IN,OUT,BOTH})
          | COMMUNITY_LABEL(algorithm: CommAlg)
          -- TrailExpr single-relaxation (Bound→Free or Free→Bound):
          | OUTGOING_SIGNATURES(max_depth?: ℕ)
          | INCOMING_SIGNATURES(max_depth?: ℕ)
          | DOMINANT_OUTGOING_SIGNATURE(max_depth?: ℕ)
          | DOMINANT_INCOMING_SIGNATURE(max_depth?: ℕ)
          | SIGNATURE_DIVERSITY

CommAlg ::= LOUVAIN|LABEL_PROPAGATION|CONNECTED_COMPONENTS|TARJAN_SCC

PairAlg ::= SHORTEST_PATH_LENGTH|MIN_WEIGHT_PATH_LENGTH|REACHABLE
          -- TrailExpr fixed-endpoint (Bound→Bound):
          | DISTINCT_SIGNATURES(max_depth?: ℕ)
          | DOMINANT_SIGNATURE(max_depth?: ℕ)
          | SIGNATURE_SIMILARITY(reference: list[L])

SubgraphAlg ::= MAX_FLOW(source,target,capacity)|DENSITY|DIAMETER
              -- TrailExpr fully-free (Free→Free):
              | GLOBAL_SIGNATURE_COUNT(max_depth?: ℕ)
              | GLOBAL_DOMINANT_SIGNATURE(max_depth?: ℕ)
              | SIGNATURE_ENTROPY(max_depth?: ℕ)
```

**TrailExpr definitions:**

OUTGOING_SIGNATURES(A): count of distinct label sequences on all simple paths starting at A.

INCOMING_SIGNATURES(B): count of distinct label sequences on all simple paths ending at B. Computed via BFS on G^T.

DOMINANT_OUTGOING_SIGNATURE(A): most frequent label sequence in forward cone of A.

DOMINANT_INCOMING_SIGNATURE(B): most frequent label sequence in backward cone of B.

SIGNATURE_DIVERSITY: ratio of distinct label sequences to total path instances in the applicable cone. Scalar in [0,1].

DISTINCT_SIGNATURES(A,B): count of distinct label sequences connecting A to B.

DOMINANT_SIGNATURE(A,B): most frequent label sequence connecting A to B.

SIGNATURE_SIMILARITY(A,B,ref): fraction of A→B paths whose label sequence matches ref.

GLOBAL_SIGNATURE_COUNT: distinct label sequences across the full scope.

GLOBAL_DOMINANT_SIGNATURE: most frequent label sequence globally.

SIGNATURE_ENTROPY: Shannon entropy over path signature distribution:

```
H = -Σ p(s) × log₂ p(s)   where p(s) = count(paths with sequence s) / total paths
```

H=0: all paths same type (focused). H=log₂(k): k equally frequent types (maximally diverse). Range [0, log₂(|distinct signatures|)].

scope on SubgraphExpr: accepts any endpoint case. All four cases satisfy the scope requirement for granularity placement.

**Granularity placement rule:**

| Kind | In Agg | In Window |
|---|---|---|
| NodeExpr (incl. single-relaxation TrailExpr) | ✅ | ✅ always |
| PairExpr (incl. fixed-endpoint TrailExpr) | ✅ | ✅ always |
| SubgraphExpr (incl. free-free TrailExpr) | ✅ | ✅ with non-empty partition or scope only |

---

## 5  Query Structure

### 5.1  Query forms

**Snapshot query:** Q = TemporalContext? ∘ B₁ ∘ B₂? ∘ B₃?

```
TemporalContext(default_as_of: Timestamp,
                node_resolution: STRICT|LAX, edge_resolution: STRICT|LAX)
```

STRICT: reject missing versions. LAX: silently drop. Explicit TemporalJoin overrides TemporalContext for its sub-join. Valid forms: B₁, B₁∘B₂, B₁∘B₃, B₁∘B₂∘B₃.

**Delta query:**

```
Q_delta = (t₁, t₂, spec: DeltaSpec, filter?: T_W)

DeltaSpec ::= ADDED_NODES(τ_N) | REMOVED_NODES(τ_N)
            | ADDED_EDGES(τ_E) | REMOVED_EDGES(τ_E)
            | CHANGED_PROPERTY(alias,prop,comparator)
            | ROLE_DRIFT(alias,from_type,to_type)
```

Q_delta operates outside the three-band structure.

### 5.2  Bands

B₁ = what, B₂ = how to collapse, B₃ = which tuples. Cross-band Ref rules enforced at construction.

#### 5.2.1  Band 1 — Context layer

```
B₁ = (Join, Where?)

Join ::= Anchor(node_type: τ_N, alias: A)
       | PathJoin(alias: A, path: Path, alias': A')
       | CrossJoin(Join, Join)
       | TemporalJoin(join, as_of, node_resolution?, edge_resolution?)
       | TrimJoin(join, criterion: TrimCriterion)

TrimCriterion ::= REACHABLE_BETWEEN(source: A, target: A)  -- Bound→Bound
                | REACHABLE_FROM(source: A)                 -- Bound→Free (new)
                | REACHABLE_TO(target: A)                   -- Free→Bound (new)
                | MIN_DEGREE(n,direction) | SINK_FREE | SOURCE_FREE
```

REACHABLE_FROM(A): restricts context to forward cone G_A* via BFS on G.
REACHABLE_TO(B): restricts context to backward cone G_*B via BFS on G^T.

TemporalJoin resolves both node SCD versions and edge validity. SCDType(SCD1) causes planner to strip temporal resolution entirely.

Where: T [ PropRef from C, PathPred(BoundPath,TemporalMode) and SignaturePred allowed ]

#### 5.2.2  Band 2 — Aggregation layer

```
B₂ = (GroupBy, Agg, Having)
```

GroupBy: PropRef list from C'. Degenerate dims excluded. Agg: { aᵢ := Exprᵢ }. Grain-aware. Free→Free SubgraphExpr valid in Agg — scalar broadcast to all groups. Having: T [AggRef].

#### 5.2.3  Band 3 — Ranking layer

```
B₃ = (Window, Qualify)

WinExpr ::= RankFn(fn,partition,order)
          | RunningAgg(fn,ref,partition,order)
          | StatFn(fn,ref,partition,order?)
          | GraphExpr(NodeExpr|PairExpr|SubgraphExpr[partition or scope])

RankFn ::= ROW_NUMBER|RANK|DENSE_RANK|NTILE(n)|LEAD(ref,offset)|LAG(ref,offset)
StatFn ::= PERCENT_RANK|CUME_DIST|PERCENTILE_CONT(p)|PERCENTILE_DISC(p)
```

Qualify: T [WinRef].

### 5.3  Evaluation order

#### Snapshot query

| Phase | Clause | Action |
|---|---|---|
| 0 | TemporalContext | Set snapshot policy; SCD annotation overrides. |
| 1 | Join | Traverse (incl. VerbHop⁻¹); resolve validity per Σ; TrimJoin incl. G^T BFS for REACHABLE_TO; build C. |
| 2 | Where | T[PropRef]; PathPred with TemporalMode over C∪L; SignaturePred; L-promotion. |
| 3 | GroupBy | Partition C' (degenerate excluded). |
| 4 | Agg | Evaluate Expr per group; grain-aware; Free→Free scalars broadcast. |
| 5 | Having | T[AggRef]; retain groups. |
| 6 | Window | WinExpr per tuple; SubgraphExpr per partition or scope. |
| 7 | Qualify | T[WinRef]; retain tuples. |

L-promotion phase 2 → available from phase 3. Phase 4 → available from phase 5.

#### Delta query

Phase 1: Materialise G(t₁), G(t₂) per SCDType. Phase 2: Compute ΔG. Phase 3: Apply T_W. Phase 4: Return typed diff.

---

## 6  Execution Architecture

### 6.1  BDD canonical predicate representation

BDD provides O(1) equivalence, satisfiability/tautology, implication, minterm enumeration, and foundations for recommender and planner.

Core: BDDNode (var, low, high), FALSE_NODE=0, TRUE_NODE=1. Unique table (var,low,high)→id; computed cache (op,u,v)→result.

Operations: make (elimination+sharing), apply (Shannon expansion; memoised), negate (single-pass), FORALL→NOT EXISTS NOT at construction.

PathPred + TemporalMode opaque atoms. SQL templates:

```
EVENTUALLY  → EXISTS (CTE to any node)
GLOBALLY    → NOT EXISTS (NOT sub_filter anywhere)
NEXT        → EXISTS, single-hop CTE
UNTIL(ψ)   → EXISTS recursive CTE, hold_pred at every intermediate node
SINCE(ψ)   → EXISTS reverse-direction CTE on G^T
ONCE        → EXISTS backward CTE on G^T
PREVIOUSLY  → EXISTS single backward hop via G^T
```

SignaturePred: opaque atom, expanded as label sequence filter on CTE path array.

Variable ordering: PropRef(B₁)→AggRef(B₂)→WinRef(B₃); within B₁: ScalarPred→PathPred→SignaturePred; within ScalarPred: by selectivity.

SQL translation: if |minterms| ≤ THRESHOLD → UNION ALL; else → BDD-structured CASE expression.

Logical optimisation pipeline:

```
1. Tautology / empty-result short-circuit
2. Redundant clause elimination (AND)
3. Predicate subsumption (OR)
4. Cross-band implication (Where → Having)
5. PathPred boolean context propagation
6. TemporalMode SQL template selection
7. Minterm count → SQL translation strategy
8. FORALL → NOT EXISTS NOT (at construction)
```

### 6.2  Query planner

Formal structure:

```
Planner = (cost_model, statistics: GraphStatistics, annotations: Σ,
           rules, query_target: QueryTarget, sink_target: SinkTarget?)

GraphStatistics = (node_count, edge_count, degree_dist,
                   path_cardinality: BoundPath→ℕ,
                   property_dist: PropRef→Distribution,
                   temporal_density: PropRef×Duration→ℝ,
                   scc_sizes: N→ℕ,
                   transposed_adj: N→list[N])   -- G^T adjacency; built at load time

ExportPlan = (query_target, query_text, pre_compute: list[PreComputation],
              sink_target?, write_plan?, cost_estimate,
              alternatives: list[(ExportPlan, CostEstimate)])
```

alternatives: structured plan explanation — mechanical, auditable, no natural language.

**Rule 1a — Path execution strategy:**

```
Bound→Bound (REACHABLE_BETWEEN):
    if path_card ≤ SMALL: recursive CTE with cycle prevention
        -- CAUSAL: drop cycle guard; DAG BFS
    elif strategy ∈ {SHORTEST,MIN_WEIGHT}: recursive CTE ORDER BY + LIMIT 1
        -- TEMPORAL: push ordering into CTE WHERE
    else: pre-materialise path table

Bound→Free (REACHABLE_FROM(A)):
    forward BFS/DFS from A on G
    -- CAUSAL: DAG BFS (no visited-set)
    -- SHORTEST: BFS terminates at first visit per node
    -- ALL: full DFS collecting all simple paths

Free→Bound (REACHABLE_TO(B)):
    forward BFS/DFS from B on G^T (using transposed_adj)
    -- CAUSAL: reverse topological order BFS on G^T
    -- semantically equivalent to backward BFS in G

Free→Free:
    weakly-connected-component from C
    -- only in Agg or Window
    -- pre-materialise when |N| > CLOSURE_THRESHOLD
```

**Rule 1b — Grain-aware aggregation:**

```
Grain(PERIOD):        SUM → reject; suggest LAST; TemporalAgg(SUM) → rewrite
Grain(ACCUMULATING):  cross-row agg → warn; NULL → COALESCE
FactlessFact:         SUM/AVG/MIN/MAX → reject at construction
```

**Rule 1c — SCD resolution:**

```
SCD1: strip TemporalJoin predicate
SCD2: standard effective_from/to
SCD3: PropRef(d.current_value)/PropRef(d.previous_value)
SCD6: lateral join — SCD2 for versioned_attrs; direct for others
```

**Rule 1d — TemporalMode SQL:** see §6.1 template table.

**Rule 2 — Floyd-Warshall for PairExpr:**

```
if pair_count × avg_path_len > node_count²:
    pre-compute Floyd-Warshall distance matrix
else:
    per-pair CTE with LIMIT 1
```

**Rule 3 — GraphExpr scheduling:**

```
for each GraphExpr:
    if target.supports_native(algorithm): emit inline
    else: schedule PreComputation

NodeExpr(OUTGOING_SIGNATURES|DOMINANT_OUTGOING|SIGNATURE_DIVERSITY):
    forward BFS/DFS per anchor alias; pre-compute if stable

NodeExpr(INCOMING_SIGNATURES|DOMINANT_INCOMING):
    BFS on G^T per anchor alias; pre-compute if stable

SubgraphExpr(GLOBAL_SIGNATURE_COUNT|GLOBAL_DOMINANT|SIGNATURE_ENTROPY):
    schedule PreComputation unless |N| < SMALL_GRAPH_THRESHOLD
    broadcast graph-level scalar to all tuples
    SIGNATURE_ENTROPY: compute distribution p(s), then -Σ p(s) log₂ p(s)

SubgraphExpr(scope=RelationshipSubgraph(Bound,Free) or (Free,Bound)):
    compute cone via BFS / G^T BFS first; then run algorithm over cone
```

**Rule 4 — Band reordering:**

```
for each having_pred:
    if bdd.implies(where_bdd, having_bdd): remove having_pred
```

**Rule 5 — Hierarchy execution:**

```
Hierarchy(depth ≤ 4): unroll CTE into fixed-depth join chain
Hierarchy(RAGGED):    recursive CTE with depth limit
|N| > CLOSURE_THRESHOLD: pre-materialised closure table
```

**Rule 6 — Annotation-driven optimisations:**

```
RolePlaying:             single scan under multiple aliases
ProjectsFrom(mini,full): eliminate mini join if full ∈ C
DerivedFact(f,srcs,e):   inline if srcs ∩ C ≠ ∅
WeightConstraint(ALLOC): PathAgg without weight → warn
BridgeSemantics(CAUSAL): drop cycle guard; DAG BFS on G and G^T
BridgeSemantics(SUPERS): CASE WHEN superseded THEN -1*measure ELSE measure
Degenerate(d):           exclude from GroupBy candidates
```

**Rule 7 — TemporalAgg window scheduling:**

```
density > DENSE: pre-aggregate to daily summary
window = ROLLING: recursive window frame
else: date filter + AggFn
```

**Rule 8 — Sink-aware write planning:**

```
DUCKDB/MOTHERDUCK: CREATE TABLE AS / INSERT INTO
POSTGRESQL:        COPY via postgres extension
PARQUET:           COPY TO (FORMAT PARQUET, PARTITION_BY)
DELTA:             CREATE OR REPLACE / INSERT INTO delta_scan
ICEBERG:           INSERT INTO iceberg_scan

if TemporalAgg and sink ∈ {PARQUET,DELTA,ICEBERG}: partition by timestamp unit
if Q_delta and sink = DELTA: APPEND mode
if Grain(ACCUMULATING) and CHANGED_PROPERTY: APPEND mode
```

**Rule 9 — Cone containment optimisation (new):**

```
if TrimJoin(REACHABLE_FROM(A)) and TrimJoin(REACHABLE_TO(B))
both present on the same Join branch:
    replace with TrimJoin(REACHABLE_BETWEEN(A, B))
```

Grounded in the lattice identity G_A* ∩ G_*B = G_AB (§8.7). Always correct; not a heuristic. Fires naturally when both constraints are added incrementally — the planner collapses them to the strictly smaller fixed-endpoint subgraph.

### 6.3  Query exporter

```
QueryTarget ::= SQL_DUCKDB|SQL_POSTGRESQL|SQL_MOTHERDUCK|CYPHER|SPARQL|DGM_JSON|DGM_YAML
SinkTarget  ::= DUCKDB|POSTGRESQL|MOTHERDUCK|PARQUET(URI)|DELTA(URI)|ICEBERG(URI)
```

Expressiveness constraints:

```
Construct                 DuckDB  PG(DD)  Parq  Delta  Ice  MDuck  Cypher   SPARQL
──────────────────────────────────────────────────────────────────────────────────────
VerbHop⁻¹                 CTE     CTE     CTE   CTE    CTE  CTE    MATCH    ✗
BoundPath/PathPred        CTE     CTE     CTE   CTE    CTE  CTE    MATCH    FILTER
TemporalMode UNTIL/SINCE  CTE     CTE     CTE   CTE    CTE  CTE    ✗        ✗
SignaturePred             CTE     CTE     CTE   CTE    CTE  CTE    partial  ✗
TemporalJoin (node)       native  native  nat   nat    nat  native ✗        partial
TemporalAgg               native  native  nat   nat    nat  native ✗        ✗
TrailExpr Bound→Bound     ext/py  ext/py  py    py     py   ext/py native   partial
TrailExpr Bound→Free      py      py      py    py     py   py     partial  ✗
TrailExpr Free→Bound      py      py      py    py     py   py     partial  ✗
TrailExpr Free→Free       py      py      py    py     py   py     ✗        ✗
SIGNATURE_ENTROPY         py      py      py    py     py   py     ✗        ✗
REACHABLE_FROM/TO         CTE     CTE     CTE   CTE    CTE  CTE    MATCH*   ✗
QUALIFY                   native  via DD  nat   nat    nat  native ✗        ✗
Q_delta                   SQL     SQL     SQL   SQL    SQL  SQL    ✗        ✗
```

MATCH*: Cypher variable-length pattern approximates REACHABLE_FROM/TO but does not enforce exact cone boundary.

native=direct; SQL=standard SQL; CTE=recursive CTE; ext/py=extension or Python pre-computation; py=Python only; partial=approximate with warning; ✗=rejected at construction.

SQL emitter flow:

```
1. PreComputation steps → temporary columns in C'
   (incl. G^T BFS for Free→Bound and INCOMING_* algorithms)
2. WITH clauses: recursive CTEs (cone CTEs for REACHABLE_FROM/TO)
3. Annotation-aware transforms: SCD1 strip, CAUSAL no-cycle, SUPERSESSION negation
4. Main SELECT: B₁ JOIN, B₂ GROUP BY/AGG, B₃ WINDOW
5. QUALIFY (or subquery for PostgreSQL direct)
6. HAVING
7. WritePlan emission
```

Cypher: REACHABLE_FROM(A) → MATCH (a)-[*]->(n). INCOMING_SIGNATURES → apoc.path.incomingRelationshipTypes. Full cone enforcement requires Python pre-computation.

DGM_JSON/YAML: round-trippable AST including endpoint cases, SIGNATURE_ENTROPY scope, cone containment applied flag, annotation optimisations.

Sink write semantics:

```
DUCKDB/MOTHERDUCK:  CREATE TABLE AS / INSERT INTO
POSTGRESQL:         ATTACH ... AS pg; CREATE/INSERT
PARQUET:            COPY TO (FORMAT PARQUET, PARTITION_BY)
DELTA:              CREATE OR REPLACE / INSERT INTO delta_scan
ICEBERG:            INSERT INTO iceberg_scan (preferred)
```

Delta+Q_delta: APPEND. Parquet+TemporalAgg: PARTITION_BY timestamp.

---

## 7  Recommender Architecture

### 7.1  Three-layer architecture

```
Layer 1 — Schema atom generation     (bounded)
       ↓
Layer 2 — BDD feasibility filter      (O(n × BDD_size))
       ↓
Layer 3 — Data scoring                (small surviving set)
       ↓
Ranked suggestions
```

### 7.2  Two-stage trail exploration flow

The endpoint relaxation extension gives the recommender a natural two-stage exploration protocol:

**Stage 1 — Free-endpoint characterisation.** Before fixing a target, surface OUTGOING_SIGNATURES(A) or INCOMING_SIGNATURES(B): "Your anchor node has N distinct outgoing path types. Dominant type: X (Y% of paths). SIGNATURE_DIVERSITY = Z."

High entropy / high diversity → recommend Stage 1 characterisation before drilling down.
Low entropy / low diversity → recommend direct Bound→Bound query.

**Stage 2 — Fixed-endpoint deep-dive.** Once a path type is identified via SignaturePred, narrow to a specific Bound→Bound pair using DISTINCT_SIGNATURES and DOMINANT_SIGNATURE. "REACHABLE_BETWEEN subgraph: DENSITY=D, MAX_FLOW=F."

### 7.3  Suggestion types per band

**B₁:** ScalarPred; PathPred including constellation paths; TrimJoin (incl. REACHABLE_FROM/TO); TemporalCompose; strategy lattice; temporal property classes; trail space pivots.

**B₂:** GroupBy alternatives (hierarchy roll-ups, cross-role); TemporalAgg variants; PathAgg over unused bridges; Having calibration.

**B₃:** Community partition; K for K_SHORTEST; percentile thresholds; TARJAN_SCC partition; RelationshipSubgraph characterisation.

**Temporal pivots:** rolling window; period-over-period; Q_delta; temporal property class alternatives.

**Trail pivots:** "Other DISTINCT_SIGNATURES connecting these nodes." "Dominant path is X — restrict via SignaturePred." "Before fixing target: OUTGOING_SIGNATURES inventory from anchor."

### 7.4  Annotation-driven suggestion rules

| Annotation | Suppressed | Added |
|---|---|---|
| Degenerate(d) | GroupBy; PathPred from d | ScalarPred on d.key |
| Conformed(d,Fs) | — | Constellation paths; Bound→Free from d to confirm/extend Fs |
| Grain(PERIOD) | SUM across periods | LAST; Q_delta |
| Grain(ACCUMULATING) | Row aggregation | Stage predicates; time-between-stages |
| SCDType(SCD1) | Temporal pivots | — |
| SCDType(SCD3) | — | PropRef(d.previous_value) comparison |
| FactlessFact | SUM/AVG/MIN/MAX | COUNT; EXISTS |
| DerivedFact | Direct measures | Drill-down to sources |
| WeightConstraint(ALLOC) | Unweighted PathAgg | Weighted form |
| BridgeSemantics(CAUSAL) | — | BETWEENNESS on G_AB; TARJAN_SCC; Free→Bound provenance |
| BridgeSemantics(SUPERS) | Plain aggregation | Negation-aware aggregation |
| Hierarchy(root,depth) | — | Drill-down/roll-up |
| RolePlaying(d,roles) | — | Cross-role comparisons |

### 7.5  TrailExpr-driven suggestion rules

| Condition | Suppressed | Added |
|---|---|---|
| OUTGOING_SIGNATURES(d) high | — | SignaturePred to isolate dominant; TrimJoin(REACHABLE_FROM(d)) |
| INCOMING_SIGNATURES(f) high | — | TrimJoin(REACHABLE_TO(f)); BridgeSemantics(CAUSAL) if absent |
| SIGNATURE_DIVERSITY low on d | — | Bound→Bound (focused; Free adds noise) |
| SIGNATURE_DIVERSITY high on d | Bound→Bound suggestions | DOMINANT_OUTGOING_SIGNATURE first |
| CAUSAL + Free→Bound | — | BETWEENNESS on G_*B; ancestor path queries |
| Conformed(dim,Fs) | — | Bound→Free from dim to surface all reachable fact types |
| SIGNATURE_ENTROPY low | — | Restrict scope to dominant signature |
| SIGNATURE_ENTROPY high | — | GLOBAL_DOMINANT_SIGNATURE as first characterisation |

### 7.6  Graph algorithm signals

PAGE_RANK → GroupBy/Where on high-importance nodes.
COMMUNITY_LABEL → community-aligned GroupBy.
TARJAN_SCC + CAUSAL → event cycle detection.
BETWEENNESS on G_AB → pivotal intermediates.
OUTGOING_SIGNATURES high → two-stage exploration.
SIGNATURE_ENTROPY high → GLOBAL_DOMINANT_SIGNATURE first.

---

## 8  Theoretical Foundations

### 8.1  DGM as Kripke structure

**Theorem.** G = (N,E,τ_N,τ_E,P,Σ) is a Kripke structure K=(S,R,L) under S=N, R=E, L=P.

Consequences: CTL/LTL model checking applies directly. BDD layer is standard symbolic CTL machinery (Bryant 1986; Clarke et al. 1986). PathPred(FORALL/EXISTS,ALL) are CTL path quantifiers A/E. TemporalMode realises full CTL operator hierarchy.

G^T is the time-reversal of K. Backward-cone RelationshipSubgraph(Free,Bound(B)) is the computation tree for past-temporal formulas about "all paths leading to B." SINCE/ONCE/PREVIOUSLY TemporalMode execute as model checking on G^T.

### 8.2  Functional completeness

**Theorem.** DGM filter tree language over ScalarPred atoms is functionally complete.

Proof sketch: AND/OR/NOT/ScalarPred generates all DNF formulas over finite atom set A. PathPred and SignaturePred extend without restricting. ∎

### 8.3  Minterms, maxterms, canonical form

Satisfiable minterms bounded by |C|. Equivalence: same minterm set. Unsatisfiability: empty. Tautology: full. Implication: subset.

### 8.4  PathPred as exponential compression

PathPred(EXISTS,ALL,φ) over k instances: k-disjunct DNF compressed. FORALL: dual k-conjunct CNF via NOT EXISTS NOT. TemporalMode extends to continuous path properties.

### 8.5  EXISTS/FORALL as minterm/maxterm duality

EXISTS = disjunction/minterms. FORALL = conjunction/maxterms. SHORTEST/MIN_WEIGHT collapse both to single term.

### 8.6  Finite traversal space

total ≤ Σₖ |N|! / (|N|-k)! (finite). BridgeSemantics(CAUSAL): DAG — eliminates cycles.

### 8.7  RelationshipSubgraph containment lattice

The four endpoint cases form a containment lattice:

```
G_AB ⊆ G_A*  ⊆  G_**
G_AB ⊆ G_*B  ⊆  G_**
```

**Cone intersection theorem.** G_A* ∩ G_*B = G_AB.

Proof: n ∈ G_A* iff A →* n. n ∈ G_*B iff n →* B. Their intersection is {n | A →* n →* B} = N_AB. ∎

Rule 9 exploits this: replacing two independent cone computations with one fixed-endpoint computation is provably correct and produces a strictly smaller (or equal) subgraph. The optimisation is always safe to apply.

### 8.8  Allen's interval algebra completeness

Allen's 13 relations are mutually exclusive and jointly exhaustive over non-degenerate time intervals. Complete coverage of all interval-to-interval temporal relationships.

### 8.9  Temporal property class hierarchy

```
SAFETY, LIVENESS ∈ Π₁/Σ₁.  RESPONSE ∈ Π₂.  PERSISTENCE ∈ Σ₂.  RECURRENCE ∈ GF.
```

### 8.10  SIGNATURE_ENTROPY as information-theoretic measure

H = -Σ p(s) log₂ p(s). H=0: maximally focused. H=log₂(k): k equally frequent types.

Relationship to recommender routing: high H → Stage 1 characterisation (Free endpoint) before fixing target. Low H → proceed directly to Bound→Bound deep-dive. SIGNATURE_ENTROPY is the formal routing signal for the two-stage exploration flow.

---

## 9  Implementation Guide

### 9.1  Alignment with sqldim

| DGM Concept | sqldim | Status |
|---|---|---|
| Dimension nodes (D) | DimensionModel | ✅ Aligned |
| Fact nodes (F) | FactModel → binary edge | ⚠️ Partial |
| Verb edges + VerbHop⁻¹ | FK; no reverse | ⚠️ Partial |
| Bridge edges B ⊆ N×N×L | dim→dim only | ⚠️ Partial |
| Schema annotation layer Σ | Not present | ❌ Missing |
| TemporalJoin (node+edge) | .as_of() node only | ⚠️ Partial |
| TemporalContext | Not implemented | ❌ Missing |
| TrimJoin (incl. REACHABLE_FROM/TO) | Not implemented | ❌ Missing |
| PathPred + TemporalMode | Not implemented | ❌ Missing |
| SignaturePred / SignatureRef | Not implemented | ❌ Missing |
| CTL temporal property classes | Not implemented | ❌ Missing |
| Allen interval ordering | Not implemented | ❌ Missing |
| RelationshipSubgraph (all 4 cases) | Not named | ⚠️ Case 1 only via REACHABLE_BETWEEN |
| G^T transposed graph | Not present | ❌ Missing |
| TrailExpr (all forms) | Not implemented | ❌ Missing |
| GroupBy | .by(*attributes) | ✅ Aligned |
| PathAgg | aggregate_sql(weighted=True) | ⚠️ Partial |
| TemporalAgg | Not implemented | ❌ Missing |
| Having / Qualify | Not public | ❌ / ⚠️ |
| GraphExpr | Not implemented | ❌ Missing |
| Q_delta | Schema diff only | ⚠️ Partial |
| BDD predicate layer | Not implemented | ❌ Missing |
| Query planner | Not implemented | ❌ Missing |
| Query exporter (multi-target) | Implicit SQL only | ⚠️ Partial |
| Sink export (Parquet/Delta/Iceberg) | Sinks exist; not planner-integrated | ⚠️ Partial |
| Recommender | Not implemented | ❌ Missing |

### 9.2  Implementation roadmap

**Phase 1 — Schema layer (2–3 days)**
- Extend GraphSchema to B ⊆ N×N×L; τ_E; P(e).valid_from/to; fact-as-node.
- Build G^T adjacency list at load time.

**Phase 2 — Query DSL (1 week)**
- PathJoin / VerbHop / VerbHop⁻¹ / BridgeHop.
- TemporalJoin, TemporalContext, TrimJoin (incl. REACHABLE_FROM, REACHABLE_TO).
- Full TemporalOrdering (point + Allen + UNTIL/SINCE).
- PathPred with Quantifier, Strategy, TemporalMode, promote.
- SignaturePred; SignatureRef; PathAgg; TemporalAgg; Qualify public.

**Phase 3a — Filter tree typing (3–4 days)**
- ScalarPred / PathPred / SignaturePred with Ref kind enforcement.
- FORALL → NOT EXISTS NOT; TemporalMode SQL templates.

**Phase 3b — BDD layer (~1.5 weeks)**
- Core BDD; DGMPredicateBDD compiler (TemporalMode, SignaturePred atoms).
- Equivalence, satisfiability, implication; minterm enumeration.
- Logical optimisation pipeline (8 steps).

**Phase 4 — Schema annotation layer (1 week)**
- SchemaAnnotation grammar; Σ in GraphSchema.
- Annotation consequences wired into planner, recommender, exporter.

**Phase 5 — Graph algorithms (ongoing)**
- NodeExpr: PageRank, centrality, SCC, community, OUTGOING/INCOMING_SIGNATURES, SIGNATURE_DIVERSITY.
- PairExpr: shortest/min-weight, reachability, DISTINCT_SIGNATURES, DOMINANT_SIGNATURE, SIGNATURE_SIMILARITY.
- SubgraphExpr: max-flow, density, diameter, GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE, SIGNATURE_ENTROPY.
- RelationshipSubgraph all four endpoint cases; G^T BFS for Case 3; cone containment (Rule 9).

**Phase 6 — Recommender (ongoing)**
- Three-layer recommender; annotation-driven (§7.4) and TrailExpr-driven (§7.5) rules.
- Two-stage trail exploration flow; SIGNATURE_ENTROPY as routing signal.
- Temporal property class suggestions; Q_delta full implementation.

**Phase 7 — Planner and exporter (~2 weeks)**
- GraphStatistics with transposed_adj, scc_sizes, temporal_density.
- Planning rules 1–9 (Rules 1a cone extension, Rule 3 TrailExpr, Rule 9 cone containment).
- SQL emitter with G^T BFS for Free→Bound; cone containment optimisation.
- Sink integration; Cypher, SPARQL, DGM_JSON/YAML exporters.

---

## 10  Reference

### 10.1  Worked examples

**Example 1 — Constellation path**

```
-- Σ: Conformed(customer, {Sale, Return})
Join: Anchor(fact, sale)
      PathJoin(sale, Compose(VerbHop⁻¹(sale,'placed',c,node_alias=customer),
                             VerbHop(c,'initiated',ret,node_alias=return_ev)), ret)
Where: AND(ScalarPred(PropRef(sale.occurred_on.year),(= 2024)),
           ScalarPred(PropRef(return_ev.occurred_on),(> PropRef(sale.occurred_on))))
GroupBy: [ customer.region ]
Agg: return_rate := COUNT(return_ev) / COUNT(sale)
```

**Example 2 — Forward cone: outgoing signature inventory (Stage 1)**

```
Join: Anchor(dim, c)
Where: ScalarPred(PropRef(c.lifetime_value), (> 10000))
GroupBy: [ c.id ]
Agg:
  out_sigs  := GraphExpr(NodeExpr(OUTGOING_SIGNATURES(max_depth=4), c))
  dom_sig   := GraphExpr(NodeExpr(DOMINANT_OUTGOING_SIGNATURE(max_depth=4), c))
  diversity := GraphExpr(NodeExpr(SIGNATURE_DIVERSITY, c))
-- Stage 1: characterise space; use dom_sig to construct SignaturePred for Stage 2
```

**Example 3 — Backward cone: provenance of a high-value sale**

```
Join: TrimJoin(Anchor(fact, s), REACHABLE_TO(s))
Where: ScalarPred(PropRef(s.revenue), (> 50000))
GroupBy: [ τ_N(n) ]
Agg:
  path_count := COUNT(n)
  in_sigs    := GraphExpr(NodeExpr(INCOMING_SIGNATURES(max_depth=3), s))
```

**Example 4 — Cone containment (Rule 9 fires automatically)**

```
Join:
  TrimJoin(
    TrimJoin(Anchor(dim,c) PathJoin(c,VerbHop(c,'placed',s),s),
             REACHABLE_FROM(c)),   -- forward cone of c
    REACHABLE_TO(s)                -- backward cone of s
  )
-- Planner Rule 9: REACHABLE_FROM(c) ∩ REACHABLE_TO(s) → REACHABLE_BETWEEN(c,s)
-- Single cone CTE emitted; strictly smaller subgraph
```

**Example 5 — Global signature entropy**

```
Join: Anchor(dim,c) PathJoin(c,VerbHop(c,'placed',s),s)
GroupBy: [ c.region ]
Agg:
  entropy := GraphExpr(SubgraphExpr(SIGNATURE_ENTROPY(max_depth=3),
               scope=RelationshipSubgraph(Free, Free)))
-- High entropy → diverse region; route to Stage 1 exploration
-- Low entropy  → focused region; Bound→Bound queries are efficient
```

**Example 6 — CTL safety + UNTIL**

```
Join: Anchor(dim,c) PathJoin(c,VerbHop(c,'placed',s),s)
Where:
  AND(
    PathPred(c, VerbHop(c,'placed',s2,node_alias=sale),
             FORALL, ALL, GLOBALLY,
             ScalarPred(PropRef(sale.revenue),(>= 0))),       -- AG safety
    PathPred(c, Compose(VerbHop(c,'placed',s3,node_alias=sc),
               temporal=UNTIL(hold_pred=ScalarPred(PropRef(sc.revenue),(> 0)),
                              trigger_pred=ScalarPred(PropRef(sc.churn_flag),(= true)))),
             EXISTS, ALL,
             ScalarPred(PropRef(sc.churn_flag),(= true)))     -- UNTIL
  )
```

**Example 7 — Full pipeline**

```
-- Σ: Conformed(customer,{Sale,Return}), Grain(sale,EVENT),
--    SCDType(customer,SCD2), BridgeSemantics(promo,CAUSAL), WeightConstraint(promo,ALLOCATIVE)

TemporalContext(default_as_of='2024-06-30', node_resolution=STRICT, edge_resolution=LAX)

Join:
  TrimJoin(
    Anchor(dim,c) PathJoin(c,VerbHop(c,'placed',s),s)
    PathJoin(s,VerbHop(d,'included in',s),d)
    PathJoin(d,BridgeHop(d,'promoted in',st,edge_alias=promo),st),
    SINK_FREE
  )
Where:
  AND(ScalarPred(PropRef(c.segment),(= "retail")),
      ScalarPred(PropRef(s.occurred_on.year),(= 2024)),
      PathPred(c,VerbHop(c,'placed',s2,node_alias=fs),
               FORALL,ALL,EVENTUALLY, ScalarPred(PropRef(fs.revenue),(> 500))))
GroupBy: [ c.id, c.region ]
Agg:
  weighted_rev  := PathAgg(d,BridgeHop(d,'promoted in',st,edge_alias=p2),
                           ALL,SUM,ArithExpr(PropRef(s.revenue),*,PropRef(p2.weight)))
  rev_30d       := TemporalAgg(SUM,s.revenue,s.occurred_on,ROLLING(30 days))
  out_diversity := GraphExpr(NodeExpr(SIGNATURE_DIVERSITY, c))
Having:
  AND(ScalarPred(AggRef(weighted_rev),(> 5000)),
      ScalarPred(AggRef(rev_30d),(> 1000)))
Window:
  scc_label   := GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC),s))
  rank_by_rev := RANK() OVER (PARTITION BY c.id ORDER BY s.revenue DESC)
Qualify:
  AND(ScalarPred(WinRef(scc_label),IS_NOT_NULL),
      ScalarPred(WinRef(rank_by_rev),(<= 2)))
```

### 10.2  Summary tables

**RelationshipSubgraph endpoint cases:**

| Source | Target | Subgraph | Tier | TrimCriterion | Primary algorithms |
|---|---|---|---|---|---|
| Bound(A) | Bound(B) | Paths A→B | PairExpr | REACHABLE_BETWEEN | DISTINCT_SIGNATURES, DOMINANT_SIGNATURE, DENSITY, DIAMETER, MAX_FLOW |
| Bound(A) | Free | Forward cone of A | NodeExpr | REACHABLE_FROM | OUTGOING_SIGNATURES, DOMINANT_OUTGOING, SIGNATURE_DIVERSITY |
| Free | Bound(B) | Backward cone of B | NodeExpr | REACHABLE_TO | INCOMING_SIGNATURES, DOMINANT_INCOMING, SIGNATURE_DIVERSITY |
| Free | Free | Full traversal space | SubgraphExpr | — | GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT, SIGNATURE_ENTROPY |

**Schema annotations (Σ):**

| Annotation | Annotates | Key consequence |
|---|---|---|
| Conformed | dim | Constellation paths; Bound→Free discovery |
| Grain | fact | Aggregation validity; TemporalAgg semantics |
| SCDType | dim | Temporal resolution strategy |
| Degenerate | dim | Excluded from GroupBy |
| RolePlaying | dim | Shared scan; cross-role suggestions |
| ProjectsFrom | dim | Join elimination |
| FactlessFact | fact | COUNT only |
| DerivedFact | fact | Inline computation |
| WeightConstraint | bridge | Allocation warning |
| BridgeSemantics | bridge | CTE optimisation; negation; DAG BFS on G and G^T |
| Hierarchy | dim | CTE vs unrolled join |

**CTL operator map:**

| Mode | EXISTS | FORALL |
|---|---|---|
| EVENTUALLY | EF φ | AF φ |
| GLOBALLY | EG φ | AG φ |
| NEXT | EX φ | AX φ |
| UNTIL(ψ) | E(φ U ψ) | A(φ U ψ) |

**Query forms:** Q = TemporalContext? ∘ B₁ ∘ B₂? ∘ B₃? | Q_delta = (t₁,t₂,DeltaSpec,filter?)

**Strategy lattice:** SHORTEST ⊆ K_SHORTEST(1) ⊆ K_SHORTEST(k) ⊆ ALL; MIN_WEIGHT ⊆ ALL

**Export targets:** SQL_DUCKDB|SQL_POSTGRESQL|SQL_MOTHERDUCK|CYPHER|SPARQL|DGM_JSON|DGM_YAML; DUCKDB|POSTGRESQL|MOTHERDUCK|PARQUET|DELTA|ICEBERG

### 10.3  Related work

**Graph OLAP.** Chen et al. (2008, 2009) [1,2] — DGM adds Σ, CTL, Allen, relaxed-endpoint trail semantics, planner/exporter.

**Graphoids.** Gómez et al. (2017, 2019) [3,4] — DGM adds Σ, VerbHop⁻¹, full endpoint lattice, CTL.

**Multi-dimensional event data.** Esser and Fahland (2021) [5] — DGM extends with constellation paths, SCD, UNTIL/SINCE, Allen, endpoint relaxation.

**KG-OLAP.** Schuetz et al. (2021) [6] — complementary; DGM adds temporal logic and trail space analysis.

**BDD.** Bryant (1986) [7] — DGM applies to graph dimensional predicates; Kripke equivalence grounds it.

**Model checking.** Clarke et al. (1986) [8] — PathPred×TemporalMode realises full CTL; G^T enables past-temporal checking. **Interval algebra.** Allen (1983) [9] — 13 relations in TemporalOrdering.

Novel contributions added in v0.16: Endpoint type with Bound/Free variants; four-case RelationshipSubgraph endpoint lattice; forward cone via forward BFS; backward cone via BFS on G^T; full traversal space as SubgraphExpr; containment lattice theorem and cone intersection identity; REACHABLE_FROM/REACHABLE_TO as TrimCriterion; TrailExpr single-relaxation algorithms as NodeAlg; SIGNATURE_DIVERSITY; SIGNATURE_ENTROPY as Shannon entropy; global SubgraphAlg variants; granularity tier shift by endpoint case; Rule 9 cone containment optimisation; G^T at GraphStatistics load time; two-stage trail exploration flow; SIGNATURE_ENTROPY as recommender routing signal.

### 10.4  Design decisions

**D1** — G as 6-tuple with Σ (operational decision table, not metadata).
**D2** — VerbHop⁻¹ as query-time operation; V ⊆ D×F×L unchanged.
**D3** — TemporalMode realises CTL; Kripke equivalence gives 70-year formal foundation.
**D4** — L-promotion: SHORTEST/MIN_WEIGHT only; cardinality 1 guarantee.
**D5** — No implicit weighting; WeightConstraint(ALLOC) → warning not error.
**D6** — Fact nodes: bridge endpoints permitted; verb sources prohibited.
**D7** — Allen's relations: PERIOD/ACCUMULATING grains only; point events use BEFORE/AFTER.
**D8** — RelationshipSubgraph first-class; reveals implicit schema of node-pair relationships.
**D9** — Temporal property classes as syntactic sugar over PathPred compositions.
**D10** — Annotation consequence table as integration contract.
**D11** — Q_delta outside three-band structure.
**D12** — BDD = logical layer; planner = physical layer; DuckDB = cost-based physical.
**D13** — Three-band structure: B₁=what, B₂=how to collapse, B₃=which tuples.
**D14** — Floyd-Warshall as planner rule; threshold |pairs|×avg_path_len > |N|².
**D15** — ExportPlan.alternatives: mechanical, auditable plan explanation.
**D16** — Endpoint relaxation shifts granularity tier (Bound→Free = NodeExpr; Free→Free = SubgraphExpr). Preserves placement rule without modification.
**D17** — Free→Free scope restricted to Agg/Window. Graph-scope computation in Where PathPred scope is semantically undefined and computationally unscalable.
**D18** — G^T built at load time O(|E|), amortised across all queries needing backward traversal (SINCE, ONCE, PREVIOUSLY, REACHABLE_TO, INCOMING_SIGNATURES).
**D19** — Cone containment (Rule 9) as structural theorem, not heuristic. Grounded in G_A* ∩ G_*B = G_AB. Always correct; fires whenever both constraints present on same Join branch.

### 10.5  References

[1] Chen, C., et al. (2008). Graph OLAP: Towards Online Analytical Processing on Graphs. *ICDM 2008*. https://doi.org/10.1109/ICDM.2008.45

[2] Chen, C., et al. (2009). Graph OLAP: A Multi-Dimensional Framework. *Knowledge and Information Systems*, 21(1). https://doi.org/10.1007/s10115-009-0228-8

[3] Gómez, L. I., et al. (2017). Performing OLAP over Graph Data. *BIRTE 2017*.

[4] Gómez, L. I., et al. (2019). Online Analytical Processing on Graph Data. *Intelligent Data Analysis*, 24(3). https://doi.org/10.3233/IDA-194576

[5] Esser, S., Fahland, D. (2021). Multi-Dimensional Event Data in Graph Databases. *Journal on Data Semantics*, 10(1–2). https://doi.org/10.1007/s13740-021-00122-1

[6] Schuetz, C. G., et al. (2021). Knowledge Graph OLAP. *Semantic Web*, 12(4). https://doi.org/10.3233/SW-200419

[7] Bryant, R. E. (1986). Graph-Based Algorithms for Boolean Function Manipulation. *IEEE Transactions on Computers*, C-35(8). https://doi.org/10.1109/TC.1986.1676819

[8] Clarke, E. M., Emerson, E. A., Sistla, A. P. (1986). Automatic Verification of Finite-State Concurrent Systems. *ACM TOPLAS*, 8(2). https://doi.org/10.1145/5397.5399

[9] Allen, J. F. (1983). Maintaining Knowledge About Temporal Intervals. *CACM*, 26(11). https://doi.org/10.1145/182.358434

---

*— end of specification —*