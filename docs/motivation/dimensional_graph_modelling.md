# Dimensional Graph Model — Formal Specification
**Version 0.22 — Draft**

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
11. [Natural Language Interface](#11-natural-language-interface)
    - 11.1 Architecture overview
    - 11.2 LLM responsibilities
    - 11.3 Semantic grounding layer
    - 11.4 Temporal and compositional intent mapping
    - 11.5 Confirmation and refinement loop
    - 11.6 Ambiguity resolution
    - 11.7 Schema evolution handling
    - 11.8 Hallucination prevention
    - 11.9 Integration with execution budget gate
    - 11.10 Implementation: LangGraph + Pydantic-AI

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
- **ExecutionBudget and pre-execution gate** — a runtime policy layer between the planner and execution that intercepts expensive queries and redirects gracefully via cost-aware rewriting, progressive streaming, sampling with Hoeffding error bounds, or async execution with PipelineArtifact materialisation.
- **Natural language interface** — a five-layer architecture mapping NL utterances to formally bounded Q_valid(A,B) positions. The LLM navigates the canonical question lattice rather than generating SQL from scratch.
- **LangGraph + Pydantic-AI implementation stack** — five specialist Pydantic-AI agents with output schemas derived from DGM formal types, orchestrated by a LangGraph state graph with explicit HIL checkpoints and cyclic refinement workflows. DGM's bounded vocabulary is enforced as Pydantic validators.

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
  -- Pipeline annotations
  | PipelineArtifact(f: F,
                     pipeline_id: K,
                     ttl: Duration,
                     backfill_horizon: Duration,
                     write_mode: BACKFILL | REFRESH | ADAPTIVE)
```

Key annotation semantics:

**Conformed(d, fact_types)** — d reachable via verb edges from each fact type. Enables constellation paths. Bound→Free TrailExpr can discover candidates empirically.

**Grain(f, grain)** — EVENT: additive; PERIOD: state (SUM invalid); ACCUMULATING: lifecycle row.

**SCDType** — SCD1: strip temporal resolution. SCD2: effective_from/to. SCD3: current/previous props. SCD6: per-attribute policy.

**BridgeSemantics(CAUSAL)** — DAG; drop cycle guard; use DAG BFS on both G and G^T.

**PipelineArtifact(f, pipeline_id, ttl, backfill_horizon, write_mode)** — composite annotation. Implies `Grain(f, ACCUMULATING)`. Models five-state machine over `P(f).state ∈ {Missing, In-flight, Complete, Stale, Failed}`. Transition bridge edges carry implied BridgeSemantics:
```
Missing    →[scheduled]→   In-flight   TEMPORAL
In-flight  →[succeeded]→   Complete    CAUSAL
In-flight  →[failed]→      Failed      CAUSAL  (SUPERSESSION if REFRESH run with prior Complete)
Complete   →[expired]→     Stale       TEMPORAL  (ttl-triggered)
Stale      →[rescheduled]→ In-flight   TEMPORAL
Failed     →[retried]→     In-flight   SUPERSESSION
```
`write_mode`: BACKFILL → always APPEND; REFRESH → always MERGE (atomic swap); ADAPTIVE → inferred from state (planner Rule 10).

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

**Rule 10 — PipelineArtifact state-aware write planning:**

```
if PipelineArtifact(f, _, ttl, horizon, write_mode) ∈ Σ:

    -- TTL expiry: transition Complete → Stale
    if P(f).state = Complete AND P(f).completed_at < now - ttl:
        emit state transition: Complete → Stale
        WritePlan(mode=MERGE, sink=current_sink)

    -- ADAPTIVE write mode inference from P(f).state:
    if write_mode = ADAPTIVE:
        if P(f).state ∈ {Missing, Failed}: WritePlan(mode=APPEND)
        elif P(f).state = Stale:           WritePlan(mode=MERGE)

    -- Backfill gap predicate (injected into Where by planner):
        AND(
          ScalarPred(PropRef(f.state), IN {Missing, Failed}),
          ScalarPred(PropRef(f.window_end), (< ArithExpr(now, -, horizon)))
        )

    -- Transition bridge semantics inference:
    if transition = In-flight → Failed:
        if write_mode IN {REFRESH, ADAPTIVE with prior Complete}:
            BridgeSemantics(SUPERSESSION)   -- prior Complete preserved; regress to Stale
        else:
            BridgeSemantics(CAUSAL)         -- backfill; no prior artifact; window stays Failed
```

**Rule 11 — Cross-CTE Common Subexpression Elimination (CSE):**

When a composed query contains multiple CTEs (question chains, correlation queries), the planner identifies shared sub-computations and materialises each once:

```
for each pair of CTEs (q_i, q_j) in a composed query:
    shared = identify_shared_subexpressions(q_i, q_j)
    for each s in shared:
        -- BDD node id equality: same id = semantically equivalent
        if bdd_id(s in q_i) = bdd_id(s in q_j):
            materialise s as a new CTE q_s, hoisted before q_i and q_j
            replace s references in q_i, q_j with q_s

-- Shared subexpression types eligible for hoisting:
--   PathPred with identical BoundPath + TemporalMode
--   TemporalAgg with identical window + timestamp ref
--   GraphExpr with identical algorithm + alias
--   Join subgraph over identical node set + filters
```

Cross-query BDD ids are comparable because the BDD is constructed once per query session over the same atom set (§6.1). Two sub-predicates with the same BDD node id in different CTEs are semantically equivalent and share a single materialised CTE. This extends the existing within-query CSE (optimisation pipeline step 2) to cross-query scope.

**Rule 11 extended — query DAG minimisation (new in v0.20).** Beyond shared subexpression hoisting, Rule 11 now applies the full semiring elimination laws before emitting CTEs:

```
Semiring elimination rules applied during query DAG make():

UNION laws:
  Q ∪ Q            = Q           (idempotence; same DAG node id)
  Q ∪ ∅            = Q           (identity; remove empty branch)
  Q ∪ Q_top        = Q_top       (absorption; top dominates)
  Q₁ ∪ Q₂ where Q₁⊆Q₂ = Q₂    (containment absorption; Band 1 only)

INTERSECT laws:
  Q ∩ Q            = Q           (idempotence)
  Q ∩ ∅            = ∅           (annihilation)
  Q ∩ Q_top        = Q           (identity)
  Q₁ ∩ Q₂ where Q₁⊆Q₂ = Q₁    (containment selection; Band 1 only)

Distributivity (applied when it reduces node count):
  Q₁ ∩ (Q₂ ∪ Q₃)  = (Q₁ ∩ Q₂) ∪ (Q₁ ∩ Q₃)

Containment check:
  Q₁ ⊆ Q₂  iff  bdd.implies(Q₁.where_bdd, Q₂.where_bdd)  [Band 1]
            NP-complete (homomorphism test) but tractable for small queries
            Not applied for Band 2/3 (undecidable in general)
```

After elimination, the canonical query DAG is topologically sorted. Each node becomes exactly one CTE in the SQL output. The minimum CTE count equals the number of nodes in the canonical DAG — a tight lower bound: no correct query with fewer CTEs exists.

### 6.3  Execution budget and pre-execution gate

The pre-execution gate sits between the planner and execution. It intercepts every `ExportPlan` before it reaches the SQL emitter, compares the cost estimate against a declared `ExecutionBudget`, classifies the failure mode if any, and applies one of four graceful handling strategies.

**ExecutionBudget type:**

```
ExecutionBudget = (
  max_estimated_cost:   CostEstimate,  -- planner cost unit ceiling
  max_result_rows:      ℕ,             -- row count ceiling
  max_wall_time:        Duration,       -- synchronous execution timeout
  max_precompute_time:  Duration,       -- per PreComputation timeout
  streaming_threshold:  CostEstimate,  -- above → stream results progressively
  async_threshold:      CostEstimate   -- above → async with callback + materialisation
)

BudgetDecision ::= PROCEED                      -- within budget; execute synchronously
                 | STREAM                        -- above streaming_threshold; progressive delivery
                 | ASYNC(sink: SinkTarget,
                         callback: Endpoint,
                         ttl: Duration)          -- above async_threshold; execute async
                 | REWRITE(plan: ExportPlan,
                           warning: string)      -- cost-reduced rewrite + transparency note
                 | SAMPLE(fraction: ℝ,
                          method: RANDOM | STRATIFIED | RESERVOIR,
                          error_bound: ℝ)        -- approximate result with confidence interval
                 | PAGINATE(page_size: ℕ)        -- rewrite to paginate result set
                 | CLARIFY(options: list[ExportPlan],
                           cost_per_option: list[CostEstimate])
                                                 -- ask user to narrow; surface lattice refinements
                 | REJECT(reason: string)         -- hard ceiling exceeded; refuse
```

**Pre-execution decision procedure:**

```
gate(plan: ExportPlan, budget: ExecutionBudget) → BudgetDecision:

  est = plan.cost_estimate

  -- Hard ceiling
  if est > budget.max_estimated_cost * HARD_CEILING_FACTOR:
    return REJECT("Query exceeds hard cost ceiling. " + narrowing_hint(plan))

  -- Async threshold
  if est > budget.async_threshold:
    return ASYNC(sink=default_sink, callback=user_endpoint, ttl=24h)

  -- Streaming threshold
  if est > budget.streaming_threshold:
    return STREAM

  -- Cost-aware rewrite candidates (preserves analytical intent):
  rewrites = [
    rewrite_strategy(plan, SHORTEST)    if has_ALL_PathPred(plan),
    rewrite_depth_limit(plan, depth=3)  if has_unbounded_TrailExpr(plan),
    rewrite_cone(plan, BOUND_FREE)      if has_FREE_FREE_scope(plan),
  ]
  viable = [r for r in rewrites if r.cost_estimate ≤ budget.streaming_threshold]
  if viable:
    return REWRITE(best(viable), warning=rewrite_explanation(viable[0]))

  -- Result size guard
  if estimated_rows(plan) > budget.max_result_rows:
    return PAGINATE(page_size=default_page_size)

  -- PreComputation timeout guard
  for pc in plan.pre_compute:
    if estimated_time(pc) > budget.max_precompute_time:
      -- substitute approximation algorithm
      plan = substitute_approximation(plan, pc)

  return PROCEED
```

**Four graceful handling strategies:**

**Strategy 1 — Cost-aware rewriting.** Moves the query down the strategy lattice while preserving analytical intent. Rewrites are transparent — the user sees a note explaining what was approximated.

```
Rewrite rules:
  PathPred(FORALL/EXISTS, ALL, φ)  →  PathPred(..., SHORTEST, φ)
    warning: "Checking only shortest paths. Use ALL strategy for exhaustive check."

  DISTINCT_SIGNATURES(max_depth=∞) →  DISTINCT_SIGNATURES(max_depth=3)
    warning: "Path depth limited to 3."

  SubgraphExpr(scope=Free→Free)    →  SubgraphExpr(scope=Bound(A)→Free)
    warning: "Scoped to forward cone of anchor node."

  GraphExpr(PAGE_RANK, exact=true) →  GraphExpr(PAGE_RANK, iterations=10)
    warning: "Using 10-iteration approximation (convergence threshold relaxed)."
```

**Strategy 2 — Progressive streaming.** The query DAG topological order is the streaming schedule — independent leaves execute in parallel and deliver results as they complete. For recursive CTE traversals, intermediate depths are delivered incrementally.

```
Streaming schedule from DAG topology:
  for node in topological_sort(query_dag):
    if node.is_leaf: execute_async(node); stream result when ready
    if node.is_join:  wait for both children; stream joined result
    if node.is_union: stream each child result as it arrives

Per-depth streaming for PathPred(ALL):
  depth=1 complete → deliver: "142 nodes reachable (depth 1)"
  depth=2 complete → deliver: "891 nodes reachable (depth 2)"
  depth=3 complete → deliver: "2,340 nodes reachable (converged)"
```

**Strategy 3 — Sampling with error bounds.** Applied at the `Anchor` step in Band 1. The error bound is computed from the sampling fraction via Hoeffding's inequality. For `GraphExpr` algorithms, provably approximate variants are substituted.

```
Sampling:
  anchor_sample = SAMPLE(N_AB, fraction=f, method=STRATIFIED)
  error_bound   = sqrt(log(2/δ) / (2 * f * |N_AB|))   -- Hoeffding
  -- returned with: "Estimated (±{error_bound*100:.1f}% at 95% confidence)"

GraphExpr approximations:
  BETWEENNESS_CENTRALITY  →  Monte Carlo approximation (k=200 samples)
  PAGE_RANK               →  power iteration with ε=0.01 convergence threshold
  SIGNATURE_ENTROPY       →  reservoir sample of path instances (k=1000)
```

**Strategy 4 — Async execution with materialisation.** Above `async_threshold`, the query is scheduled as a `PipelineArtifact` (§2.4) — the result is written to a `SinkTarget` and the query DAG node id is the cache key. The user receives an immediate acknowledgement; subsequent identical questions are answered from the cached result.

```
Async lifecycle:
  Missing (not yet run)
  → In-flight (executing)
  → Complete (result in sink, cached by q_dag_id)
  → Stale (after ttl expires)

WritePlan inferred from ADAPTIVE mode (PipelineArtifact Rule 10):
  First run: WritePlan(mode=APPEND)
  Re-run after stale: WritePlan(mode=MERGE)
```

**Clarification messages are lattice refinements.** When `CLARIFY` is returned, the options are not generic suggestions — they are specific BDD-valid descent steps in `L(G_AB)` that bring the query within the `streaming_threshold`:

```
Example clarification for Free→Free query over dense G:
  "This query traverses 2.1M paths. Here are three narrowings
   that each run in under 5 seconds:
   A) Restrict to retail customers (+ScalarPred c.segment='retail' → 180K paths)
   B) Restrict to last 30 days (+TemporalAgg ROLLING 30d → 340K paths)
   C) Fix the target product (Bound→Bound endpoint → 42K paths)
   Which would you like? [A / B / C / show more options]"

Cost estimates per option come from GraphStatistics.path_cardinality;
options are generated by the recommender navigating L(G_AB).
```

### 6.4  Query exporter

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

### 7.2  Two-stage trail exploration and compositional suggestions

**Two-stage trail exploration.** The endpoint relaxation extension gives the recommender a natural two-stage exploration protocol:

**Stage 1 — Free-endpoint characterisation.** Before fixing a target, surface OUTGOING_SIGNATURES(A) or INCOMING_SIGNATURES(B): "Your anchor node has N distinct outgoing path types. Dominant type: X (Y% of paths). SIGNATURE_DIVERSITY = Z."

High entropy / high diversity → recommend Stage 1 characterisation before drilling down.
Low entropy / low diversity → recommend direct Bound→Bound query.

**Stage 2 — Fixed-endpoint deep-dive.** Once a path type is identified via SignaturePred, narrow to a specific Bound→Bound pair using DISTINCT_SIGNATURES and DOMINANT_SIGNATURE. "REACHABLE_BETWEEN subgraph: DENSITY=D, MAX_FLOW=F."

**Compositional suggestions (new in v0.19).** CTE composition introduces a fourth suggestion type alongside Refinement, Generalisation, and Pivot:

```
Suggestion types:
  Refinement:    Q → Q'  (more specific predicate / finer grain)
  Generalisation: Q → Q'  (less specific predicate / coarser grain)
  Pivot:         Q → Q'  (different dimension / path, same specificity)
  Correlation:   (Q₁, Q₂) → Q₁ JOIN Q₂ ON shared_key
                 "here are two questions whose intersection is analytically valuable"
```

**Correlation suggestion generation.** Given a current query `Q_current` and the question algebra `Q_algebra(A, B)`, candidate correlation partners `Q_j` are identified by:

```
1. BDD atom intersection: find Q_j where atoms(Q_current) ∩ atoms(Q_j) ≠ ∅
   -- shared PropRef atoms = natural join keys between result sets
2. BDD feasibility: ensure Q_current ∩ Q_j is satisfiable (non-empty intersection)
   -- checked via bdd.apply(AND, current_bdd, q_j_bdd) != FALSE_NODE
3. Value scoring: estimate analytical value of Q_current JOIN Q_j ON shared_atoms
   -- high value when the join reduces both result sets substantially (high selectivity)
4. Rank and surface top-k correlation partners
```

**Question chain suggestions.** A question chain `Q₁ → Q₂ → Q₃` is a descent path in `L(G_AB)` — each step narrows the scope. The recommender can suggest entire chains rather than single steps:

```
"Start with Q₁ (all retail customers).
 Narrow to Q₂ (customers with >5 orders in last 30 days).
 Then ask Q₃ (top-2 sales per customer in Q₂ by revenue)."
```

This is a three-step CTE chain whose final result is not expressible as any single `Q_valid` query. The BDD validates each step's feasibility; trail metrics score the chain's analytical value.

**Canonical space navigation.** The query DAG canonical form (§8.14) changes what "neighbourhood" means for the recommender. Two syntactically different suggestions with the same query DAG node id are the same question — the recommender surfaces only one. The neighbourhood is defined over canonical DAG nodes:

```
neighbourhood(q_current) = {
  q' | q' differs from q_current by exactly one DAG operation:
       add one leaf node (Q_valid query)          -- refinement / generalisation
       remove one leaf node                        -- generalisation
       change one leaf node's operation (∪/∩/JOIN) -- pivot
       add one correlation JOIN node               -- new correlation
}
```

Correlation candidates identified by DAG node sharing are essentially free — the shared subgraph is already materialised. Cross-session result caching uses the query DAG node id as the cache key: same id, same result set (given same `G` snapshot). Cache invalidation is structural: a cached result for DAG node `q_id` is stale when any node or edge referenced by `q_id`'s leaf queries has changed in `G`.

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

### 8.11  Question completeness theorem

**Theorem (Question completeness).** For any two nodes `A, B ∈ N` with `RelationshipSubgraph G_AB = (N_AB, E_AB, τ_N, τ_E, P, Σ|_AB)`, the set of valid analytical questions `Q_valid(A, B)` is finite and constructively enumerable from `G_AB` and `Σ|_AB` alone.

**Proof sketch.** Three results combine:

(1) From §8.2 (functional completeness): the predicate language over `ScalarPred` atoms derived from `{P(n) | n ∈ N_AB}` and `{P(e) | e ∈ E_AB}` is functionally complete. The atom set is finite given finite `N_AB` and `E_AB`.

(2) From §8.6 (finite traversal space): the number of distinct `BoundPath` structures over `N_AB, E_AB` is bounded by `|N_AB|!`. The `PathPred` and `PathAgg` spaces are therefore finite.

(3) From §8.3 (minterm enumeration): the satisfiable minterms over the join context `C` derived from `G_AB` are bounded by `|C| ≤ |N_AB|^k` for join depth `k`. Finite.

The Band 2 aggregation space is finite given finite measure properties in `F ∩ N_AB` and finite `TemporalWindow` variants. The Band 3 ranking space is finite given the finite Band 2 output schema. The cross-product of all three bands over `G_AB` is therefore finite. ∎

**Definition (Question space).** The full question space is:

```
Q_space(A, B) = all valid Q = TemporalContext? ∘ B₁ ∘ B₂? ∘ B₃?
                where the Join scope ⊆ G_AB
```

The valid question space pruned by annotations is:

```
Q_valid(A, B) = Q_space(A, B) \ { q | q violates any annotation in Σ|_AB }
```

Each annotation in `Σ|_AB` removes a structurally invalid subset:
`FactlessFact(f)` removes SUM/AVG/MIN/MAX; `Grain(f, PERIOD)` removes cross-period SUM; `Degenerate(d)` removes `d` from GroupBy candidates; `SCDType(d, SCD1)` removes temporal join questions on `d`.

**Definition (Question lattice).** `Q_valid(A, B)` forms a lattice `L(G_AB)` ordered by semantic specificity:

```
top    = tautological query: Anchor(N_AB), no Where, no GroupBy, no Window
           — returns all tuples in G_AB
bottom = contradictory query: Where(FALSE_NODE)
           — returns no tuples
every other q ∈ Q_valid(A, B) lies strictly between top and bottom
```

The lattice has three specificity dimensions:

```
Traversal depth:    shallow paths → deep paths → full G_AB traversal
Predicate strength: tautology → targeted minterms → contradiction
Aggregation grain:  no grouping → fine grouping → per-entity grain
```

The recommender (§7) is a **guided navigator of `L(G_AB)`** — using BDD feasibility filtering to rule out invalid or redundant points, and trail metrics to score remaining points by analytical value, without enumerating the full lattice.

### 8.12  Semantic scoring of the question lattice

The trail space metrics induce a **non-uniform distribution over `L(G_AB)`**, making certain questions analytically more valuable than others without requiring full enumeration.

**`SIGNATURE_ENTROPY(G_AB)` as lattice routing signal.** High entropy: many equally frequent path types — the most informative questions stratify by path type (`SignaturePred`, `GroupBy` dominant signature). Low entropy: one path type dominates — the most informative questions go deep on measures along that path (`TemporalAgg`, `PathAgg` with specific strategy). Single path type (`DISTINCT_SIGNATURES = 1`): the entire question space collapses to variations on one pattern.

**`BETWEENNESS_CENTRALITY` on `G_AB` as GroupBy pivot signal.** Nodes with high betweenness within `G_AB` are the most discriminating `GroupBy` keys — they partition the tuple space most finely. Questions that group by high-betweenness nodes are near the fine-grain end of the aggregation specificity dimension.

**`SIGNATURE_DIVERSITY(A)` as strategy signal.** Low diversity at `A`: relationship is focused — `Bound→Bound` queries with `SHORTEST` strategy cover most of the analytical value. High diversity at `A`: relationship is varied — `ALL` strategy or `K_SHORTEST` with a signature filter is needed to capture the full structure.

**`DOMINANT_SIGNATURE(A, B)` as predicate seed.** The dominant label sequence directly yields the most analytically productive `PathPred` pattern — it is the path type that appears in the highest-value minterms of `L(G_AB)`.

**Formal statement.** Let `score: L(G_AB) → ℝ` be a scoring function derived from trail metrics. The recommender problem is:

```
argmax_{q ∈ neighbourhood(q_current)} score(q)
```

where `neighbourhood(q_current)` is the set of queries reachable from the current query by one step in `L(G_AB)` (add/remove one predicate, one aggregation, one window expression). The BDD ensures `neighbourhood` contains only valid, non-redundant steps. Trail metrics supply `score`. The three-layer recommender architecture (§7.1) is the computational realisation of this optimisation.

### 8.13  Question algebra and CTE composition

**Definition (Question algebra).** The **question algebra** `Q_algebra(A, B)` is the closure of `Q_valid(A, B)` under CTE composition operations:

```
Q_algebra(A, B) = closure of Q_valid(A, B) under {JOIN, UNION, INTERSECT, EXCEPT, WITH}
```

`Q_algebra(A, B)` is still finite (the graph is finite; all result sets are bounded subsets of `N_AB × propvals`). Its cardinality is combinatorially larger than `Q_valid(A, B)` — it includes all results obtainable by composing individual questions.

**Semiring structure.** The set operations form a semiring:

```
(Q_algebra(A, B), UNION, INTERSECT, ∅, Q_top)
```

where `∅` (empty result) is the additive identity and `Q_top` (all tuples in `G_AB`) is the multiplicative identity. This is the relational algebra semiring — `Q_algebra(A, B)` inherits all of relational algebra's compositional power.

**Three composition patterns in DGM:**

`Q₁ INTERSECT Q₂` — returns tuples satisfying both questions. For Band 1 queries, equivalent to `AND(Q₁.where, Q₂.where)` already handled by the BDD. For Band 2/3 compositions, intersection is not reducible to a single DGM query.

`Q₁ UNION Q₂` — returns tuples satisfying either. For Band 1 queries, equivalent to `OR(Q₁.where, Q₂.where)`, handled by the BDD's `UNION ALL` SQL strategy (§6.1). For Band 2/3 compositions, union has no single-query equivalent.

`Q₁ JOIN Q₂ ON key` — correlates the answers to two questions on a shared key. This is the most powerful composition: it asks "where do the answers to Q₁ and Q₂ agree on key?" Always a new point in `Q_algebra` not in `Q_valid`.

**Theorem (Datalog equivalence).** Under CTE composition, `Q_algebra(A, B)` is equivalent to **Datalog** over `G_AB` — the query language whose semantics are exactly least-fixpoint computations over relations.

*Proof sketch.* (→) Every Datalog rule is a conjunctive query with recursion, expressible as a CTE with a `WITH RECURSIVE` clause over `G_AB`. (←) Every `Q_algebra` composition is a finite chain of relational operations, expressible as a Datalog program without negation. ∎

**Corollary (PTIME bound).** By the Immerman-Vardi theorem, Datalog over ordered structures captures exactly PTIME. Under recursive CTE composition, `Q_algebra(A, B)` captures all PTIME-computable properties of `G_AB`. Questions not answerable within DGM under CTE composition require super-polynomial computation.

This bound justifies the D17 restriction: `Free→Free` scope in a `Where PathPred` position would push the filter into an unrestricted fixpoint, threatening decidability. The restriction to `Agg`/`Window` maintains the PTIME bound.

**Fixpoint semantics of recursive CTEs.** DGM already uses recursive CTEs for `BoundPath` traversal — path expansion is a least-fixpoint computation over the reachability relation. The question algebra generalises this: any CTE chain `Q_n` conditioned on `Q_{n-1}` is a fixpoint sequence. Question chains are **descent paths in `L(G_AB)`** — they trace routes from broad questions to specific answers, converging either at the empty result (bottom of `L`) or at a stable fixpoint (most specific achievable answer).

**Two-level predicate system.** The BDD covers single-query predicates. Cross-query predicate reasoning — "Q₂'s Where is implied by Q₁'s Agg result" — requires a second level:

```
Level 1: BDD over single-query predicates (current; §6.1)
Level 2: Inter-query containment over CTE result schemas
          -- decidable for conjunctive Band 1 queries (BDD implication check extended to result schema)
          -- harder for Band 2/3 compositions
```

For Band 1 queries, cross-query containment is decidable via the BDD's implication check extended to result schema `PropRef` sets. For Band 2/3 compositions, the problem reduces to query containment under aggregation — tractable for common cases (same GroupBy keys, monotone aggregates) but not in general.

**Extended theorem (Question completeness, v0.19).** `Q_valid(A, B)` is a finite generating set for `Q_algebra(A, B)`. Under CTE composition, `Q_algebra(A, B)` is equivalent to Datalog over `G_AB` and captures all PTIME-computable analytical questions about the `A→B` relationship. The question lattice `L(G_AB)` extends to the question algebra `A(G_AB)` with semiring structure. Question chains are descent paths in `A(G_AB)`.

### 8.14  Query DAG canonical form

**Motivation.** The BDD is the canonical form for predicate trees — two predicates reduce to the same BDD node id if and only if they are semantically equivalent. `Q_algebra(A, B)` has an analogous canonical form: the **query DAG**, a directed acyclic graph of relational operations whose leaf nodes are BDD-canonical `Q_valid` queries and whose internal nodes are semiring operations (`UNION`, `INTERSECT`, `JOIN`).

**The BDD / query DAG parallel:**

```
BDD                               Query DAG
─────────────────────────────────────────────────────────────────
Atom set (ScalarPred leaves)      Q_valid(A,B) leaf queries
Variable ordering                 Dependency partial order (data flow)
Sharing rule (unique table)       CSE: shared sub-queries = single DAG node
Elimination rule (low==high→low)  Semiring laws (idempotence, absorption,
                                   annihilation, containment)
BDD node id = canonical identity  Query DAG node id = canonical identity
O(1) equivalence: same id         O(1) equivalence: same id
```

**Definition (query DAG).** A query DAG `D` over `Q_valid(A, B)` is a DAG where:
- **Leaf nodes** are elements of `Q_valid(A, B)`, each in BDD-canonical form.
- **Internal nodes** have one of three types: `UNION(left, right)`, `INTERSECT(left, right)`, `JOIN(left, right, key: PropRef)`.
- The DAG is **reduced** when no semiring elimination rule applies and no two nodes have identical `(type, left_id, right_id)` triples.
- A reduced query DAG is **canonical** — unique up to the variable ordering on leaf BDD atoms.

**Construction algorithm** (mirrors BDD `make`):

```python
class QueryDAG:
    nodes:  dict[int, QueryNode]     # id → (type, left, right) or leaf Q_valid
    unique: dict[tuple, int]         # (type, left_id, right_id) → id
    bdd:    DGMPredicateBDD          # Level 1; shared across all leaves

    def make(self, op, left_id, right_id) -> int:
        # ── Elimination rules ─────────────────────────────────────────────
        if op == UNION:
            if left_id == right_id:           return left_id       # Q ∪ Q
            if left_id == EMPTY_ID:           return right_id      # ∅ ∪ Q
            if right_id == EMPTY_ID:          return left_id
            if left_id == TOP_ID:             return left_id       # Q_top ∪ Q
            if right_id == TOP_ID:            return right_id
            if self.contains(left_id, right_id):  return right_id  # Q₁⊆Q₂
            if self.contains(right_id, left_id):  return left_id
        if op == INTERSECT:
            if left_id == right_id:           return left_id       # Q ∩ Q
            if left_id == EMPTY_ID:           return EMPTY_ID      # ∅ ∩ Q
            if right_id == EMPTY_ID:          return EMPTY_ID
            if left_id == TOP_ID:             return right_id      # Q_top ∩ Q
            if right_id == TOP_ID:            return left_id
            if self.contains(left_id, right_id):  return left_id   # Q₁⊆Q₂
            if self.contains(right_id, left_id):  return right_id
        # ── Sharing rule ──────────────────────────────────────────────────
        key = (op, left_id, right_id)
        if key in self.unique: return self.unique[key]
        nid = self._new_id()
        self.nodes[nid] = QueryNode(op, left_id, right_id)
        self.unique[key] = nid
        return nid

    def contains(self, q1_id, q2_id) -> bool:
        # Q₁ ⊆ Q₂: applies BDD implication for Band 1 leaf queries
        # Returns False (conservative) for Band 2/3 compositions
        n1, n2 = self.nodes[q1_id], self.nodes[q2_id]
        if n1.is_leaf and n2.is_leaf and n1.band == B1 and n2.band == B1:
            return self.bdd.implies(n1.where_bdd_id, n2.where_bdd_id)
        return False
```

**Canonical form theorem.** Two composed queries are semantically equivalent over `G_AB` if and only if they reduce to the same query DAG node id. Proof: the elimination rules preserve semantics (each rule is a tautological semiring identity); the sharing rule enforces structural uniqueness; the BDD canonical form at the leaves ensures leaf equivalence is decided by id comparison. ∎

**Two-level canonical form.** The full DGM canonical form is the query DAG whose leaves are BDD-canonical `Q_valid` queries:

```
Full canonical form:
  Level 1 (predicate): BDD over ScalarPred/PathPred/SignaturePred atoms
                        — canonical form for single-query predicates
                        — equivalence: same BDD node id  (O(1))

  Level 2 (composition): query DAG over Q_valid leaf nodes
                          — canonical form for composed queries
                          — equivalence: same query DAG node id  (O(1))

  Composed query Q is fully normalised when:
    (a) every leaf is BDD-canonical, AND
    (b) no elimination rule applies to any internal DAG node
```

**Minimum CTE count.** The number of nodes in the canonical query DAG equals the minimum number of CTEs needed to express the composed query. This is a tight lower bound — no semantically equivalent composed query exists with fewer CTEs. The topological sort of the DAG gives the canonical CTE ordering.

**Complexity stratification of containment.**

The `contains(q1, q2)` check is the hard step. Its tractability depends on band:

```
Band 1 conjunctive:        NP-complete (Chandra-Merlin homomorphism test)
                            tractable for small queries in practice
Band 1 + Band 2 monotone:  Polynomial (monotone aggregate containment)
Band 1 + Band 2 arbitrary: Undecidable in general
Band 1-3 with recursion:   Σ₁-hard (Datalog containment)
```

The planner applies containment-based elimination only for Band 1 leaf nodes (tractable case). For Band 2/3 nodes it applies only the syntactic laws (idempotence, identity, annihilation) which do not require containment checking.

**Cross-session caching.** The query DAG node id is the cache key for composed query results:

```
cache[q_dag_id] = (result_set, G_snapshot_version)

Validity: cache hit valid iff G_snapshot_version = current
Invalidation: stale when any n ∈ N_AB or e ∈ E_AB referenced by
              q_dag_id's leaf queries has changed in G
              -- derivable from the DAG's dependency structure
```

This enables semantic-level caching: two users asking the same question in syntactically different forms get the same cached result because they reduce to the same DAG node id. SQL text caching cannot achieve this.

**Query explanation via DAG decomposition.** The canonical query DAG is the minimum mechanical explanation of what a composed query does:

```
For each leaf node q_i:  "question i: [what q_i asks, from DGM_JSON serialisation]"
For each UNION node:     "either question i or question j"
For each INTERSECT node: "both question i and question j simultaneously"
For each JOIN node:      "question i correlated with question j on [shared PropRef key]"
```

Reading the DAG bottom-up gives a complete, minimal, language-free explanation of the composed question. `ExportPlan.alternatives` (§6.2) are different canonical DAGs for the same semantic question, ranked by cost estimate.

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
| `PipelineArtifact` annotation | Not present | ❌ Missing |
| `PipelineArtifact` annotation | Not present | ❌ Missing |

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
- Planning rules 1–10 (Rules 1a cone extension, Rule 3 TrailExpr, Rule 9 cone containment, Rule 10 PipelineArtifact, Rule 11 query DAG CSE + minimisation).
- SQL emitter with G^T BFS for Free→Bound; cone containment optimisation.
- Sink integration; Cypher, SPARQL, DGM_JSON/YAML exporters.
- ExecutionBudget type and pre-execution gate (§6.3): BudgetDecision types, decision procedure, four graceful handling strategies.
- Cost-aware rewrite rules; progressive streaming schedule from DAG topology.
- Sampling with Hoeffding error bounds; async execution via PipelineArtifact.
- Clarification message generation from recommender lattice refinements.

**Phase 8 — Natural language interface (~3 weeks)**
- Implementation stack: Pydantic-AI (five specialist agents) + LangGraph (state graph with HIL checkpoints).
- DGMContext dataclass as Pydantic-AI dependency: carries graph G, entity_registry, BDD, query_dag, recommender, budget, q_current.
- Pydantic output schemas derived from DGM formal types: EntityResolutionResult, TemporalClassificationResult, CompositionalDetectionResult, CandidateRankingResult, ExplanationRenderResult.
- Agent 1 (entity_resolution): PropRef resolution against closed EntityRegistry vocabulary; get_valid_prop_refs and get_verb_labels tools.
- Agent 2 (temporal_classification): TemporalModeEnum + TemporalPropertyEnum + TemporalWindow classification.
- Agent 3 (compositional_detection): QAlgebraOpEnum detection (UNION/INTERSECT/JOIN/CHAIN).
- Agent 4 (candidate_ranking): linguistic proximity ranking over BDD-pre-validated Q_valid candidates.
- Agent 5 (explanation_rendering): DAG decomposition rendering via dag_decomposition and get_next_suggestions tools.
- Six non-LLM nodes: candidate_generation (recommender), confirmation_loop (interrupt/HIL), dag_construction (QueryDAG.make_from_candidate), budget_gate (§6.3 gate), execution (QueryDAG.execute), clarification (recommender).
- Explicit LangGraph cycles: candidate_generation → confirmation_loop → candidate_generation (refinement); clarification → confirmation_loop (budget clarification).
- MemorySaver checkpointer for multi-turn state persistence across HIL pauses.
- Pydantic validation as hallucination prevention: agents retry on validation failure; UNRESOLVED on retry exhaustion.
- Parallel leaf execution in STREAM mode via asyncio.gather over topologically independent DAG leaves.

---

## 11  Natural Language Interface

### 11.1  Architecture overview

A DGM-backed NL interface narrows the LLM's role from query synthesiser to formal-space navigator. The LLM does not generate SQL or Cypher from schema text. It maps natural language utterances to positions in `Q_valid(A, B)`, ranks candidates by linguistic proximity, and renders the query DAG decomposition as a natural language explanation. All analytical guarantees come from the DGM formalism; the LLM provides the language bridge.

```
Natural language utterance
         ↓
┌──────────────────────────────────────────────────────────┐
│  Layer 1 — Semantic grounding                            │
│  Entity resolution:      NL terms → PropRef in P(n)     │
│  Relationship resolution: NL → BoundPath via trail metrics│
│  Band identification:    intent → B₁ / B₂ / B₃          │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│  Layer 2 — Q_valid navigation                            │
│  Candidate generation:  recommender over L(G_AB)         │
│  Candidate ranking:     LLM scores by linguistic proximity│
│  Temporal mapping:      NL → TemporalMode / CTL property │
│  Compositional mapping: NL → Q_algebra operation         │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│  Layer 3 — Confirmation / refinement loop                │
│  Surface top-k candidates with plain-language labels     │
│  User selects or refines → lattice navigation step       │
│  Ambiguity: present as lattice positions, not errors     │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│  Layer 4 — Query DAG construction + budget gate          │
│  BDD canonical form (predicate level)                   │
│  Query DAG canonical form (composition level)            │
│  Cache check: same DAG node id → return cached result    │
│  Pre-execution gate: ExecutionBudget → BudgetDecision    │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│  Layer 5 — Execution and explanation                     │
│  Exporter: target SQL / Cypher / SPARQL per backend      │
│  Planner rules applied (annotation-aware)                │
│  Result + DAG decomposition → structured explanation     │
│  LLM renders explanation in natural language             │
│  Recommender surfaces next-step suggestions              │
└──────────────────────────────────────────────────────────┘
```

### 11.2  LLM responsibilities (narrowly scoped)

The LLM is responsible for five tasks only. All analytical guarantees are provided by the DGM formalism; the LLM cannot introduce invalid queries, hallucinated properties, or incorrect aggregation semantics because the question space is formally bounded before the LLM acts.

```
1. Entity resolution
   Input:  NL term (e.g. "retail customers", "Q3 revenue")
   Output: PropRef value in P(n) for n ∈ G, or UNRESOLVED
   Constraint: output must exist in {P(n).keys | n ∈ N}
   Hallucination prevention: unresolved terms trigger clarification,
                              not query generation

2. Candidate ranking
   Input:  top-k Q_valid candidates from recommender + NL utterance
   Output: ranking by linguistic proximity
   Constraint: candidates are pre-validated by BDD; LLM cannot introduce
               invalid candidates

3. Temporal intent classification
   Input:  NL temporal expression (e.g. "has always been", "eventually reaches")
   Output: TemporalMode ∈ {EVENTUALLY, GLOBALLY, NEXT, UNTIL, SINCE, ONCE, PREVIOUSLY}
           or TemporalProperty ∈ {SAFETY, LIVENESS, RESPONSE, PERSISTENCE, RECURRENCE}
   This is classification, not generation

4. Compositional intent detection
   Input:  NL question with multiple sub-questions
   Output: Q_algebra operation ∈ {UNION, INTERSECT, JOIN, CHAIN}
   Examples: "and also" → INTERSECT; "compared to" → JOIN; "then" → CHAIN

5. Explanation rendering
   Input:  query DAG decomposition (structured, mechanical)
   Output: natural language explanation of what the query does
   This is generation from structured input, not from raw schema
```

### 11.3  Semantic grounding layer

**Entity resolution.** Grounded in `P(n)` property names, verb edge labels, and annotation vocabulary — not raw column names. The entity registry is built once from `G` and updated via `Q_delta` when `G` changes.

```
EntityRegistry = {
  prop_terms:    { NL term → PropRef }   -- "revenue" → PropRef(sale.revenue)
  node_terms:    { NL term → alias → A } -- "customer" → alias c of Customer dim
  verb_terms:    { NL term → VerbHop }   -- "purchased" → VerbHop(c,'placed',s)
  temporal_terms:{ NL term → TemporalWindow } -- "Q3 2024" → PERIOD(...)
  agg_terms:     { NL term → AggFn }     -- "total" → SUM; "average" → AVG
}
```

**Relationship resolution.** Trail metrics constrain path selection. When `DISTINCT_SIGNATURES(A, B) = 1`, the path is unambiguous and the LLM does not need to choose. When `SIGNATURE_ENTROPY` is high, the interface asks for clarification before resolving.

```
path_resolution(A: NodeRef, B: NodeRef, utterance: str) → BoundPath | AMBIGUOUS:
  sigs = DISTINCT_SIGNATURES(A, B)
  if sigs == 1:
    return canonical_path(A, B)          -- unambiguous; determined by G_AB
  dominant = DOMINANT_SIGNATURE(A, B)
  llm_match = score_path_by_utterance(dominant, utterance)
  if llm_match > CONFIDENCE_THRESHOLD:
    return dominant_path(A, B)
  return AMBIGUOUS(candidates=top_k_signatures(A, B, k=3))
```

**Band identification.** A lightweight classifier maps intent signals to band structure. The classifier is trained on patterns grounded in DGM vocabulary — not SQL keywords.

```
Intent signals → Band mapping:
  "total", "sum", "average", "count"     → B₂ AggFn
  "highest", "lowest", "top N", "rank"   → B₃ RankFn / Qualify
  "where", "which", "filter"             → B₁ Where ScalarPred
  "over time", "rolling", "YTD"          → B₂ TemporalAgg
  "has always", "never", "eventually"    → B₁ PathPred + TemporalMode
  "whenever X, does Y"                   → RESPONSE temporal property
  "compared to last year"                → Q_algebra JOIN on time dimension
  "customers who X and also Y"           → Q_algebra INTERSECT
```

### 11.4  Temporal and compositional intent mapping

**Temporal patterns map to CTL operators:**

```
NL expression                          → TemporalMode / Property
─────────────────────────────────────────────────────────────────
"has always been true"                 → GLOBALLY (AG)
"will eventually happen"               → EVENTUALLY (AF)
"at the next step"                     → NEXT (AX)
"positive until churn occurred"        → UNTIL(positive, churn)
"rising since the last campaign"       → SINCE(rising, campaign_end)
"was once a premium customer"          → ONCE (O)
"the previous value was higher"        → PREVIOUSLY (Y)
"this condition never violated"        → SAFETY(NOT condition)
"this outcome always eventually reached"→ LIVENESS(outcome)
"whenever X, Y follows"               → RESPONSE(X, Y)
```

These are classification mappings — the interface recognises the pattern and selects the correct `TemporalMode` or `TemporalProperty`. The LLM does not generate the temporal logic formula; it classifies the pattern from a closed vocabulary.

**Compositional patterns map to Q_algebra operations:**

```
NL pattern                             → Q_algebra operation
─────────────────────────────────────────────────────────────────
"customers who X and also Y"           → INTERSECT(Q_X, Q_Y)
"customers who X or Y"                 → UNION(Q_X, Q_Y)
"X compared to Y" / "X alongside Y"   → JOIN(Q_X, Q_Y, ON shared_key)
"first X, then among those, Y"         → CHAIN: Q_X → Q_Y
"for each X, what is Y?"               → JOIN with key = X's GroupBy
```

### 11.5  Confirmation and refinement loop

Rather than generating a query and hoping it is correct, the interface proposes the top-k most likely intended queries from `Q_valid(A, B)` and asks for confirmation. Each candidate is a valid point in `L(G_AB)` generated by the recommender.

```
Example:
  User: "How are my retail customers doing this year?"

  Entity resolution:
    "retail customers" → ScalarPred(c.segment = 'retail')
    "this year"        → TemporalAgg window YTD
    "doing"            → ambiguous measure; band = B₂

  Relationship: Customer →[placed]→ Sale (DOMINANT_SIGNATURE = ['placed'])

  Recommender generates top-3 candidates from L(G_AB):
    A) Total YTD revenue (SUM revenue, YTD)
    B) Order count YTD (COUNT sale, YTD)
    C) YTD revenue rolling 30-day trend

  Interface: "I think you are asking about one of these:
    A) Total revenue from retail customers year-to-date
    B) Number of orders from retail customers year-to-date
    C) Rolling 30-day revenue trend for retail customers

    Which fits best, or show more options?"
```

User selection triggers a descent step in `L(G_AB)` — the selected candidate becomes `q_current`, and the recommender's neighbourhood provides the next set of refinement suggestions.

### 11.6  Ambiguity resolution grounded in the question space

Ambiguity is not "the LLM is uncertain." It is "the utterance maps to multiple positions in `Q_valid(A, B)`." The positions are known; the task is to identify which one the user intended.

```
Ambiguity types and resolution strategies:

Measure ambiguity:  "how are customers doing?"
  Multiple valid AggFn over different PropRef values
  → surface top-k by data distribution (highest-variance measures first)

Path ambiguity:  DISTINCT_SIGNATURES > 1, high SIGNATURE_ENTROPY
  Multiple valid BoundPath structures between resolved entities
  → surface dominant signatures as named path type options

Temporal ambiguity:  "last year" → calendar year? rolling 12 months?
  Multiple valid TemporalWindow interpretations
  → surface both; apply user preference if previously expressed

Grain ambiguity:  no explicit GroupBy in B₂ question
  Multiple valid GroupBy keys (region, segment, product category)
  → surface annotation-driven suggestions (Hierarchy roll-up; RolePlaying cross-role)

Compositional ambiguity:  "customers who bought X and returned Y"
  INTERSECT or sequential chain?
  → detect temporal ordering cue ("then", "after") → CHAIN
  → no temporal cue → INTERSECT (default for "and")
```

### 11.7  Schema evolution handling

When `G` changes, `Q_valid(A, B)` changes with it. Saved NL questions may become invalid or gain new interpretations.

```
Q_delta(t₁, t₂) triggers:

  ADDED_NODES(dim):    new GroupBy candidates; new constellation paths
                       → suggest new questions to users who query related nodes
  REMOVED_NODES:       invalidate saved questions referencing removed nodes
                       → notify: "Your saved question about X is no longer valid"
  ADDED_EDGES(verb):   new path types available; DISTINCT_SIGNATURES increases
                       → suggest constellation paths if Conformed annotation added
  ROLE_DRIFT:          dim→fact reclassification changes valid AggFn set
                       → revalidate saved questions; notify if Band 2 becomes invalid
  CHANGED_PROPERTY:    PropRef values in EntityRegistry may be stale
                       → re-resolve saved NL terms against updated P(n)
```

Saved questions are stored by query DAG node id. When `G` changes, the interface checks whether the node id is still valid by attempting to re-derive the DAG from the new `G`. If derivation fails, the question is invalidated. If derivation succeeds but produces a different DAG, the user is notified of the semantic change.

### 11.8  Hallucination prevention

Three formal mechanisms prevent the LLM from producing invalid queries:

**Closed PropRef vocabulary.** Entity resolution maps NL terms to `{P(n).keys | n ∈ N}`. A term that does not resolve to any existing property triggers `UNRESOLVED` — the interface asks for clarification rather than guessing a column name.

**BDD-validated candidates.** The recommender generates candidates from `Q_valid(A, B)` using the BDD feasibility filter. Every candidate surfaced to the LLM for ranking is already structurally valid and non-redundant. The LLM cannot introduce an invalid candidate by ranking — it can only choose among pre-validated options.

**Query DAG canonical form.** Even if the LLM's ranking is imperfect, the canonical form catches semantic equivalents — two syntactically different queries that reduce to the same DAG node id are treated as identical, and the cached result (if any) is returned without re-execution.

### 11.9  Integration with the execution budget gate

The NL interface feeds directly into the pre-execution gate (§6.3). When the gate returns `CLARIFY`, the clarification options are generated by the recommender as lattice refinements — not as generic "try a simpler query" messages. The NL interface renders them in natural language using the entity registry.

When the gate returns `ASYNC`, the NL interface immediately acknowledges with the query DAG node id as a reference:

```
"This analysis will take approximately 3 minutes.
 I've queued it (reference: q_7f3a2b).
 You'll be notified when it's ready.
 In the meantime, here's a 10% sample result with ±3.2% error bound:
 [sampled result]"
```

The sample is produced by Strategy 3 (sampling with error bounds) in parallel with the async execution — the user gets an immediate approximate result while the full computation runs.

### 11.10  Implementation: LangGraph + Pydantic-AI

The NL interface is implemented as a LangGraph state graph of specialist Pydantic-AI agents. This pairing is structurally forced by two properties of the DGM NL interface: the workflow is stateful and cyclic (LangGraph), and the output vocabulary is formally bounded (Pydantic-AI).

**Why Pydantic-AI for each specialist agent.**

DGM's formal types are the Pydantic output schemas. Every agent output must be drawn from a closed set validated against the live graph. An agent that produces a `PropRef` not in `P(n)` fails Pydantic validation and retries with a corrected prompt — hallucination is structurally prevented, not post-hoc filtered.

**Pydantic output schemas (DGM formal types as models):**

```python
from pydantic import BaseModel, field_validator
from pydantic_ai import Agent, RunContext
from dataclasses import dataclass
from enum import Enum

@dataclass
class DGMContext:
    graph:           object          # G = (N, E, τ_N, τ_E, P, Σ)
    entity_registry: object          # NL term → PropRef mapping
    bdd:             object          # DGMPredicateBDD (Level 1)
    query_dag:       object          # QueryDAG (Level 2)
    recommender:     object          # L(G_AB) navigator
    budget:          object          # ExecutionBudget
    q_current:       object | None   # current lattice position

class TemporalModeEnum(str, Enum):
    EVENTUALLY="EVENTUALLY"; GLOBALLY="GLOBALLY"; NEXT="NEXT"
    UNTIL="UNTIL"; SINCE="SINCE"; ONCE="ONCE"; PREVIOUSLY="PREVIOUSLY"

class TemporalPropertyEnum(str, Enum):
    SAFETY="SAFETY"; LIVENESS="LIVENESS"; RESPONSE="RESPONSE"
    PERSISTENCE="PERSISTENCE"; RECURRENCE="RECURRENCE"

class QAlgebraOpEnum(str, Enum):
    UNION="UNION"; INTERSECT="INTERSECT"; JOIN="JOIN"; CHAIN="CHAIN"

class PropRefModel(BaseModel):
    alias: str
    prop:  str   # validated against live P(n) via agent tool

class EntityResolutionResult(BaseModel):
    resolved:     list[PropRefModel]
    unresolved:   list[str]           # trigger CLARIFY
    node_aliases: dict[str, str]      # "customer" → alias "c"

class TemporalClassificationResult(BaseModel):
    mode:        TemporalModeEnum | None = None
    property_:   TemporalPropertyEnum | None = None
    window_type: str | None = None    # ROLLING|TRAILING|PERIOD|YTD|QTD|MTD
    confidence:  float

class CompositionalDetectionResult(BaseModel):
    operation:  QAlgebraOpEnum | None = None
    join_key:   PropRefModel | None = None
    confidence: float

class QueryCandidate(BaseModel):
    dag_node_id:   int               # must exist in query_dag.nodes
    description:   str
    band_coverage: list[str]
    cost_estimate: float

class CandidateRankingResult(BaseModel):
    ranked:     list[QueryCandidate]  # pre-validated by BDD; LLM only reorders
    confidence: list[float]

class ExplanationRenderResult(BaseModel):
    explanation:           str
    follow_up_suggestions: list[str]
```

**The five specialist agents:**

```python
# Agent 1 — Entity Resolution
entity_agent = Agent(
    model="claude-sonnet-4-6",
    deps_type=DGMContext,
    result_type=EntityResolutionResult,
    system_prompt=(
        "Map natural language terms to DGM PropRef values. "
        "Use get_valid_prop_refs to check what exists. "
        "Only return PropRef values the tool confirms. "
        "Mark anything not found as unresolved — do not guess."
    ),
)

@entity_agent.tool
def get_valid_prop_refs(ctx: RunContext[DGMContext], node_type: str) -> list[str]:
    """Returns all valid PropRef keys for nodes of the given type."""
    return [
        alias + "." + k
        for n, alias in ctx.deps.entity_registry.node_terms.items()
        if n == node_type
        for k in ctx.deps.graph.P(n).keys()
    ]

@entity_agent.tool
def get_verb_labels(ctx: RunContext[DGMContext],
                    from_type: str, to_type: str) -> list[str]:
    """Returns verb labels connecting two node types."""
    return ctx.deps.entity_registry.verb_terms.get((from_type, to_type), [])

# Agent 2 — Temporal Classifier
temporal_agent = Agent(
    model="claude-sonnet-4-6",
    deps_type=DGMContext,
    result_type=TemporalClassificationResult,
    system_prompt=(
        "Classify temporal intent. Map to exactly one of: "
        "TemporalMode enum, TemporalProperty enum, or TemporalWindow type string. "
        "If no temporal intent, return all None with confidence=0."
    ),
)

# Agent 3 — Compositional Detector
compositional_agent = Agent(
    model="claude-sonnet-4-6",
    deps_type=DGMContext,
    result_type=CompositionalDetectionResult,
    system_prompt=(
        "Detect multi-question composition. "
        "UNION='or'; INTERSECT='and both'; JOIN='compared to/alongside'; "
        "CHAIN='then/among those'. Return None for atomic questions."
    ),
)

# Agent 4 — Candidate Ranking
ranking_agent = Agent(
    model="claude-sonnet-4-6",
    deps_type=DGMContext,
    result_type=CandidateRankingResult,
    system_prompt=(
        "Rank provided Q_valid candidates by linguistic proximity. "
        "Do not add or remove candidates — only reorder the pre-validated list. "
        "Assign confidence scores in [0,1]."
    ),
)

# Agent 5 — Explanation Renderer
explanation_agent = Agent(
    model="claude-sonnet-4-6",
    deps_type=DGMContext,
    result_type=ExplanationRenderResult,
    system_prompt=(
        "Render the query DAG decomposition as natural language (2-4 sentences). "
        "Use dag_decomposition and get_next_suggestions tools."
    ),
)

@explanation_agent.tool
def dag_decomposition(ctx: RunContext[DGMContext], dag_node_id: int) -> dict:
    """Returns the structured DAG decomposition for the given query DAG node."""
    return ctx.deps.query_dag.decompose(dag_node_id)

@explanation_agent.tool
def get_next_suggestions(ctx: RunContext[DGMContext],
                         dag_node_id: int, k: int = 3) -> list[str]:
    """Returns top-k recommender suggestions from the current lattice position."""
    return ctx.deps.recommender.neighbourhood_descriptions(dag_node_id, k)
```

**The LangGraph state graph:**

```python
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import interrupt
from typing import Literal

class NLInterfaceState(BaseModel):
    utterance:       str
    dgm_ctx:         DGMContext
    entity_result:   EntityResolutionResult | None = None
    temporal_result: TemporalClassificationResult | None = None
    compositional:   CompositionalDetectionResult | None = None
    candidates:      list[QueryCandidate] = []
    ranking_result:  CandidateRankingResult | None = None
    q_current:       int | None = None    # query DAG node id
    user_selection:  object | None = None
    budget_decision: str | None = None
    partial_result:  object | None = None
    final_result:    object | None = None
    explanation:     str | None = None

# Non-LLM nodes (pure DGM computation):

def candidate_generation_node(state: NLInterfaceState) -> dict:
    candidates = state.dgm_ctx.recommender.top_k_candidates(
        entity_result=state.entity_result,
        temporal_result=state.temporal_result,
        compositional=state.compositional,
        q_current=state.q_current, k=5
    )
    return {"candidates": candidates}

def confirmation_loop_node(state: NLInterfaceState) -> dict:
    selection = interrupt({
        "type": "confirmation",
        "candidates": [c.description for c in state.ranking_result.ranked],
        "prompt": "Which fits best, or type 'more' for additional options?"
    })
    return {"user_selection": selection}

def dag_construction_node(state: NLInterfaceState) -> dict:
    selected = state.ranking_result.ranked[state.user_selection]
    dag_node_id = state.dgm_ctx.query_dag.make_from_candidate(
        selected, state.entity_result,
        state.temporal_result, state.compositional,
    )
    return {"q_current": dag_node_id}

def budget_gate_node(state: NLInterfaceState) -> dict:
    from dgm.execution import gate
    plan     = state.dgm_ctx.query_dag.to_export_plan(state.q_current)
    decision = gate(plan, state.dgm_ctx.budget)
    partial  = None
    if decision.type == "ASYNC":
        partial = state.dgm_ctx.query_dag.execute_sample(
            state.q_current, fraction=0.10
        )
    return {"budget_decision": decision.type, "partial_result": partial}

def clarification_node(state: NLInterfaceState) -> dict:
    refinements = state.dgm_ctx.recommender.clarification_options(
        state.q_current, budget=state.dgm_ctx.budget
    )
    return {"candidates": refinements}

# Routing functions:

def route_after_confirmation(state: NLInterfaceState) -> Literal[
        "confirmed", "refine", "more_options"]:
    if state.user_selection == "more":   return "more_options"
    if state.user_selection == "refine": return "refine"
    return "confirmed"

def route_budget(state: NLInterfaceState) -> str:
    return state.budget_decision

# Graph assembly:

graph = StateGraph(NLInterfaceState)

# LLM nodes (5):
graph.add_node("entity_resolution",
    lambda s: {"entity_result":
        entity_agent.run_sync(s.utterance, deps=s.dgm_ctx).data})
graph.add_node("temporal_classification",
    lambda s: {"temporal_result":
        temporal_agent.run_sync(s.utterance, deps=s.dgm_ctx).data})
graph.add_node("compositional_detection",
    lambda s: {"compositional":
        compositional_agent.run_sync(s.utterance, deps=s.dgm_ctx).data})
graph.add_node("candidate_ranking",
    lambda s: {"ranking_result":
        ranking_agent.run_sync(
            "Utterance: " + s.utterance + " Candidates: " + str(s.candidates),
            deps=s.dgm_ctx).data})
graph.add_node("explanation_rendering",
    lambda s: {"explanation":
        explanation_agent.run_sync(
            "DAG node: " + str(s.q_current) +
            " Budget: " + str(s.budget_decision),
            deps=s.dgm_ctx).data.explanation})

# Non-LLM nodes (6):
graph.add_node("candidate_generation", candidate_generation_node)
graph.add_node("confirmation_loop",    confirmation_loop_node)
graph.add_node("dag_construction",     dag_construction_node)
graph.add_node("budget_gate",          budget_gate_node)
graph.add_node("execution",
    lambda s: {"final_result":
        s.dgm_ctx.query_dag.execute(s.q_current, mode=s.budget_decision)})
graph.add_node("clarification", clarification_node)

# Edges:
graph.add_edge("entity_resolution",       "temporal_classification")
graph.add_edge("temporal_classification", "compositional_detection")
graph.add_edge("compositional_detection", "candidate_generation")
graph.add_edge("candidate_generation",    "candidate_ranking")
graph.add_edge("candidate_ranking",       "confirmation_loop")

graph.add_conditional_edges("confirmation_loop", route_after_confirmation,
    {"confirmed":    "dag_construction",
     "refine":       "candidate_generation",
     "more_options": "candidate_generation"})

graph.add_edge("dag_construction", "budget_gate")

graph.add_conditional_edges("budget_gate", route_budget,
    {"PROCEED":  "execution",   "STREAM":   "execution",
     "ASYNC":    "explanation_rendering",
     "REWRITE":  "dag_construction",
     "SAMPLE":   "execution",   "PAGINATE": "execution",
     "CLARIFY":  "clarification",
     "REJECT":   "explanation_rendering"})

graph.add_edge("execution",            "explanation_rendering")
graph.add_edge("explanation_rendering", END)
graph.add_edge("clarification",         "confirmation_loop")

graph.set_entry_point("entity_resolution")
nl_app = graph.compile(checkpointer=MemorySaver())
```

**Key structural properties:**

*Five LLM nodes, six deterministic nodes.* Only `entity_resolution`, `temporal_classification`, `compositional_detection`, `candidate_ranking`, and `explanation_rendering` invoke an LLM. The remaining six nodes are pure DGM computation — formal, deterministic, and verifiable.

*Explicit cycles with state persistence.* The `candidate_generation → confirmation_loop` refinement cycle and the `clarification → confirmation_loop` budget cycle are first-class LangGraph constructs. `MemorySaver` persists state across the human-in-the-loop pause at `confirmation_loop`, enabling multi-turn conversations without bespoke session management.

*Bounded retry via Pydantic-AI.* If an agent produces output that fails Pydantic validation, Pydantic-AI retries with a corrected prompt. After exhausting retries, the agent returns `UNRESOLVED` items rather than propagating invalid output downstream.

*Parallel leaf execution in streaming mode.* When `budget_gate` returns `STREAM`, the `execution_node` runs topologically independent `query_dag` leaves in parallel using `asyncio.gather`.

**The ontology-to-Pydantic mapping (formal relationship):**

```
DGM formal constraint                  Pydantic enforcement
────────────────────────────────────────────────────────────────────────
PropRef alias ∈ EntityRegistry         field_validator checks registry
PropRef prop ∈ P(n).keys              get_valid_prop_refs tool confirms pre-generation
TemporalMode ∈ {EVENTUALLY,...}        TemporalModeEnum; no free strings permitted
QAlgebraOp ∈ {UNION,INTERSECT,...}     QAlgebraOpEnum; closed set
QueryCandidate.dag_node_id valid       field_validator checks query_dag.nodes
candidates pre-validated by BDD        generated by recommender, not LLM
BudgetDecision variant ∈ fixed set     discriminated union enforces per-variant fields
```

This is the machine-executable form of §11.8 (hallucination prevention): not a post-hoc filter, but a generation constraint baked into every agent's output schema. The DGM specification is the formal source; the Pydantic schema is its executable projection.


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

**Example 6 — Backfill-incremental state machine with PipelineArtifact**

A `daily_revenue` pipeline produces one accumulating artifact per day-window. Artifacts expire after 7 days (TTL) and must be backfilled within 90 days.

```
-- Schema:
--   PipelineArtifact(daily_rev, 'daily_revenue',
--                   ttl=7 days, backfill_horizon=90 days,
--                   write_mode=ADAPTIVE) ∈ Σ
-- Implies: Grain(daily_rev, ACCUMULATING)
-- P(daily_rev).state ∈ {Missing, In-flight, Complete, Stale, Failed}

-- Step 1: Backfill — find Missing/Failed within horizon
Q_backfill:
  Anchor(fact, a)
  Where:
    AND(
      ScalarPred(PropRef(a.state), IN {Missing, Failed}),
      ScalarPred(PropRef(a.window_end),
                 (< ArithExpr(now, -, 90 days)))
    )
  GroupBy: [ a.pipeline_id, a.window_start ]
  Agg:     age_days := TemporalAgg(MAX, a.window_end, a.window_end, TRAILING(1,DAY))
  Window:  priority := RANK() OVER (ORDER BY age_days DESC)
  Qualify: ScalarPred(WinRef(priority), (<= MAX_CONCURRENT_RUNS))
-- Planner infers WritePlan(mode=APPEND) from state ∈ {Missing, Failed}

-- Step 2: Refresh — find Stale windows
Q_refresh:
  Anchor(fact, a)
  Where:   ScalarPred(PropRef(a.state), (= Stale))
  GroupBy: [ a.pipeline_id, a.window_start ]
  Agg:     staleness := TemporalAgg(MAX, a.window_end, a.completed_at, ROLLING(7 days))
  Window:  priority  := RANK() OVER (ORDER BY staleness DESC)
  Qualify: ScalarPred(WinRef(priority), (<= MAX_CONCURRENT_RUNS))
-- Planner infers WritePlan(mode=MERGE) from state = Stale

-- Step 3: Monitor completions and failures since last run
Q_delta(
  t₁ = last_scheduler_run, t₂ = now,
  spec = CHANGED_PROPERTY(a, state,
           comparator = (old ∈ {In-flight}, new ∈ {Complete, Failed}))
)
-- ACCUMULATING grain: CHANGED_PROPERTY captures stage progression
-- WritePlan(mode=APPEND, sink=DELTA): appends each transition record

-- Step 4: CTL correctness assertions

-- SAFETY: no window older than horizon stays Missing
PathPred(a, NodeRef(a), FORALL, ALL, GLOBALLY,
  NOT(AND(
    ScalarPred(PropRef(a.state), (= Missing)),
    ScalarPred(PropRef(a.window_end), (< ArithExpr(now, -, 90 days)))
  ))
)
-- AG(¬(Missing ∧ window_end < now-90d))

-- LIVENESS: every Missing/Failed window eventually reaches Complete
PathPred(a,
  BridgeHop(a, 'succeeded', done, node_alias=completed_art),
  FORALL, ALL, EVENTUALLY,
  ScalarPred(PropRef(completed_art.state), (= Complete))
)
-- AF(Complete)

-- RESPONSE: every Stale window eventually refreshes
RESPONSE(
  trigger  = ScalarPred(PropRef(a.state), (= Stale)),
  response = ScalarPred(PropRef(a.state), (= Complete))
)
-- AG(Stale → AF(Complete))

-- Step 5: Asymmetric failure — UNTIL distinguishes refresh vs backfill failure
PathPred(
  a,
  Compose(
    BridgeHop(a, 'failed', failed_art, node_alias=fa),
    temporal=UNTIL(
      hold_pred    = ScalarPred(PropRef(fa.prior_state), (= Complete)),
      trigger_pred = ScalarPred(PropRef(fa.state), (= Failed))
    )
  ),
  EXISTS, ALL,
  ScalarPred(PropRef(fa.prior_state), (= Complete))
)
-- EXISTS path where Complete held until failure → refresh failure
-- No such path for backfill failures (no prior Complete)
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

**Datalog and fixpoint query languages.** Datalog [13] is the query language whose semantics are least-fixpoint computations over relations, equivalent to first-order logic with least fixpoint (FO+LFP). The Immerman-Vardi theorem establishes that FO+LFP captures PTIME on ordered structures. DGM's question algebra `Q_algebra(A, B)` under CTE composition is equivalent to Datalog over `G_AB` (§8.13) — establishing that DGM's full compositional question space is exactly PTIME-bounded. This positions DGM precisely in the database query language complexity hierarchy and grounds the D17 restriction (no `Free→Free` in `Where PathPred` scope) as a decidability preservation measure.

**Ontologies and knowledge representation.** RDF [10], RDFS, and OWL [11] constitute the standard ontological stack. An RDF triple `(subject, predicate, object)` is structurally identical to DGM's verb edge `(dim, label, fact)` — both are typed directed labeled edges between entities. The `τ_N : N → {dim, fact}` type function is a two-class hierarchy equivalent to `rdf:type` with `rdfs:subClassOf`. SPARQL 1.1 property paths (`+`, `*`, `|`, `^`) correspond to DGM's `BoundPath` traversal, and SPARQL's `FILTER EXISTS` / `FILTER NOT EXISTS` correspond to `PathPred(EXISTS)` / `PathPred(FORALL)`. DGM independently arrived at the same graph primitive as RDF — evidence that the triple / typed directed labeled edge is the correct foundational structure.

However, DGM and ontologies diverge sharply above that shared primitive, along five axes:

**(i) The dim/fact typed split.** OWL has no concept of a measurable event node distinct from a domain entity node. Everything is either a class or an individual. The structural invariant `V ⊆ D×F×L` — which enforces that verb edges connect dimensions to facts and not arbitrary node pairs — has no OWL equivalent. KG-OLAP [6] works around this by layering context dimensions on top of OWL but does not enforce the type discipline at the schema level.

**(ii) Aggregation and analytics.** OWL is a representation and reasoning language, not a query and analytics language. It has no `GroupBy`, `Agg`, `Having`, `Window`, or `Qualify`. SPARQL has limited aggregation (`SUM`, `COUNT`, `GROUP BY`) but no window functions, no temporal aggregation, no path aggregation (`PathAgg`, `TemporalAgg`), and no graph algorithm expressions (`GraphExpr`). The three-band query structure `B₁ ∘ B₂? ∘ B₃?` has no counterpart in ontological tooling.

**(iii) The schema annotation layer.** OWL axioms are logical assertions that drive inference. DGM's `Σ` carries operational metadata — `Grain`, `SCDType`, `BridgeSemantics`, `PipelineArtifact` — that drives execution planning, not logical inference. An OWL reasoner has no concept of "this node is SCD2 — apply a different temporal join strategy". The annotation layer is a planner concern without ontological precedent.

**(iv) Temporal logic over paths.** OWL-Time [12] describes temporal relationships between intervals as ontological assertions. DGM's `TemporalMode` (AG, AF, UNTIL, SINCE, Allen's 13 interval relations) is CTL/LTL model checking over the Kripke structure of the graph — a different computational apparatus from OWL inference entirely. The Kripke equivalence (§8.1) grounds DGM in model checking theory, not description logic.

**(v) Trail space analysis and question completeness.** The `RelationshipSubgraph` constructs, `TrailExpr` algorithms, `SIGNATURE_ENTROPY`, and the question completeness theorem (§8.11) have no counterpart in ontological frameworks. Ontologies represent what is true; DGM enumerates what can be measured and asked — a fundamentally different epistemic orientation.

The precise positioning: DGM is an **analytical graph model**, not a knowledge representation language. It borrows the graph primitive from RDF and the dimensional structure from Kimball-style data warehousing, then adds a formal query language grounded in CTL/LTL and an annotation-driven execution stack. It is closest to KG-OLAP [6] at the intersection of graphs and OLAP, but extends that framework with the typed schema discipline, the full CTL operator set, the planner/exporter stack, trail space analysis, and the question completeness result. A DGM graph can be exported to SPARQL as one of its query targets (§6.3), confirming the relationship while clarifying the distinction: DGM is the source formalism; SPARQL is one possible execution dialect.

Novel contributions added in v0.16: Endpoint type with Bound/Free variants; four-case RelationshipSubgraph endpoint lattice; forward cone via forward BFS; backward cone via BFS on G^T; full traversal space as SubgraphExpr; containment lattice theorem and cone intersection identity; REACHABLE_FROM/REACHABLE_TO as TrimCriterion; TrailExpr single-relaxation algorithms as NodeAlg; SIGNATURE_DIVERSITY; SIGNATURE_ENTROPY as Shannon entropy; global SubgraphAlg variants; granularity tier shift by endpoint case; Rule 9 cone containment optimisation; G^T at GraphStatistics load time; two-stage trail exploration flow; SIGNATURE_ENTROPY as recommender routing signal.

Novel contributions added in v0.17: `PipelineArtifact` composite annotation for backfill-incremental state machines; five-state artifact lifecycle modelled as `Grain(ACCUMULATING)` with implied `BridgeSemantics`; backfill/refresh asymmetry via `BridgeSemantics(SUPERSESSION)` vs `CAUSAL` on `In-flight → Failed` transition; `ADAPTIVE` write mode inferring `WritePlan` from `P(f).state` at planning time; planner Rule 10 for state-aware write planning and TTL expiry; scheduler loop expressed as composable DGM query pipeline (Q_backfill, Q_refresh, Q_delta monitoring); CTL LIVENESS/SAFETY/RESPONSE as formal scheduler correctness properties over the artifact graph; `UNTIL` operator distinguishing refresh failure (prior Complete preserved) from backfill failure (no prior artifact).

Novel contributions added in v0.18:

Novel contributions added in v0.19: Question algebra `Q_algebra(A, B)` as closure of `Q_valid` under CTE composition; semiring structure `(Q_algebra, UNION, INTERSECT, ∅, Q_top)`; Datalog equivalence theorem for `Q_algebra` under CTE composition; PTIME complexity bound via Immerman-Vardi; fixpoint semantics of recursive CTEs as descent paths in `L(G_AB)`; two-level predicate system (BDD for single-query, containment for cross-query); extended question completeness theorem (v0.19); planner Rule 11 cross-CTE CSE using BDD node id equality; compositional suggestion type (Correlation) in recommender; question chain suggestions as multi-step descent paths; Datalog reference [13] added; D23 design decision on CTE composition complexity.

Novel contributions added in v0.20: Query DAG as canonical form for `Q_algebra(A, B)`; BDD/query DAG parallel (variable ordering ↔ dependency partial order, sharing rule ↔ CSE, elimination rule ↔ semiring laws); canonical form theorem (two composed queries equivalent iff same DAG node id); two-level canonical form (BDD at leaves, query DAG at composition); construction algorithm with elimination + sharing rules; minimum CTE count as tight lower bound (= number of canonical DAG nodes); complexity stratification of containment checking (Band 1: NP-complete; monotone Band 2: polynomial; arbitrary Band 2: undecidable; recursive: Σ₁-hard); cross-session caching via DAG node id as cache key; query explanation via DAG decomposition; canonical-space recommender navigation (neighbourhood over DAG nodes); Rule 11 extended with full semiring elimination laws; D24 design decision on canonical form.

Novel contributions added in v0.21:

Novel contributions added in v0.22: §11.10 Implementation architecture — LangGraph state graph with five Pydantic-AI specialist agents; DGMContext as Pydantic-AI dependency injection carrier; Pydantic output schemas as executable projection of DGM formal types (EntityResolutionResult, TemporalClassificationResult, CompositionalDetectionResult, CandidateRankingResult, ExplanationRenderResult); TemporalModeEnum / TemporalPropertyEnum / QAlgebraOpEnum as closed-set Pydantic validators; five LLM nodes + six deterministic non-LLM nodes in the graph; explicit refinement and clarification cycles; MemorySaver HIL state persistence; bounded retry for hallucination prevention; parallel DAG leaf execution in STREAM mode; ontology-to-Pydantic formal mapping table; Phase 8 roadmap updated with specific implementation artefacts; D27 design decision. `ExecutionBudget` type with `BudgetDecision` taxonomy (`PROCEED`, `STREAM`, `ASYNC`, `REWRITE`, `SAMPLE`, `PAGINATE`, `CLARIFY`, `REJECT`); pre-execution gate (§6.3) as formal decision procedure between planner and execution; four graceful handling strategies (cost-aware rewriting via strategy lattice, progressive streaming from DAG topology, sampling with Hoeffding error bounds, async execution via `PipelineArtifact` lifecycle); clarification messages grounded in recommender lattice refinements; §11 Natural Language Interface — five-layer architecture; LLM responsibilities narrowed to five tasks (entity resolution, candidate ranking, temporal classification, compositional detection, explanation rendering); `EntityRegistry` as entity/relationship/temporal vocabulary; closed `PropRef` vocabulary for hallucination prevention; BDD-validated candidate generation for LLM ranking; temporal intent classification to CTL operators; compositional intent detection to Q_algebra operations; confirmation/refinement loop as lattice navigation; ambiguity resolution grounded in question space positions; schema evolution handling via Q_delta; sample-parallel async execution pattern; D25 and D26 design decisions. Question completeness theorem (§8.11) — `Q_valid(A, B)` finite and enumerable from `G_AB` and `Σ|_AB`; question lattice `L(G_AB)` with tautological top, contradictory bottom, and three specificity dimensions; formal characterisation of recommender as guided navigator of `L(G_AB)`; semantic scoring theorem — trail metrics induce non-uniform distribution over `L(G_AB)` without enumeration; `BETWEENNESS_CENTRALITY` as GroupBy pivot signal; `DOMINANT_SIGNATURE` as predicate seed; ontology relationship paragraph positioning DGM against RDF/OWL/SPARQL/OWL-Time along five axes; references [10] RDF, [11] OWL, [12] OWL-Time; D22 design decision on DGM/ontology distinction.

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
**D20** — PipelineArtifact is syntactic sugar, not a new fact type. The backfill-incremental state machine is modelled entirely within existing constructs: Grain(ACCUMULATING), BridgeSemantics, WritePlan, Q_delta(CHANGED_PROPERTY), and CTL temporal properties. PipelineArtifact assembles these into one declaration and infers semantics from P(f).state at planning time (planner Rule 10). Follows the same pattern as TemporalProperty classes (D9).
**D21** — Backfill and refresh require different WritePlan modes by construction. Backfill (APPEND) has no prior artifact — failure leaves the window durably in Failed. Refresh (MERGE) preserves the prior Complete via atomic swap — failure regresses to Stale, not Failed. ADAPTIVE mode makes this automatic from P(f).state at planning time.
**D22** — DGM shares the graph primitive with RDF but diverges above it. The triple / typed directed labeled edge is the correct foundational structure — DGM and RDF arrived at it independently. The divergence is complete above that primitive: DGM adds the dim/fact typed split, the three-band analytical query structure, the annotation-driven planner, CTL/LTL temporal logic, trail space analysis, and the question completeness theorem — none of which exist in ontological tooling.
**D23** — CTE composition extends `Q_valid` to `Q_algebra` (Datalog-equivalent, PTIME-bounded). The BDD covers single-query predicate optimisation; cross-CTE CSE (Rule 11) extends it to multi-query scope. The D17 restriction (no `Free→Free` in `Where PathPred`) is not arbitrary — it is the boundary that keeps `Q_algebra` within PTIME. Allowing unrestricted fixpoints in the filter position would push DGM toward the complexity class of stratified Datalog with negation, where decidability is more fragile.
**D24** — The query DAG canonical form is the composition-level analogue of the BDD. The analogy is exact: variable ordering ↔ dependency partial order; sharing rule ↔ CSE; elimination rule ↔ semiring laws. Containment-based elimination is applied only for Band 1 leaf nodes (NP-complete but tractable in practice). For Band 2/3 nodes, only syntactic laws are applied — this is the conservative choice that preserves correctness at the cost of some missed simplifications. Cross-session caching via DAG node id is the semantic-level generalisation of SQL text caching; it correctly identifies equivalent questions expressed in different syntactic forms.
**D25** — The pre-execution gate produces `BudgetDecision` rather than raising errors or blocking. Every failure mode has a graceful path: cost explosion → rewrite or clarify; result explosion → paginate; slow pre-computation → approximate; very expensive → async. `REJECT` is reserved for hard ceilings only — most queries reach `PROCEED`, `STREAM`, or `REWRITE`. The clarification path surfaces lattice refinements, not generic "simplify your query" messages — this is only possible because `Q_valid(A, B)` is formally defined.
**D26** — The NL interface does not generate queries from the LLM. It navigates a formally bounded question space. The LLM's five responsibilities are each a constrained mapping task — entity resolution against a closed vocabulary, candidate ranking over pre-validated options, pattern classification to a closed CTL vocabulary, compositional detection, and structured explanation rendering. This architecture prevents the two failure modes of SQL-generation NL interfaces: hallucinated schema references (prevented by closed PropRef vocabulary) and semantically invalid queries (prevented by BDD-validated candidates).
**D27** — LangGraph is chosen for the NL interface workflow and Pydantic-AI for each specialist agent. This pairing is structurally forced, not arbitrary. LangGraph handles the two properties that linear pipelines cannot: stateful cyclic workflows (the refinement and clarification cycles) and human-in-the-loop checkpoints with multi-turn state persistence. Pydantic-AI handles the two properties that unstructured LLM calls cannot: closed-vocabulary output enforcement (DGM formal types as Pydantic schemas) and bounded retry on validation failure. The majority of the LangGraph graph nodes (6 of 11) are deterministic non-LLM DGM computation — only 5 nodes invoke LLMs, each with a formally bounded output type. This ratio is intentional: the more computation that is formal and deterministic, the fewer guarantees depend on LLM reliability. The triple / typed directed labeled edge is the correct foundational structure — DGM and RDF arrived at it independently. The divergence is complete above that primitive: DGM adds the dim/fact typed split, the three-band analytical query structure, the annotation-driven planner, CTL/LTL temporal logic, trail space analysis, and the question completeness theorem — none of which exist in ontological tooling. DGM is an analytical graph model; ontologies are knowledge representation languages. The relationship is complementary, not competitive: DGM can export to SPARQL as one execution target.

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

[10] Lassila, O., Swick, R. R. (1999). Resource Description Framework (RDF) Model and Syntax Specification. W3C Recommendation. https://www.w3.org/TR/1999/REC-rdf-syntax-19990222/

[11] McGuinness, D. L., van Harmelen, F. (2004). OWL Web Ontology Language Overview. W3C Recommendation. https://www.w3.org/TR/owl-features/

[12] Cox, S., Little, C. (2022). Time Ontology in OWL (OWL-Time). W3C Recommendation. https://www.w3.org/TR/owl-time/

[13] Ceri, S., Gottlob, G., Tanca, L. (1989). What You Always Wanted to Know About Datalog (And Never Dared to Ask). *IEEE Transactions on Knowledge and Data Engineering*, 1(1), 146–166. https://doi.org/10.1109/69.43410

---

*— end of specification —*