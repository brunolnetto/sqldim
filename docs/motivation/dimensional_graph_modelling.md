# Dimensional Graph Model — Formal Specification
**Version 0.26 — Draft**

---

## Contents

1. [Overview](#1-overview)
2. [Graph Model](#2-graph-model)
   - 2.1 Structure and invariants
   - 2.2 Node types
   - 2.3 Edge types
   - 2.4 Schema annotation layer (Σ)
   - 2.5 Temporal properties and snapshots
   - 2.6 Relational mapping
3. [Traversal Paths](#3-traversal-paths)
   - 3.1 Unbound paths
   - 3.2 Bound paths and TemporalOrdering
   - 3.3 Path strategy and refinement order
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
   - 8.1 DGM as Kripke structure
   - 8.2 Functional completeness
   - 8.3 Minterms, maxterms, canonical form
   - 8.4 PathPred as exponential compression
   - 8.5 EXISTS/FORALL as minterm/maxterm duality
   - 8.6 Finite traversal space
   - 8.7 RelationshipSubgraph containment lattice
   - 8.8 Allen's interval algebra completeness
   - 8.9 Temporal property class hierarchy
   - 8.10 SIGNATURE_ENTROPY as information-theoretic measure
   - 8.11 Question completeness theorem
   - 8.12 Semantic scoring of the question lattice
   - 8.13 Question algebra and CTE composition
   - 8.14 Query DAG canonical form
   - 8.15 Parameter space
   - 8.16 Vocabulary decomposition and analytical space characterisation
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
12. [Supervisor Agent Architecture](#12-supervisor-agent-architecture)
    - 12.1 DGM as a composable tool
    - 12.2 Five-point tool contract
    - 12.3 Nine core tools
    - 12.4 Supervisor graph
    - 12.5 Budget propagation
    - 12.6 Six supervisor workflows
    - 12.7 Knowledge base with lattice-adjacent retrieval
    - 12.8 Meta-tool: question completeness theorem operationalized
    - 12.9 Relationship to §7 recommender

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
- **DGM is a Kripke structure** — nodes are states, edges are transitions, property bags are labels. The transposed graph G^T reverses adjacency: it enables backward-cone computation, and past-temporal model checking on the edge types that carry a temporal meaning.
- **Temporal queries** come in two forms: snapshot queries (Q) and delta queries (Q_delta).
- **A query planner** makes DGM-specific physical execution decisions informed by schema annotations, including cone containment optimisation (Rule 9) for relaxed-endpoint traversals.
- **A query exporter** translates physical plans to multiple query and sink targets.
- **A recommender** surfaces analytically useful next steps via a two-stage trail exploration flow grounded in endpoint relaxation.
- **ExecutionBudget and pre-execution gate** — a runtime policy layer between the planner and execution that intercepts expensive queries and redirects gracefully via cost-aware rewriting, progressive streaming, sampling with Hoeffding error bounds, or async execution with PipelineArtifact materialisation.
- **Natural language interface** — a five-layer architecture mapping NL utterances to formally bounded Q_valid(A,B) positions. The LLM navigates the canonical question lattice rather than generating SQL from scratch.
- **RDF 1.2 relationship formalised** — DGM and RDF 1.2 converge on the N-ary event representation problem (RDF 1.2 triple terms ≈ DGM fact nodes) while diverging on six formal axes: typed schema invariant, aggregation, operational annotation semantics, CTL temporal logic, trail space analysis, and enforcement strictness. The DGM SPARQL exporter encodes fact nodes more faithfully as RDF 1.2 matures.
- **LangGraph + Pydantic-AI implementation stack** — five specialist Pydantic-AI agents with output schemas derived from DGM formal types, orchestrated by a LangGraph state graph with explicit HIL checkpoints and cyclic refinement workflows. DGM's bounded vocabulary is enforced as Pydantic validators.
- **Medallion architecture formally mapped** — DGM sits at the Silver–Gold boundary and extends upward to the Diamond layer. Bronze is the data substrate captured by the `Provenance` annotation (§2.4); Silver is where the typed graph `G` is constructed; Gold is DGM query execution materialised via `WritePlan` to Delta/Iceberg sinks; Diamond is the formal semantic layer (`Q_valid`, recommender, NL interface, `ExecutionBudget`). The `MedallionLayer` annotation tags each node's layer origin for governance, lineage, and planner quality signalling.
- **Parameter space formalised** — each question `q ∈ Q_valid(A, B)` admits a parameter space `Π(q)` across six families (temporal, strategy, filter, aggregation, window, algorithm). The full analytical capability is the dependent product `A(A, B) = Σ_{q ∈ Q_valid} Π_valid(q)`. Annotation constraints project onto parameter space via `BDD_parametric`; the recommender navigates both structural and parametric dimensions; `ExecutionBudget` evaluates `cost(q, π)` jointly.

---

## 2  Graph Model

### 2.1  Structure and invariants

A DGM model has two levels. The **schema graph** `G_S` holds the types — entity types, fact types and relationship types. The **instance graph** `G_I` holds the occurrences — individual entities, individual events and the links between them. A **typing homomorphism** `h` assigns every occurrence to its type.

```
DGM = (G_S, G_I, h)

G_S = (N_S, E_S, τ_N, τ_E, K_S, Σ)      -- schema graph
G_I = (N_I, E_I, P)                      -- instance graph
h   = (h_N, h_E) : G_I → G_S             -- typing homomorphism
```

**Schema graph.**

| Symbol | Definition |
|---|---|
| N_S | Finite set of node types. |
| E_S ⊆ N_S×N_S×L | Set of directed labeled edge types. L is the finite set of verb labels. |
| τ_N : N_S → {dim, fact} | Node kind function. Partitions N_S into D_S (dimension types) and F_S (fact types). |
| τ_E : E_S → {verb, bridge} | Edge kind function. Partitions E_S into V_S (verb edge types) and B_S (bridge edge types). |
| K_S : N_S∪E_S → (K→Type) | Property signature. Maps every node type and edge type to its declared property keys and their value types. |
| Σ | Schema annotation set over G_S. See §2.4. |

**Instance graph.**

| Symbol | Definition |
|---|---|
| N_I | Finite set of node instances. |
| E_I ⊆ N_I×N_I×L | Set of directed labeled edge instances. |
| P : N_I∪E_I → (K→V) | Property function. Maps every node instance and edge instance to a key-value property bag. |

**Typing homomorphism.** `h_N : N_I → N_S` is total, and `h_E` is determined by it:

```
H1  (a, b, l) ∈ E_I  ⟹  h_E(a, b, l) = (h_N(a), h_N(b), l) ∈ E_S       -- edges map to edge types; labels are preserved
H2  dom P(x) ⊆ dom K_S(h(x))                                            -- only declared properties occur
H3  P(x)(k) ∈ K_S(h(x))(k) ∪ {NULL}                                     -- values inhabit the declared type
```

`h` is not required to be injective (many instances share a type) or surjective (a type may have no instances yet). Kinds are defined on `G_S` and inherited by instances through `h`:

```
kind(n) = τ_N(h_N(n))        D_I = h_N⁻¹(D_S)      F_I = h_N⁻¹(F_S)
kind(e) = τ_E(h_E(e))        V_I = h_E⁻¹(V_S)      B_I = h_E⁻¹(B_S)
```

Structural invariants are stated on the schema graph:

```
D_S ∩ F_S = ∅,   D_S ∪ F_S = N_S
V_S ∩ B_S = ∅,   V_S ∪ B_S = E_S
V_S ⊆ D_S × F_S × L     -- verb edge types: dim → fact only (schema direction)
B_S ⊆ N_S × N_S × L     -- bridge edge types: any node type → any node type
```

By H1 the same invariants hold on the instance graph with every subscript S replaced by I; they need not be checked separately.

**Two levels, one example each.** In a retail model, `Customer ∈ D_S` and `Sale ∈ F_S` are types and `(Customer, Sale, 'placed') ∈ V_S` is a verb edge type; the customer `c₄₂` and the sale `s₁₀₀₁` are instances with `h_N(c₄₂) = Customer`, `h_N(s₁₀₀₁) = Sale`, and `(c₄₂, s₁₀₀₁, 'placed') ∈ V_I`. In the BookClub model of §8.16.8, `Leitor`, `Livro` and `Genero` are the three elements of `N_S`; the ten individual readers and eight individual books are elements of `N_I`. `|N_S| = 3` whatever the number of readers.

**Level of each construction.** Every construction in this specification belongs to exactly one level. Where a later section writes an unsubscripted `N`, `E`, `D`, `F`, `V`, `B` or `G_AB`, it is read at the level given here.

| Construction | Level | Reads |
|---|---|---|
| τ_N, τ_E, K_S, Σ and every `SchemaAnnotation` argument (§2.4) | Schema | `D_S`, `F_S`, `B_S`, `N_S` |
| `Path`, `BoundPath`, `Anchor(node_type)`, labels, `Strategy`, `TemporalOrdering` (§3.1–§3.3, §5.2) | Schema — patterns over types | `G_S` |
| Aliases, join context `C`, path-local context `L`, path instances (§3.5) | Instance — bound to occurrences | `G_I` |
| Property bags `P`, temporal properties, snapshot `G(t)`, delta `ΔG` (§2.5) | Instance | `G_I` |
| Transposed graph `G^T`, Kripke structure (§8.1) | Instance | `G_I` |
| `RelationshipSubgraph` with `Bound`/`Free` endpoints, `TrimJoin`, cone identities (§3.4, §8.7) | Instance | `G_AB = G_AB^I` |
| `PathPred`, `PathAgg`, `TrailExpr`, `GraphExpr` evaluation (§4) | Instance — evaluated over path instances | `G_I` |
| `GraphStatistics`, cost estimates, planner thresholds on `|N|`, `|E|` (§6.2) | Instance | `|N_I|`, `|E_I|` |
| Finite traversal space (§8.6) | Instance | `|N_I|` |
| Predicate typing, `Ref` validity, structural BDD (§4.1, §6.1) | Schema | `K_S`, `Σ` |
| `Q_space`, `Q_valid(A, B)`, question lattice `L(G_AB)`, `Σ|_AB` (§8.11–§8.16) | Schema — `A, B ∈ N_S` | `G_AB = G_AB^S` |
| Recommender candidate generation and feasibility filtering (§7.1 Layers 1–2) | Schema | `G_AB^S`, `Σ` |
| Recommender scoring (§7.1 Layer 3, §8.12) | Instance | statistics of `G_I` |
| Closed `PropRef` vocabulary of the NL interface (§11) | Schema | `K_S` |

The two readings of `G_AB` are defined in §3.4.2 (instance) and §3.4.5 (schema) and related by the projection lemma of §3.4.5.

**Transposed graph.** G_I^T = (N_I, E_I^R, P) where E_I^R = { (b,a,l) | (a,b,l) ∈ E_I }, typed by h over the transposed schema graph G_S^T. Reverses all edge directions. It is a logical object: a reverse hop reads the same edge relation by its target key (§2.6). Reversed adjacency may additionally be materialised (O(|E_I|)) and shared across queries when the graph fits in memory. Used by the planner for Free→Bound backward-cone computation and by SINCE/ONCE/PREVIOUSLY TemporalMode SQL generation. Written G^T elsewhere. G^T is a reversal of adjacency and nothing more: following an edge backwards reaches a predecessor in path order, which is an earlier moment only where the edge type is time-ordered (§2.3).

**Kripke equivalence.** G_I is formally a Kripke structure K = (S, R, L) under S=N_I, R=E_I, L=P. G_I^T is the transpose of K. Restricted to time-ordered edge types it is the time-reversal of K, and past-temporal model checking runs on that restriction. See §8.1.

### 2.2  Node types

**Dimension nodes** — domain entities. A dimension type T ∈ D_S declares the attributes K_S(T); a dimension node d ∈ D_I with h_N(d) = T carries them in P(d), for filtering and grouping. May be SCD-versioned (SCDType annotation on the type, §2.4). Notation: d.attr.

**Fact nodes** — measurable events or states. A fact type T ∈ F_S declares measures in K_S(T); a fact node f ∈ F_I with h_N(f) = T carries them in P(f), with a timestamp or interval depending on the grain annotation of T (§2.4). Notation: f.measure, f.occurred_on.

Fact types may not be the source of verb edge types, hence fact nodes may not be the source of verb edges (§10.4, D6).

A fact node with no outgoing bridge edge is a terminal of every forward path (§4.1).

### 2.3  Edge types

Edge types live in G_S and edge instances in G_I; an instance (a, b, label) is admitted only if (h_N(a), h_N(b), label) is an edge type (H1, §2.1).

**Verb edges** v = (d, f, label). Schema direction: dim → fact. Query-time reverse traversal permitted via VerbHop⁻¹ (§3.2).

```
V_S ⊆ D_S × F_S × L        V_I ⊆ D_I × F_I × L
```

**Bridge edges** b = (n₁, n₂, label). The weight and validity window are properties of each bridge edge instance.

```
B_S ⊆ N_S × N_S × L        B_I ⊆ N_I × N_I × L
P(b).weight ∈ ℝ⁺ (optional; defaults to 1)
P(b).valid_from/to : Timestamp (optional)
```

| Source | Target | Typical use | Bridge semantics |
|---|---|---|---|
| dim | dim | Hierarchy, geography, organisation | STRUCTURAL |
| fact | fact | Causal chain, succession, supersession | CAUSAL/TEMPORAL/SUPERSESSION |
| fact | dim | Event-to-entity structural link | STRUCTURAL |
| dim | fact | Structural reference | STRUCTURAL |

**Direction is not time.** The direction of an edge type is a schema convention. A verb edge `(Customer, Sale, 'placed')` says who placed the sale; it does not say the customer precedes the sale, and a customer attribute read along that edge may have been written after the sale occurred. The same holds for STRUCTURAL bridges: a category is not earlier than its parent, nor a fact earlier than the entity it links to.

An edge type is **time-ordered** iff it is a bridge edge type annotated `BridgeSemantics(CAUSAL)`, `BridgeSemantics(TEMPORAL)` or `BridgeSemantics(SUPERSESSION)` (§2.4). Such an annotation asserts that, for every instance `(a, b)`, `a` occurs no later than `b`. No other edge type carries a temporal meaning: verb edge types and STRUCTURAL bridge edge types are never time-ordered, and nothing temporal may be inferred from their direction. A path is time-ordered iff every hop traverses a time-ordered edge type in its schema direction; a path with no hops (`NodeRef`) is trivially time-ordered.

### 2.4  Schema annotation layer (Σ)

Σ carries semantic metadata. It belongs to the schema graph: every annotation argument is a node type or edge type of G_S, never an instance, and an annotation on a type governs all instances that h maps to it. Annotations do not change graph topology or property bags.

```
Σ ⊆ SchemaAnnotation*

SchemaAnnotation ::=
    Conformed(d: D_S, fact_types: set[F_S])
  | Grain(f: F_S, grain: EVENT | PERIOD | ACCUMULATING)
  | SCDType(d: D_S, scd: SCD1|SCD2|SCD3|SCD6,
             versioned_attrs?: set[K], overwrite_attrs?: set[K], prev_attrs?: set[K])
  | Degenerate(f: F_S, key: K)                    -- key ∈ dom K_S(f)
  | RolePlaying(d: D_S, roles: list[L])
  | ProjectsFrom(d_mini: D_S, d_full: D_S)
  | FactlessFact(f: F_S)
  | DerivedFact(f: F_S, sources: list[F_S], expr: Expr)
  | WeightConstraint(b: B_S, constraint: ALLOCATIVE | UNCONSTRAINED)
  | BridgeSemantics(b: B_S, sem: CAUSAL|TEMPORAL|SUPERSESSION|STRUCTURAL)
  | Hierarchy(root: D_S, depth: ℕ | RAGGED, via?: L)
  -- Pipeline annotations
  | PipelineArtifact(f: F_S,
                     pipeline_id: K,
                     ttl: Duration,
                     backfill_horizon: Duration,
                     write_mode: BACKFILL | REFRESH | ADAPTIVE)
  -- Medallion layer annotation
  | MedallionLayer(n: N_S,
                   layer: BRONZE | SILVER | GOLD | DIAMOND,
                   promoted_at?: Timestamp)
  -- Provenance annotation
  | Provenance(target: N_S | E_S,
               record_source: K,                  -- String property: origin of the instance
               load_timestamp: K,                 -- Timestamp property: when it was loaded
               source_version?: K)                -- String property: batch, snapshot or commit of the origin
```

Key annotation semantics:

**Conformed(d, fact_types)** — d reachable via verb edges from each fact type. Enables constellation paths. Bound→Free TrailExpr can discover candidates empirically.

**Hierarchy(root, depth, via)** — `root` carries a self-referential bridge edge type `(root, root, via) ∈ B_S` relating a member to its parent. `via` may be omitted when `root` has exactly one self-referential bridge edge type. `depth = n` declares at most n levels; `RAGGED` declares no fixed depth. Traversal is written with Repeat (§3.2).

**Grain(f, grain)** — EVENT: additive; PERIOD: state (SUM invalid); ACCUMULATING: lifecycle row.

**SCDType** — SCD1: strip temporal resolution. SCD2: effective_from/to. SCD3: current/previous props. SCD6: per-attribute policy.

**Degenerate(f, key)** — the property `key` of fact type `f` is a degenerate dimension: a dimensional attribute carried by the fact itself, such as an order or invoice number, with no dimension node type behind it. It is declared in `K_S(f)` and stored as a column of the fact's NodeTable (§2.6). It is referenced as `PropRef(f.key)` and is groupable and filterable like any other fact property. It differs from a regular dimension in what it lacks: no node type, no verb edge, no attributes of its own, no SCD history, and it cannot be an `Anchor`, a path endpoint or the target of `Conformed`.

```
-- K_S(Sale) ⊇ { order_number: String, revenue: Real, occurred_on: Timestamp }
-- Σ ∋ Degenerate(Sale, order_number)

Join:    Anchor(Sale, s)
GroupBy: [ s.order_number ]
Agg:     lines := COUNT(s),  order_total := SUM(s.revenue)
-- one row per order; no Order dimension is created
```

**BridgeSemantics(CAUSAL)** — declares that the instances of the edge type form a DAG. The declaration is a claim about the data and is validated, not assumed:

```
Validation    At ingestion, and before a snapshot is published, the subgraph of G_I formed by all
              CAUSAL edge instances is checked for cycles by strongly connected components.
              A component with more than one node, or a self-loop, is a causal cycle.

Outcome       GraphStatistics.causal_acyclic : snapshot version → VALIDATED | VIOLATED(report) | UNKNOWN
              report = list[(component_id, nodes: list[N_I], edges: list[E_I])]
              VIOLATED: the batch is rejected and the snapshot is not published as causally valid.
              UNKNOWN:  validation has not run for that snapshot version.

Traversal     The acyclic optimisation (DAG BFS without a visited set, on G and G^T) is enabled only
              when causal_acyclic = VALIDATED for the snapshot version being queried. Otherwise CAUSAL
              edge types are traversed like any other, with cycle prevention.
              In both cases MAX_REPEAT_DEPTH (§3.2) applies, and a traversal cut by it is reported
              as truncated.
```

No rule removes cycle protection without the VALIDATED precondition. `COMMUNITY_LABEL(TARJAN_SCC)` over CAUSAL edges (§7.6) computes the same components as the validation and is the diagnostic for a VIOLATED or UNKNOWN snapshot; it runs with a visited set and terminates on cyclic input.

**PipelineArtifact(f, pipeline_id, ttl, backfill_horizon, write_mode)** — composite annotation. Implies `Grain(f, ACCUMULATING)`. Models five-state machine over `P(x).state ∈ {Missing, In-flight, Complete, Stale, Failed}` for each instance x of the fact type f. Transition bridge edges carry implied BridgeSemantics:
```
Missing    →[scheduled]→   In-flight   TEMPORAL
In-flight  →[succeeded]→   Complete    CAUSAL
In-flight  →[failed]→      Failed      CAUSAL  (SUPERSESSION if REFRESH run with prior Complete)
Complete   →[expired]→     Stale       TEMPORAL  (ttl-triggered)
Stale      →[rescheduled]→ In-flight   TEMPORAL
Failed     →[retried]→     In-flight   SUPERSESSION
```
`write_mode`: BACKFILL → always APPEND; REFRESH → always MERGE (atomic swap); ADAPTIVE → inferred from state (planner Rule 10).

Each artifact covers a window `[window_start, window_end)`; both are non-NULL Timestamp properties of the fact type. `backfill_horizon` bounds how far back a gap is still repaired. With `now` the evaluation instant and `H = backfill_horizon`, a window is **eligible for backfill** iff

```
state ∈ {Missing, Failed}   and   now − H ≤ window_end ≤ now
```

- The lower bound is inclusive: a window ending exactly at `now − H` is still eligible.
- The upper bound excludes windows that have not closed yet.
- A window with `window_end < now − H` is outside the horizon. It is not backfilled; if it is still Missing it violates the SAFETY assertion of Example 6 (§10.1), which uses the complementary strict inequality.
- `window_end` NULL makes both comparisons UNKNOWN, so the window is not selected; it is a contract violation of the annotation, not a gap.
- Timestamps are instants and `H` is an elapsed duration (90 days = 90 × 24 h), so eligibility does not depend on the time zone in which a window is displayed. A source that stores zone-less local times is normalised to instants at ingestion.

**Provenance(target, record_source, load_timestamp, source_version)** — states where the instances of a node type or edge type come from. It applies to a type; the provenance itself is per instance, held in the named properties, which must be declared in `K_S(target)` and non-NULL on every instance. For a versioned node type (SCD2, SCD6) each version row has its own values.

- `record_source` identifies the origin: the system and object the instance was read from.
- `load_timestamp` is when the instance, or version, was recorded. It is transaction time and is independent of `effective_from/to`: a snapshot `G_I(t)` (§2.6) is decided by validity, never by `load_timestamp`.
- `source_version` identifies the batch, snapshot or commit of the origin that supplied it.

Provenance is not implied by `MedallionLayer`. The layer says how refined a type is; Provenance says which source produced each of its instances. A SILVER type normally carries both.

```
-- Σ ∋ MedallionLayer(Customer, SILVER)
-- Σ ∋ Provenance(Customer, record_source = src_system, load_timestamp = loaded_at, source_version = src_batch)

version_key  entity_key  segment    effective_from  src_system      loaded_at             src_batch
101          C42         retail     2023-01-01      crm.customers   2023-01-01T02:05:00Z  crm-2023-01-01
102          C42         wholesale  2024-03-01      crm.customers   2024-03-01T02:10:00Z  crm-2024-03-01
```

Operational consequences: (1) the exporter carries the three properties into `DGM_JSON` output and into every `WritePlan` that copies instances of the target, so provenance survives promotion between layers; (2) the recommender does not offer provenance properties as GroupBy or measure suggestions; (3) `Q_delta` may filter on `load_timestamp` to select what a given load changed.

**MedallionLayer(n, layer, promoted_at)** — Tags which Medallion layer the instances of a node type originate from. Operational consequences: (1) Planner warns when `PathPred` traversal crosses a node with `MedallionLayer(BRONZE)` — raw data in analytical paths may be unvalidated. (2) Recommender suppresses `BRONZE` nodes from `GroupBy` suggestions. (3) Exporter tags `DGM_JSON` output with the minimum layer of all nodes in the query — touching Bronze nodes gets tagged `min_layer: BRONZE`.

**Σ is an operational decision table**: every annotation drives specified planner, recommender, and exporter behaviour.

### 2.5  Temporal properties and snapshots

```
P(n).effective_from/to : Timestamp (NULL = open)
P(e).valid_from/to     : Timestamp (NULL = open)
```

Temporal properties belong to instances. Snapshot G(t) = G_I(t): the node and edge instances valid at t, typed by the restriction of h; G_S is the same for every t. Delta ΔG(t₁, t₂): N_added, N_removed, E_added, E_removed, P_changed — sets of instances. §2.6 defines both as relational expressions.

### 2.6  Relational mapping

`G_S` and `G_I` are a logical view over relations. Nothing in this specification requires the instance graph to be loaded: every node, edge, snapshot and traversal is defined here as a relational expression over the tables that hold the data.

```
M = (M_N, M_E)          M_N : N_S → NodeMapping          M_E : E_S → EdgeMapping

NodeMapping ::= NodeTable(relation: R,
                          entity_key: list[Col],          -- identity of the node across versions
                          version_key?: Col,              -- identity of one version (surrogate key)
                          props: K → Col,                 -- one column per key of K_S
                          effective_from?: Col, effective_to?: Col)

EdgeMapping ::= ForeignKey(holder: SOURCE | TARGET,       -- which endpoint's NodeTable stores the column
                           columns: list[Col],
                           references: ENTITY_KEY | VERSION_KEY)
              | RelationTable(relation: R,
                              source: list[Col], target: list[Col],
                              references: ENTITY_KEY | VERSION_KEY,
                              props: K → Col,
                              valid_from?: Col, valid_to?: Col)
              | Derived(view: R,                          -- read-only
                        source: list[Col], target: list[Col],
                        valid_from?: Col, valid_to?: Col)
```

**Mapping table.**

| `G_S` construct | Relational artefact | Notes |
|---|---|---|
| Dimension type `T ∈ D_S` | `NodeTable` — table or view | `entity_key` is the natural (durable) key. With `SCDType(T, SCD2 \| SCD6)`: one row per version, `version_key`, `effective_from`, `effective_to`. Otherwise one row per entity. |
| Fact type `T ∈ F_S` | `NodeTable` — table or view | `entity_key` is the grain key. Measures and timestamps are `props`. |
| Property key `k ∈ dom K_S(T)` | Column of the NodeTable | Column type conforms to `K_S(T)(k)`. |
| Verb edge type, N:1 from fact to dimension | `ForeignKey(holder = TARGET)` | A column of the fact table referencing the dimension. One column per verb label; role-playing dimensions have one column per role. |
| Verb edge type, N:N (multi-valued dimension) | `RelationTable` | One row per (dimension, fact) pair. |
| Bridge edge type, N:1 (hierarchy parent, succession) | `ForeignKey(holder = SOURCE)` | A column of the source table referencing the target, e.g. `employee.manager_key`. |
| Bridge edge type with weight, validity window, properties, or N:N multiplicity | `RelationTable` — bridge table | `weight`, `valid_from`, `valid_to` and `K_S(e)` are columns. |
| Edge type computed from other data | `Derived` — view | Pairs are defined by a query. Read-only: never a `WritePlan` target. |
| Edge property key `k ∈ dom K_S(e)` | Column of the RelationTable or Derived view | A ForeignKey edge type has no properties. |
| Annotations `Σ` | Not stored in data relations | Held with the mapping as schema metadata. |

**Mapping invariants.**

```
M1  M_N and M_E are total: every node type and edge type of G_S has a mapping.
M2  A NodeTable declares effective_from, effective_to and version_key iff its type is SCD2 or SCD6.
    The version intervals of one entity_key are half-open [effective_from, effective_to) and
    pairwise disjoint; effective_to = NULL is an open interval.
M3  ForeignKey: each row of the holder yields at most one edge instance; a NULL column yields none.
    It carries no validity window and no edge property.
M4  An edge type that declares properties, a weight or a validity window, or that is N:N,
    is mapped by RelationTable or Derived.
M5  A reference that matches no row of the referenced NodeTable yields no edge instance.
M6  references = VERSION_KEY stores a version surrogate; the endpoint is the entity that version
    belongs to. Whether a traversal then reads the stored version or the version valid at as_of
    is the SCD resolution policy (§2.5), not part of the mapping.
```

**The instance graph as a view.**

```
N_I restricted to type T   = the distinct entity_key values of M_N(T)
h_N(n)                     = the type whose NodeTable holds n
E_I restricted to type e   = the (source, target) pairs produced by M_E(e), subject to M5
P(n), P(e)                 = the props columns of the row; for a versioned node, of the version in force
```

A node instance is an entity, not a version row. A versioned entity has one property bag per version; which one is `P(n)` is fixed by the instant of evaluation.

**Version and validity at an instant.**

```
version(T, key, t)  = the row r of M_N(T) with r.entity_key = key
                      and r.effective_from ≤ t and (r.effective_to IS NULL or t < r.effective_to)
                      -- at most one row, by M2; for an unversioned type, the single row of key

valid(e, t)         = (e.valid_from IS NULL or e.valid_from ≤ t)
                      and (e.valid_to IS NULL or t < e.valid_to)
                      -- TRUE for every t when the mapping declares no validity columns
```

Both windows are half-open: an instant equal to `effective_to` or `valid_to` belongs to the next version or to no validity. The snapshot of §2.5 is then

```
N_I(t) = { n | version(h_N(n), n, t) exists }
E_I(t) = { e = (a, b, l) ∈ E_I | valid(e, t) and a, b ∈ N_I(t) }
P_t(n) = props of version(h_N(n), n, t)
```

A node with no version at `t` is absent from `G_I(t)`; `TemporalContext.node_resolution` (§5.1) decides whether a query that reaches it is rejected (STRICT) or drops the tuple (LAX).

**Traversal applies validity per hop.** A hop is a join between the current frontier and one edge relation, with `valid(e, t)` and `version(·, ·, t)` in the join condition. For a bridge table and a forward hop:

```sql
SELECT b.target_key
FROM   frontier f
JOIN   bridge_e b ON b.source_key = f.node_key
                 AND (b.valid_from IS NULL OR b.valid_from <= :as_of)
                 AND (b.valid_to   IS NULL OR :as_of < b.valid_to)
JOIN   node_t  n ON n.entity_key = b.target_key
                 AND n.effective_from <= :as_of
                 AND (n.effective_to IS NULL OR :as_of < n.effective_to)
```

The reverse hop is the same statement with `source_key` and `target_key` exchanged. A `Repeat` (§3.2) is this join as the recursive step of a recursive CTE. `G^T` is therefore not a second structure: it is the same edge relations read by their target key. An implementation may additionally keep a reversed adjacency list in memory for graphs that fit there; it is an optimisation, never a precondition.

No step reads rows outside the frontier's neighbourhood, so memory is bounded by the frontier and the visited set — not by `|N_I| + |E_I|` — and an edge invalid at `as_of` is never followed.

**SCD2 at two instants.** `SCDType(Customer, SCD2)`; the verb edge type `(Customer, Sale, 'placed')` is `ForeignKey(holder = TARGET, columns = [customer_key], references = ENTITY_KEY)`.

```
dim_customer                                              fact_sale
version_key  entity_key  segment    effective_from  effective_to     sale_id  customer_key  occurred_on  revenue
101          C42         retail     2023-01-01      2024-03-01       S1       C42           2023-06-10   600
102          C42         wholesale  2024-03-01      NULL
```

| `as_of` | `version(Customer, C42, as_of)` | `c.segment` reached from `S1` |
|---|---|---|
| 2022-12-31 | none — `C42 ∉ N_I(t)` | STRICT: rejected; LAX: tuple dropped |
| 2023-12-31 | 101 | `retail` |
| 2024-03-01 | 102 — the boundary instant belongs to the new version | `wholesale` |
| 2024-06-30 | 102 | `wholesale` |

The edge `(C42, S1, 'placed')` has no validity window and is present at every instant; only the version read at its dimension endpoint changes.

**Reverse traversal with edge validity.** `(Employee, Employee, 'reports_to')` is a `RelationTable` with a validity window:

```
bridge_reports_to
source_key  target_key  valid_from  valid_to
E7          M1          2023-01-01  2024-01-01
E7          M2          2024-01-01  NULL
```

"Direct reports of M1" is a reverse hop — rows with `target_key = 'M1'` and `valid(e, as_of)`. At `as_of = 2023-06-30` it returns `E7`; at `as_of = 2024-06-30` it returns nothing, and the same hop from `M2` returns `E7`. Each answer reads the rows for one target key through the index I4 below.

**Minimum physical access paths.** Each is an index, or on a columnar store without secondary indexes, a clustering or sort order on the same leading columns.

| # | Relation | Leading columns | Serves |
|---|---|---|---|
| I1 | NodeTable | `entity_key`, `effective_from` | `version(T, key, t)`; fact → dimension hop |
| I2 | NodeTable holding a ForeignKey | the foreign key columns | hop from the referenced node to the holders (dimension → facts) |
| I3 | RelationTable | `source`, `valid_from` | forward hop |
| I4 | RelationTable | `target`, `valid_from` | reverse hop |
| I5 | NodeTable with `version_key` | `version_key` | resolving a VERSION_KEY reference to its entity (M6) |

**Cost.** With these access paths:

| Operation | Cost |
|---|---|
| `version(T, key, t)` | O(log \|rel(T)\|) — the last version starting at or before `t`, then one check of `effective_to` |
| One hop from one node, forward or reverse | O(log \|rel(e)\| + d), `d` the number of edge rows of that node |
| Traversal visiting node set `X` and edge rows `Y` | O((\|X\| + \|Y\|) · log \|E_I\|) time; O(\|X\|) memory for the visited set |
| Materialising reversed adjacency (optional) | O(\|E_I\|) time and memory, once |

A mapping that lacks an access path is still valid, but the hop it serves degrades to a scan of the relation, O(|rel(e)|) per step. The planner costs it as such (§6.2) and `GraphStatistics` records which access paths exist. A `Derived` edge type costs whatever its view costs; it has an access path only if the view is materialised with one.

---

## 3  Traversal Paths

### 3.1  Unbound paths

A path is a pattern over the schema graph: its arguments are node types and each hop names an edge type of E_S.

```
Path ::= NodeRef(n: N_S)
       | VerbHop(d: D_S, label: L, f: F_S)
       | VerbHop⁻¹(f: F_S, label: L, d: D_S)    -- reverse: fact → dim (query-time)
       | BridgeHop(n₁: N_S, label: L, n₂: N_S)
       | Compose(Path, Path)
       | Repeat(path: Path, min: ℕ, max: ℕ | UNBOUNDED, mode?: RepeatMode)    -- §3.2
```

A **path instance** of a pattern π is a sequence of node and edge instances of G_I whose image under h is π, hop by hop. Patterns are finite objects of the schema; the number of instances of a pattern depends on the data.

### 3.2  Bound paths and TemporalOrdering

A BoundPath is a Path whose positions carry aliases. The pattern stays at the schema level; each alias is bound, per path instance, to the node or edge instance occupying that position.

```
BoundPath ::= NodeRef(n: N_S)
            | VerbHop(d,label,f, node_alias?,edge_alias?)
            | VerbHop⁻¹(f,label,d, node_alias?,edge_alias?)
            | BridgeHop(n₁,label,n₂, node_alias?,edge_alias?)
            | Compose(BoundPath, BoundPath, temporal?: TemporalOrdering)
            | Repeat(path: BoundPath, min: ℕ, max: ℕ | UNBOUNDED,
                     mode?: RepeatMode, depth_alias?: A)

RepeatMode ::= SIMPLE | TRAIL | WALK        -- default SIMPLE

TemporalOrdering ::=
  -- Point-based (left_ts, right_ts: TimePoint; tolerance: SExpr of type Duration):
    BEFORE(left_ts,right_ts) | AFTER(left_ts,right_ts)
  | CONCURRENT(left_ts,right_ts,tolerance?)
  -- Allen's 13 interval relations (l, r: Interval):
  | MEETS(l,r) | MET_BY(l,r) | OVERLAPS(l,r) | OVERLAPPED_BY(l,r)
  | STARTS(l,r) | STARTED_BY(l,r) | DURING(l,r) | CONTAINS(l,r)
  | FINISHES(l,r) | FINISHED_BY(l,r) | EQUALS(l,r)
  -- LTL operators:
  | UNTIL(hold_pred: Pred, trigger_pred: Pred)
  | SINCE(hold_pred: Pred, trigger_pred: Pred)

TimePoint ::= SExpr                         -- of type Timestamp or Date (§4.1)
Interval  ::= (start: TimePoint, end: TimePoint)   -- same type at both ends
```

Allen's relations apply to PERIOD/ACCUMULATING grain facts. UNTIL(φ,ψ): φ holds at every intermediate hop until first ψ. SINCE is its mirror image over predecessors, and is admitted only on time-ordered paths (§4.1).

**Path order and clock time.** A path instance has two independent orderings, and the specification never derives one from the other.

```
PathPosition   the index i ≥ 0 of a node in a path instance: 0 is the start node, i the node
               reached after i hops. Path order is the order of PathPositions. It is structural.

Clock time     the values of Timestamp, Date and Interval properties. Clock order is asserted
               only by a TemporalOrdering, which compares typed TimePoints or Intervals.
```

- The point-based and Allen constructors of TemporalOrdering compare clock values named explicitly by their arguments. Both TimePoints of a point-based constructor have the same type; a Date is not compared with a Timestamp. They constrain the instance whatever the direction of the hops between the two aliases.
- Path order alone never implies clock order. `Compose(VerbHop(c,'placed',s), …)` puts `c` at an earlier PathPosition than `s`; it does not make any timestamp of `c` earlier than `s.occurred_on`. To require that, the query states `BEFORE(c.effective_from, s.occurred_on)`.
- On a time-ordered path (§2.3) the two orderings agree by annotation: a later PathPosition is no earlier in time. Only there may path order be read as "before" and "after".

Example — a hop dim → fact carries no chronology:

```
Compose(VerbHop(Customer,'placed',Sale, node_alias=s),
        BridgeHop(Sale,'refunded_by',Refund, node_alias=r))        -- BridgeSemantics(refunded_by, CAUSAL)

-- PathPositions: customer 0, s 1, r 2.
-- s → r is time-ordered: the sale occurs no later than its refund.
-- customer → s is not: the customer version joined here may have become effective after the sale.
-- The path as a whole is not time-ordered, so ONCE, SINCE and PREVIOUSLY are rejected on it.
```

**Constellation path** — two-hop via conformed dimension:

```
Compose(VerbHop⁻¹(f₁,verb₁,dim,node_alias=shared_dim),
        VerbHop(shared_dim,verb₂,f₂,node_alias=fact2))
-- valid when Conformed(dim,{type(f₁),type(f₂)}) ∈ Σ
```

Alias uniqueness enforced at construction: C ∩ L ≠ ∅ rejected.

**Repetition.** `Repeat(path, min, max)` matches `path` traversed k times in sequence for some k with min ≤ k ≤ max. It is the only recursion construct of BoundPath: variable-depth traversal, transitive closure and ragged hierarchies are all written with it.

```
Repeat(p, 0, UNBOUNDED)     -- 0..N hops: reflexive-transitive closure
Repeat(p, 1, UNBOUNDED)     -- 1..N hops: transitive closure
Repeat(p, m, n)             -- bounded interval, m ≤ n
Repeat(p, n, n)             -- exactly n
```

Well-formedness, checked at construction on the schema graph:

```
R1  The body p ends at the node type it starts from, so that it composes with itself.
R2  0 ≤ min ≤ max and max ≥ 1. UNBOUNDED is admitted only as max.
R3  The body contains no Repeat. Repeats may follow one another under Compose.
R4  mode = WALK requires a finite max.
```

`mode` fixes what an instance may revisit *within the repeated segment*:

| Mode | Node instance repeated | Edge instance repeated | With `UNBOUNDED` |
|---|---|---|---|
| SIMPLE (default) | no | no | terminates: at most `|N_I| − 1` iterations |
| TRAIL | yes | no | terminates: at most `|E_I|` iterations |
| WALK | yes | yes | rejected (R4) — a cycle would give infinitely many instances |

A cycle in the data therefore never makes a Repeat diverge: under SIMPLE and TRAIL the cycle is traversed at most once per instance, and under WALK the iteration count is bounded by `max`. Independently of the mode, an implementation enforces a ceiling `MAX_REPEAT_DEPTH` on the iteration count. A traversal cut by the ceiling rather than by exhaustion is reported as truncated; it is never returned as if complete.

With `min = 0` the zero-iteration instance is the start node alone, with no edge.

**Aliases under Repeat.** An alias declared inside the body is bound once per iteration. Inside the path — in a `sub_filter`, a `PathAgg` argument or a TemporalOrdering — it denotes the node or edge of the current iteration, and each iteration's end node is one evaluation position (§4.1), in iteration order; with `min = 0` the start node is the first position. Outside the path, a body `node_alias` denotes the node at which the instance ends: the last iteration's end node, or the start node for zero iterations. A body `edge_alias` is not visible outside the path. `depth_alias` binds the iteration count k of the instance, of type Int, and has the same visibility as the body `node_alias`.

**Strategy.** Strategy (§3.3) selects among the instances of the whole BoundPath after repetition is expanded: ALL keeps every instance of every admitted length; SHORTEST and K_SHORTEST order instances by total hop count; MIN_WEIGHT by total bridge weight. Strategy does not change `mode`.

**Hierarchies.** `Hierarchy(root, depth, via)` (§2.4) names a self-referential bridge edge type `(root, root, via) ∈ B_S`. Its traversal is a Repeat over that hop:

```
Hierarchy(root, depth = n, via)     ↦  Repeat(BridgeHop(root, via, root), 1, n)
Hierarchy(root, RAGGED,    via)     ↦  Repeat(BridgeHop(root, via, root), 1, UNBOUNDED, mode=SIMPLE)
```

Example — a ragged reporting line, every manager above an employee with the number of levels between them:

```
-- Σ: Hierarchy(Employee, RAGGED, via='reports_to')
Join: Anchor(Employee, e)
      PathJoin(e,
               Repeat(BridgeHop(Employee, 'reports_to', Employee, node_alias=mgr),
                      min=1, max=UNBOUNDED, mode=SIMPLE, depth_alias=lvl),
               mgr)
GroupBy: [ e.id ]
Agg: levels_to_top := MAX(lvl)

-- "reports, at any distance, to a vice-president":
PathPred(e,
         Repeat(BridgeHop(Employee, 'reports_to', Employee, node_alias=m), 1, UNBOUNDED),
         EXISTS, ALL, EVENTUALLY,
         ScalarPred(PropRef(m.title), (= "VP")))
```

Reachability operators — `TrimJoin(REACHABLE_FROM | REACHABLE_TO | REACHABLE_BETWEEN)` and TrailExpr with `max_depth` — are not path patterns. They range over every edge type of the subgraph and are defined by reachability (§3.4), not by a BoundPath; they do not provide a second way to write a recursive pattern.

### 3.3  Path strategy and refinement order

```
Strategy ::= ALL | SHORTEST | K_SHORTEST(k: ℕ) | MIN_WEIGHT        -- k ≥ 1
```

A strategy selects a subset of `I`, the set of instances of a BoundPath for one binding of its endpoints (one tuple of C). Selection is deterministic: it depends only on the snapshot, never on physical row order, parallelism or backend.

```
len(π)     number of hops of instance π
weight(π)  sum of P(b).weight over its bridge hops; a verb hop and an unweighted bridge count 1
key(π)     the sequence of its edge instances, each written (label, source entity_key, target entity_key)
           with the entity keys of §2.6; keys are compared lexicographically

π ≺ π'     iff  (len(π), key(π)) < (len(π'), key(π'))  lexicographically        -- a total order on I
```

| Strategy | Selects | Cardinality |
|---|---|---|
| ALL | every instance | \|I\| |
| SHORTEST | the ≺-least instance — fewest hops; ties broken by `key` | 1 if I ≠ ∅, else 0 |
| K_SHORTEST(k) | the k ≺-least instances | min(k, \|I\|) |
| MIN_WEIGHT | the instance least in (weight, len, key) | 1 if I ≠ ∅, else 0 |

SHORTEST returns one path, not every path of minimum length. When several instances tie on length, the one with the least `key` is returned, and it is the same one on every execution over the same snapshot. A caller that needs all minimum-length paths uses ALL with a filter on length, or K_SHORTEST.

Single-instance (SHORTEST, MIN_WEIGHT): promote: true compatible — the cardinality bound of 1 holds by the tie-break, not by an assumption about the data.
Multi-instance (ALL, K_SHORTEST): explicit fan-out.

**Refinement order.** Write `s ⊑ s'` when `s(I) ⊆ s'(I)` for every instance set I. Because all strategies but MIN_WEIGHT select prefixes of the same order ≺:

```
SHORTEST = K_SHORTEST(1) ⊑ K_SHORTEST(k) ⊑ K_SHORTEST(k') ⊑ ALL        for 1 ≤ k ≤ k'
MIN_WEIGHT ⊑ ALL
```

`⊑` is a partial order with greatest element ALL. MIN_WEIGHT is incomparable with every K_SHORTEST(k): the lightest path need not be among the k shortest. Any two strategies have a least upper bound (the larger k, or ALL), but MIN_WEIGHT and K_SHORTEST(k) have no common lower bound, so the order is not a lattice and is not called one. A rewrite that replaces a strategy by one below it (§6.3) returns a subset of the original instances.

### 3.4  RelationshipSubgraph

#### 3.4.1  Endpoint type

```
Endpoint ::= Bound(alias: A)   -- fixed node, resolved in C
           | Free               -- all nodes satisfying traversal criterion
```

#### 3.4.2  Extended definition

RelationshipSubgraph is evaluated on the instance graph. Throughout §3.4.2–§3.4.4, A and B denote the node instances bound to the endpoint aliases in the current tuple of C, N and E abbreviate N_I and E_I, and G_AB abbreviates G_AB^I.

```
RelationshipSubgraph(source: Endpoint, target: Endpoint, strategy: Strategy)
  → G_src_tgt = (N_src_tgt, E_src_tgt, P)        -- subgraph of G_I, typed by the restriction of h
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

#### 3.4.5  Schema-level subgraph and projection

The question space (§8.11) is defined over types, not occurrences. For node types `A, B ∈ N_S` the schema-level relationship subgraph is taken by reachability in G_S:

```
G_AB^S = (N_AB^S, E_AB^S, τ_N, τ_E, K_S, Σ|_AB)

N_AB^S = { T ∈ N_S | A →* T →* B in G_S }
E_AB^S = { (T, U, l) ∈ E_S | A →* T  and  U →* B in G_S }
Σ|_AB  = { σ ∈ Σ | every type named by σ lies in N_AB^S or E_AB^S }
```

Reachability uses walks: a type may be visited more than once, since a simple path of instances can pass through several instances of the same type (reader → reader → book). `G_AB^S` depends only on G_S.

**Projection lemma.** For node instances a, b ∈ N_I with h_N(a) = A and h_N(b) = B, h maps G_ab^I into G_AB^S:

```
h_N(N_ab^I) ⊆ N_AB^S        h_E(E_ab^I) ⊆ E_AB^S
```

Proof: let n lie on a simple path a →* n →* b in G_I. By H1 every edge of the path maps to an edge type with the same label, so the image is a walk A →* h_N(n) →* B in G_S; hence h_N(n) ∈ N_AB^S. The same walk places the image of each path edge in E_AB^S. ∎

Consequently every path instance evaluated between a and b is an instance of a pattern over `G_AB^S`. A question formulated over `G_AB^S` is well-typed for every pair of instances of (A, B), and no instance pair gives rise to a question outside it. The inclusion may be strict: a type in `N_AB^S` need not have an instance on any path between a particular a and b.

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
Pred ::= ScalarPred(cond: ScalarCond)             -- ScalarCond defined below
       | PathPred(alias: A, path: BoundPath,
                  quantifier: Quantifier,
                  strategy: Strategy,
                  temporal_mode?: TemporalMode,
                  promote?: Bool,
                  on_empty?: REQUIRE_WITNESS | VACUOUS,     -- FORALL only; default REQUIRE_WITNESS
                  sub_filter: Pred)
       | SignaturePred(path_alias: A,
                       sequence: list[L | WILDCARD],
                       match: EXACT|PREFIX|CONTAINS|REGEX)

Quantifier   ::= EXISTS | FORALL

TemporalMode ::= EVENTUALLY | GLOBALLY | NEXT
               | UNTIL(φ: Pred) | SINCE(φ: Pred)
               | ONCE | PREVIOUSLY
```

**FORALL elimination.** FORALL is eliminated at construction, leaving only EXISTS-quantified PathPreds. Without a `temporal_mode` this is De Morgan over the finite set of path instances:

```
PathPred(FORALL, ..., φ)  ≡  NOT PathPred(EXISTS, ..., NOT φ)
```

In this construction and in those below, the inner `NOT φ` under EXISTS stands for `NotTrue(φ)` — the complement of φ's truth set (§6.1) — so that a position where φ is UNKNOWN counts against a universal condition.

With a `temporal_mode` the quantifier cannot be swapped alone: CTL duality dualises the temporal operator together with the path quantifier. Writing φ for `sub_filter`:

| FORALL form | CTL duality | Construction |
|---|---|---|
| FORALL, GLOBALLY | AG φ = ¬EF ¬φ | NOT PathPred(EXISTS, EVENTUALLY, NOT φ) |
| FORALL, EVENTUALLY | AF φ = ¬EG ¬φ | NOT PathPred(EXISTS, GLOBALLY, NOT φ) |
| FORALL, NEXT | AX φ = ¬EX ¬φ | NOT PathPred(EXISTS, NEXT, NOT φ) |
| FORALL, UNTIL(ψ) | A(φ U ψ) = ¬E(¬ψ U (¬φ ∧ ¬ψ)) ∧ ¬EG ¬ψ | AND(NOT PathPred(EXISTS, UNTIL(AND(NOT φ, NOT ψ)), NOT ψ), NOT PathPred(EXISTS, GLOBALLY, NOT ψ)) |
| FORALL, PREVIOUSLY | past-time AX; self-dual | NOT PathPred(EXISTS, PREVIOUSLY, NOT φ) |

EVENTUALLY and GLOBALLY exchange; NEXT and PREVIOUSLY are self-dual; UNTIL needs both a dualised UNTIL and a GLOBALLY term. Swapping only the quantifier under EVENTUALLY would yield ¬EF ¬φ, which is AG φ, not AF φ. These are dualities over the maximal traces defined below, and they give the `on_empty = VACUOUS` reading of FORALL. Under the default `REQUIRE_WITNESS`, each construction is conjoined with a non-emptiness guard: `PathPred(EXISTS, EVENTUALLY, TRUE)` for GLOBALLY, EVENTUALLY and UNTIL, and `PathPred(EXISTS, NEXT, TRUE)` for NEXT.

**Path semantics.** The transition relation is not total: fact nodes cannot be the source of verb edges and are often sinks. CTL is therefore interpreted over maximal paths, without adding transitions (§10.4, D30; ADR 0001). For a PathPred evaluated at a start node s:

```
evaluation position   a node of a path instance at which every path-local alias used by sub_filter is bound
trace                 the evaluation positions of one path instance from s, in path order
terminal              a position whose node has no outgoing edge instance matching the next hop
maximal trace         a trace that is not a proper prefix of another; finite (ends at a terminal) or infinite (cycles only)
T(s)                  the set of maximal traces from s
Succ(s)               the nodes reached from s by exactly one hop of the path
```

| Form | Holds at s iff | Empty domain |
|---|---|---|
| EF φ | ∃π∈T(s) ∃i. φ(πᵢ) | false |
| EG φ | ∃π∈T(s) ∀i. φ(πᵢ) | false |
| E(φ U ψ) | ∃π∈T(s) ∃j. ψ(πⱼ) ∧ ∀i<j. φ(πᵢ) | false |
| EX φ | ∃m∈Succ(s). φ(m) | false |
| AF φ, AG φ, A(φ U ψ) | as above with ∀π∈T(s) | by `on_empty` |
| AX φ | ∀m∈Succ(s). φ(m) | by `on_empty` |

A finite trace is complete: EG φ and AG φ hold on a trace where φ holds at every position up to and including the terminal. UNTIL is strong. An infinite trace is decided on its lasso.

**Empty domain.** T(s) = ∅ when no path instance from s has an evaluation position; Succ(s) = ∅ when s is terminal. EXISTS is false on an empty domain. FORALL follows `on_empty`: `REQUIRE_WITNESS` makes it false, `VACUOUS` makes it true. A customer with no sales does not satisfy "all sales exceed 500" unless the query sets `on_empty = VACUOUS`.

FORALL with ONCE or SINCE(ψ) is rejected at construction. Their duals require a past-time GLOBALLY operator, which `TemporalMode` does not contain; EXISTS with ONCE or SINCE is unaffected.

**Path-order modes and temporal modes.** EVENTUALLY, GLOBALLY, NEXT and UNTIL quantify over the PathPositions of a trace (§3.2). They are valid on any path. On a path that is not time-ordered they mean "at some position", "at every position", "after one hop" and "at every position before the first one where ψ holds" — statements about the structure traversed, with no claim about when anything happened. They are future-time operators only on a time-ordered path (§2.3).

SINCE, ONCE and PREVIOUSLY are past-time operators and have no structural reading. A PathPred using one of them is rejected at construction unless its path is time-ordered. A backward structural traversal is written with reverse hops (`VerbHop⁻¹`, a bridge edge type traversed against its direction) under the path-order modes; it is not a query about the past.

**AF and AG are distinct.** Two minimal graphs, evaluated at s₀; s₁ and s₂ are terminals and every node is an evaluation position:

```
G₁:  s₀ → s₁                 φ holds at s₁ only      paths from s₀: (s₀ s₁)
G₂:  s₀ → s₁,  s₀ → s₂       φ holds at s₁ only      paths from s₀: (s₀ s₁), (s₀ s₂)
```

| | AF φ | ¬EG ¬φ | AG φ | ¬EF ¬φ |
|---|---|---|---|---|
| G₁ | true — the only path reaches φ at s₁ | true — no path stays in ¬φ | false — s₀ violates φ | false |
| G₂ | false — (s₀ s₂) never reaches φ | false — (s₀ s₂) stays in ¬φ | false | false |

On G₁, `PathPred(FORALL, EVENTUALLY, φ)` is true, while the quantifier-only reduction ¬EF ¬φ is false. On G₂ the path (s₀ s₂) that never reaches φ is exactly the EG ¬φ witness that falsifies AF φ.

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

Semantics: maximal paths over a non-total transition relation; FORALL on an empty domain follows `on_empty` (§4.1, D30).

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

**Scalar conditions.** A ScalarPred wraps one condition over typed scalar expressions. The comparison operator is a separate argument, not part of an opaque function, so both sides of a comparison may be arbitrary expressions — including two references.

```
ScalarCond ::= Compare(left: SExpr, op: ComparisonOp, right: SExpr)
             | In(expr: SExpr, set: Collection)
             | IsNull(expr: SExpr)
             | IsNotNull(expr: SExpr)
             | Holds(expr: SExpr)                    -- expr of type Bool

ComparisonOp ::= = | ≠ | < | ≤ | > | ≥

Collection   ::= SetLit(elems: list[Lit | Param], type: Type)    -- non-empty; every element of the stated type

SExpr ::= Lit(value: V, type: Type)                  -- typed literal; NULL is not a literal
        | Ref                                         -- PropRef | AggRef | WinRef, subject to the band table above
        | Param(name: K, type: Type)                  -- named constant, bound before planning
        | Now                                         -- Timestamp of the evaluation context (§5.1)
        | ArithExpr(left: SExpr, op: ArithOp, right: SExpr)
        | Func(fn: ScalarFn, args: list[SExpr])
        | TypeOf(alias: A)                            -- node type of the instance bound to alias
        | Old | New                                   -- only inside a ChangeCond (§5.1)

ArithOp  ::= + | - | * | /

ScalarFn ::= YEAR | QUARTER | MONTH | DAY | HOUR      -- Date | Timestamp → Int
           | ABS                                      -- numeric → same type
           | LOWER | UPPER                            -- String → String
           | LENGTH                                   -- String → Int
           | COALESCE                                 -- T, …, T → T

Type ::= Bool | Int | Real | String | Date | Timestamp | Duration | Enum(name: K) | LabelSeq
```

`SignatureRef` is not an SExpr; it is consumed only by SignaturePred.

**Typing rules.** Every SExpr has exactly one type, computed bottom-up; a ScalarPred whose condition does not type-check is rejected at construction.

```
Refs      PropRef has the declared type of the property. AggRef and WinRef have the result type
          of the Expr that defines them (§4.2). All Refs in one ScalarPred belong to the band of
          the filter that contains it (Where: PropRef; Having: AggRef; Qualify: WinRef).

Widening  Int widens implicitly to Real. There is no other implicit conversion.

ArithExpr Int  (+|-|*) Int            → Int          Int / Int                  → Real
          numeric op numeric          → Real         (any other numeric mix)
          Timestamp - Timestamp       → Duration     Date - Date                → Duration
          Timestamp (+|-) Duration    → Timestamp    Date (+|-) Duration        → Date
          Duration  (+|-) Duration    → Duration     Duration (*|/) numeric     → Duration
          every other combination     → type error

Compare   Both sides have the same type after widening. <, ≤, >, ≥ require an ordered type:
          Int, Real, String, Date, Timestamp, Duration. Bool, Enum and LabelSeq admit only = and ≠.

In        expr has the Collection's type after widening. An Enum collection may contain only
          members of that Enum.

Holds     expr has type Bool.

TypeOf    has type Enum(NodeType), whose members are the node types of the schema.

Old, New  have the declared type of the property named by the enclosing CHANGED_PROPERTY.

LabelSeq  the type of a path signature (a sequence of labels). Admits only = and ≠; no arithmetic.
```

**NULL rules.** NULL is a value a Ref may evaluate to, never a literal. ArithExpr and Func return NULL when any operand is NULL, except COALESCE, which returns its first non-NULL argument; division by zero returns NULL. Compare, In and Holds evaluate to UNKNOWN when an operand is NULL. IsNull and IsNotNull always evaluate to TRUE or FALSE. A filter keeps a tuple only when its predicate is TRUE. Testing for NULL is therefore written `IsNull(e)`; `Compare(e, =, NULL)` is not expressible.

**Concrete notation.** Examples in this specification use the following shorthands. Each abbreviates exactly one AST; nothing else is admitted.

| Shorthand | AST |
|---|---|
| `ScalarPred(e, (op e'))` | `ScalarPred(Compare(e, op, e'))` |
| `ScalarPred(e, IN {x₁, …, xₙ})` | `ScalarPred(In(e, SetLit([x₁, …, xₙ], type(e))))` |
| `ScalarPred(e, IS_NULL)`, `ScalarPred(e, IS_NOT_NULL)` | `ScalarPred(IsNull(e))`, `ScalarPred(IsNotNull(e))` |
| `2024`, `10000`, `0.5` | `Lit(2024, Int)`, `Lit(10000, Int)`, `Lit(0.5, Real)` |
| `"retail"`, `'retail'` | `Lit("retail", String)` |
| `90 days` | `Lit(90 days, Duration)` |
| `Missing`, `Complete` — a bare capitalised name compared with an Enum-typed expression | `Lit(Missing, Enum(E))`, where `Enum(E)` is the type of the other operand |
| `now` | `Now` |
| `horizon`, `MAX_CONCURRENT_RUNS` — any other bare name | `Param(name, T)`, with `T` declared where the parameter is bound |
| `PropRef(x.ts.year)` — a temporal field after a Date or Timestamp property | `Func(YEAR, [PropRef(x.ts)])`; likewise `.quarter`, `.month`, `.day`, `.hour` |
| `Const(c)` | `Lit(c, Real)` |
| `x.prop` — a bare dotted name in an argument position or in a GroupBy, PARTITION BY or ORDER BY list | `PropRef(x.prop)` |
| `name` — in a Window or Qualify position, a bare name defined in Agg or Window | `AggRef(name)` or `WinRef(name)` respectively |
| `'2024-06-30'` — a quoted ISO date in a Date or Timestamp position | `Lit(2024-06-30, Date)` or `Lit(2024-06-30T00:00:00, Timestamp)` by the type of the position |
| `τ_N(n)` | `TypeOf(n)` |
| `FN() OVER (PARTITION BY p₁, …  ORDER BY o₁ dir₁, …)` | `RankFn(FN, partition=[p₁, …], order=[(o₁, dir₁), …])`; an absent clause is the empty list |
| `(old ∈ S, new ∈ S')` | `AND(ScalarPred(In(Old, SetLit(S, T))), ScalarPred(In(New, SetLit(S', T))))`, `T` the type of the property |
| `SUM(e)`, `COUNT(e)`, … | `AggFn(SUM, e)`, `AggFn(COUNT, e)`, … |
| `e₁ / e₂` and the other infix operators | `ArithExpr(e₁, /, e₂)` |

Translations of the predicate, expression, window and delta forms that occur in §6.2 and §10.1:

```
ScalarPred(PropRef(sale.occurred_on.year), (= 2024))
  ≡ ScalarPred(Compare(Func(YEAR, [PropRef(sale.occurred_on)]), =, Lit(2024, Int)))

ScalarPred(PropRef(return_ev.occurred_on), (> PropRef(sale.occurred_on)))
  ≡ ScalarPred(Compare(PropRef(return_ev.occurred_on), >, PropRef(sale.occurred_on)))      -- two references

ScalarPred(PropRef(a.state), IN {Missing, Failed})
  ≡ ScalarPred(In(PropRef(a.state),
                  SetLit([Lit(Missing, Enum(ArtifactState)), Lit(Failed, Enum(ArtifactState))],
                         Enum(ArtifactState))))

ScalarPred(PropRef(a.window_end), (< ArithExpr(now, -, 90 days)))
  ≡ ScalarPred(Compare(PropRef(a.window_end), <,
                       ArithExpr(Now, -, Lit(90 days, Duration))))                          -- Timestamp - Duration → Timestamp

ScalarPred(WinRef(priority), (<= MAX_CONCURRENT_RUNS))
  ≡ ScalarPred(Compare(WinRef(priority), ≤, Param(MAX_CONCURRENT_RUNS, Int)))

ScalarPred(WinRef(scc_label), IS_NOT_NULL)
  ≡ ScalarPred(IsNotNull(WinRef(scc_label)))

ArithExpr(PropRef(s.revenue), *, PropRef(p2.weight))                                        -- argument of PathAgg(SUM)
  : Real, given revenue: Real and weight: Real

return_rate := COUNT(return_ev) / COUNT(sale)
  ≡ ArithExpr(AggFn(COUNT, NodeArg(return_ev)), /, AggFn(COUNT, NodeArg(sale)))              -- Int / Int → Real

TemporalAgg(SUM, s.revenue, s.occurred_on, ROLLING(30 days))
  ≡ TemporalAgg(SUM, PropRef(s.revenue), PropRef(s.occurred_on), ROLLING(Lit(30 days, Duration)))

priority := RANK() OVER (ORDER BY age_days DESC)
  ≡ RankFn(RANK, partition=[], order=[(AggRef(age_days), DESC)])                              : Int

rank_by_rev := RANK() OVER (PARTITION BY c.id ORDER BY s.revenue DESC)
  ≡ RankFn(RANK, partition=[PropRef(c.id)], order=[(PropRef(s.revenue), DESC)])              : Int

scc_label := GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC), s))                              : Int

GroupBy: [ τ_N(n) ]
  ≡ GroupBy: [ TypeOf(n) ]                                                                    : Enum(NodeType)

CHANGED_PROPERTY(a, state, comparator = (old ∈ {In-flight}, new ∈ {Complete, Failed}))
  ≡ CHANGED_PROPERTY(a, state,
      AND(ScalarPred(In(Old, SetLit([Lit(In-flight, Enum(ArtifactState))], Enum(ArtifactState)))),
          ScalarPred(In(New, SetLit([Lit(Complete, Enum(ArtifactState)),
                                     Lit(Failed, Enum(ArtifactState))], Enum(ArtifactState))))))

Q_delta(t₁ = last_scheduler_run, t₂ = now, …)
  ≡ Q_delta(Param(last_scheduler_run, Timestamp), Now, …)
```

**Rejected by validation:**

| Input | Reason |
|---|---|
| `Compare(PropRef(c.segment), >, Lit(10000, Int))` with `segment: String` | String and Int are not the same type; no implicit conversion. |
| `Compare(PropRef(a.state), <, Lit(Stale, Enum(ArtifactState)))` | Enum admits only = and ≠. |
| `In(PropRef(a.state), SetLit([Lit(Missing, …), Lit(Paused, …)], Enum(ArtifactState)))` | `Paused` is not a member of `ArtifactState`. |
| `ArithExpr(Now, +, Now)` | Timestamp + Timestamp is not defined. |
| `Compare(PropRef(s.occurred_on), <, Lit(90 days, Duration))` with `occurred_on: Timestamp` | Timestamp and Duration are not the same type. |
| `Holds(PropRef(s.revenue))` with `revenue: Real` | Holds requires Bool. |
| `Where: ScalarPred(Compare(AggRef(total_rev), >, Lit(5000, Int)))` | AggRef is not valid in Band 1; belongs in Having. |
| `Compare(PropRef(s.revenue), =, NULL)` | NULL is not a literal; use `IsNull`. |
| `Where: ScalarPred(Compare(Old, =, Lit(Stale, Enum(ArtifactState))))` | `Old` is valid only inside a ChangeCond. |
| `RankFn(RANK, partition=[], order=[])` | A rank function requires a non-empty order. |
| `RunningAgg(SUM, PropRef(c.segment), …)` with `segment: String` | SUM requires a numeric argument. |
| `Compare(WinRef(dom_sig), <, …)` with `dom_sig := DOMINANT_OUTGOING_SIGNATURE` | LabelSeq admits only = and ≠. |
| `StatFn(PERCENTILE_CONT(1.5), …)` | The percentile must lie in [0, 1]. |

### 4.2  Expression language

```
Expr ::= AggFn(fn: AggFn, arg: SExpr | NodeArg(alias: A))
                                                     -- arg over PropRefs of C; NodeArg with COUNT only
       | PathAgg(alias, path: BoundPath, strategy, promote?, fn: AggFn, arg: SExpr)
                                                     -- arg over PropRefs of C ∪ L
       | TemporalAgg(fn: AggFn, arg: SExpr, timestamp: PropRef, window: TemporalWindow)
       | GraphExpr(algorithm: GraphAlgorithm)
       | ArithExpr(Expr, op: ArithOp, Expr) | Lit(value: V, type: Type)

AggFn          ::= SUM|COUNT|AVG|MIN|MAX|PROD
TemporalWindow ::= ROLLING(d: SExpr)                  -- d: Duration
                 | TRAILING(n: ℕ, unit: TimeUnit)    -- n ≥ 1
                 | PERIOD(start: SExpr, end: SExpr)  -- both Date or both Timestamp
                 | YTD | QTD | MTD
TimeUnit       ::= DAY | WEEK | MONTH | QUARTER | YEAR
```

Expr is the aggregate level; SExpr (§4.1) is the row level beneath it. An aggregate argument is an SExpr — a bare reference or an arithmetic combination of references — typed by the rules of §4.1. ArithExpr at the Expr level combines aggregate results under the same ArithOp typing rules. `AggFn(COUNT, NodeArg(x))` counts the tuples in which the node alias `x` is bound; NodeArg is rejected with every other AggFn. Result types: COUNT → Int for any argument; AVG → Real; SUM and PROD require a numeric argument and return its type; MIN and MAX require an ordered type and return it. These result types are the types of the AggRef and WinRef that name the expression. In TemporalAgg, `timestamp` has type Date or Timestamp and the window bounds are literals or parameters.

GraphExpr result types:

| Algorithm | Type |
|---|---|
| PAGE_RANK, BETWEENNESS_CENTRALITY, CLOSENESS_CENTRALITY, SIGNATURE_DIVERSITY, SIGNATURE_SIMILARITY, MIN_WEIGHT_PATH_LENGTH, MAX_FLOW, DENSITY, SIGNATURE_ENTROPY | Real |
| DEGREE, COMMUNITY_LABEL, SHORTEST_PATH_LENGTH, DIAMETER, OUTGOING_SIGNATURES, INCOMING_SIGNATURES, DISTINCT_SIGNATURES, GLOBAL_SIGNATURE_COUNT | Int |
| REACHABLE | Bool |
| DOMINANT_OUTGOING_SIGNATURE, DOMINANT_INCOMING_SIGNATURE, DOMINANT_SIGNATURE, GLOBAL_DOMINANT_SIGNATURE | LabelSeq |

Algorithm arguments are literals or parameters, not row-level expressions: `damping`: Real in (0, 1); `iterations`, `max_depth`: Int ≥ 1; `direction` ∈ {IN, OUT, BOTH}; `reference`: a LabelSeq literal; `source`, `target`: aliases; `capacity`: a PropRef of numeric type on the traversed edges.

A GraphExpr evaluates to NULL where the algorithm has no value for the node or pair — no path between the pair, no path in the cone, a node outside every community.

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

DeltaSpec ::= ADDED_NODES(N_S) | REMOVED_NODES(N_S)      -- instances of the given node type
            | ADDED_EDGES(E_S) | REMOVED_EDGES(E_S)      -- instances of the given edge type
            | CHANGED_PROPERTY(alias,prop,comparator)
            | ROLE_DRIFT(alias,from_type,to_type)
```

`t₁` and `t₂` are SExprs of type Timestamp built from literals, parameters and `Now`. In `CHANGED_PROPERTY(alias, prop, comparator)` the comparator is optional and is a ChangeCond; `from_type` and `to_type` in ROLE_DRIFT are node types.

```
ChangeCond ::= T [Old, New]      -- a Boolean filter tree (§4.1) whose ScalarPreds reference only
                                 -- Old, New, literals and parameters
```

`Old` and `New` denote the value of `prop` at t₁ and at t₂. A change is reported when the two values differ and the ChangeCond, if present, is TRUE; with no ChangeCond every change of `prop` is reported.

Q_delta operates outside the three-band structure.

### 5.2  Bands

B₁ = what, B₂ = how to collapse, B₃ = which tuples. Cross-band Ref rules enforced at construction.

#### 5.2.1  Band 1 — Context layer

```
B₁ = (Join, Where?)

Join ::= Anchor(node_type: N_S, alias: A)
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

GroupBy: list of SExpr over PropRefs from C' — a bare PropRef, a Func of one, or TypeOf(alias). Degenerate dims excluded. Agg: { aᵢ := Exprᵢ }. Grain-aware. Free→Free SubgraphExpr valid in Agg — scalar broadcast to all groups. Having: T [AggRef].

#### 5.2.3  Band 3 — Ranking layer

```
B₃ = (Window, Qualify)

WinExpr ::= RankFn(fn: RankFn, partition: list[SExpr], order: list[OrderKey])
          | RunningAgg(fn: AggFn, arg: SExpr, partition: list[SExpr], order: list[OrderKey])
          | StatFn(fn: StatFn, arg: SExpr, partition: list[SExpr], order?: list[OrderKey])
          | GraphExpr(NodeExpr|PairExpr|SubgraphExpr[partition or scope])

OrderKey ::= (key: SExpr, dir: ASC | DESC)

RankFn ::= ROW_NUMBER|RANK|DENSE_RANK|NTILE(n)|LEAD(ref,offset)|LAG(ref,offset)
StatFn ::= PERCENT_RANK|CUME_DIST|PERCENTILE_CONT(p)|PERCENTILE_DISC(p)
```

SExprs in a WinExpr range over PropRefs and AggRefs, typed by §4.1. Typing:

```
RankFn     order is non-empty; every order key has an ordered type.
           ROW_NUMBER, RANK, DENSE_RANK, NTILE(n)   → Int        (n ≥ 1)
           LEAD(ref, offset), LAG(ref, offset)      → type of ref (offset ≥ 1); NULL outside the partition

RunningAgg order is non-empty; arg and result follow the AggFn rules of §4.2.

StatFn     PERCENT_RANK, CUME_DIST                  → Real; order is required, arg is unused
           PERCENTILE_CONT(p)                       → Real; numeric arg; 0 ≤ p ≤ 1
           PERCENTILE_DISC(p)                       → type of arg; ordered arg; 0 ≤ p ≤ 1

GraphExpr  result type as in §4.2.
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

BDD provides O(1) equivalence, logical satisfiability/tautology, implication, minterm enumeration, and foundations for recommender and planner.

**What the BDD decides.** The BDD represents propositional formulas over atoms and decides properties of those formulas. It never inspects data. Three notions are kept apart:

| Notion | Meaning | Decided by | Depends on data |
|---|---|---|---|
| `logical_sat(φ)` | Some assignment of the BDD variables consistent with the axioms Δ makes φ TRUE | The BDD | No |
| `non_empty(q, G_I(t))` | Query q returns at least one row on snapshot `G_I(t)` | Executing q | Yes |
| `estimated_rows(q)` | The planner's estimate of the row count, from `GraphStatistics` | Cost model (§6.2) | Yes — on statistics, which may be stale |

The only inferences permitted between them:

```
¬logical_sat(φ)      ⟹  ¬non_empty(q, G_I(t)) for every snapshot       -- contradiction: short-circuit to the empty result
tautology(φ)         ⟹  the filter φ may be removed                    -- says nothing about the row count
logical_sat(φ)       ⟹  nothing about non_empty or estimated_rows
estimated_rows(q)=0  ⟹  nothing about non_empty                        -- an estimate never short-circuits
```

A satisfiable filter over an empty table is still logically satisfiable and still returns no rows. Atoms are opaque to the BDD, so `logical_sat` is also weaker than satisfiability in the theory of the atoms: `x > 5 AND x < 3` is two distinct variables and is logically satisfiable until an atom theory relates them.

**Variables.** A BDD variable is an atom in canonical form:

```
ScalarPred     the ScalarCond AST after normalisation: operands of commutative operators ordered;
               a > b rewritten b < a;  a ≠ b rewritten NOT(a = b);  a ≥ b rewritten NOT(a < b);
               a ≤ b rewritten NOT(b < a). The Ref kind (PropRef, AggRef, WinRef) is part of the atom.
PathPred       (path, strategy, temporal_mode, on_empty, BDD id of sub_filter), EXISTS-quantified
SignaturePred  (path_alias, sequence, match)
```

Two occurrences are the same variable iff their canonical forms are identical. Atoms over different Ref kinds are never the same variable. No relation between distinct variables is assumed beyond the axioms Δ declared in this section and the bridge axioms of Rule 4 (§6.2).

**Three-valued logic.** A ScalarPred can evaluate to TRUE, FALSE or UNKNOWN (§4.1 NULL rules), and AND, OR and NOT follow Kleene logic:

| p | q | p AND q | p OR q | NOT p |
|---|---|---|---|---|
| T | T | T | T | F |
| T | F | F | T | F |
| T | U | U | T | F |
| F | T | F | T | T |
| F | F | F | F | T |
| F | U | F | U | T |
| U | T | U | T | U |
| U | F | F | U | U |
| U | U | U | U | U |

A filter keeps a tuple only when its predicate is TRUE. The BDD is two-valued; it is made exact for Kleene logic by encoding *which* truth value an atom takes:

```
Nullability    IsNull, IsNotNull, PathPred and SignaturePred are two-valued: one variable p, with p⁺ = p, p⁻ = ¬p.
               A Compare, In or Holds atom all of whose Refs are declared non-nullable is two-valued likewise.
               Every other ScalarPred atom is nullable and has two variables:
                 p⁺  "p is TRUE"        p⁻  "p is FALSE"        (neither: p is UNKNOWN)

Translation    T⁺(φ) is the BDD of "φ is TRUE", T⁻(φ) of "φ is FALSE":
                 T⁺(p) = p⁺                          T⁻(p) = p⁻
                 T⁺(NOT φ) = T⁻(φ)                   T⁻(NOT φ) = T⁺(φ)
                 T⁺(φ AND ψ) = T⁺(φ) ∧ T⁺(ψ)         T⁻(φ AND ψ) = T⁻(φ) ∨ T⁻(ψ)
                 T⁺(φ OR ψ)  = T⁺(φ) ∨ T⁺(ψ)         T⁻(φ OR ψ)  = T⁻(φ) ∧ T⁻(ψ)
                 T⁺(NotTrue φ) = ¬T⁺(φ)              T⁻(NotTrue φ) = T⁺(φ)

Axioms Δ       Δ_excl   ¬(p⁺ ∧ p⁻)                       for every nullable atom p
               Δ_null   IsNull(e) → ¬p⁺ ∧ ¬p⁻            for every nullable atom p in which e occurs
                                                         outside a COALESCE
```

The BDD of a filter φ is T⁺(φ), and every property is decided relative to Δ:

```
tautology(φ)      iff  Δ → T⁺(φ)            is the TRUE node
logical_sat(φ)    iff  Δ ∧ T⁺(φ)            is not the FALSE node
implies(φ, ψ)     iff  Δ ∧ T⁺(φ) → T⁺(ψ)    is the TRUE node
equivalent(φ, ψ)  iff  Δ → (T⁺(φ) ↔ T⁺(ψ))  is the TRUE node       -- the two filters keep the same tuples
```

`NotTrue(φ)` is the complement of φ's truth set — SQL `(φ) IS NOT TRUE` — and is two-valued. It is not written by users; FORALL elimination (§4.1) produces it, so that a position where `sub_filter` is UNKNOWN counts as a violation of a universal condition.

The encoding is sound for Kleene logic: whatever it reports as a tautology, contradiction, implication or equivalence holds for every assignment of TRUE, FALSE and UNKNOWN to the atoms. It is not complete with respect to the theory of the atoms — a filter that is TRUE on every tuple for arithmetic reasons is not recognised.

Short-circuit behaviour, with p a nullable atom and n a non-nullable one:

| Filter φ | φ at p=T | φ at p=F | φ at p=U | T⁺(φ) | Verdict | Action |
|---|---|---|---|---|---|---|
| `p OR NOT p` | T | T | U | p⁺ ∨ p⁻ | not a tautology — the assignment p⁺ = p⁻ = 0 falsifies it | filter kept; UNKNOWN tuples are dropped at run time |
| `n OR NOT n` | T | T | — | n ∨ ¬n | tautology | filter removed |
| `p AND NOT p` | F | F | U | p⁺ ∧ p⁻ | contradiction, by Δ_excl | empty-result short-circuit |
| `NOT (p OR NOT p)` | F | F | U | p⁻ ∧ p⁺ | contradiction, by Δ_excl | empty-result short-circuit |
| `p OR NotTrue(p)` | T | T | T | p⁺ ∨ ¬p⁺ | tautology | filter removed |
| `IsNull(x) AND x > 5` | — | — | U | IsNull(x) ∧ p⁺ | contradiction, by Δ_null | empty-result short-circuit |
| `p AND q` | as table above | | | p⁺ ∧ q⁺ | satisfiable | no short-circuit; nothing is inferred about the row count |

Core: BDDNode (var, low, high), FALSE_NODE=0, TRUE_NODE=1. Unique table (var,low,high)→id; computed cache (op,u,v)→result.

Operations: make (elimination+sharing), apply (Shannon expansion; memoised), negate (single-pass), FORALL elimination by CTL duality at construction (§4.1).

PathPred + TemporalMode opaque atoms. After FORALL elimination every atom is EXISTS-quantified, so the SQL templates are the E-forms:

```
EVENTUALLY  → EXISTS (CTE reaches a node satisfying sub_filter)                  -- EF
GLOBALLY    → EXISTS (CTE restricted to nodes satisfying sub_filter
                      reaches the end of a maximal path)                         -- EG
              -- end of a maximal path: a node with no outgoing edge matching the next hop
              -- (terminal), or the closing of a cycle (§4.1)
NEXT        → EXISTS, single-hop CTE
UNTIL(ψ)   → EXISTS recursive CTE, hold_pred at every intermediate node
SINCE(ψ)   → EXISTS reverse-direction CTE on G^T     -- time-ordered paths only (§4.1)
ONCE        → EXISTS backward CTE on G^T               -- time-ordered paths only
PREVIOUSLY  → EXISTS single backward hop via G^T       -- time-ordered paths only
```

The A-forms are never emitted as templates of their own; they arise from the negated E-atoms produced by FORALL elimination (§4.1):

```
AG φ = NOT EF(NOT φ)  → NOT EXISTS (CTE reaches a node violating sub_filter)
AF φ = NOT EG(NOT φ)  → NOT EXISTS (CTE restricted to nodes violating sub_filter
                                    reaches the end of a maximal path)
```

Under `on_empty = REQUIRE_WITNESS` each A-form is additionally conjoined with `EXISTS (CTE reaches any evaluation position)`, or for NEXT with `EXISTS (single-hop CTE)`; under `VACUOUS` the guard is omitted.

AF φ is not `NOT EXISTS EVENTUALLY NOT` — that expression is AG φ. Because the atoms are opaque to the BDD, an atom built with the wrong dual would be preserved unchanged through optimisation; correctness depends on the construction step.

SignaturePred: opaque atom, expanded as label sequence filter on CTE path array.

Variable ordering: PropRef(B₁)→AggRef(B₂)→WinRef(B₃); within B₁: ScalarPred→PathPred→SignaturePred; within ScalarPred: by selectivity.

SQL translation: if |minterms| ≤ THRESHOLD → UNION ALL; else → BDD-structured CASE expression.

Logical optimisation pipeline:

```
1. Tautology removal / contradiction short-circuit (relative to Δ; never from estimated_rows)
2. Redundant clause elimination (AND)
3. Predicate subsumption (OR)
4. Cross-band implication (Where → Having) through bridge axioms only (Rule 4, §6.2)
5. PathPred boolean context propagation
6. TemporalMode SQL template selection
7. Minterm count → SQL translation strategy
8. FORALL elimination by CTL duality (at construction; §4.1)
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
                   scc_sizes: N_I→ℕ,
                   transposed_adj?: N_I→list[N_I],  -- G^T adjacency; optional materialisation (§2.6)
                   access_paths: E_S→set[I1..I5],   -- physical access paths present per mapping (§2.6)
                   causal_acyclic: Version→VALIDATED|VIOLATED|UNKNOWN)  -- §2.4
-- GraphStatistics describes G_I. It informs cost and scoring; it never changes Q_valid.

ExportPlan = (query_target, query_text, pre_compute: list[PreComputation],
              sink_target?, write_plan?, cost_estimate,
              alternatives: list[(ExportPlan, CostEstimate)])
```

alternatives: structured plan explanation — mechanical, auditable, no natural language.

**Rule 1a — Path execution strategy:**

```
Bound→Bound (REACHABLE_BETWEEN):
    if path_card ≤ SMALL: recursive CTE with cycle prevention
        -- CAUSAL with causal_acyclic = VALIDATED (§2.4): DAG BFS, no visited set
    elif strategy ∈ {SHORTEST,MIN_WEIGHT}: recursive CTE ORDER BY (len | weight, len), key + LIMIT 1
        -- key is the canonical edge sequence of §3.3; it makes the LIMIT deterministic
        -- TEMPORAL: push ordering into CTE WHERE
    else: pre-materialise path table

Bound→Free (REACHABLE_FROM(A)):
    forward BFS/DFS from A on G
    -- CAUSAL: DAG BFS (no visited-set)
    -- SHORTEST: BFS terminates at first visit per node; among equal-length arrivals the least key (§3.3) is kept
    -- ALL: full DFS collecting all simple paths

Free→Bound (REACHABLE_TO(B)):
    forward BFS/DFS from B on G^T (reverse access path of §2.6, or transposed_adj when materialised)
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
-- Where atoms range over PropRef variables and Having atoms over AggRef variables. The two sets
-- are disjoint (§6.1), so bdd.implies(where_bdd, having_bdd) on its own can only succeed trivially.
-- A Having predicate is removed only through a declared bridge axiom.

derived = { a' | a is a Where-vocabulary atom with bdd.implies(where_bdd, a),
                 and a bridge axiom maps a to the Having-vocabulary atom a' }
for each having_pred:
    if bdd.implies(AND(derived), having_pred): remove having_pred
```

Bridge axioms. Preconditions for all of them: GroupBy is non-empty, so every group holds at least one tuple; the aggregate is a plain `AggFn` over the tuples that Where admits (not PathAgg, not TemporalAgg); `e` is the same SExpr on both sides; `c` is a literal or parameter.

| # | Where entails, for every tuple | Having atom derived |
|---|---|---|
| BX1 | `e op c`, op ∈ {<, ≤, >, ≥} | `MIN(e) op c`, `MAX(e) op c`, `AVG(e) op c` |
| BX2 | `e = c` | `MIN(e) = c`, `MAX(e) = c`, `AVG(e) = c` |
| BX3 | `e ≥ c` or `e > c`, with `c ≥ 0` | `SUM(e) ≥ c`, respectively `SUM(e) > c` |
| BX4 | `e ≤ c` or `e < c`, with `c ≤ 0` | `SUM(e) ≤ c`, respectively `SUM(e) < c` |
| BX5 | — | `COUNT(*) ≥ 1` |

Each is a theorem about aggregates of a non-empty multiset. A tuple with `e` NULL fails the Where atom and is not aggregated, so the derived atom is two-valued. No other relation between a PropRef and an AggRef is assumed.

```
Where: s.revenue > 500     GroupBy: [c.id]     Agg: min_rev := MIN(s.revenue), units := SUM(s.quantity)

Having: min_rev > 500      -- removed: BX1 derives MIN(s.revenue) > 500
Having: units > 100        -- kept: no axiom relates s.revenue to SUM(s.quantity)
```

With no GroupBy the rule does not fire: a global aggregate over zero tuples yields one row with NULL aggregates, which the Having predicate would drop and its removal would not.

**Rule 5 — Hierarchy execution:**

```
Hierarchy(depth ≤ 4): Repeat(hop, 1, depth) unrolled into a fixed-depth join chain
Hierarchy(RAGGED):    Repeat(hop, 1, UNBOUNDED, SIMPLE) as a recursive CTE with a visited set,
                      capped at MAX_REPEAT_DEPTH (truncation reported, §3.2)
|N| > CLOSURE_THRESHOLD: pre-materialised closure table
```

**Rule 6 — Annotation-driven optimisations:**

```
RolePlaying:             single scan under multiple aliases
ProjectsFrom(mini,full): eliminate mini join if full ∈ C
DerivedFact(f,srcs,e):   inline if srcs ∩ C ≠ ∅
WeightConstraint(ALLOC): PathAgg without weight → warn
BridgeSemantics(CAUSAL): if causal_acyclic = VALIDATED: DAG BFS on G and G^T; else cycle prevention
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
    -- Missing or Failed windows that closed within the backfill horizon (§2.4):
    --   now − horizon ≤ window_end ≤ now
        AND(
          ScalarPred(PropRef(f.state), IN {Missing, Failed}),
          ScalarPred(PropRef(f.window_end), (>= ArithExpr(now, -, horizon))),
          ScalarPred(PropRef(f.window_end), (<= now))
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

The pre-execution gate sits between the planner and execution. It intercepts every `ExportPlan` before it reaches the SQL emitter, compares the cost estimate against a declared `ExecutionBudget`, classifies the failure mode if any, and applies one of four graceful handling strategies. Rewriting is always tried before a delivery mode is chosen: streaming and async execution are decided for the best plan that remains, not for the plan as submitted.

**ExecutionBudget type:**

```
ExecutionBudget = (
  max_estimated_cost:   CostEstimate,  -- executable ceiling: no plan above it runs as submitted
  max_result_rows:      ℕ,             -- row count ceiling
  max_wall_time:        Duration,       -- synchronous execution timeout
  max_precompute_time:  Duration,       -- per PreComputation timeout
  streaming_threshold:  CostEstimate,  -- strictly above → stream results progressively
  async_threshold:      CostEstimate   -- strictly above → async with callback + materialisation
)

Policy constants:
  HARD_CEILING_FACTOR  : ℝ, finite, ≥ 1      -- hard_ceiling(b) = b.max_estimated_cost × HARD_CEILING_FACTOR
  SAMPLE_DELTA         : ℝ ∈ (0,1)           -- failure probability of a SAMPLE guarantee (default 0.05)
  SAMPLE_MAX_ERROR     : ℝ ∈ (0,1]           -- largest error half-width a SAMPLE may report (default 0.05)
  MAX_CLARIFY_OPTIONS  : ℕ, ≥ 1              -- options returned by one CLARIFY (default 3)
```

`CostEstimate` values are compared by their scalar total in planner cost units; `≤` and `>` below denote that total order.

**Budget invariants.** Checked by the `ExecutionBudget` constructor. A budget that violates any of them is rejected with a configuration error at construction, before any plan is gated — `gate()` is never called with an invalid budget and therefore never has to classify one.

```
I1   0 ≤ streaming_threshold ≤ async_threshold ≤ max_estimated_cost
I2   all three are finite (no NaN, no ∞)
I3   max_estimated_cost ≤ hard_ceiling(b)        -- follows from HARD_CEILING_FACTOR ≥ 1
```

The ordering is forced by what the thresholds mean: each one opens a more deferred delivery mode, and `max_estimated_cost` closes the last of them. An inverted pair would give one cost two contradictory classifications; a negative threshold has no meaning in cost units.

**Cost zones.** I1–I3 partition the cost axis into five zones. Every threshold is *inclusive on the cheaper side*: a plan whose cost equals a threshold stays in the lower zone, and only a strictly greater cost escalates.

```
Zone ::= SYNC | STREAMING | DEFERRED | OVER | FORBIDDEN       -- ordered SYNC < … < FORBIDDEN

zone(c: CostEstimate, b: ExecutionBudget) → Zone:
  if c ≤ b.streaming_threshold:  return SYNC         -- execute synchronously
  if c ≤ b.async_threshold:      return STREAMING    -- execute now, deliver progressively
  if c ≤ b.max_estimated_cost:   return DEFERRED     -- execute asynchronously
  if c ≤ hard_ceiling(b):        return OVER         -- not executable as submitted; must be reduced
  return FORBIDDEN                                   -- refused outright
```

`zone` is total and single-valued for every valid budget. Equal thresholds are legal and empty the zone between them:

| Configuration | Effect |
|---|---|
| `streaming_threshold = async_threshold` | STREAMING is empty; costs above the shared value go straight to DEFERRED. Disables streaming. |
| `async_threshold = max_estimated_cost` | DEFERRED is empty; costs above the shared value are OVER. Disables async execution. |
| `HARD_CEILING_FACTOR = 1` | OVER is empty; every cost above `max_estimated_cost` is FORBIDDEN. Disables SAMPLE and CLARIFY. |
| `streaming_threshold = 0` | Only zero-cost plans are SYNC. |

`max_estimated_cost` and the hard ceiling are different limits. Above `max_estimated_cost` the submitted plan is never executed, but the gate still tries to reduce it (rewrite, sample, clarify). Above `hard_ceiling(b)` the gate does no further work and refuses.

```
BudgetDecision ::= PROCEED                      -- SYNC zone; execute synchronously
                 | STREAM                        -- STREAMING zone; progressive delivery
                 | ASYNC(sink: SinkTarget,
                         callback: Endpoint,
                         ttl: Duration)          -- DEFERRED zone; execute async
                 | REWRITE(plan: ExportPlan,
                           warning: string,
                           then: Delivery)       -- cost-reduced rewrite + transparency note
                 | SAMPLE(fraction: ℝ,
                          method: RANDOM | STRATIFIED | RESERVOIR,
                          error_bound: ℝ,
                          confidence: ℝ,
                          then: Delivery)        -- approximate result with a stated guarantee
                 | PAGINATE(page_size: ℕ)        -- rewrite to paginate result set
                 | CLARIFY(options: list[ExportPlan],
                           cost_per_option: list[CostEstimate])
                                                 -- ask user to narrow; surface lattice refinements
                 | REJECT(reason: string)         -- refuse; nothing is executed

Delivery ::= PROCEED | STREAM | ASYNC(…) | PAGINATE(…)        -- the decisions that execute a plan as given
```

`REWRITE.then` and `SAMPLE.then` carry the delivery decision the gate has already taken for the rewritten or sampled plan. The caller executes that plan under `then`; it does **not** submit it to `gate()` again, so a rewrite can never trigger another rewrite.

**Pre-execution decision procedure:**

```
gate(plan: ExportPlan, budget: ExecutionBudget) → BudgetDecision:
  -- Precondition: budget satisfies I1–I3.

  est = plan.cost_estimate
  z   = zone(est, budget)

  -- Step 1 — hard ceiling. Takes precedence over every other step:
  -- no rewrite, sample or clarification is computed for a FORBIDDEN plan.
  if z = FORBIDDEN:
    return REJECT("Query exceeds hard cost ceiling. " + narrowing_hint(plan))

  -- Step 2 — cost-aware rewrite, before any delivery decision.
  -- Candidates preserve analytical intent; each applies one rewrite rule.
  if z > SYNC:
    rewrites = [
      rewrite_strategy(plan, SHORTEST)    if has_ALL_PathPred(plan),
      rewrite_depth_limit(plan, depth=3)  if has_unbounded_TrailExpr(plan),
      rewrite_cone(plan, BOUND_FREE)      if has_FREE_FREE_scope(plan),
    ]
    viable = [r for r in rewrites if zone(r.cost_estimate, budget) < z]
    if viable:
      r = best(viable)
      return REWRITE(plan=r, warning=rewrite_explanation(r), then=deliver(r, budget))

  -- Step 3 — over the executable ceiling and no viable rewrite:
  -- approximate with a guarantee, else ask the user to narrow, else refuse.
  if z = OVER:
    s = sampling_plan(plan, budget)                    -- NONE unless S1–S4 hold
    if s ≠ NONE:
      return SAMPLE(fraction=s.fraction, method=RANDOM,
                    error_bound=SAMPLE_MAX_ERROR, confidence=1 − SAMPLE_DELTA,
                    then=deliver(s.plan, budget))
    options = clarification_options(plan, budget)      -- [] unless C1–C2 hold
    if options ≠ []:
      return CLARIFY(options, cost_per_option=[o.cost_estimate for o in options])
    return REJECT("Query exceeds max_estimated_cost and admits no rewrite, "
                  + "guaranteed sample or refinement. " + narrowing_hint(plan))

  -- Step 4 — the submitted plan is executable; choose its delivery.
  return deliver(plan, budget)


deliver(plan: ExportPlan, budget: ExecutionBudget) → Delivery:
  -- Precondition: zone(plan.cost_estimate, budget) ≤ DEFERRED.
  -- Never rewrites, samples, clarifies or rejects.

  match zone(plan.cost_estimate, budget):
    DEFERRED:  return ASYNC(sink=default_sink, callback=user_endpoint, ttl=24h)
    STREAMING: return STREAM
    SYNC:
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

**Rewrite precedence.** A rewrite is *viable* only when it moves the plan to a strictly cheaper zone. This fixes every case:

- A rewrite that lowers cost without leaving the plan's zone is discarded. It would give up exactness and buy no better delivery, so the submitted plan is delivered as-is (Step 4) or, in the OVER zone, falls through to Step 3.
- A rewrite that still lands above a threshold is returned as `REWRITE` with the matching delivery: `then=STREAM` in the STREAMING zone, `then=ASYNC` in the DEFERRED zone. `STREAM` and `ASYNC` are returned for the submitted plan only after every rewrite candidate has been estimated and found non-viable.
- A viable rewrite always lands in DEFERRED or below (viable means `zone < z` and `z ≤ OVER`), so the precondition of `deliver` holds.
- `best(viable)` selects the lowest zone; ties are broken by rule order in the `rewrites` list (strategy, depth limit, cone), then by lowest `cost_estimate`. The warning describes the rewrite actually returned.
- A SYNC plan is never rewritten: no cheaper zone exists.

**SAMPLE branch.** `sampling_plan(plan, budget)` returns a sampled plan only when all of S1–S4 hold; otherwise it returns `NONE` and the gate moves on to CLARIFY.

```
S1  plan is a snapshot query with Band 2, empty GroupBy, no Having and no Band 3; not a Q_delta.
S2  The aggregated context holds exactly one tuple per anchor node (no fan-out Join),
    so the sampling unit is the aggregation unit.
S3  Every aggregate is covered by the guarantee:
      (a) AVG(PropRef p) with Where = TRUE, where the schema declares a finite range [lo, hi] for p; or
      (b) COUNT(*) under an arbitrary Where φ, reported as the proportion of anchor nodes satisfying φ.
    Observed minima and maxima in GraphStatistics are not a declared range.
S4  |A|, the anchor cardinality, is known from GraphStatistics, and with
      n* = ⌈ ln(2k / SAMPLE_DELTA) / (2 · SAMPLE_MAX_ERROR²) ⌉      -- k = number of aggregates
      f* = n* / |A|
    both n* < |A| and zone(cost(sample(plan, f*)), budget) ≤ DEFERRED hold.

sampling_plan returns (fraction = f*, plan = sample(plan, f*)).
```

Data required: `GraphStatistics` anchor cardinality, the declared value range of every averaged property, and a cost estimate for the sampled plan. The gate emits `method=RANDOM` — uniform sampling of anchor nodes without replacement. `STRATIFIED` and `RESERVOIR` remain members of the type for callers that configure sampling explicitly; the gate does not derive an `error_bound` for them.

`error_bound` is stated on the range-normalised scale and holds jointly for all `k` aggregates with probability at least `confidence`: ±`error_bound · (hi − lo)` for an average, ±`error_bound` for a proportion, ±`error_bound · |A|` for the count recovered from it. No bound is promised for anything outside S3 — SUM, MIN, MAX, PROD, distinct counts, PathAgg, TemporalAgg, grouped or windowed aggregates, row-level results, and TrailExpr/GraphExpr values. A plan containing any of these is not sampled by the gate; it is clarified instead.

**CLARIFY branch.** `clarification_options(plan, budget)` returns refinements of the submitted question; the gate asks the user to choose rather than approximating whenever no guarantee covers the query.

```
C1  Each option is a single descent step in L(G_AB) produced by the recommender (§7)
    and is BDD-valid.
C2  Each option o is executable: zone(o.cost_estimate, budget) ≤ DEFERRED.

Options are ordered by zone, then cost_estimate ascending, then recommender rank,
and truncated to MAX_CLARIFY_OPTIONS. cost_per_option[i] is the cost of options[i].
```

Data required: the recommender's refinement candidates and `GraphStatistics.path_cardinality` for their cost estimates. Fallback: when no option satisfies C1–C2 the gate returns `REJECT`. An option chosen by the user is a new question and is submitted to `gate()` as such; by C2 it is executable, so the exchange ends in a delivery or a `REWRITE`.

**Decision precedence.** `REJECT` (hard ceiling) › `REWRITE` › `SAMPLE` › `CLARIFY` › `REJECT` (fallback) › `ASYNC` › `STREAM` › `PAGINATE` › `PROCEED`. SAMPLE, CLARIFY and the fallback REJECT are reachable only in the OVER zone.

**Boundary cases.** Normative for any implementation of `gate()`. Budget: `streaming_threshold = 100`, `async_threshold = 1 000`, `max_estimated_cost = 10 000`, `HARD_CEILING_FACTOR = 2` (hard ceiling 20 000); rows within `max_result_rows` unless stated.

| # | `est` | Rewrite candidates | S1–S4 | Options (C1–C2) | Decision |
|---|---:|---|---|---|---|
| T1 | 100 | — | — | — | `PROCEED` — equals `streaming_threshold`, stays SYNC |
| T2 | 100, rows over limit | — | — | — | `PAGINATE` |
| T3 | 101 | none | — | — | `STREAM` |
| T4 | 1 000 | none | — | — | `STREAM` — equals `async_threshold` |
| T5 | 1 001 | none | — | — | `ASYNC` |
| T6 | 10 000 | none | — | — | `ASYNC` — equals `max_estimated_cost` |
| T7 | 5 000 | SHORTEST → 80 | — | — | `REWRITE(then=PROCEED)` |
| T8 | 5 000 | SHORTEST → 600 | — | — | `REWRITE(then=STREAM)` |
| T9 | 5 000 | SHORTEST → 1 200 | — | — | `ASYNC` — rewrite stays in DEFERRED, not viable |
| T10 | 15 000 | cone → 900 | hold | 2 options | `REWRITE(then=STREAM)` — rewrite precedes SAMPLE |
| T11 | 15 000 | cone → 12 000 | hold, sampled cost 40 | 2 options | `SAMPLE(then=PROCEED)` — rewrite stays in OVER |
| T12 | 15 000 | none | fail (GroupBy present) | costs 700, 4 000 | `CLARIFY`, options in that order |
| T13 | 15 000 | none | fail | none | `REJECT` — fallback |
| T14 | 20 000 | none | fail | 1 option | `CLARIFY` — equals the hard ceiling, still OVER |
| T15 | 20 001 | SHORTEST → 50 | hold | 2 options | `REJECT` — hard ceiling precedes; nothing else is evaluated |

Budgets rejected at construction: `streaming_threshold = 200, async_threshold = 100` (inverted); `streaming_threshold = −1` (negative); `async_threshold = 20 000, max_estimated_cost = 10 000` (above the ceiling); any threshold NaN or ∞. A budget with `streaming_threshold = async_threshold = 100` is valid: `est = 100` gives `PROCEED` and `est = 101` gives `ASYNC`.

**Four graceful handling strategies:**

**Strategy 1 — Cost-aware rewriting.** Moves the query down the strategy refinement order (§3.3) while preserving analytical intent. Rewrites are transparent — the user sees a note explaining what was approximated.

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

**Strategy 3 — Sampling with error bounds.** Applied at the `Anchor` step in Band 1, and only to plans that satisfy S1–S4 above. The sample size is derived from the target error via Hoeffding's inequality for the mean of range-normalised values, with `n = f · |A|` anchor nodes drawn uniformly without replacement.

```
Sampling (k aggregates, δ = SAMPLE_DELTA, ε = SAMPLE_MAX_ERROR):
  n*            = ceil(log(2k/δ) / (2 * ε²))           -- Hoeffding, union bound over k
  anchor_sample = SAMPLE(A, fraction = n*/|A|, method=RANDOM)
  error_bound   = sqrt(log(2k/δ) / (2 * n*))  ≤  ε
  -- k=1, δ=0.05, ε=0.05  →  n* = 738
  -- returned with: "Estimated (±{error_bound*100:.1f}% of range at 95% confidence)"
```

`GraphExpr` algorithms are not sampled by the gate. Their approximate variants are substituted by the PreComputation timeout guard (`substitute_approximation`) or by a rewrite rule, and carry the accuracy statement of the substituted algorithm — not the Hoeffding bound above, and not a `SAMPLE` decision.

```
GraphExpr approximations:
  BETWEENNESS_CENTRALITY  →  Monte Carlo approximation (k=200 samples)
  PAGE_RANK               →  power iteration with ε=0.01 convergence threshold
  SIGNATURE_ENTROPY       →  reservoir sample of path instances (k=1000)
```

**Strategy 4 — Async execution with materialisation.** In the DEFERRED zone (cost above `async_threshold`, at most `max_estimated_cost`), once no viable rewrite exists, the query is scheduled as a `PipelineArtifact` (§2.4) — the result is written to a `SinkTarget` and the query DAG node id is the cache key. The user receives an immediate acknowledgement; subsequent identical questions are answered from the cached result.

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

**Clarification messages are lattice refinements.** When `CLARIFY` is returned, the options are not generic suggestions — they are specific BDD-valid descent steps in `L(G_AB)` that bring the query back within `max_estimated_cost` (conditions C1–C2), cheapest first:

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

Layers 1 and 2 operate on the schema graph: candidates are drawn from `Q_valid(A, B)` over `G_AB^S` and filtered against `Σ`, so the set of suggestions that can be offered is fixed by the schema. Layer 3 alone reads the instance graph, through `GraphStatistics` and trail metrics, to rank the survivors.

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
2. BDD feasibility: ensure Q_current ∩ Q_j is logically satisfiable (not provably empty; §6.1)
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

**B₁:** ScalarPred; PathPred including constellation paths; TrimJoin (incl. REACHABLE_FROM/TO); TemporalCompose; strategy refinement order; temporal property classes; trail space pivots.

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
TARJAN_SCC + CAUSAL → causal cycle report: the components of §2.4 validation, surfaced when
                      causal_acyclic is VIOLATED or UNKNOWN. Never a precondition-free signal that
                      the DAG optimisation is safe.
BETWEENNESS on G_AB → pivotal intermediates.
OUTGOING_SIGNATURES high → two-stage exploration.
SIGNATURE_ENTROPY high → GLOBAL_DOMINANT_SIGNATURE first.

### 7.7  Parametric suggestions

The parameter space `Π(q)` (§8.15) introduces a second axis of recommender navigation — parametric moves that change the configuration of a question without changing its structural skeleton.

**Parametric suggestion types:**

| Suggestion | Navigates | Example |
|---|---|---|
| Temporal refinement | Π_temporal | "Try ROLLING(30d) instead of ROLLING(7d) for trend visibility" |
| Strategy shift | Π_strategy | "Use K_SHORTEST(3) — three distinct path types connect these nodes" |
| Aggregation alternative | Π_agg | "Switch from SUM to AVG to normalize by entity count" |
| Algorithm variant | Π_algo | "LOUVAIN communities are too coarse — try LABEL_PROPAGATION" |
| Threshold relaxation | Π_filter | "Relax revenue > 10000 to revenue > 5000 — current filter eliminates 95% of tuples" |

Parametric suggestions are validated by `BDD_parametric` feasibility (§8.15) and scored by the same trail metrics that score structural suggestions. The distinction is mechanical: structural suggestions change `BDD_structural`; parametric suggestions change `BDD_parametric` variables.

**Formal basis justification.** The polynomial tractability of parametric recommendation follows from the semantic basis (§8.16.5): the recommender navigates `Basis(A_full)` rather than `A_full` itself, and every basis element has polynomial neighbourhood size (§8.16.6). Parametric moves within a fixed structural skeleton navigate Level 3 of the vocabulary hierarchy (§8.16.3), which grows polynomially — making them inherently cheaper than structural moves (Level 1, factorial).

**Cost-aware parametric navigation.** The `ExecutionBudget` gate (§6.3) evaluates `cost(q, π)`. When a `REWRITE` decision is returned, the recommender searches `Π_valid(q)` for alternative parameterizations with lower cost — this is **parametric rewriting**. Example: `Strategy = ALL` triggers `REWRITE` → suggest `K_SHORTEST(5)` with 100× lower cost and 95% of the analytical coverage.

**Annotation-driven parametric rules:**

| Annotation | Parametric constraint | Parametric suggestion |
|---|---|---|
| Grain(EVENT) | Allen excluded from Π_temporal | Use point-based BEFORE/AFTER |
| Grain(PERIOD) | SUM excluded from Π_agg | Use LAST or AVG |
| FactlessFact | SUM/AVG/MIN/MAX excluded from Π_agg | Use COUNT |
| MedallionLayer(BRONZE) | BRONZE excluded from GroupBy in Π_agg | Filter to SILVER+ before GroupBy |
| WeightConstraint(ALLOC) | Unweighted PathAgg excluded | Use weighted form with explicit weight ref |

---

## 8  Theoretical Foundations

### 8.1  DGM as Kripke structure

**Theorem.** The instance graph G_I = (N_I, E_I, P) is a Kripke structure K=(S,R,L) under S=N_I, R=E_I, L=P. In §8.1–§8.10, G, N and E denote G_I, N_I and E_I.

R is not assumed total; CTL formulas are interpreted over maximal paths (§4.1, D30).

Consequences: CTL/LTL model checking applies directly. BDD layer is standard symbolic CTL machinery (Bryant 1986; Clarke et al. 1986). PathPred(FORALL/EXISTS,ALL) are CTL path quantifiers A/E. TemporalMode realises full CTL operator hierarchy.

G^T is the transpose of K: it reverses adjacency. Backward-cone RelationshipSubgraph(Free,Bound(B)) is the set of paths leading to B in path order. It is the computation tree for past-temporal formulas about B only when restricted to time-ordered edge types (§2.3); over other edge types it is a structural predecessor set and says nothing about earlier moments. SINCE/ONCE/PREVIOUSLY TemporalMode execute as model checking on G^T under that restriction.

**RDF named graphs as multiple accessibility relations.** An RDF Dataset consists of a default graph and zero or more named graphs — each named graph is a distinct set of triples. In Kripke terms, an RDF Dataset with named graphs G₁, G₂, ..., Gₙ is a Kripke structure with multiple accessibility relations R₁, R₂, ..., Rₙ — one per named graph. DGM's computed `RelationshipSubgraph(A, B, strategy)` is a *restriction* of the single accessibility relation R = E to the subset of edges lying on simple paths between A and B. This is a structural distinction: RDF named graphs are asserted by the author; DGM's `G_AB` is derived from the graph topology by traversal. The analytical consequence is significant — DGM's scoped subgraph has formal completeness guarantees (§8.11) that RDF named graphs do not.

### 8.2  Functional completeness

**Theorem.** DGM filter tree language over ScalarPred atoms is functionally complete.

Proof sketch: AND/OR/NOT/ScalarPred generates all DNF formulas over finite atom set A. PathPred and SignaturePred extend without restricting. ∎

### 8.3  Minterms, maxterms, canonical form

Logical properties, over the BDD variables and relative to Δ (§6.1): equivalence — same minterm set; unsatisfiability — empty minterm set; tautology — full minterm set; implication — subset. These are properties of the formula. The number of minterms *realised* by the tuples of a join context C is at most |C|; that is a property of the data, and a logically satisfiable formula may be realised by no tuple at all.

### 8.4  PathPred as exponential compression

PathPred(EXISTS,ALL,φ) over k instances: k-disjunct DNF compressed. FORALL: dual k-conjunct CNF via NOT EXISTS NOT over path instances. TemporalMode extends to continuous path properties.

### 8.5  EXISTS/FORALL as minterm/maxterm duality

EXISTS = disjunction/minterms. FORALL = conjunction/maxterms. SHORTEST/MIN_WEIGHT collapse both to single term. This duality is over path instances; with a TemporalMode the dual pairs are (EF, AG), (EG, AF) and (EX, AX), so eliminating FORALL exchanges EVENTUALLY and GLOBALLY (§4.1).

### 8.6  Finite traversal space

The number of simple path instances in G_I is finite: total ≤ Σₖ |N_I|! / (|N_I|-k)!. BridgeSemantics(CAUSAL), once validated acyclic (§2.4): DAG — no cycles. This bounds execution, and grows with the data. The number of path *patterns* is a schema quantity: at most Σ_{j=0}^{k} (2·|E_S|)^j patterns of at most k hops, each hop taking an edge type forwards or (VerbHop⁻¹) backwards. A Repeat contributes its body's hops once: `min` and `max` are parameters (§8.15), not part of the pattern.

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

**Level.** This section is stated on the schema graph. `A` and `B` are node types in `N_S`; `G_AB`, `N_AB` and `E_AB` abbreviate `G_AB^S`, `N_AB^S` and `E_AB^S` (§3.4.5). A question is a *structural* question: constants such as thresholds and windows are parameters in `Π(q)` (§8.15), not part of `q`. Path patterns are taken up to a hop bound `k` (`max_hops`, §8.16.2); where `G_AB` is acyclic, `k` may be taken as the length of its longest path.

**Theorem (Question completeness).** For any two node types `A, B ∈ N_S` with schema-level subgraph `G_AB = (N_AB, E_AB, τ_N, τ_E, K_S, Σ|_AB)`, the set of valid analytical questions `Q_valid(A, B)` is finite and constructively enumerable from `G_AB` and `Σ|_AB` alone.

**Proof sketch.** Three results combine:

(1) From §8.2 (functional completeness): the predicate language over `ScalarPred` atoms derived from the property signatures `{K_S(T) | T ∈ N_AB}` and `{K_S(e) | e ∈ E_AB}` is functionally complete. A structural atom is a declared property together with a comparison operator, its constant left as a parameter; the atom set is finite given finite `N_AB`, `E_AB` and finite signatures.

(2) From §8.6 (path patterns): the number of distinct `BoundPath` patterns of at most `k` hops over `G_AB` is at most `Σ_{j=0}^{k} (2·|E_AB|)^j`. The `PathPred` and `PathAgg` spaces are therefore finite.

(3) From §8.3 (minterm enumeration): the minterms over a finite set of `m` structural atoms number at most `2^m`. Finite. The number of minterms *realised* by a particular instance graph is further bounded by the size of the join context `C`, but that is a property of the data and plays no part in the size of the question space.

The Band 2 aggregation space is finite given the finite measure properties declared by the fact types in `F_S ∩ N_AB` and finite `TemporalWindow` variants. The Band 3 ranking space is finite given the finite Band 2 output schema. The cross-product of all three bands over `G_AB` is therefore finite. ∎

**Corollary (Schema determinacy).** `Q_valid(A, B)` and its cardinality are functions of `(G_AB, K_S, Σ|_AB, k)` only. Two instance graphs typed over the same schema graph have the same `Q_valid(A, B)`; inserting or deleting instances changes query results, `GraphStatistics` and recommender scores (§8.12), never the question space. By the projection lemma (§3.4.5), every question in `Q_valid(A, B)` is well-typed for every pair of instances `(a, b)` with `h_N(a) = A`, `h_N(b) = B`.

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
top    = tautological query: Anchor(A), no Where, no GroupBy, no Window
           — returns all tuples of instances typed in G_AB
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

**Size characterisation.** While `Q_valid(A, B)` is finite (above), its cardinality ranges from trivially small to factorial in graph size. §8.16 decomposes the full analytical space `A_full(A, B)` by vocabulary level, identifies the dominant growth factor (path combinatorics), and proves that the recommender's navigation neighbourhood remains polynomial despite the factorial worst-case — see §8.16.6 for the bound and §8.16.7 for the per-level navigation strategies.

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

### 8.15  Parameter space

**Motivation.** `Q_valid(A, B)` (§8.11) enumerates the set of structurally valid analytical questions. Each question `q ∈ Q_valid(A, B)` is a skeleton with parameterizable slots — temporal windows, aggregation functions, filter thresholds, strategy choices, algorithm hyperparameters. The question space captures *what can be asked*; the parameter space captures *how each question can be configured*. The full analytical capability is their dependent product, not the question space alone.

**Definition (Parameter space).** Given a question `q ∈ Q_valid(A, B)` with structural skeleton `S(q)`, the **parameter space** `Π(q)` is the product of all parameter slots in `q`:

```
Π(q) = Π_temporal(q) × Π_strategy(q) × Π_filter(q) × Π_agg(q) × Π_window(q) × Π_algo(q)
```

Each factor corresponds to a family of parameterizable DGM constructs:

```
Family           Parameters                              Domain
─────────────────────────────────────────────────────────────────────────────
Π_temporal       as_of                                   Timestamp (continuous)
                 node_resolution                         {STRICT, LAX}
                 edge_resolution                         {STRICT, LAX}
                 per-hop TemporalOrdering                point | Allen | UNTIL/SINCE

Π_strategy       per BoundPath strategy                  {ALL, SHORTEST, K_SHORTEST(k), MIN_WEIGHT}
                 per Repeat min, max, mode               ℕ; ℕ ∪ {UNBOUNDED}; {SIMPLE, TRAIL, WALK}
                 K_SHORTEST cardinality k                ℕ

Π_filter         per ScalarPred threshold                property domain (ℝ, String, ...)
                 per PathPred sub_filter                 recursive Π_filter

Π_agg            per Agg slot function                   {SUM, COUNT, AVG, MIN, MAX, PROD}
                 per TemporalAgg window                  TemporalWindow variant + params
                 GroupBy attribute selection              𝒫(attributes of N_AB in join scope)

Π_window         PARTITION BY                            subset of Band 2 output attrs
                 ORDER BY                                attr × {ASC, DESC}
                 frame                                   {ROWS, RANGE} × bounds
                 Qualify threshold                        ℝ

Π_algo           PAGE_RANK damping                       (0, 1)  (continuous)
                 PAGE_RANK iterations                    ℕ
                 K_SHORTEST k                            ℕ
                 SIGNATURE_ENTROPY max_depth             ℕ
                 COMMUNITY_LABEL variant                 {LOUVAIN, LABEL_PROPAGATION,
                                                          CONNECTED_COMPONENTS, TARJAN_SCC}
```

**Definition (Full analytical space).** The full analytical space is the dependent product:

```
A(A, B) = Σ_{q ∈ Q_valid(A, B)} Π(q)
```

This is a sigma type (dependent sum) — `Π(q)` depends on which `q` we are parameterizing, because different questions have different parameter slots. A snapshot query with no Band 2 has no `Π_agg` factor; a query with no `GraphExpr` has no `Π_algo` factor.

**Annotation constraints on parameters.** `Σ|_AB` constrains the parameter space just as it constrains the question space:

```
Π_valid(q) = { π ∈ Π(q) | π satisfies all annotation constraints from Σ|_AB }
```

Annotation projection rules:

| Annotation | Constraint on Π(q) |
|---|---|
| Grain(f, EVENT) | Allen interval relations excluded from Π_temporal; only BEFORE/AFTER |
| Grain(f, PERIOD) | SUM excluded from Π_agg for cross-period aggregation |
| FactlessFact(f) | {SUM, AVG, MIN, MAX} excluded from Π_agg; only COUNT |
| SCDType(d, SCD1) | Temporal resolution parameters in Π_temporal irrelevant |
| Degenerate(d) | d excluded from GroupBy selection in Π_agg |
| BridgeSemantics(CAUSAL) | DAG traversal constraints on Π_strategy |
| MedallionLayer(n, BRONZE) | BRONZE nodes excluded from GroupBy in Π_agg; planner warning activated |
| WeightConstraint(ALLOCATIVE) | Unweighted PathAgg excluded from Π_agg |

**Theorem (Parameter completeness).** For any question `q ∈ Q_valid(A, B)`, the valid parameter space `Π_valid(q)` is constructively characterizable from the query structure `q` and `Σ|_AB`.

*Proof sketch.* Each parameter slot has a type-determined domain. Each annotation constraint projects out a definable subset. Finite Σ → finite constraints → definable complement. The discrete projection `Π_discrete_valid(q)` is finite and enumerable; the continuous projection is characterized by interval constraints on timestamps and thresholds. ∎

**Parameter type stratification.** Parameters fall into three computational types:

```
Type                Examples                                   Cardinality
────────────────────────────────────────────────────────────────────────────
Discrete finite     Strategy choice, AggFn, resolution mode,   |Π_discrete| finite,
                    CommAlg variant                             enumerable

Discrete infinite   K_SHORTEST(k), max_depth, iterations       countable (ℕ-valued)

Continuous          as_of timestamp, filter thresholds,         uncountable (ℝ-valued)
                    PAGE_RANK damping
```

The discrete finite projection is the **parametric skeleton** — the finite set of structurally distinct configurations. The continuous parameters fill metric-valued slots within each discrete configuration.

**Corollary (Finite parametric skeleton).** The discrete parametric skeleton `Π_discrete_valid(q)` is finite for every `q ∈ Q_valid(A, B)`. The full discrete analytical skeleton `|Q_valid| × max|Π_discrete_valid|` is therefore finite — the recommender can enumerate all structurally distinct parameterizations without visiting the continuous dimensions.

**Parameter equivalence.** Two parameterizations can yield identical results:

```
π₁ ≡_q π₂  iff  q(G_AB, π₁) = q(G_AB, π₂)  for all valid G_AB
```

Examples: `SHORTEST ≡ K_SHORTEST(1)` always; `as_of = t₁ ≡ as_of = t₂` when no nodes have SCD-versioned properties between t₁ and t₂. The quotient `Π_valid(q) / ≡_q` is the **effective parameter space** — distinct parameterizations that can produce different answers.

**Parameter sensitivity and cost.** Different parameterizations of the same structural question can have vastly different execution costs:

```
Strategy = SHORTEST           → O(|E|) BFS
Strategy = ALL                → O(|N|! / (|N|-k)!) path enumeration
TemporalWindow = ROLLING(7d)  → small scan window
TemporalWindow = ROLLING(365d) → large scan window
```

The `ExecutionBudget` gate (§6.3) evaluates `cost(q, π)` — cost is a function of both structure and parameters. The `BudgetDecision` taxonomy applies: a `PROCEED` decision for `π₁` may become `REWRITE` or `SAMPLE` for a more expensive `π₂` of the same structural question. The `REWRITE` decision can suggest alternative parameterizations in `Π_valid(q)` with lower cost — this is **parametric rewriting**, distinct from structural rewriting (which changes `q`).

**BDD extension for parametric feasibility.** The BDD (§6.1) currently encodes structural predicates. Discrete parameter validity can be encoded as additional BDD variables, extending the single-check feasibility guarantee:

```
BDD_full = BDD_structural × BDD_parametric

BDD_parametric encodes:
  ¬(Grain(PERIOD) ∧ AggFn = SUM)
  ¬(FactlessFact ∧ AggFn ∈ {SUM, AVG, MIN, MAX})
  ¬(Grain(EVENT) ∧ TemporalOrdering ∈ Allen_interval_set)
  ¬(SCDType(SCD1) ∧ temporal_resolution ≠ identity)
  ¬(MedallionLayer(BRONZE) ∧ in_GroupBy_selection)
```

Verification that a parameterization `(q, π)` is valid reduces to a single BDD evaluation — O(1) after BDD construction.

**Recommender navigation in A(A, B).** The recommender (§7) currently navigates `L(G_AB)` — the question lattice. Parameter space adds a second axis. The full recommender navigation space is:

```
Nav(A, B) = L(G_AB) × Π

Structural moves:    change q (add/remove predicate, change join path)     — in L(G_AB)
Parametric moves:    change π (different AggFn, wider window, new strategy) — in Π_valid(q)
Mixed moves:         change both simultaneously                             — in Nav(A, B)
```

Parametric moves are often lower-cost than structural moves — they do not change the query skeleton, only its configuration. The recommender can prioritize parametric suggestions when the current structural position is analytically productive but the parameterization is suboptimal.

**Relationship to question algebra.** The question algebra `Q_algebra(A, B)` (§8.13) is the closure of `Q_valid` under composition. Each element of `Q_algebra` is itself a structural skeleton with its own parameter space. The full composed analytical space is:

```
A_composed(A, B) = Σ_{q ∈ Q_algebra(A, B)} Π(q)
```

The query DAG canonical form (§8.14) extends naturally: two parameterized composed queries `(q₁, π₁)` and `(q₂, π₂)` are equivalent iff they reduce to the same DAG node id with the same discrete parametric configuration on each leaf. Continuous parameters (timestamps, thresholds) are not part of the canonical form — they affect result values but not query structure.

### 8.16  Vocabulary decomposition and analytical space characterisation

§8.11 proves that `Q_valid(A, B)` is finite. §8.15 adds parameter space `Π`. This section decomposes the full analytical space `A_full(A, B)` into vocabulary levels, quantifies its size, and establishes bounds that make the recommender (§7) tractable.

#### 8.16.1  Six vocabularies

Every analytical question is assembled from six vocabularies inherent to the schema-level subgraph `G_AB = G_AB^S` (§3.4.5):

| Vocabulary | Symbol | Source | Size driver |
|---|---|---|---|
| **Node terms** | `V_N` | `N_AB^S` — dimension and fact node types | `|N_S|` (number of node types) |
| **Edge terms** | `V_E` | `E_AB^S` — edge types | `|E_S|` (number of edge types) |
| **Property terms** | `V_P` | `K_S(T)` for all `T ∈ N_AB^S` — declared properties | `Σ |K_S(T)|` |
| **Predicate terms** | `V_Pred` | `PathPred`, `TemporalMode`, `SignaturePred` | Fixed per §4–§5 |
| **Operator terms** | `V_Op` | `AggFn`, `ComparisonOp`, `JoinKind` | Small constant set |
| **Parameter terms** | `V_Par` | `Π(q)` for each `q ∈ Q_valid` | §8.15 |

The first three vocabularies are **schema-derived**: they grow with the size of the schema graph and are unaffected by the number of instances. The last three are **formalism-derived** (fixed or slowly growing).

#### 8.16.2  Factored size estimate

The full analytical space factors as:

```
|A_full(A, B)| ≤ f_structure × f_predicate × f_operator × f_parameter
```

where:

- `f_structure = |paths(G_AB, max_hops)|` — the number of distinct traversal paths (factorial in worst case, bounded in practice by §8.6 acyclicity)
- `f_predicate = Π_{q} |applicable_predicates(q)|` — predicates applicable per path
- `f_operator = |V_Op|^k` — operator combinations over `k` property references (small constant)
- `f_parameter = Σ_{q} |Π_valid(q)|` — parametric combinations per structural skeleton

#### 8.16.3  Four-level vocabulary hierarchy

The factors fall into four asymptotic levels ordered by growth rate:

| Level | Factor | Growth | Dominant term |
|---|---|---|---|
| **Level 1 — Structure** | `f_structure` | Factorial / exponential in `|E_AB|` | Path combinatorics |
| **Level 2 — Predicate** | `f_predicate` | Exponential in predicate count | `2^|V_Pred|` subsets |
| **Level 3 — Parameter** | `f_parameter` | Polynomial in `|V_P|` and `|V_Par|` | Cartesian product of finite ranges |
| **Level 4 — Operator** | `f_operator` | Constant | `|V_Op|` is fixed by the formalism |

**Level 1 dominates.** The structural path space (Level 1) is the primary source of analytical space size. All other levels are multiplicative but grow strictly more slowly.

#### 8.16.4  Semantic sparsity and effective analytical space A_eff

Not every syntactically valid combination is semantically meaningful. The **effective analytical space** `A_eff ⊆ A_full` contains only those `(q, π)` pairs where:

1. `q ∈ Q_valid(A, B)` — the structural query is valid (§8.3 BDD-satisfiable)
2. `π ∈ Π_valid(q)` — the parameter configuration is compatible (§8.15)
3. `∂result/∂p ≠ 0` for at least one parameter `p` — the result is **sensitive** to the parameterization (non-degenerate)

The sensitivity function `∂result/∂p` identifies degenerate parameter regions (where changes have no effect on the result) and excludes them. In practice, `A_eff` is orders of magnitude smaller than `A_full`:

```
|A_eff(A, B)| ≪ |A_full(A, B)|
```

The ratio `|A_eff| / |A_full|` is the **semantic density** of the model — higher density means the schema is more efficiently utilized.

#### 8.16.5  Semantic basis

Define the **semantic basis** as the minimal generating set of the effective space:

```
Basis(A_full) = minterms(Q_valid) × interesting_params(P_Σ)
```

where:

- `minterms(Q_valid)` are the ∧-irreducible elements of the BDD lattice — the atomic valid queries from which all others are composed via `Q_algebra`
- `interesting_params(P_Σ)` are the parameter configurations where sensitivity is non-zero for at least one minterm

The basis is the **minimal set of questions the system must be able to answer** — every other question is a composition (§8.13) of basis elements with parameter specialization. The recommender (§7) navigates `Basis(A_full)` rather than `A_full` itself.

#### 8.16.6  Polynomial neighbourhood bound

For any point `(q, π)` in `A_eff`, the **navigation neighbourhood** — the set of reachable points in one recommender step — has polynomial size:

```
|neighbourhood(q, π)| ≤ O(|V_P| + |free_params(q)| × max(cardinalities))
```

where `|V_P|` bounds the structural moves (property substitutions), `|free_params(q)|` bounds the parametric moves (§8.15), and `cardinalities` are the domain sizes of the free parameters.

This bound is polynomial in graph size despite the factorial worst-case of `A_full` itself. The reason: the recommender performs **local** navigation (one-hop lattice/parameter moves) rather than global enumeration.

#### 8.16.7  Navigation strategies per vocabulary level

Each vocabulary level calls for a different navigation strategy:

| Level | Strategy | Mechanism |
|---|---|---|
| **Level 1 — Structure** | **Local navigation** — never enumerate; explore via lattice adjacency | Recommender (§7) moves in `L(G_AB)` |
| **Level 2 — Predicate** | **BDD pruning** — enumerate satisfiable predicate subsets only | §8.3 BDD compaction |
| **Level 3 — Parameter** | **Sensitivity-guided** — sweep only non-degenerate regions | `GraphStatistics` (§8.15) |
| **Level 4 — Operator** | **Enumerate all** — the constant-size operator set is always fully explored | Direct |

The key insight is that the dominant factor (Level 1) is the one that is **never enumerated** — the recommender navigates it locally. The sub-dominant factors are progressively more enumerable, and the smallest factor (Level 4) is fully enumeratable.

#### 8.16.8  Worked example: BookClub social network

For the `social_network` domain the schema graph has 3 node types and 5 edge types. The instance graph used for illustration has 10 readers, 8 books and 7 genres — 25 node instances and 69 edge instances — and none of those four counts enters the table below:

| Vocabulary | Size | Notes |
|---|---|---|
| `V_N` | 3 | Leitor, Livro, Genero — node types |
| `V_E` | 5 | SEGUE, RECOMENDOU, LEU, PERTENCE_AO_GENERO, INTERESSE_EM — edge types |
| `V_P` | ~15 | nome, idade, titulo, autor, nota, forca_conexao, ... |
| `V_Pred` | ~10 | PathPred + TemporalMode (formalism-fixed) |
| `V_Op` | ~6 | COUNT, SUM, AVG, MIN, MAX, EXISTS |
| `V_Par` | ~20 | nota thresholds, forca_conexao ranges, date windows |

Estimated: `f_structure ≈ 5! = 120` (short-path dominated), `f_predicate ≈ 2^10 = 1024` (but BDD compacts to ~50 satisfiable), `f_parameter ≈ 20^2 = 400`, `f_operator ≈ 6`. Naive `|A_full| ≈ 120 × 1024 × 400 × 6 ≈ 2.9 × 10^8`. With semantic sparsity: `|A_eff| ≈ 50 × 50 × 40 × 6 = 600,000`. Neighbourhood size: `O(15 + 3 × 20) = O(75)` — polynomial. Every figure in this example is computed from the schema graph and its annotations: an eleventh reader, or ten thousand more, leaves all of them unchanged. An individual reader such as `Ana` is an element of `N_I` with `h_N(Ana) = Leitor`; she contributes to query results and to recommender scores, not to `V_N`.

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
| `MedallionLayer` annotation | Not present | ❌ Missing |

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
- FORALL elimination by CTL duality (§4.1); TemporalMode SQL templates.

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
   Constraint: output must exist in {dom K_S(T) | T ∈ N_S}
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
    "retail customers" → ScalarPred(PropRef(c.segment), (= 'retail'))
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

**Closed PropRef vocabulary.** Entity resolution maps NL terms to the declared properties `{dom K_S(T) | T ∈ N_S}`. A term that does not resolve to any existing property triggers `UNRESOLVED` — the interface asks for clarification rather than guessing a column name.

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
    graph:           object          # DGM = (G_S, G_I, h)
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
                 (>= ArithExpr(now, -, 90 days))),
      ScalarPred(PropRef(a.window_end), (<= now))
    )
  -- With now = 2024-06-30T00:00Z the horizon starts at 2024-04-01T00:00Z:
  --   window_end 2024-06-01, Missing   → selected   (inside the horizon)
  --   window_end 2024-04-01T00:00Z     → selected   (boundary is inclusive)
  --   window_end 2024-01-15, Missing   → excluded   (older than the horizon; SAFETY violation below)
  --   window_end 2024-07-01            → excluded   (window not closed yet)
  --   window_end NULL                  → excluded   (UNKNOWN)
  --   window_end 2024-06-01, Complete  → excluded   (state)
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
| PipelineArtifact | fact | Five-state lifecycle; WritePlan inference; scheduler CTL |
| MedallionLayer | node/edge | Layer origin tagging; Bronze quality warning; min_layer export |

**Parameter families (Π):**

| Family | Key parameters | Domain |
|---|---|---|
| Π_temporal | as_of, node/edge resolution, TemporalOrdering | Timestamp; {STRICT, LAX}; point/Allen/UNTIL |
| Π_strategy | per-path Strategy, K_SHORTEST k | {ALL, SHORTEST, K_SHORTEST(k), MIN_WEIGHT}; ℕ |
| Π_filter | ScalarPred thresholds, PathPred sub_filters | property domain (ℝ, String, ...); recursive |
| Π_agg | AggFn, TemporalWindow, GroupBy selection | {SUM,COUNT,AVG,MIN,MAX,PROD}; window variants; 𝒫(attrs) |
| Π_window | PARTITION BY, ORDER BY, frame, Qualify threshold | subset; attr×dir; bounds; ℝ |
| Π_algo | damping, iterations, k, max_depth, CommAlg variant | (0,1); ℕ; {LOUVAIN,...} |

**CTL operator map:**

| Mode | EXISTS | FORALL |
|---|---|---|
| EVENTUALLY | EF φ | AF φ |
| GLOBALLY | EG φ | AG φ |
| NEXT | EX φ | AX φ |
| UNTIL(ψ) | E(φ U ψ) | A(φ U ψ) |

Semantics: maximal paths over a non-total transition relation; FORALL on an empty domain follows `on_empty` (§4.1, D30).

**Query forms:** Q = TemporalContext? ∘ B₁ ∘ B₂? ∘ B₃? | Q_delta = (t₁,t₂,DeltaSpec,filter?)

**Strategy refinement order:** SHORTEST = K_SHORTEST(1) ⊑ K_SHORTEST(k) ⊑ ALL; MIN_WEIGHT ⊑ ALL

**Export targets:** SQL_DUCKDB|SQL_POSTGRESQL|SQL_MOTHERDUCK|CYPHER|SPARQL|DGM_JSON|DGM_YAML; DUCKDB|POSTGRESQL|MOTHERDUCK|PARQUET|DELTA|ICEBERG

### 10.3  Related work

**Graph OLAP.** Chen et al. (2008, 2009) [1,2] — DGM adds Σ, CTL, Allen, relaxed-endpoint trail semantics, planner/exporter.

**Graphoids.** Gómez et al. (2017, 2019) [3,4] — DGM adds Σ, VerbHop⁻¹, full endpoint lattice, CTL.

**Multi-dimensional event data.** Esser and Fahland (2021) [5] — DGM extends with constellation paths, SCD, UNTIL/SINCE, Allen, endpoint relaxation.

**KG-OLAP.** Schuetz et al. (2021) [6] — complementary; DGM adds temporal logic and trail space analysis.

**BDD.** Bryant (1986) [7] — DGM applies to graph dimensional predicates; Kripke equivalence grounds it.

**Model checking.** Clarke et al. (1986) [8] — PathPred×TemporalMode realises full CTL; G^T enables past-temporal checking. **Interval algebra.** Allen (1983) [9] — 13 relations in TemporalOrdering.

**Datalog and fixpoint query languages.** Datalog [13] is the query language whose semantics are least-fixpoint computations over relations, equivalent to first-order logic with least fixpoint (FO+LFP). The Immerman-Vardi theorem establishes that FO+LFP captures PTIME on ordered structures. DGM's question algebra `Q_algebra(A, B)` under CTE composition is equivalent to Datalog over `G_AB` (§8.13) — establishing that DGM's full compositional question space is exactly PTIME-bounded. This positions DGM precisely in the database query language complexity hierarchy and grounds the D17 restriction (no `Free→Free` in `Where PathPred` scope) as a decidability preservation measure.

**Ontologies, RDF 1.2, and knowledge representation.** RDF [10], RDFS, and OWL [11] constitute the standard ontological stack, currently evolving toward RDF 1.2 [14]. An RDF triple `(subject, predicate, object)` is structurally identical to DGM's verb edge `(dim, label, fact)` — both are typed directed labeled edges between entities. The `τ_N : N_S → {dim, fact}` kind function is a two-class hierarchy equivalent to `rdf:type` with `rdfs:subClassOf`. SPARQL 1.1 property paths (`+`, `*`, `|`, `^`) correspond to DGM's `BoundPath` traversal, and SPARQL's `FILTER EXISTS` / `FILTER NOT EXISTS` correspond to `PathPred(EXISTS)` / `PathPred(FORALL)`. DGM independently arrived at the same graph primitive as RDF — evidence that the triple / typed directed labeled edge is the correct foundational structure.

**RDF 1.2 and triple terms.** RDF 1.2 introduces *triple terms* — the ability to use an RDF triple as a term (in the object position of another triple) — together with a reification mechanism via `rdf:reifies`. This significantly narrows a historical gap with DGM: N-ary events, which previously required verbose standard reification or blank-node workarounds, can now be represented more cleanly in RDF. A triple term annotated via `rdf:reifies` is the RDF 1.2 approximation of DGM's fact node — a named resource that participates in multiple relationships simultaneously. However, the gap is narrowed, not closed: DGM's fact node is enforced by a structural invariant (`V ⊆ D×F×L`) that prevents fact nodes from being verb sources and ensures the dim/fact separation at the schema level. RDF 1.2 triple terms are syntactic sugar with no enforced type discipline — any triple can be reified and any reifier can be used in any position. DGM arrived at the clean solution (fact-as-typed-node) earlier and more strictly by encoding it as a schema invariant rather than a modelling convention.

DGM and ontologies diverge sharply above the shared graph primitive, along six axes:

**(i) The dim/fact typed split.** OWL has no concept of a measurable event node distinct from a domain entity node. The structural invariant `V ⊆ D×F×L` — which enforces that verb edges connect dimensions to facts and not arbitrary node pairs — has no OWL equivalent. RDF 1.2 triple terms narrow the representation gap for N-ary events but do not enforce the type discipline. KG-OLAP [6] layers context dimensions on top of OWL without schema-level enforcement.

**(ii) Aggregation and analytics.** OWL is a representation and reasoning language, not a query and analytics language. It has no `GroupBy`, `Agg`, `Having`, `Window`, or `Qualify`. SPARQL 1.1 and SPARQL 1.2 have limited aggregation but no window functions, no temporal aggregation, no path aggregation (`PathAgg`, `TemporalAgg`), and no graph algorithm expressions (`GraphExpr`). The three-band query structure `B₁ ∘ B₂? ∘ B₃?` has no counterpart in ontological tooling.

**(iii) The schema annotation layer and operational vs assertional semantics.** OWL axioms are logical assertions that drive inference. RDF 1.2 reification (`rdf:reifies`) provides edge-level annotation — the equivalent of DGM's `P(e)` property bags — but these annotations are assertional with no enforced semantics. DGM's `Σ` carries *operational* metadata — `Grain`, `SCDType`, `BridgeSemantics`, `WeightConstraint`, `PipelineArtifact` — that drives execution planning, planner rule selection, and recommender behaviour. The distinction is fundamental: RDF annotations state facts; DGM annotations drive decisions. No RDF reasoner has the concept of "this edge annotation changes which CTE the planner generates."

**(iv) Temporal logic over paths.** OWL-Time [12] describes temporal relationships between intervals as ontological assertions. DGM's `TemporalMode` (AG, AF, UNTIL, SINCE, Allen's 13 interval relations in `TemporalOrdering`) is CTL/LTL model checking over the Kripke structure of the graph (§8.1) — a different computational apparatus from OWL inference. The Kripke equivalence grounds DGM in model checking theory, not description logic.

**(v) Trail space analysis and question completeness.** The `RelationshipSubgraph` constructs, `TrailExpr` algorithms, `SIGNATURE_ENTROPY`, the question completeness theorem (§8.11), and the question algebra `Q_algebra(A, B)` with its Datalog equivalence (§8.13) have no counterpart in ontological frameworks. RDF named graphs are asserted scopes; DGM's `G_AB` is a computed scope derived from traversal topology with formal completeness guarantees. Ontologies represent what is true; DGM enumerates what can be measured and asked — a fundamentally different epistemic orientation.

**(vi) Reification and fact modelling — convergence in v1.2.** RDF 1.2 triple terms and DGM's fact nodes are independently derived solutions to the same problem: how to represent N-ary events as first-class participants in multiple relationships. RDF 1.2 achieves this through syntactic extension (triple terms + `rdf:reifies`); DGM achieves it through a typed schema invariant (`τ_N(f) = fact`, `V ⊆ D×F×L`). The solutions converge on the same representational pattern — a named node participating in multiple directed relationships — while differing in enforcement strictness (none in RDF 1.2; structural invariant in DGM) and operational consequence (none in RDF 1.2; planner rules 1b, 1c, 10 in DGM).

**Convergent research: SPARQL query relaxation.** Active research on SPARQL query relaxation — finding similar queries when exact matches fail — addresses the same problem as DGM's question lattice navigation and recommender architecture, but from different directions. SPARQL relaxation works bottom-up from query syntax via RDFS entailment and instance similarity; DGM works top-down from a formally defined question space `Q_valid(A, B)` using BDD feasibility filtering and trail metric scoring. The challenge is especially significant for statement-level annotations (reification models), where relaxation behaviour varies across standard reification, named graphs, n-ary relations, singleton properties, and RDF-Star. DGM's question algebra `Q_algebra(A, B)` with its semiring structure and canonical query DAG provides a formal framework for query relaxation that subsumes SPARQL query relaxation for the DGM-representable fragment.

The precise positioning: DGM is an **analytical graph model**, not a knowledge representation language. It borrows the graph primitive from RDF and the dimensional structure from Kimball-style data warehousing, adds a formal query language grounded in CTL/LTL, an annotation-driven execution stack, and a formally complete question space. It is closest to KG-OLAP [6] at the intersection of graphs and OLAP. RDF 1.2's triple terms narrow the N-ary event representation gap; DGM remains strictly more constrained (typed invariant vs convention) and analytically richer (three-band query language, trail space analysis, question completeness). A DGM graph can be exported to SPARQL as one of its query targets (§6.4), confirming the relationship while clarifying the distinction: DGM is the source formalism; SPARQL is one possible execution dialect.

**Medallion architecture.** The Medallion architecture (Bronze/Silver/Gold/Diamond) is a data design pattern describing pipeline quality progression — how data flows, is refined, and gains quality across layers. DGM and Medallion are complementary: Medallion describes the pipeline; DGM describes the semantic model. The relationship is layered, not competitive. DGM sits at the Silver–Gold boundary and extends upward to the Diamond layer:

| Medallion layer | DGM construct | Role |
|---|---|---|
| Bronze | `Provenance(n, record_source, load_timestamp)` + `WritePlan(APPEND)` | Data substrate: raw, append-only, audit trail |
| Silver | `DGM = (G_S, G_I, h)` — τ_N typing + Σ annotation layer on `G_S`, instances in `G_I` | Semantic model: typed nodes, annotated edges, conformed dimensions, SCD2 history |
| Gold | `Q = B₁ ∘ B₂? ∘ B₃?` execution → `ExportPlan` → `WritePlan` | Query delivery: business-ready aggregates materialised to Delta/Iceberg |
| Diamond/Semantic | `Q_valid(A, B)` + recommender + NL interface (§11) + `ExecutionBudget` (§6.3) | Governed semantic layer: formal question space, NL access, completeness |

Silver is the most important intersection: Silver is precisely where DGM’s typed graph `G` is constructed — entity matching/merging maps to hub node identity, schema conformation maps to `τ_N` assignment, SCD2 history maps to `SCDType(SCD2)`, relationship capture maps to `V ⊆ D×F×L`, and business rule encoding maps to the `Σ` annotation layer. Gold is DGM query execution materialising to `WritePlan` → DELTA/ICEBERG sinks, replacing static Kimball star schema views with a formal query language. Diamond is DGM’s formal semantic layer — `Q_valid(A, B)` with completeness guarantees (§8.11), `Q_algebra(A, B)` with Datalog equivalence (§8.13), recommender navigating `L(G_AB)` (§7), and the NL interface with bounded vocabulary and BDD-validated candidates (§11).

Pipeline lifecycle unification: each layer transition is modelled by a `PipelineArtifact` state machine (Bronze ingest `BACKFILL`, Silver construction `REFRESH`, Gold materialisation `ADAPTIVE`). CTL SAFETY, LIVENESS, and RESPONSE properties (§2.4) are correctness assertions over the full four-layer artifact graph. Three genuine gaps: Medallion has layer-specific access controls, Bronze schema-on-read flexibility, and CDC streaming ingestion mechanics that DGM does not model. Five genuine DGM exclusivities in the Diamond layer: the formal typed dim/fact split (`V ⊆ D×F×L`), the three-band query language with CTL/LTL, `Q_valid(A, B)` with completeness guarantees, `Q_algebra(A, B)` with Datalog equivalence and PTIME bound, and the NL interface with formally bounded vocabulary and BDD-validated candidates. The `MedallionLayer` annotation (§2.4) tags each node’s layer origin for governance, lineage, and planner quality signalling.

Novel contributions added in v0.16: Endpoint type with Bound/Free variants; four-case RelationshipSubgraph endpoint lattice; forward cone via forward BFS; backward cone via BFS on G^T; full traversal space as SubgraphExpr; containment lattice theorem and cone intersection identity; REACHABLE_FROM/REACHABLE_TO as TrimCriterion; TrailExpr single-relaxation algorithms as NodeAlg; SIGNATURE_DIVERSITY; SIGNATURE_ENTROPY as Shannon entropy; global SubgraphAlg variants; granularity tier shift by endpoint case; Rule 9 cone containment optimisation; G^T at GraphStatistics load time; two-stage trail exploration flow; SIGNATURE_ENTROPY as recommender routing signal.

Novel contributions added in v0.17: `PipelineArtifact` composite annotation for backfill-incremental state machines; five-state artifact lifecycle modelled as `Grain(ACCUMULATING)` with implied `BridgeSemantics`; backfill/refresh asymmetry via `BridgeSemantics(SUPERSESSION)` vs `CAUSAL` on `In-flight → Failed` transition; `ADAPTIVE` write mode inferring `WritePlan` from `P(f).state` at planning time; planner Rule 10 for state-aware write planning and TTL expiry; scheduler loop expressed as composable DGM query pipeline (Q_backfill, Q_refresh, Q_delta monitoring); CTL LIVENESS/SAFETY/RESPONSE as formal scheduler correctness properties over the artifact graph; `UNTIL` operator distinguishing refresh failure (prior Complete preserved) from backfill failure (no prior artifact).

Novel contributions added in v0.18:

Novel contributions added in v0.19: Question algebra `Q_algebra(A, B)` as closure of `Q_valid` under CTE composition; semiring structure `(Q_algebra, UNION, INTERSECT, ∅, Q_top)`; Datalog equivalence theorem for `Q_algebra` under CTE composition; PTIME complexity bound via Immerman-Vardi; fixpoint semantics of recursive CTEs as descent paths in `L(G_AB)`; two-level predicate system (BDD for single-query, containment for cross-query); extended question completeness theorem (v0.19); planner Rule 11 cross-CTE CSE using BDD node id equality; compositional suggestion type (Correlation) in recommender; question chain suggestions as multi-step descent paths; Datalog reference [13] added; D23 design decision on CTE composition complexity.

Novel contributions added in v0.20: Query DAG as canonical form for `Q_algebra(A, B)`; BDD/query DAG parallel (variable ordering ↔ dependency partial order, sharing rule ↔ CSE, elimination rule ↔ semiring laws); canonical form theorem (two composed queries equivalent iff same DAG node id); two-level canonical form (BDD at leaves, query DAG at composition); construction algorithm with elimination + sharing rules; minimum CTE count as tight lower bound (= number of canonical DAG nodes); complexity stratification of containment checking (Band 1: NP-complete; monotone Band 2: polynomial; arbitrary Band 2: undecidable; recursive: Σ₁-hard); cross-session caching via DAG node id as cache key; query explanation via DAG decomposition; canonical-space recommender navigation (neighbourhood over DAG nodes); Rule 11 extended with full semiring elimination laws; D24 design decision on canonical form.

Novel contributions added in v0.21:

Novel contributions added in v0.22:

Novel contributions added in v0.23: §10.3 extended to six axes of RDF/DGM comparison; RDF 1.2 triple terms / reification analysis — convergent solutions to N-ary event representation with DGM's typed invariant remaining strictly more constrained; axis (iii) updated to distinguish RDF 1.2 assertional annotations from DGM's operational `Σ`; axis (vi) added on reification and fact modelling convergence; SPARQL query relaxation positioned as convergent research addressed by DGM's question lattice and question algebra; `G^T` as time-reversal of Kripke structure with named graph / multiple accessibility relation note added to §8.1; RDF 1.1 reference [10] updated to RDF 1.1 Concepts 2014; RDF 1.2 reference [14] added; D28 design decision on RDF 1.2 relationship. §11.10 Implementation architecture — LangGraph state graph with five Pydantic-AI specialist agents; DGMContext as Pydantic-AI dependency injection carrier; Pydantic output schemas as executable projection of DGM formal types (EntityResolutionResult, TemporalClassificationResult, CompositionalDetectionResult, CandidateRankingResult, ExplanationRenderResult); TemporalModeEnum / TemporalPropertyEnum / QAlgebraOpEnum as closed-set Pydantic validators; five LLM nodes + six deterministic non-LLM nodes in the graph; explicit refinement and clarification cycles; MemorySaver HIL state persistence; bounded retry for hallucination prevention; parallel DAG leaf execution in STREAM mode; ontology-to-Pydantic formal mapping table; Phase 8 roadmap updated with specific implementation artefacts; D27 design decision. `ExecutionBudget` type with `BudgetDecision` taxonomy (`PROCEED`, `STREAM`, `ASYNC`, `REWRITE`, `SAMPLE`, `PAGINATE`, `CLARIFY`, `REJECT`); pre-execution gate (§6.3) as formal decision procedure between planner and execution; four graceful handling strategies (cost-aware rewriting via strategy lattice, progressive streaming from DAG topology, sampling with Hoeffding error bounds, async execution via `PipelineArtifact` lifecycle); clarification messages grounded in recommender lattice refinements; §11 Natural Language Interface — five-layer architecture; LLM responsibilities narrowed to five tasks (entity resolution, candidate ranking, temporal classification, compositional detection, explanation rendering); `EntityRegistry` as entity/relationship/temporal vocabulary; closed `PropRef` vocabulary for hallucination prevention; BDD-validated candidate generation for LLM ranking; temporal intent classification to CTL operators; compositional intent detection to Q_algebra operations; confirmation/refinement loop as lattice navigation; ambiguity resolution grounded in question space positions; schema evolution handling via Q_delta; sample-parallel async execution pattern; D25 and D26 design decisions. Question completeness theorem (§8.11) — `Q_valid(A, B)` finite and enumerable from `G_AB` and `Σ|_AB`; question lattice `L(G_AB)` with tautological top, contradictory bottom, and three specificity dimensions; formal characterisation of recommender as guided navigator of `L(G_AB)`; semantic scoring theorem — trail metrics induce non-uniform distribution over `L(G_AB)` without enumeration; `BETWEENNESS_CENTRALITY` as GroupBy pivot signal; `DOMINANT_SIGNATURE` as predicate seed; ontology relationship paragraph positioning DGM against RDF/OWL/SPARQL/OWL-Time along five axes; references [10] RDF, [11] OWL, [12] OWL-Time; D22 design decision on DGM/ontology distinction.

Novel contributions added in v0.24: `MedallionLayer` annotation in §2.4 — formal capture of Medallion layer origin with three operational consequences (planner quality warning for BRONZE traversal, recommender BRONZE suppression, exporter minimum-layer tagging in `DGM_JSON` output). §1 Overview updated with Medallion architecture bullet. §9.1 alignment table updated with `MedallionLayer` row; duplicate `PipelineArtifact` row removed. §10.3 extended with Medallion architecture entry — four-layer formal correspondence table (Bronze=`Provenance`+`WritePlan(APPEND)`, Silver=`G` construction, Gold=`Q` execution, Diamond=`Q_valid`+recommender+NL+`ExecutionBudget`), Silver as DGM graph construction boundary, pipeline lifecycle unification via `PipelineArtifact`, genuine gaps (access controls, schema-on-read, CDC streaming) and DGM exclusivities (typed dim/fact split, CTL query language, question completeness, PTIME bound, bounded NL interface). D29 design decision on DGM/Medallion layering.

Novel contributions added in v0.25: Parameter space `Π(q)` formalization (§8.15) — six parameter families (temporal, strategy, filter, aggregation, window, algorithm) as dependent product over `Q_valid(A, B)`; annotation constraint projection onto parameter space; parameter completeness theorem; parameter type stratification (discrete finite / discrete infinite / continuous); finite parametric skeleton corollary; parameter equivalence and effective parameter space; parametric BDD extension (`BDD_full = BDD_structural × BDD_parametric`); full analytical space `A(A, B) = Σ_{q ∈ Q_valid} Π_valid(q)` as sigma type (dependent sum); `ExecutionBudget` cost sensitivity `cost(q, π)` and parametric rewriting; recommender navigation space `Nav(A, B) = L(G_AB) × Π` with structural/parametric/mixed moves; composed analytical space `A_composed`. §7.7 parametric suggestion types (temporal refinement, strategy shift, aggregation alternative, algorithm variant, threshold relaxation) with annotation-driven parametric rules and cost-aware parametric rewriting via `ExecutionBudget`. §10.2 updated with PipelineArtifact and MedallionLayer rows in schema annotations table; parameter families summary table added. D30 design decision on parameter space separation.

Novel contributions added in v0.26: §8.16 Vocabulary decomposition and analytical space characterisation — six vocabularies (`V_N`, `V_E`, `V_P`, `V_Pred`, `V_Op`, `V_Par`); factored size estimate via multiplicative decomposition; four-level hierarchy (term → phrase → sentence → paragraph); semantic sparsity and effective analytical space `A_eff`; semantic basis as minimal generating set; polynomial neighbourhood bound; per-level navigation strategies for recommender and NL interface; worked example for BookClub `social_network` domain (10 nodes, 5 edge types → 14-element semantic basis, ~2.3 k effective questions). §8.11 cross-reference to §8.16.6–8.16.7 for size characterisation. §7.7 cross-reference to §8.16.5–8.16.6 for formal basis justification. §12 Supervisor Agent Architecture — six agent-safe properties; five-point tool contract (`BaseTool` ABC: `capability_schema`, `cost_estimate`, `execute`, `compose`, `explain`); nine core tools (`dgm_query`, `what_can_i_answer`, `correlate`, `explore_from`, `sensitivity_sweep`, `validate_invariant`, `store_finding`, `retrieve_related`, `map_analytical_space`); `TOOL_REGISTRY` dict; five-node LangGraph supervisor graph (plan → select_tool → execute_tool → synthesize → respond); budget propagation via `budget_fraction` on `ToolCallRequest`; six supervisor workflows (single-domain, cross-domain, hypothesis-driven, exhaustive-survey, validation, meta-analytical); knowledge base with lattice-adjacent retrieval keyed by `(A, B, q, π)`; meta-tool operationalising §8.11 question completeness; relationship to §7 recommender as supplier of `Q_valid`/lattice to supervisor navigation.

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
**D18** — G^T is logical: backward traversal (SINCE, ONCE, PREVIOUSLY, REACHABLE_TO, INCOMING_SIGNATURES) reads each edge relation by its target key and needs only the reverse access path of §2.6. Materialising reversed adjacency costs O(|E|) once and is amortised across queries; it is an optimisation for graphs that fit in memory, not a requirement.
**D19** — Cone containment (Rule 9) as structural theorem, not heuristic. Grounded in G_A* ∩ G_*B = G_AB. Always correct; fires whenever both constraints present on same Join branch.
**D20** — PipelineArtifact is syntactic sugar, not a new fact type. The backfill-incremental state machine is modelled entirely within existing constructs: Grain(ACCUMULATING), BridgeSemantics, WritePlan, Q_delta(CHANGED_PROPERTY), and CTL temporal properties. PipelineArtifact assembles these into one declaration and infers semantics from P(f).state at planning time (planner Rule 10). Follows the same pattern as TemporalProperty classes (D9).
**D21** — Backfill and refresh require different WritePlan modes by construction. Backfill (APPEND) has no prior artifact — failure leaves the window durably in Failed. Refresh (MERGE) preserves the prior Complete via atomic swap — failure regresses to Stale, not Failed. ADAPTIVE mode makes this automatic from P(f).state at planning time.
**D22** — DGM shares the graph primitive with RDF but diverges above it. The triple / typed directed labeled edge is the correct foundational structure — DGM and RDF arrived at it independently. The divergence is complete above that primitive: DGM adds the dim/fact typed split, the three-band analytical query structure, the annotation-driven planner, CTL/LTL temporal logic, trail space analysis, and the question completeness theorem — none of which exist in ontological tooling.
**D23** — CTE composition extends `Q_valid` to `Q_algebra` (Datalog-equivalent, PTIME-bounded). The BDD covers single-query predicate optimisation; cross-CTE CSE (Rule 11) extends it to multi-query scope. The D17 restriction (no `Free→Free` in `Where PathPred`) is not arbitrary — it is the boundary that keeps `Q_algebra` within PTIME. Allowing unrestricted fixpoints in the filter position would push DGM toward the complexity class of stratified Datalog with negation, where decidability is more fragile.
**D24** — The query DAG canonical form is the composition-level analogue of the BDD. The analogy is exact: variable ordering ↔ dependency partial order; sharing rule ↔ CSE; elimination rule ↔ semiring laws. Containment-based elimination is applied only for Band 1 leaf nodes (NP-complete but tractable in practice). For Band 2/3 nodes, only syntactic laws are applied — this is the conservative choice that preserves correctness at the cost of some missed simplifications. Cross-session caching via DAG node id is the semantic-level generalisation of SQL text caching; it correctly identifies equivalent questions expressed in different syntactic forms.
**D25** — The pre-execution gate produces `BudgetDecision` rather than raising errors or blocking. Every failure mode has a graceful path: cost explosion → rewrite or clarify; result explosion → paginate; slow pre-computation → approximate; very expensive → async. `REJECT` is reserved for the hard ceiling and for the terminal fallback of a query above `max_estimated_cost` that admits no rewrite, guaranteed sample or refinement — most queries reach `PROCEED`, `STREAM`, or `REWRITE`. Rewriting precedes the choice of delivery mode; sampling is offered only where its error guarantee applies, and clarification otherwise. The clarification path surfaces lattice refinements, not generic "simplify your query" messages — this is only possible because `Q_valid(A, B)` is formally defined.
**D26** — The NL interface does not generate queries from the LLM. It navigates a formally bounded question space. The LLM's five responsibilities are each a constrained mapping task — entity resolution against a closed vocabulary, candidate ranking over pre-validated options, pattern classification to a closed CTL vocabulary, compositional detection, and structured explanation rendering. This architecture prevents the two failure modes of SQL-generation NL interfaces: hallucinated schema references (prevented by closed PropRef vocabulary) and semantically invalid queries (prevented by BDD-validated candidates).
**D27** — LangGraph is chosen for the NL interface workflow and Pydantic-AI for each specialist agent.
**D28** — The DGM/RDF relationship is one of convergence on the graph primitive and divergence on everything above it. RDF 1.2's triple terms narrow the N-ary event representation gap (axis vi), making the solutions more structurally similar than in RDF 1.1. DGM's position is not that RDF is wrong, but that it is underconstrained for analytical purposes: DGM adds the type invariant that RDF leaves as convention, the operational annotation layer that RDF leaves as assertions, the three-band query language that SPARQL leaves incomplete, and the formal question space that SPARQL relaxation research approximates from below. As RDF 1.2 matures, the DGM SPARQL exporter target (§6.4) becomes a more faithful representation — triple terms enable cleaner encoding of fact nodes in SPARQL output. This pairing is structurally forced, not arbitrary. LangGraph handles the two properties that linear pipelines cannot: stateful cyclic workflows (the refinement and clarification cycles) and human-in-the-loop checkpoints with multi-turn state persistence. Pydantic-AI handles the two properties that unstructured LLM calls cannot: closed-vocabulary output enforcement (DGM formal types as Pydantic schemas) and bounded retry on validation failure. The majority of the LangGraph graph nodes (6 of 11) are deterministic non-LLM DGM computation — only 5 nodes invoke LLMs, each with a formally bounded output type. This ratio is intentional: the more computation that is formal and deterministic, the fewer guarantees depend on LLM reliability. The triple / typed directed labeled edge is the correct foundational structure — DGM and RDF arrived at it independently. The divergence is complete above that primitive: DGM adds the dim/fact typed split, the three-band analytical query structure, the annotation-driven planner, CTL/LTL temporal logic, trail space analysis, and the question completeness theorem — none of which exist in ontological tooling. DGM is an analytical graph model; ontologies are knowledge representation languages. The relationship is complementary, not competitive: DGM can export to SPARQL as one execution target.

**D29** — DGM sits at the Silver–Gold boundary of the Medallion architecture and extends upward to the Diamond layer. Medallion's Bronze layer is the data substrate formally captured by DGM's `Provenance` annotation and `WritePlan(APPEND)`. The Silver layer is where DGM's typed graph `G = (N, E, τ_N, τ_E, P, Σ)` is constructed (entity matching, SCD2 history, `Σ` encoding of business rules). Gold is `Q` execution producing materialised outputs via `ExportPlan`. Diamond is DGM's formal semantic layer — `Q_valid(A, B)` with completeness guarantees, `Q_algebra(A, B)` with Datalog equivalence, recommender navigating `L(G_AB)`, and the NL interface with bounded vocabulary. The `MedallionLayer` annotation tags node and edge origin for governance, lineage, and planner quality signalling — following the same pattern as `PipelineArtifact` (D20): syntactic sugar over existing `Σ` constructs, not a new graph primitive.
**D30** — CTL is interpreted over maximal paths, not over a totalised transition relation. A path ends where the graph ends: a finite trace that reaches a terminal is complete, and EG/AG are decided on it without an implicit self-loop. Self-loops were rejected because they are edges no event produced — visible to hop counts, SHORTEST, signatures and PathAgg — and because they make NEXT at a terminal mean "now". Quantification over an empty domain is governed by `PathPred.on_empty`: FORALL is false by default (`REQUIRE_WITNESS`) and true only when `VACUOUS` is requested, so a universal condition never holds silently for a node with no path instance. Recorded in `docs/adr/0001-ctl-finite-trace-semantics.md`.
**D30** — The parameter space `Π(q)` is structurally separated from the question space `Q_valid(A, B)`. Question space determines *what can be asked*; parameter space determines *how each question can be configured*. The separation preserves the BDD's role: structural validity is checked once per question skeleton; parametric validity is encoded as additional BDD variables over the discrete parameter dimensions (`BDD_full = BDD_structural × BDD_parametric`). The recommender navigates both dimensions independently — structural moves change the BDD; parametric moves change parametric BDD variables — with the same trail-metric scoring and annotation-constraint filtering. The `ExecutionBudget` gate evaluates `cost(q, π)` jointly, enabling parametric rewriting as a cost-reduction mechanism distinct from structural rewriting. The full analytical space is the dependent product `A(A, B) = Σ_{q ∈ Q_valid} Π_valid(q)` — a sigma type whose fibre over each question is the valid parameter space for that question.

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

[10] Cyganiak, R., Wood, D., Lanthaler, M. (2014). RDF 1.1 Concepts and Abstract Syntax. W3C Recommendation. https://www.w3.org/TR/rdf11-concepts/

[14] W3C RDF-Star Working Group (2024). RDF 1.2 Concepts and Abstract Syntax. W3C Working Draft (in progress). https://www.w3.org/TR/rdf12-concepts/

[11] McGuinness, D. L., van Harmelen, F. (2004). OWL Web Ontology Language Overview. W3C Recommendation. https://www.w3.org/TR/owl-features/

[12] Cox, S., Little, C. (2022). Time Ontology in OWL (OWL-Time). W3C Recommendation. https://www.w3.org/TR/owl-time/

[13] Ceri, S., Gottlob, G., Tanca, L. (1989). What You Always Wanted to Know About Datalog (And Never Dared to Ask). *IEEE Transactions on Knowledge and Data Engineering*, 1(1), 146–166. https://doi.org/10.1109/69.43410

---

## 12  Supervisor Agent Architecture

The NL interface (§11) processes a single question against a single dataset per invocation. This section specifies a **supervisor agent** that orchestrates multiple DGM tool calls — across datasets, with cross-domain correlation, iterative refinement, and compound analytical synthesis.

### 12.1  DGM as a composable tool

Six formal properties make DGM **agent-safe** — suitable for autonomous use by a supervisor without human-in-the-loop per step:

| # | Property | Guarantee |
|---|---|---|
| 1 | **Closed output vocabulary** | Every tool output is typed; no free-text unless explicitly requested |
| 2 | **Formal question space** | Tools operate on `Q_valid(A, B)` — the supervisor cannot ask an invalid question |
| 3 | **Deterministic equivalence** | Same `(q, π)` input yields the same canonical DAG (§8.14) |
| 4 | **Formal failure modes** | Errors are typed (`BudgetDecisionKind`), not exceptions — the supervisor can react programmatically |
| 5 | **Cost estimation before execution** | Every tool provides `cost_estimate(args)` before `execute(args)` |
| 6 | **Composable via Q_algebra** | Tool outputs are joinable/unionable via `Q_algebra` operations (§8.13) |

### 12.2  Five-point tool contract

Each DGM tool exposes a uniform interface:

```
ToolContract:
  capability_schema  → ToolCapabilitySchema   # what the tool can do
  cost_estimate(args) → float                  # estimated cost before execution
  execute(args, ctx)  → ToolCallResult         # run the tool
  compose(left, right) → ToolCallResult        # combine two results
  explain(args)        → str                   # human-readable description
```

The `capability_schema` declares input/output types and `composable_with` — the list of tools whose outputs can be fed into this tool. This enables the supervisor to validate tool chains at plan time rather than at execution time.

### 12.3  Nine core tools

| Tool | Purpose | Maps to |
|---|---|---|
| `dgm_query` | Full NL→SQL pipeline against a dataset | `run_ask_from_source` (§11.10) |
| `dgm_what_can_i_answer` | List answerable questions from `Q_valid(A, B)` | BDD enumeration (§8.3) |
| `dgm_correlate` | Cross-domain JOIN of two results | `Q_algebra.JOIN` (§8.13) |
| `dgm_explore_from` | Navigate from a result to related questions | Recommender (§7) |
| `dgm_sensitivity_sweep` | Parameter sweep over `P_Σ` | §8.15 + §8.16.7 Level 3 strategy |
| `dgm_validate_invariant` | Check a formal model invariant | Schema validation (§3) |
| `dgm_store_finding` | Persist an analytical finding | Knowledge base (§12.7) |
| `dgm_retrieve_related` | Lattice-adjacent retrieval from KB | **Not** vector similarity — lattice distance in `L(G_AB)` |
| `dgm_map_analytical_space` | Map vocabulary decomposition | §8.16 computation |

### 12.4  Supervisor graph

The supervisor is a five-node LangGraph state machine:

```
plan → select_tool → execute_tool → synthesize → respond
                 ↑          |
                 └── more ──┘
```

- **plan**: Decompose the user utterance into a `SupervisorPlan` — an ordered list of `ToolCallRequest` items, each with a `budget_fraction` summing to ≤ 1.0
- **select_tool**: Pick the next unexecuted step from the plan
- **execute_tool**: Invoke the selected tool via `TOOL_REGISTRY`; append `ToolCallResult` to state
- **synthesize**: Combine all results into a coherent answer (composition via `Q_algebra` or natural language)
- **respond**: Format the final output

The `execute_tool → select_tool` loop repeats until all plan steps are executed, then routes to `synthesize`.

### 12.5  Budget propagation

The supervisor's total budget (an `ExecutionBudget` from §6.3) is distributed across child tool calls according to `budget_fraction`:

```
child_budget(step_i) = supervisor_budget × step_i.budget_fraction
```

Each child tool invocation passes through its own `ExecutionGate` (§6.3), but with the scaled budget. If a child tool returns `REJECT`, the supervisor can:

1. **Replan** — reassign the rejected step's budget to remaining steps
2. **Degrade** — substitute a cheaper tool (e.g., `dgm_what_can_i_answer` instead of `dgm_query`)
3. **Abort** — propagate REJECT to the user

### 12.6  Six supervisor workflows

| Workflow | Pattern | Tools used |
|---|---|---|
| **Analytical research** | Multi-dataset comparison | `dgm_query` × N → `dgm_correlate` → `dgm_explore_from` |
| **Schema monitoring** | Invariant checking across datasets | `dgm_validate_invariant` × N |
| **Report generation** | Comprehensive domain survey | `dgm_map_analytical_space` → `dgm_query` × K (basis elements) |
| **Contract validation** | Verify cross-domain consistency | `dgm_query` × 2 → `dgm_correlate` → `dgm_validate_invariant` |
| **Knowledge base** | Accumulate and retrieve findings | `dgm_query` → `dgm_store_finding` → ... → `dgm_retrieve_related` |
| **Multi-agent debate** | Synthesize conflicting analyses | `dgm_query` × N (different params) → `dgm_sensitivity_sweep` |

### 12.7  Knowledge base with lattice-adjacent retrieval

The `dgm_store_finding` / `dgm_retrieve_related` pair implements a **knowledge base** indexed by position in the question lattice `L(G_AB)`:

- **Storage**: Each finding is tagged with its `(q, π)` coordinates in `A_eff` (§8.16.4)
- **Retrieval**: Given a query point `(q', π')`, retrieve findings whose lattice distance `d_L(q, q') ≤ k` for a configurable radius `k`
- **Not vector similarity**: Retrieval uses **lattice adjacency** (structural similarity in the BDD), not embedding-space cosine similarity. This ensures retrieved findings are formally related to the current question, not just textually similar.

### 12.8  Meta-tool: question completeness theorem operationalized

The `dgm_map_analytical_space` tool is the **operational form** of the question completeness theorem (§8.11):

```
∀ utterance u: if u has a valid mapping, then ∃ (q, π) ∈ A_eff(A, B)
```

By computing `A_eff` (§8.16), the tool can answer: "For this dataset, how many distinct analytical questions exist? What vocabulary levels dominate? Where should the analyst focus?" This transforms the abstract completeness theorem into a practical navigation aid for the supervisor.

### 12.9  Relationship to §7 recommender

The supervisor's `dgm_explore_from` tool wraps the recommender (§7) as a first-class tool. The key difference from direct recommender use:

- **§7 recommender**: Suggests next questions within a single session and dataset
- **§12 supervisor**: Uses recommender suggestions as **navigation signals** across datasets — a recommendation in dataset A may trigger a correlated query in dataset B

The polynomial neighbourhood bound (§8.16.6) guarantees that each `dgm_explore_from` call returns a manageable number of suggestions, preventing the supervisor from being overwhelmed by the combinatorial space.

---

*— end of specification —*