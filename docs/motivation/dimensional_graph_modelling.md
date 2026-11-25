# Dimensional Graph Model — Formal Specification
**Version 0.26 — Draft**

---

## Contents

1. [Overview](#1--overview)
2. [Graph Model](#2--graph-model)
   - [2.1 Structure and invariants](#21--structure-and-invariants)
   - [2.2 Node types](#22--node-types)
   - [2.3 Edge types](#23--edge-types)
   - [2.4 Schema annotation layer (Σ)](#24--schema-annotation-layer-σ)
   - [2.5 Temporal properties and snapshots](#25--temporal-properties-and-snapshots)
   - [2.6 Relational mapping](#26--relational-mapping)
3. [Traversal Paths](#3--traversal-paths)
   - [3.1 Unbound paths](#31--unbound-paths)
   - [3.2 Bound paths and TemporalOrdering](#32--bound-paths-and-temporalordering)
   - [3.3 Path strategy and refinement order](#33--path-strategy-and-refinement-order)
   - [3.4 RelationshipSubgraph](#34--relationshipsubgraph)
   - [3.5 Evaluation contexts and L-promotion](#35--evaluation-contexts-and-l-promotion)
4. [Query Language](#4--query-language)
   - [4.1 Predicate language](#41--predicate-language)
   - [4.2 Expression language](#42--expression-language)
5. [Query Structure](#5--query-structure)
   - [5.1 Query forms](#51--query-forms)
   - [5.2 Bands](#52--bands)
   - [5.3 Evaluation order](#53--evaluation-order)
6. [Execution Architecture](#6--execution-architecture)
   - [6.1 BDD canonical predicate representation](#61--bdd-canonical-predicate-representation)
   - [6.2 Query planner](#62--query-planner)
   - [6.3 Execution budget and pre-execution gate](#63--execution-budget-and-pre-execution-gate)
   - [6.4 Query exporter](#64--query-exporter)
7. [Recommender Architecture](#7--recommender-architecture)
   - [7.1 Three-layer architecture](#71--three-layer-architecture)
   - [7.2 Two-stage trail exploration and compositional suggestions](#72--two-stage-trail-exploration-and-compositional-suggestions)
   - [7.3 Suggestion types per band](#73--suggestion-types-per-band)
   - [7.4 Annotation-driven suggestion rules](#74--annotation-driven-suggestion-rules)
   - [7.5 TrailExpr-driven suggestion rules](#75--trailexpr-driven-suggestion-rules)
   - [7.6 Graph algorithm signals](#76--graph-algorithm-signals)
   - [7.7 Parametric suggestions](#77--parametric-suggestions)
8. [Theoretical Foundations](#8--theoretical-foundations)
   - [8.1 DGM as a labelled transition system](#81--dgm-as-a-labelled-transition-system)
   - [8.2 Propositional completeness of the filter tree](#82--propositional-completeness-of-the-filter-tree)
   - [8.3 Minterms, maxterms, canonical form](#83--minterms-maxterms-canonical-form)
   - [8.4 PathPred as compact quantification over paths](#84--pathpred-as-compact-quantification-over-paths)
   - [8.5 EXISTS/FORALL as disjunction/conjunction duality](#85--existsforall-as-disjunctionconjunction-duality)
   - [8.6 Finite traversal space](#86--finite-traversal-space)
   - [8.7 RelationshipSubgraph containment lattice](#87--relationshipsubgraph-containment-lattice)
   - [8.8 Allen's interval algebra: domain of exhaustiveness](#88--allens-interval-algebra-domain-of-exhaustiveness)
   - [8.9 Temporal property class hierarchy](#89--temporal-property-class-hierarchy)
   - [8.10 SIGNATURE_ENTROPY as information-theoretic measure](#810--signature_entropy-as-information-theoretic-measure)
   - [8.11 Question completeness theorem](#811--question-completeness-theorem)
   - [8.12 Semantic scoring of the question lattice](#812--semantic-scoring-of-the-question-lattice)
   - [8.13 Question algebra and CTE composition](#813--question-algebra-and-cte-composition)
   - [8.14 Query DAG canonical form](#814--query-dag-canonical-form)
   - [8.15 Parameter space](#815--parameter-space)
   - [8.16 Vocabulary decomposition and analytical space characterisation](#816--vocabulary-decomposition-and-analytical-space-characterisation)
9. [Implementation Guide](#9--implementation-guide)
   - [9.1 Alignment with sqldim](#91--alignment-with-sqldim)
   - [9.2 Implementation roadmap](#92--implementation-roadmap)
10. [Reference](#10--reference)
   - [10.1 Worked examples](#101--worked-examples)
   - [10.2 Summary tables](#102--summary-tables)
   - [10.3 Related work](#103--related-work)
   - [10.4 Design decisions](#104--design-decisions)
   - [10.5 References](#105--references)
   - [10.6 Changelog](#106--changelog)
11. [Natural Language Interface](#11--natural-language-interface)
   - [11.1 Architecture overview](#111--architecture-overview)
   - [11.2 LLM responsibilities (narrowly scoped)](#112--llm-responsibilities-narrowly-scoped)
   - [11.3 Semantic grounding layer](#113--semantic-grounding-layer)
   - [11.4 Temporal and compositional intent mapping](#114--temporal-and-compositional-intent-mapping)
   - [11.5 Confirmation and refinement loop](#115--confirmation-and-refinement-loop)
   - [11.6 Ambiguity resolution grounded in the question space](#116--ambiguity-resolution-grounded-in-the-question-space)
   - [11.7 Schema evolution handling](#117--schema-evolution-handling)
   - [11.8 Hallucination prevention: what is guaranteed and what is not](#118--hallucination-prevention-what-is-guaranteed-and-what-is-not)
   - [11.9 Integration with the execution budget gate](#119--integration-with-the-execution-budget-gate)
   - [11.10 Implementation: LangGraph + Pydantic-AI](#1110--implementation-langgraph--pydantic-ai)
12. [Supervisor Agent Architecture](#12--supervisor-agent-architecture)
   - [12.1 DGM as a composable tool](#121--dgm-as-a-composable-tool)
   - [12.2 Tool contract and types](#122--tool-contract-and-types)
   - [12.3 Nine core tools](#123--nine-core-tools)
   - [12.4 Supervisor graph](#124--supervisor-graph)
   - [12.5 Budget propagation and recovery](#125--budget-propagation-and-recovery)
   - [12.6 Six supervisor workflows](#126--six-supervisor-workflows)
   - [12.7 Knowledge base with move-distance retrieval](#127--knowledge-base-with-move-distance-retrieval)
   - [12.8 Meta-tool: grounding check and question-space map](#128--meta-tool-grounding-check-and-question-space-map)
   - [12.9 Relationship to §7 recommender](#129--relationship-to-7-recommender)
   - [12.10 Authorization and dataset isolation](#1210--authorization-and-dataset-isolation)
   - [12.11 Partial failures and terminal outcomes](#1211--partial-failures-and-terminal-outcomes)

---

## 1  Overview

The **Dimensional Graph Model (DGM)** is a typed directed graph framework that unifies dimensional analysis, graph modelling, and temporal logic. It provides a formal basis for representing domain entities, their relationships, measurable events, structural connections, and their evolution over time.

The framework rests on twenty-one core ideas (21 items below):

- **Dimensions and facts are both first-class nodes**, distinguished by the kind of data they carry.
- **Relationships are typed directed edges**: verb edges bind dimensions to facts (V ⊆ D×F×L); bridge edges are general structural connectors (B ⊆ N×N×L), optionally bounded by validity windows.
- **A schema annotation layer Σ** carries semantic metadata — grain, SCD type, weight constraints, bridge semantics, hierarchy structure, and fact/dimension taxonomy — that drives decisions in the planner, recommender, and exporter without changing graph topology.
- **Joins are path traversals** expanding the query context. Temporal resolution, edge validity, and structural trimming are first-class Join modifiers.
- **Path strategy is explicit** — every path-based operation declares how paths are selected and what quantifier scopes the truth condition.
- **Reverse verb hops** are permitted in BoundPath, enabling traversal from fact to dimension against the natural verb direction.
- **Temporal path ordering** supports point-based ordering and Allen's full 13-relation interval algebra over typed time points and intervals; UNTIL/SINCE are path modes of the predicate language.
- **Path-local bindings** allow hops in a BoundPath to introduce named aliases evaluated over C ∪ L.
- **A unified predicate language** governs all filter clauses via three constructors (ScalarPred, PathPred, SignaturePred). Functionally complete; realises full CTL via TemporalMode.
- **A unified expression language** governs all computed values. TrailExpr algorithms operate over RelationshipSubgraph scopes with relaxed or fixed endpoints — enabling forward-cone, backward-cone, and full traversal-space analysis.
- **DGM is a labelled transition system** — nodes are states, edges are labelled transitions, property bags label states; a Kripke structure is derived for each temporal predicate (§8.1). The transposed graph G^T reverses adjacency: it enables backward-cone computation, and past-temporal model checking on the edge types that carry a temporal meaning.
- **Temporal queries** come in two forms: snapshot queries (Q) and delta queries (Q_delta).
- **A query planner** makes DGM-specific physical execution decisions informed by schema annotations, including cone containment optimisation (Rule 9) for relaxed-endpoint traversals.
- **A query exporter** translates physical plans to multiple query and sink targets.
- **A recommender** surfaces analytically useful next steps via a two-stage trail exploration flow grounded in endpoint relaxation.
- **ExecutionBudget and pre-execution gate** — a runtime policy layer between the planner and execution that intercepts expensive queries and delivers them exactly by progressive streaming, pagination or async execution with PipelineArtifact materialisation, or reduces them by cost-aware rewriting or sampling with Hoeffding error bounds only when the caller allows that substitution; otherwise it offers the options and runs nothing (D40).
- **Natural language interface** — a five-layer architecture mapping NL utterances to formally bounded Q_valid(A,B) positions. The LLM navigates the canonical question lattice rather than generating SQL from scratch.
- **RDF 1.2 relationship stated** — DGM and RDF 1.2 converge on the N-ary event representation problem (RDF 1.2 triple terms ≈ DGM fact nodes) and differ along six axes of scope, each with its trade-off (§10.3): typed schema invariant, aggregation, operational annotation semantics, temporal predicates, trail space analysis, and enforcement strictness. The DGM SPARQL exporter encodes fact nodes more faithfully as RDF 1.2 matures.
- **LangGraph + Pydantic-AI implementation stack** — five specialist Pydantic-AI agents with output schemas derived from DGM formal types, orchestrated by a LangGraph state graph with explicit HIL checkpoints and cyclic refinement workflows. DGM's bounded vocabulary is enforced as Pydantic validators.
- **Medallion architecture mapped** — DGM sits at the Silver–Gold boundary of the Bronze/Silver/Gold pattern and proposes a fourth layer, Diamond, as its own extension (D39). Bronze is the data substrate captured by the `Provenance` annotation (§2.4); Silver is where the typed graph `G` is constructed; Gold is DGM query execution materialised via `WritePlan` to Delta/Iceberg sinks; Diamond — a DGM extension, not a layer of the standard pattern — is the formal semantic layer (`Q_valid`, recommender, NL interface, `ExecutionBudget`). The `MedallionLayer` annotation tags each node's layer origin for governance, lineage, and planner quality signalling.
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
| `Q_space`, `Q_valid(A, B)`, `Q_valid(𝒜)`, question lattice `L(G_AB)`, `Σ|_AB` (§8.11–§8.16) | Schema — anchors in `N_S` | `G_AB = G_{A,B}`, the anchor-set subgraph of §3.4.6 |
| Recommender candidate generation and feasibility filtering (§7.1 Layers 1–2) | Schema | `G_AB^S`, `Σ` |
| Recommender scoring (§7.1 Layer 3, §8.12) | Instance | statistics of `G_I` |
| Closed `PropRef` vocabulary of the NL interface (§11) | Schema | `K_S` |

The two readings of `G_AB` are defined in §3.4.2 (instance) and §3.4.5 (schema) and related by the projection lemma of §3.4.5.

**Transposed graph.** G_I^T = (N_I, E_I^R, P) where E_I^R = { (b,a,l) | (a,b,l) ∈ E_I }, typed by h over the transposed schema graph G_S^T. Reverses all edge directions. It is a logical object: a reverse hop reads the same edge relation by its target key (§2.6). Reversed adjacency may additionally be materialised (O(|E_I|)) and shared across queries when the graph fits in memory. Used by the planner for Free→Bound backward-cone computation and by SINCE/ONCE/PREVIOUSLY TemporalMode SQL generation. Written G^T elsewhere. G^T is a reversal of adjacency and nothing more: following an edge backwards reaches a predecessor in path order, which is an earlier moment only where the edge type is time-ordered (§2.3).

**Transition system.** G_I is a labelled transition system with states N_I and actions L × {fwd, rev}; CTL is evaluated on Kripke structures derived from it per path predicate (§8.1, D36). G_I^T exchanges the directions. Restricted to time-ordered edge types it is a time reversal, and past-temporal model checking runs on that restriction.

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
  | Additivity(f: F_S, key: K,                    -- key ∈ dom K_S(f), a numeric measure
               kind: ADDITIVE | SEMI_ADDITIVE(over: set[Axis]) | NON_ADDITIVE)
                                                  -- Axis ::= TIME | D_S
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
                     write_mode: BACKFILL | REFRESH | ADAPTIVE,
                     transitions: F_S)             -- the transition fact type
  -- Medallion layer annotation
  | MedallionLayer(n: N_S,
                   layer: BRONZE | SILVER | GOLD | DIAMOND,       -- DIAMOND: DGM extension (D39)
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

**Grain(f, grain)** — EVENT: one row per event; PERIOD: one row per entity per period, recording a state; ACCUMULATING: one lifecycle row per entity. Grain fixes the default additivity of the measures of `f`; whether a particular SUM is valid is decided per measure by `Additivity`.

**Additivity(f, key, kind)** — declares along which axes the measure `f.key` may be summed.

| Kind | Meaning | Typical measure |
|---|---|---|
| ADDITIVE | Summing over any set of tuples is meaningful | revenue, quantity |
| SEMI_ADDITIVE(over) | Summing is meaningful except across the axes in `over` — `TIME`, or a dimension type | account balance, inventory level: not summable across TIME |
| NON_ADDITIVE | Summing is never meaningful | rate, ratio, unit price, percentage |

A measure with no Additivity annotation takes its default from the grain: ADDITIVE under `Grain(EVENT)` and `Grain(ACCUMULATING)`, SEMI_ADDITIVE({TIME}) under `Grain(PERIOD)`.

```
AD1  ADDITIVE         SUM is valid under any GroupBy.
AD2  SEMI_ADDITIVE    SUM is valid only if each tuple set it sums over is confined to one value
                      of every axis in `over`:
                        TIME      the GroupBy contains the period of the fact, or a Where pins one period,
                                  or the aggregate is a TemporalAgg with window LATEST or EARLIEST;
                        d ∈ D_S   the GroupBy contains the key of d, or a Where pins one entity of d.
                      Otherwise the SUM is rejected at construction.
AD3  NON_ADDITIVE     SUM is rejected at construction. AVG is accepted with a warning that it is
                      unweighted; the exact figure is recomputed from additive measures
                      (an ArithExpr of SUMs).
AD4  COUNT, MIN and MAX are valid for every kind. AVG is valid for ADDITIVE and SEMI_ADDITIVE.
```

```
-- Grain(AccountBalance, PERIOD); Additivity(AccountBalance, balance, SEMI_ADDITIVE({TIME}))
-- one row per account per day

GroupBy [b.branch]             Agg: SUM(b.balance)                                   -- rejected (AD2): sums a
                                                                                     -- balance over many days
GroupBy [b.branch, b.day]      Agg: SUM(b.balance)                                   -- valid: one day per group
GroupBy [b.branch]             Agg: TemporalAgg(SUM, b.balance, b.day, LATEST)       -- valid: closing balance,
                                                                                     -- summed over accounts
GroupBy [b.branch]             Agg: AVG(b.balance)                                   -- valid (AD4)
```

**SCDType** — SCD1: strip temporal resolution. SCD2: effective_from/to. SCD3: current/previous props. SCD6: per-attribute policy. Which version a traversal reads is fixed by the SCD resolution mode, AS_WAS or AS_IS (§2.5).

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

**PipelineArtifact(f, pipeline_id, ttl, backfill_horizon, write_mode, transitions)** — composite annotation over two fact types (§10.4, D32; ADR 0002). It implies `Grain(f, ACCUMULATING)`, `Grain(transitions, EVENT)`, the bridge edge types below and `BridgeSemantics(followed_by, CAUSAL)`.

```
ArtifactState  ::= Missing | In-flight | Complete | Stale | Failed
TransitionKind ::= scheduled | succeeded | failed | expired | rescheduled | retried

f            the artifact: one instance per (pipeline, window).
             K_S(f) ⊇ { state: Enum(ArtifactState), window_start, window_end: Timestamp }
             state is the CURRENT state.

transitions  the transition event: one instance per state change.
             K_S(transitions) ⊇ { from_state, to_state: Enum(ArtifactState),
                                  kind: Enum(TransitionKind), occurred_at: Timestamp }

(transitions, f, 'transition_of')              ∈ B_S    STRUCTURAL   -- each transition belongs to one artifact
(transitions, transitions, 'followed_by')      ∈ B_S    CAUSAL       -- next transition of the same artifact
```

States are enumeration values, not nodes; no edge runs between states. The state machine is the relation δ of admissible `(from_state, kind, to_state)` triples:

```
Missing    —scheduled→    In-flight
In-flight  —succeeded→    Complete
In-flight  —failed→       Failed       -- no earlier transition reached Complete (backfill)
In-flight  —failed→       Stale        -- an earlier transition reached Complete (refresh; prior result kept)
Complete   —expired→      Stale        -- ttl elapsed since the transition into Complete
Stale      —rescheduled→  In-flight
Failed     —retried→      In-flight
```

Invariants:

```
PA1  Every transition instance has (from_state, kind, to_state) ∈ δ.
PA2  The transitions of one artifact form a single chain under 'followed_by', ordered by occurred_at;
     the first has from_state = Missing, and next.from_state = previous.to_state.
PA3  state_at(a, t) = to_state of the last transition of a with occurred_at ≤ t, or Missing if none.
PA4  P(a).state = state_at(a, now); a versioned artifact carries state_at(a, t) in the version in force at t.
```

The current state is thus a projection of the transition history: a snapshot at `t` and the history read up to `t` agree by PA3–PA4. A trace of the machine is the `followed_by` chain of one artifact; it is time-ordered (§2.3), so TemporalMode, Repeat and Monitor apply to it.

`write_mode`: BACKFILL → always APPEND; REFRESH → always MERGE (atomic swap); ADAPTIVE → inferred from state (planner Rule 10).

**Execution protocol (D46; ADR 0005).** Planning produces `WritePlan`s and `TransitionPlan`s and writes nothing. An authorised runtime executes a plan in four steps:

1. *Validate.* The plan id, artifact, sink and operation are those of the plan. The principal holds `WRITE` (§12.10). The proposed transition is a row of δ from the artifact's current state. Any failure rejects the plan, and nothing is written.
2. *Dispatch.* Append the `scheduled` transition (ON_DISPATCH) under its `idempotency_key`.
3. *Write.* Execute the `WritePlan`, tagged with `write_id = H(plan id, artifact_key, window_start, window_end)`.
4. *Record.* Append `succeeded` if the write committed, `failed` otherwise.

Steps 3 and 4 must be atomic with respect to failure:

- If the sink and the transition table are in one transactional store, such as one DuckDB or one PostgreSQL database, steps 3 and 4 run in one transaction.
- Otherwise, the write's commit must record `write_id` in commit metadata that the runtime can query. After a failure between steps 3 and 4, recovery looks up `write_id`. If it is present, recovery appends `succeeded`; if it is absent, recovery appends `failed`, and the window can be retried through `Failed —retried→ In-flight`.
- A sink that offers neither guarantee is `UNSUPPORTED` for materialisation. The runtime attempts no write; it does not choose another sink, mode or backend.

A retry repeats steps 3 and 4 under the same `write_id`, so a write that already committed is not repeated.

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

**MedallionLayer(n, layer, promoted_at)** — Tags which Medallion layer the instances of a node type originate from. BRONZE, SILVER and GOLD are the layers of the Medallion pattern; DIAMOND is DGM's proposed extension above Gold (D39). Operational consequences: (1) Planner warns when `PathPred` traversal crosses a node with `MedallionLayer(BRONZE)` — raw data in analytical paths may be unvalidated. (2) Recommender suppresses `BRONZE` nodes from `GroupBy` suggestions. (3) Exporter tags `DGM_JSON` output with the minimum layer of all nodes in the query — touching Bronze nodes gets tagged `min_layer: BRONZE`.

**Σ is an operational decision table**: every annotation drives specified planner, recommender, and exporter behaviour.

### 2.5  Temporal properties and snapshots

```
P(n).effective_from/to : Timestamp (NULL = open)
P(e).valid_from/to     : Timestamp (NULL = open)
```

Temporal properties belong to instances. Snapshot G(t) = G_I(t): the node and edge instances valid at t, typed by the restriction of h; G_S is the same for every t. Delta ΔG(t₁, t₂): N_added, N_removed, E_added, E_removed, P_changed — sets of instances. §2.6 defines both as relational expressions.

**SCD resolution: as-was and as-is.** When a traversal reaches a versioned dimension from a fact, two different versions can legitimately be meant, and the query must say which.

```
SCDResolution ::= AS_WAS | AS_IS

AS_WAS   the version in force when the fact happened
AS_IS    the version in force at the reference instant τ of the query (§5.1)
```

For a hop between a fact instance `f` and an entity `d` of a versioned dimension type `T`, with `event_time(f)` the fact's event timestamp declared in its mapping (§2.6):

| Reference kind of the edge mapping | AS_WAS reads | AS_IS reads |
|---|---|---|
| `VERSION_KEY` — the fact stores a version surrogate | the stored version | `version(T, entity of the stored version, τ)` |
| `ENTITY_KEY` — the fact stores the durable key | `version(T, key, event_time(f))` | `version(T, key, τ)` |

Precedence under AS_WAS: a stored version key is used as it is and no interval lookup is made — it is the version the fact was loaded against. An interval lookup is made only when the fact stores a durable key. Under AS_IS the stored version key serves only to identify the entity.

- **Default.** `TemporalContext.scd_resolution` defaults to AS_WAS: a historical fact keeps the attributes it had when it happened unless the query asks otherwise.
- **Interaction with `as_of`.** τ decides which edges are valid and which entities exist (§2.6), in both modes. Under AS_IS it also selects the version read. Under AS_WAS the version read depends on the fact alone and does not move when τ moves.
- **No fact on the hop.** A hop that does not start from a fact — an Anchor on a dimension, a dimension-to-dimension bridge — has no event time. Both modes read `version(T, key, τ)`.
- **Explicit instant.** `TemporalJoin(join, as_of = t)` reads `version(T, key, t)` for its sub-join, whatever the mode.
- **Other SCD types.** SCD1 has one version, so the modes coincide. SCD3 keeps no dated history: both modes read the current row, and the previous value is available only through its own property. Under SCD6, `versioned_attrs` follow the mode and `overwrite_attrs` are always current.
- **Missing version.** If the selected version does not exist, `node_resolution` applies: STRICT rejects, LAX drops the tuple.

Example — a customer changes region. `SCDType(Customer, SCD2)`; `fact_sale.customer_key` is an `ENTITY_KEY` reference and `event_time(Sale) = occurred_on`.

```
dim_customer                                                  fact_sale
version_key  entity_key  region  effective_from  effective_to     sale_id  customer_key  occurred_on  revenue
101          C42         South   2023-01-01      2024-03-01       S1       C42           2023-06-10   600
102          C42         North   2024-03-01      NULL             S2       C42           2024-05-02   400

Query: revenue by c.region, τ = 2024-06-30
```

| Mode | S1 reads | S2 reads | Result | Answers |
|---|---|---|---|---|
| AS_WAS (default) | version 101 — South | version 102 — North | South 600, North 400 | "Where was the revenue earned?" |
| AS_IS | version 102 — North | version 102 — North | North 1 000 | "How much have today's North customers bought, ever?" |

Both results are correct; they answer different questions. With `VERSION_KEY` references (S1 storing 101, S2 storing 102) the two rows of the table are unchanged: AS_WAS reads the stored versions directly, and AS_IS maps both to entity C42 and reads version 102.

### 2.6  Relational mapping

`G_S` and `G_I` are a logical view over relations. Nothing in this specification requires the instance graph to be loaded: every node, edge, snapshot and traversal is defined here as a relational expression over the tables that hold the data.

```
M = (M_N, M_E)          M_N : N_S → NodeMapping          M_E : E_S → EdgeMapping

NodeMapping ::= NodeTable(relation: R,
                          entity_key: list[Col],          -- identity of the node across versions
                          version_key?: Col,              -- identity of one version (surrogate key)
                          props: K → Col,                 -- one column per key of K_S
                          effective_from?: Col, effective_to?: Col,
                          event_time?: Col)               -- fact types: when the fact happened (§2.5)

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
    is the SCD resolution mode (§2.5), not part of the mapping.
M7  A fact type with an ENTITY_KEY reference to a versioned type declares event_time, a non-NULL
    Timestamp or Date column. Without it AS_WAS cannot be resolved and the mapping is rejected.
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

**SCD2 at two instants.** The table below shows `version(Customer, C42, t)` as the instant moves — the version an AS_IS query reads at τ = t (§2.5). `SCDType(Customer, SCD2)`; the verb edge type `(Customer, Sale, 'placed')` is `ForeignKey(holder = TARGET, columns = [customer_key], references = ENTITY_KEY)`.

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
  -- Point relations: two TimePoints
    BEFORE(left: TimePoint, right: TimePoint)
  | AFTER(left: TimePoint, right: TimePoint)
  | CONCURRENT(left: TimePoint, right: TimePoint, tolerance?: SExpr)     -- tolerance: Duration, default zero
  -- Allen's 13 interval relations: two Intervals (BEFORE and AFTER above are the point forms;
  -- the interval forms PRECEDES and PRECEDED_BY complete the thirteen)
  | PRECEDES(l: Interval, r: Interval)   | PRECEDED_BY(l: Interval, r: Interval)
  | MEETS(l: Interval, r: Interval)      | MET_BY(l: Interval, r: Interval)
  | OVERLAPS(l: Interval, r: Interval)   | OVERLAPPED_BY(l: Interval, r: Interval)
  | STARTS(l: Interval, r: Interval)     | STARTED_BY(l: Interval, r: Interval)
  | DURING(l: Interval, r: Interval)     | CONTAINS(l: Interval, r: Interval)
  | FINISHES(l: Interval, r: Interval)   | FINISHED_BY(l: Interval, r: Interval)
  | EQUALS(l: Interval, r: Interval)

TimePoint ::= SExpr                                  -- of type Timestamp or Date (§4.1)
Interval  ::= (start: TimePoint, end: TimePoint)     -- same type at both ends; half-open [start, end)
```

A TemporalOrdering compares clock values and nothing else. UNTIL and SINCE are not TemporalOrderings: they quantify over the positions of a path and are defined once, as `TemporalMode` of `PathPred` (§4.1).

**Typing of TemporalOrdering.** Checked at construction:

```
TO1  A point relation takes two TimePoints of the same type. An Interval where a TimePoint is
     required is rejected.
TO2  An Allen relation takes two Intervals whose four endpoints have the same type. A TimePoint
     where an Interval is required is rejected; a point is not implicitly widened to an interval.
TO3  tolerance has type Duration and is non-negative.
TO4  Date and Timestamp are never mixed within one TemporalOrdering.
```

Semantics, with `l = [l.start, l.end)` and `r = [r.start, r.end)`:

| Relation | Holds iff |
|---|---|
| BEFORE(a, b) / AFTER(a, b) | a < b / a > b |
| CONCURRENT(a, b, tol) | \|a − b\| ≤ tol |
| PRECEDES(l, r) / PRECEDED_BY | l.end < r.start / r.end < l.start |
| MEETS(l, r) / MET_BY | l.end = r.start / r.end = l.start |
| OVERLAPS(l, r) / OVERLAPPED_BY | l.start < r.start < l.end < r.end / mirror |
| STARTS(l, r) / STARTED_BY | l.start = r.start and l.end < r.end / mirror |
| DURING(l, r) / CONTAINS | r.start < l.start and l.end < r.end / mirror |
| FINISHES(l, r) / FINISHED_BY | l.end = r.end and r.start < l.start / mirror |
| EQUALS(l, r) | l.start = r.start and l.end = r.end |

An interval bound that is NULL is unbounded on that side, as in §2.6: a NULL `start` is earlier than every instant and a NULL `end` later than every instant; two NULL ends are equal. An interval with `start ≥ end` is degenerate and satisfies no Allen relation. A point relation with a NULL TimePoint is UNKNOWN and does not hold.

Allen's relations apply to PERIOD/ACCUMULATING grain facts, whose instances carry an interval; EVENT grain facts carry a single TimePoint and use the point relations.

Rejected at construction: `MEETS(s.occurred_on, r.occurred_on)` — two TimePoints where Intervals are required; `BEFORE((a.window_start, a.window_end), s.occurred_on)` — an Interval where a TimePoint is required; `BEFORE(s.occurred_on, d.calendar_date)` with a Timestamp and a Date.

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

A constellation path relates two facts only through the dimension they share. It does not pair the individual fact instances: one customer's two sales and three returns are not six sale–return pairs. A query over a constellation path is therefore evaluated by **drill-across** (§5.2.2), never by joining the two facts row to row.

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
Multi-instance (ALL, K_SHORTEST): explicit fan-out — one tuple per selected instance. Aggregates do not inherit the fan-out unless they say `over = PATHS` (§4.2).

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

#### 3.4.6  Anchor sets

A question may involve more than two entities — revenue by customer, product and date involves three, joined through a fact that none of them reaches by a directed path from another. The question space is therefore defined over an **anchor set**, for any number of anchors, by one definition.

```
AnchorSet 𝒜 = { (A₁, role₁, alias₁), …, (A_k, role_k, alias_k) }      k ≥ 2

Aᵢ ∈ N_S       a node type, dimension or fact
roleᵢ          a role name, needed only to tell apart two anchors of the same type
               (order date and ship date, both of type Date; cf. RolePlaying, §2.4)
aliasᵢ         the alias bound to the anchor in the Join
```

Connection is taken in `U(G_S)`, the schema graph with every edge type traversable in both directions — forwards as a hop and backwards as a reverse hop (§3.2) — and within the hop bound `h` of the question space (`max_hops`, §8.11):

```
G_𝒜 = (N_𝒜, E_𝒜, τ_N, τ_E, K_S, Σ|_𝒜)

N_𝒜 = { T ∈ N_S | T lies on a walk of at most h hops in U(G_S) between two distinct anchors }
E_𝒜 = { e ∈ E_S | e lies on such a walk }
Σ|_𝒜 = { σ ∈ Σ | every type named by σ lies in N_𝒜 or E_𝒜 }
```

An anchor set is **valid** iff:

```
AS1  Connected: every two anchors are joined by a walk of at most h hops in U(G_S).
AS2  Distinct roles: two anchors of the same node type carry different roles, each a role
     declared by RolePlaying or a distinct edge label reaching that type.
AS3  Bounded: k ≤ MAX_ANCHORS.
```

An anchor set that is not valid has no question space; in particular an anchor that nothing connects to the others is not silently dropped.

For two anchors, `G_{A,B}` contains the directed subgraph `G_AB^S` of §3.4.5 and adds what is reachable only through reverse hops — the constellation `Sale ← Customer → Return`, for instance, lies in `G_{Sale,Return}` but not in the directed `G_AB^S`. The directed form remains the one that RelationshipSubgraph and TrailExpr evaluate (§3.4.2); `G_𝒜` is the scope of the question space. The projection lemma of §3.4.5 holds for `G_𝒜` with the same proof, reading "path" as a path whose hops may be reversed.

Example — customer, product and date. Schema: `(Customer, Sale, 'placed')`, `(Product, Sale, 'included in')`, `(Date, Sale, 'occurred on')` are verb edge types; `h = 2`.

```
𝒜 = { (Customer, –, c), (Product, –, p), (Date, –, d) }

Walks of ≤ 2 hops between anchors:  Customer – Sale – Product,  Customer – Sale – Date,  Product – Sale – Date
N_𝒜 = { Customer, Product, Date, Sale }
E_𝒜 = { (Customer,Sale,'placed'), (Product,Sale,'included in'), (Date,Sale,'occurred on') }
AS1 holds (every pair meets at Sale); AS2 is vacuous; k = 3.

A question of Q_valid(𝒜):
  Join:    Anchor(Customer, c)
           PathJoin(c, VerbHop(Customer,'placed',Sale), s)
           PathJoin(s, VerbHop⁻¹(Sale,'included in',Product), p)
           PathJoin(s, VerbHop⁻¹(Sale,'occurred on',Date), d)
  GroupBy: [ c.region, p.category, d.month ]
  Agg:     revenue := SUM(s.revenue)
```

If a second fact type `Return` is also connected to the three dimensions, it enters `N_𝒜`; questions over both facts are then constellations and are evaluated by drill-across (§5.2.2).

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

**Path semantics.** The transition relation is not total: fact nodes cannot be the source of verb edges and are often sinks. CTL is therefore interpreted over maximal paths, without adding transitions (§10.4, D31; ADR 0001). For a PathPred evaluated at a start node s:

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

Semantics: maximal paths over a non-total transition relation; FORALL on an empty domain follows `on_empty` (§4.1, D31).

**Temporal property classes** (syntactic sugar over PathPred compositions):

```
TemporalProperty ::= SAFETY(bad)          -- AG(¬bad)
                   | LIVENESS(good)        -- AF(good)
                   | RESPONSE(trig,resp)   -- AG(trig → AF(resp))
                   | PERSISTENCE(good)     -- AF(AG(good))
                   | RECURRENCE(good,win)  -- good in every window period
```

**Recorded traces and monitoring verdicts.** `G_I` records what has been observed. A PathPred is evaluated on the recorded traces with the finite-trace semantics above, and its truth value is a statement about the record: `PathPred(FORALL, EVENTUALLY, good) = FALSE` means that some recorded trace has not reached `good` — not that the system never will. To judge a property of an execution that may still be running, a temporal property is evaluated by a monitor, which returns one of three verdicts.

```
Monitor(property: TemporalProperty, path: BoundPath,
        log?: OPEN | CLOSED,                              -- default OPEN
        final?: Pred,                                     -- holds at a position after which nothing can follow
        deadline?: (clock: TimePoint, within: SExpr))     -- within: Duration, measured from clock to now
  → Verdict

Verdict ::= SATISFIED | VIOLATED | UNDETERMINED
```

Monitor is a row-level expression of type `Enum(Verdict)`, one value per start node. Its path must be time-ordered (§2.3): a verdict about what has or has not happened yet is meaningless over structural hops.

A recorded trace is **closed** when no continuation is possible or admitted:

```
closed(π)  iff  log = CLOSED                          -- the record is declared complete
           or   final holds at the terminal of π      -- the execution has ended
           or   now > clock + within                  -- the deadline has expired
```

Otherwise the trace is **open**: it is a prefix of an execution that may continue. The verdict of one trace is the verdict shared by all of its admissible continuations:

```
SATISFIED      every continuation of π satisfies the property
VIOLATED       no continuation of π satisfies the property
UNDETERMINED   some continuations do and some do not
```

A closed trace has no continuation but itself, so its verdict is its finite-trace truth value. For an open trace:

| Property | SATISFIED | VIOLATED | UNDETERMINED |
|---|---|---|---|
| LIVENESS(good) | `good` holds at some recorded position | never while open | `good` not yet recorded |
| SAFETY(bad) | never while open | `bad` holds at some recorded position | `bad` not yet recorded |
| RESPONSE(trig, resp) | never while open | never while open | always — a later trigger may go unanswered, and an unanswered one may still be answered |
| PERSISTENCE(good), RECURRENCE(good, win) | never while open | never while open | always |

Over the traces of a start node, the monitor returns VIOLATED if any trace is VIOLATED, SATISFIED if all are SATISFIED, and UNDETERMINED otherwise. With no trace at all the verdict is UNDETERMINED while the log is open and follows `on_empty` once it is closed.

For a closed trace `Monitor = SATISFIED` exactly when the corresponding PathPred is TRUE; the monitor adds nothing there. Its purpose is the open case: absence of evidence before the end of an open record is UNDETERMINED, never VIOLATED.

```
-- LIVENESS(done), done = (to_state = Complete), over the transition trace of artifact w (§2.4),
-- evaluated at now = 2024-06-30T00:00Z. w is In-flight; no transition into Complete has been recorded.

Monitor(LIVENESS(done), path)                                          → UNDETERMINED   -- log open, no deadline
Monitor(LIVENESS(done), path, deadline=(w.window_end, 2 days))         → UNDETERMINED   -- w.window_end = 2024-06-29: not expired
Monitor(LIVENESS(done), path, deadline=(w.window_end, 2 days))         → VIOLATED       -- w.window_end = 2024-06-20: expired
Monitor(LIVENESS(done), path, log=CLOSED)                              → VIOLATED       -- the record is complete
Monitor(LIVENESS(done), path)   after a 'succeeded' transition         → SATISFIED      -- under any log status
```

**Path-instance duality:**

```
PathPred(EXISTS, ALL, φ) ≅ disjunction over path instances   -- one disjunct per instance
PathPred(FORALL, ALL, φ) ≅ conjunction over path instances   -- one conjunct per instance
```

Each disjunct is the condition φ on one instance: a term of a DNF, not in general a minterm (§8.3). Dually, each conjunct is a CNF clause, not in general a maxterm.

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
        | Now                                         -- the reference instant τ of the evaluation (§5.1)
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

ScalarPred(PropRef(s.shipped_on), (> PropRef(s.occurred_on)))
  ≡ ScalarPred(Compare(PropRef(s.shipped_on), >, PropRef(s.occurred_on)))                  -- two references

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

rank_by_rev := RANK() OVER (PARTITION BY c.region ORDER BY weighted_rev DESC)
  ≡ RankFn(RANK, partition=[PropRef(c.region)], order=[(AggRef(weighted_rev), DESC)])        : Int

scc_label := GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC), c))                              : Int

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
Expr ::= AggFn(fn: AggFn, arg: SExpr | NodeArg(alias: A), over?: ENTITIES | PATHS)
                                                     -- arg over PropRefs of C; NodeArg with COUNT only;
                                                     -- over defaults to ENTITIES
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
                 | LATEST | EARLIEST                 -- only the tuples of the last / first period of each group
TimeUnit       ::= DAY | WEEK | MONTH | QUARTER | YEAR
```

**What an aggregate counts.** A PathJoin under ALL or K_SHORTEST puts one tuple in C per path instance, so a node reached by several paths appears in several tuples. `over` fixes the unit of aggregation:

```
ENTITIES (default)  Within each group, each distinct combination of the instances bound to the node
                    aliases referenced by arg contributes once, however many paths reach it.
                    COUNT(NodeArg(x)) counts distinct instances of x.
PATHS               Every tuple contributes: a measure is taken once per path instance.
                    Must be written explicitly.
```

A measure is therefore never multiplied by path fan-out unless the query asks for it. `PathAgg` is by definition per path — it aggregates along the instances of its own path, typically with weights — and is unaffected by `over`. `over = PATHS` on a measure whose bridge carries `WeightConstraint(ALLOCATIVE)` without a weight raises the warning of §6.2.

```
Sale S1 (revenue 100) is linked to Store T by two paths: via campaign A and via campaign B
(allocation weights 0.5 each). PathJoin(s, …, st) with ALL yields two tuples for S1.

SUM(s.revenue)                                   = 100     -- ENTITIES: S1 once
SUM(s.revenue, over=PATHS)                       = 200     -- explicit: once per path
COUNT(s)                                         = 1       -- distinct sales
COUNT(s, over=PATHS)                             = 2       -- paths
PathAgg(…, ALL, SUM, s.revenue * p.weight)       = 100     -- per path, allocated
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
TemporalContext(default_as_of?: Timestamp,          -- explicit reference instant
                time_zone?: Zone,                   -- IANA zone name; default UTC
                scd_resolution?: AS_WAS | AS_IS,    -- default AS_WAS (§2.5)
                node_resolution: STRICT|LAX, edge_resolution: STRICT|LAX)
```

**Reference instant.** Every query evaluation has exactly one reference instant τ (§10.4, D33; ADR 0003):

```
τ = default_as_of                                   if supplied
τ = floor(clock(), REFERENCE_GRANULARITY)           otherwise — captured once, before planning
```

`clock()` is the clock of the service that plans the query. τ is passed to the backend as a value; emitted SQL never calls a backend clock function. τ is an instant; `time_zone` fixes where calendar days, months, quarters and years begin.

Every time-relative construct of the query resolves against the same τ:

| Construct | Resolves to |
|---|---|
| `Now` (§4.1) | τ |
| Validity resolution, and version resolution under AS_IS or on a hop with no fact (§2.5, §2.6), absent a TemporalJoin override | `valid(e, τ)`, `version(T, key, τ)` |
| `YTD`, `QTD`, `MTD` (§4.2) | from the start of the year, quarter or month containing τ in `time_zone`, to τ inclusive |
| `ROLLING(d)` | (τ − d, τ] |
| `TRAILING(n, unit)` | the n complete calendar units immediately before the unit containing τ |
| PipelineArtifact TTL and backfill eligibility (§2.4), Monitor deadlines (§4.1) | comparisons against τ |
| `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR` of a Timestamp | calendar fields in `time_zone` |

Two occurrences of `Now` are the same value; a composed query has one τ for all its CTEs; an asynchronous or streamed execution keeps the τ captured at submission. Supplying `default_as_of` replaces the captured instant and changes nothing else in the query, so re-running with the τ of an earlier evaluation reproduces it over the same data.

A query is **τ-dependent** iff it uses any construct of this table, including default version or validity resolution over a versioned type or an edge type with a validity window.

STRICT: reject missing versions. LAX: drop the tuple, since the node is absent from `G_I(t)`; the caller chooses the mode explicitly, as neither field has a default. Explicit TemporalJoin overrides version and validity resolution for its sub-join; it does not change τ. Valid forms: B₁, B₁∘B₂, B₁∘B₃, B₁∘B₂∘B₃.

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

GroupBy: list of SExpr over PropRefs from C' — a bare PropRef, a Func of one, or TypeOf(alias). A degenerate attribute (`Degenerate(f, key)`, §2.4) is a valid GroupBy key like any other fact property. Agg: { aᵢ := Exprᵢ }. Grain-aware. Free→Free SubgraphExpr valid in Agg — scalar broadcast to all groups. Having: T [AggRef].

**Drill-across.** The fact aliases of a Join context fall into **fact branches**: two fact aliases belong to the same branch iff some path of the Join connects them without passing through a conformed dimension by a constellation hop (`VerbHop⁻¹` into the dimension followed by `VerbHop` out of it). A context with one branch is aggregated as usual. A context with two or more branches is a constellation and is evaluated as follows — this is the definition of its result, not an optimisation:

```
1. For each fact branch Fᵢ: restrict C to the conformed dimensions and the aliases of Fᵢ;
   apply the Where predicates that concern Fᵢ; group by the GroupBy keys; compute the
   aggregates that belong to Fᵢ.
2. Align the per-branch results by FULL OUTER JOIN on the GroupBy keys.
3. Evaluate cross-branch expressions — ArithExpr over aggregates of different branches —
   and Having on the aligned rows.
```

Well-formedness of a constellation query, checked at construction:

```
DA1  Every aggregate argument references the aliases of exactly one fact branch (and conformed
     dimensions). An aggregate over PropRefs of two branches is rejected.
DA2  Every GroupBy key is an attribute of a dimension d with Conformed(d, Fs) ∈ Σ, where Fs
     contains the fact type of every branch. The GroupBy keys are the conformance keys on which
     the branches are aligned. With no GroupBy each branch yields one row and step 2 pairs them.
DA3  A Where predicate references conformed dimensions only (it applies to every branch), or
     conformed dimensions and one fact branch (it applies to that branch, before aggregation).
     A predicate that compares PropRefs of two branches is rejected: it would pair fact
     instances, which the constellation does not relate.
DA4  COUNT of an absent branch is 0; every other aggregate of an absent branch is NULL.
```

By construction each measure is aggregated over the tuples of its own branch only, so every aggregate equals the one computed by querying that fact alone with the same filters and grouping. A key present in one branch and absent from another still yields a row (step 2), with the absent branch filled by DA4; a query that wants only keys present in both adds a Having such as `COUNT(sale) > 0`.

```
Customer C1: sales S1 (100), S2 (200); returns R1 (10), R2 (20), R3 (30).
Customer C2: sales S3 (50), S4 (70); no returns.          Customer C3: return R4 (5); no sales.

GroupBy [customer.id];  Agg: n_sales := COUNT(sale), revenue := SUM(sale.revenue),
                             n_returns := COUNT(return_ev), rate := n_returns / n_sales

Row-to-row join (not the semantics):  C1 → 6 rows; n_sales = 6, revenue = 900, n_returns = 6
Drill-across:
  customer  n_sales  revenue  n_returns  rate
  C1        2        300      3          1.5
  C2        2        120      0          0
  C3        0        NULL     1          NULL      -- division by zero is NULL (§4.1)
```

Relating individual instances of two facts — "returns made after their sale" — requires an edge type between the facts, such as a bridge `(Sale, Return, 'returned_as')`. That is a single path and a single branch, not a constellation.

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

**Scope of Band 3.** What a WinExpr and Qualify may reference depends on whether Band 2 is present:

```
With B₂     GroupBy keys, AggRefs, and a node alias whose entity key is among the GroupBy keys
            (as the argument of a GraphExpr). Every other alias of C has been consumed by the
            aggregation: a PropRef through it is rejected at construction.
Without B₂  the PropRefs of C.
```

After `GroupBy: [ c.id, c.region ]`, `ORDER BY weighted_rev` and `PARTITION BY c.region` are valid; `ORDER BY s.revenue` is rejected, because `s` no longer exists at the grain of the result — a row-level value needed for ranking is aggregated in Band 2 first.

SExprs in a WinExpr are typed by §4.1. Typing:

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
| 3 | GroupBy | Partition C' by the GroupBy keys, degenerate attributes included. |
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

A satisfiable filter over an empty table is still logically satisfiable and still returns no rows. Atoms are opaque to the BDD: by itself it knows only whether two atoms are the same variable. What it knows about the *values* the atoms speak of comes exclusively from the axioms Δ, including the order theory below; an inference about values with no axiom behind it is never made.

**Variables.** A BDD variable is an atom in canonical form:

```
ScalarPred     the ScalarCond AST after normalisation: operands of commutative operators ordered;
               a > b rewritten b < a;  a ≠ b rewritten NOT(a = b);  a ≥ b rewritten NOT(a < b);
               a ≤ b rewritten NOT(b < a). A comparison with a constant is instead rewritten over
               the basis atoms L and E of the order theory below. The Ref kind (PropRef, AggRef,
               WinRef) is part of the atom.
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
               Δ_ord    the order theory of atoms, below
```

The BDD of a filter φ is T⁺(φ), and every property is decided relative to Δ:

```
tautology(φ)      iff  Δ → T⁺(φ)            is the TRUE node
logical_sat(φ)    iff  Δ ∧ T⁺(φ)            is not the FALSE node
implies(φ, ψ)     iff  Δ ∧ T⁺(φ) → T⁺(ψ)    is the TRUE node
equivalent(φ, ψ)  iff  Δ → (T⁺(φ) ↔ T⁺(ψ))  is the TRUE node       -- the two filters keep the same tuples
```

`NotTrue(φ)` is the complement of φ's truth set — SQL `(φ) IS NOT TRUE` — and is two-valued. It is not written by users; FORALL elimination (§4.1) produces it, so that a position where `sub_filter` is UNKNOWN counts as a violation of a universal condition.

**Order theory of atoms.** Propositional equivalence — two formulas over the same variables having the same truth table — is what the BDD decides natively. Inference about values — that `x > 5` entails `x > 3` — is not propositional: the two atoms are different variables. It is supplied by a theory layer that contributes axioms to Δ and nothing else.

The theory covers atoms that compare an expression with a constant:

```
Scope     Compare(e, op, c) and In(e, {c₁, …}) where e is any SExpr and c is a literal, or a Param
          already bound to a value. Atoms whose constant side contains Now or an unbound Param, and
          atoms comparing two non-constant expressions, are outside the theory and stay opaque.

Basis     For each expression e and each constant c compared with it, two base atoms:
            L(e,c)  ≡  e < c          E(e,c)  ≡  e = c
          Every comparison is rewritten over the basis before translation:
            e < c  ↦ L            e = c  ↦ E             e ≤ c  ↦ L OR E
            e ≥ c  ↦ NOT L        e ≠ c  ↦ NOT E         e > c  ↦ NOT L AND NOT E
            In(e, {c₁,…,cₙ})  ↦  E(e,c₁) OR … OR E(e,cₙ)
          A constant on the left is moved to the right first (c < e ↦ e > c).
          These rewritings are exact in Kleene logic: both sides are UNKNOWN exactly when e is NULL.
```

For one expression `e` of an ordered type (Int, Real, String, Date, Timestamp, Duration) and constants `c < c'`, the axioms Δ_ord are:

```
O1   L(e,c)⁺  → L(e,c')⁺           e < c  entails  e < c'
O2   L(e,c')⁻ → L(e,c)⁻            e ≥ c' entails  e ≥ c
O3   E(e,c)⁺  → L(e,c')⁺           e = c  entails  e < c'
O4   L(e,c')⁻ → E(e,c)⁻            e ≥ c' entails  e ≠ c
O5   E(e,c)⁺  → L(e,c)⁻            e = c  entails  not e < c            (same constant)
O6   (L(e,c)⁺ ∨ L(e,c)⁻) ↔ (E(e,c)⁺ ∨ E(e,c)⁻) ↔ (L(e,c')⁺ ∨ L(e,c')⁻)
                                    all atoms over e are defined together: exactly when e is not NULL
```

For a type that admits only = and ≠ (Bool, Enum, LabelSeq) there is no L; the axioms are `E(e,c)⁺ → E(e,c')⁻` for distinct constants, and O6 over the E atoms.

Each axiom is valid in every total order, for every value of `e` including NULL, so adding Δ_ord keeps the BDD sound. It suffices to instantiate O1–O4 for adjacent constants: with n distinct constants compared with `e`, Δ_ord has O(n) clauses.

```
x > 5  ⇒  x > 3
  T⁺(x > 5) = L(x,5)⁻ ∧ E(x,5)⁻            T⁺(x > 3) = L(x,3)⁻ ∧ E(x,3)⁻
  L(x,5)⁻ gives L(x,3)⁻ by O2 and E(x,3)⁻ by O4.  Hence Δ ∧ T⁺(x > 5) → T⁺(x > 3): implies holds.

x > 5  ∧  x < 3      is not logically satisfiable
  T⁺ = L(x,5)⁻ ∧ E(x,5)⁻ ∧ L(x,3)⁺.   L(x,3)⁺ gives L(x,5)⁺ by O1, contradicting L(x,5)⁻ by Δ_excl.

x > 5  ⇒  y > 3      is not derived: no axiom relates atoms over different expressions.
x > y  ∧  y > x      is logically satisfiable for the BDD: atoms over two expressions are outside the theory.
```

The last two lines are deliberate. A simplification is applied only when an axiom of Δ proves it; where the theory is silent the atoms stay independent and the filter is kept as written.

The encoding is sound for Kleene logic: whatever it reports as a tautology, contradiction, implication or equivalence holds for every assignment of TRUE, FALSE and UNKNOWN to the atoms consistent with their meaning. It is complete for propositional reasoning and for comparisons of one expression with constants; it is not complete beyond that — arithmetic identities, relations between different expressions and integer discreteness (`x > 5 ∧ x < 6` over Int) are not recognised.

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

Core: BDDNode (var, low, high), FALSE_NODE=0, TRUE_NODE=1. Unique table (var,low,high)→id; computed cache (op,u,v)→result. Node ids are handles local to one BDD manager; identity across processes and sessions is `predicate_hash` (§8.14).

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

SQL translation: if |paths to TRUE in the BDD| ≤ THRESHOLD → UNION ALL of the corresponding cubes (pairwise disjoint conjunctions, so no row is produced twice); else → BDD-structured CASE expression.

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
Additivity:           SUM checked per measure by AD1–AD4 (§2.4); a rejected SUM on a
                      SEMI_ADDITIVE({TIME}) measure suggests TemporalAgg(SUM, …, LATEST), AVG, or
                      grouping by the period
AggFn over=ENTITIES:  deduplicate on the entity keys of the referenced aliases within each group
                      before aggregating; skipped when the Join has no multi-instance PathJoin
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
Having: min_rev > 100      -- removed: BX1 derives MIN(s.revenue) > 500, and Δ_ord (§6.1) gives > 100
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
Degenerate(f,key):       no join — key is read from the fact row; GroupBy and Where on it are valid
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
DELTA:             INSERT INTO an attached Delta table (APPEND only) — see "Delta writes" below;
                   never through delta_scan
ICEBERG:           INSERT INTO / MERGE INTO <catalog>.<schema>.<table> on an attached Iceberg
                   REST catalog — see "Iceberg writes" below; never through iceberg_scan

if TemporalAgg and sink ∈ {PARQUET,DELTA,ICEBERG}: partition by timestamp unit
if Q_delta and sink = DELTA: APPEND mode
if Grain(ACCUMULATING) and CHANGED_PROPERTY: APPEND mode
```

**Delta writes.** `delta_scan(path)` is a table function for reading; it is not a write target, and `INSERT INTO delta_scan(...)` is a syntax error. Writes go to a Delta table attached as a catalog:

```
ATTACH 's3://bucket/path' AS sales_delta (TYPE delta);
INSERT INTO sales_delta SELECT …;
```

| Operation | DuckDB 1.5.0 (tested) | DuckDB 2.0 / 2.0.1 (documented) | WritePlan |
|---|---|---|---|
| Read: `delta_scan(path)`, `SELECT` from attached table | works | supported | — |
| Append: `INSERT INTO <attached>` | works on an existing table | supported (no `RETURNING`, no `ON CONFLICT`) | APPEND |
| Create: `CREATE TABLE <attached>.<name>` | rejected: "Delta tables do not support creating tables" | supported from 2.0, incl. `PARTITIONED BY` | APPEND to a new table |
| `CREATE TABLE … AS`, `COPY … TO … (FORMAT delta)` | not tested | supported from 2.0.1; neither appends to nor overwrites an existing table | APPEND to a new table |
| `UPDATE`, `DELETE`, `MERGE INTO` | rejected: "Can only update base table" / "Can only delete from base table" / "Can only merge into base tables!" | not supported | MERGE — rejected |
| `CREATE OR REPLACE TABLE`, overwrite | no Delta commit is made (see note) | not supported | MERGE (atomic swap) — rejected |
| `ALTER TABLE`, `DROP TABLE` | not tested | not supported | — |

Consequences for planning: a Delta `WritePlan` is APPEND only. A plan that needs MERGE — `write_mode = REFRESH`, or ADAPTIVE resolving to MERGE (§2.4, Rule 10) — with a DELTA sink is rejected at planning time with that reason; such a pipeline needs another sink or a writer outside DuckDB. Creating a new Delta table requires DuckDB 2.0 or later. Concurrent writers: from 2.0.1 a conflicting commit fails at COMMIT and is not retried by DuckDB.

Evidence. *Tested* on 2026-11-25 with the `duckdb` 1.5.0 Python package installed in this repository and the `delta` extension build `380933c`, against a minimal Delta table (protocol 1/2, one Parquet file) built for the test: attach, read, append and the rejections above were observed; after `CREATE OR REPLACE TABLE <attached name> AS …` the table's `_delta_log` was unchanged, so no Delta write took place. *Documented* refers to the DuckDB 2.0 Delta extension page (https://duckdb.org/docs/current/core_extensions/delta, redirecting to the 2.0 documentation), read 2026-11-25; it labels CREATE TABLE as new in 2.0 and CTAS and COPY as new in 2.0.1, and states "UPDATE, DELETE, MERGE INTO, ALTER TABLE, and DROP TABLE are not supported." The 2.0 rows were not tested.

**Iceberg writes.** `iceberg_scan(path)` reads an Iceberg table directly from storage and is read-only: a write must commit new table metadata, and only a catalog can do that. An Iceberg write plan therefore has a precondition and a fixed target form.

```
Precondition   An Iceberg REST catalog is attached in the session. A WritePlan with sink ICEBERG and
               no attached catalog is rejected at planning time, not at execution.

                 ATTACH 'warehouse' AS iceberg_catalog (
                    TYPE iceberg,
                    SECRET iceberg_secret,
                    URI 'https://rest_endpoint.com'
                 );

Target         The catalog-managed table, by its three-part name:

                 INSERT INTO iceberg_catalog.sales.orders SELECT …;

               iceberg_scan(...) never appears as the target of INSERT, UPDATE, DELETE or MERGE.
```

WritePlan modes map to statements as follows:

| WritePlan mode | Statement | Condition |
|---|---|---|
| APPEND | `INSERT INTO catalog.schema.table` | — |
| APPEND to a table that does not exist | `CREATE TABLE … ` or `CREATE TABLE … AS`, then `INSERT INTO` | schema created with `CREATE SCHEMA` if absent |
| MERGE | `MERGE INTO catalog.schema.table` | table uses merge-on-read; see below |
| Partitioning by timestamp unit | `PARTITIONED BY` with the `year`, `month`, `day` or `hour` transform | — |

Operations and limits, as documented for DuckDB 2.0 (Iceberg extension, "Writing to Iceberg" and "Iceberg REST Catalogs", read 2026-11-25; https://duckdb.org/docs/current/core_extensions/iceberg/writing):

| Operation | Status | Limit stated by the documentation |
|---|---|---|
| `CREATE SCHEMA`, `DROP SCHEMA`, `CREATE TABLE`, `CREATE TABLE AS`, `DROP TABLE` | supported | — |
| `INSERT INTO` | supported | — |
| `UPDATE`, `DELETE` | supported | merge-on-read only, with positional deletes; fails on a table whose `write.update.mode` or `write.delete.mode` is not merge-on-read |
| `MERGE INTO` | supported | merge-on-read only, positional deletes; the documented way to express upserts, since Iceberg tables have no primary key |
| `ALTER TABLE` (rename, add/rename/drop column, change type, defaults, partitioning, format version) | supported | metadata-only schema evolution |
| Copy-on-write for `UPDATE`, `DELETE`, `MERGE INTO` | not supported | "not yet supported" |
| Writing through `iceberg_scan` | not supported | the catalog is required to commit the new table metadata |

The capability table is tied to the engine version it was read for. The exporter records the DuckDB version it targets; for a version this table does not cover, an Iceberg `WritePlan` other than APPEND is rejected until the table is re-verified against that version's documentation. A MERGE plan against a table that is not merge-on-read is rejected at planning time with that reason.

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

    -- Declarative (D35; ADR 0005): the planner produces plans; it writes nothing.
    -- The runtime executes WritePlans and appends the transitions when their condition holds.

    -- TTL expiry: transition Complete → Stale
    if P(f).state = Complete AND occurred_at of its last transition < now - ttl:
        TransitionPlan(artifact, Complete, expired, Stale, condition=ON_DISPATCH)
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

    -- Outcome of a failed run: which row of δ applies (§2.4)
    if transition kind = failed:
        if an earlier transition of the artifact reached Complete:
            to_state = Stale       -- refresh failure: prior result kept by the atomic MERGE
        else:
            to_state = Failed      -- backfill failure: no prior artifact
    -- every planned state change becomes, when executed by the runtime, one transition fact
    -- and one 'followed_by' edge (PA1–PA2); scheduled ON_DISPATCH, succeeded ON_SUCCESS, failed ON_FAILURE
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

**Rule 12 — Drill-across for constellations:**

```
if the Join context has k ≥ 2 fact branches (§5.2.2):
    emit one CTE per branch: its Where predicates, GROUP BY the conformance keys, its aggregates
    emit FULL OUTER JOIN of the k CTEs on the conformance keys (COALESCE the keys)
    evaluate cross-branch ArithExpr and Having on the joined rows
    -- never emit a direct join between two fact branches
```

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

The pre-execution gate sits between the planner and execution. It intercepts every `ExportPlan` before it reaches the SQL emitter, compares the cost estimate against a declared `ExecutionBudget`, classifies the failure mode if any, and returns a `BudgetDecision`. A plan the budget admits is delivered exactly as submitted. A reduction that changes the answer — a rewrite, a sample or an approximate algorithm — is applied only when the caller has allowed that kind of substitution in the budget; otherwise it is offered to the caller and nothing runs until the caller chooses (D40; ADR 0010). Allowed rewrites are tried before a delivery mode is chosen: streaming and async execution are decided for the best plan that remains, not for the plan as submitted.

**ExecutionBudget type:**

```
ExecutionBudget = (
  max_estimated_cost:   CostEstimate,  -- executable ceiling: no plan above it runs as submitted
  max_result_rows:      ℕ,             -- row count ceiling
  max_wall_time:        Duration,       -- synchronous execution timeout
  max_precompute_time:  Duration,       -- per PreComputation timeout
  streaming_threshold:  CostEstimate,  -- strictly above → stream results progressively
  async_threshold:      CostEstimate,  -- strictly above → async with callback + materialisation
  substitutions:        set[Substitution]   -- answer-changing reductions the caller allows; default ∅
)

Policy constants:
  HARD_CEILING_FACTOR  : ℝ, finite, ≥ 1      -- hard_ceiling(b) = b.max_estimated_cost × HARD_CEILING_FACTOR
  SAMPLE_DELTA         : ℝ ∈ (0,1)           -- failure probability of a SAMPLE guarantee (default 0.05)
  SAMPLE_MAX_ERROR     : ℝ ∈ (0,1]           -- largest error half-width a SAMPLE may report (default 0.05)
  MAX_CLARIFY_OPTIONS  : ℕ, ≥ 1              -- options returned by one CLARIFY (default 3)
```

`CostEstimate` values are compared by their scalar total in planner cost units; `≤` and `>` below denote that total order.

**Substitutions are opt-in (D40; ADR 0010).** Every reduction the gate knows changes the answer. Each kind is applied only if the caller lists it in `substitutions`:

```
Substitution ::= STRATEGY_SHORTEST     -- PathPred(ALL) → PathPred(SHORTEST)              (Strategy 1)
               | DEPTH_LIMIT           -- unbounded TrailExpr → max_depth = 3             (Strategy 1)
               | CONE_SCOPE            -- SubgraphExpr Free→Free → Bound(A)→Free          (Strategy 1)
               | APPROX_ALGORITHM      -- exact GraphExpr → approximate variant           (Strategy 1, Step 1b)
               | SAMPLE                -- anchor sampling with a Hoeffding bound          (Strategy 3)
```

The default is `substitutions = ∅`. The gate never applies a substitution outside the set, and no later stage — interface, supervisor or executor — applies one on its behalf. A substitution that is viable but not allowed is returned to the caller as a `CLARIFY` option and runs only if the caller selects it. Transformations that do not change the answer need no permission and are not substitutions: BDD simplification (§6.1), CSE and the canonical DAG (§6.2), pagination, and the choice among `PROCEED`, `STREAM` and `ASYNC`. The set is part of the budget, so a supervisor child budget (§12.5) inherits it unchanged; a child call can never allow more than its parent.

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
| `HARD_CEILING_FACTOR = 1` | OVER is empty; every cost above `max_estimated_cost` is FORBIDDEN. Disables SAMPLE and CLARIFY in that zone. |
| `substitutions = ∅` (default) | No rewrite, sample or approximation is ever executed; viable ones are offered as `CLARIFY` options. |
| `streaming_threshold = 0` | Only zero-cost plans are SYNC. |

`max_estimated_cost` and the hard ceiling are different limits. Above `max_estimated_cost` the submitted plan is never executed, but the gate still looks for a reduction: an allowed rewrite or sample, or else options for the caller (clarify). Above `hard_ceiling(b)` the gate does no further work and refuses.

```
BudgetDecision ::= PROCEED                      -- SYNC zone; execute synchronously
                 | STREAM                        -- STREAMING zone; progressive delivery
                 | ASYNC(sink: SinkTarget,
                         callback: Endpoint,
                         ttl: Duration)          -- DEFERRED zone; execute async
                 | REWRITE(plan: ExportPlan,
                           substitution: Substitution,
                           warning: string,
                           then: Delivery)       -- allowed substitution + transparency note
                 | SAMPLE(fraction: ℝ,
                          method: RANDOM | STRATIFIED | RESERVOIR,
                          error_bound: ℝ,
                          confidence: ℝ,
                          then: Delivery)        -- allowed sample with a stated guarantee
                 | PAGINATE(page_size: ℕ)        -- deliver the exact result page by page
                 | CLARIFY(options: list[ClarifyOption])
                                                 -- ask the caller to choose; nothing is executed
                 | REJECT(reason: string)         -- refuse; nothing is executed

Delivery ::= PROCEED | STREAM | ASYNC(…) | PAGINATE(…)        -- the decisions that execute a plan as given

ClarifyOption = (plan:    ExportPlan,
                 cost:    CostEstimate,
                 kind:    REFINEMENT | SUBSTITUTION(Substitution),
                 warning: string | None)          -- present iff kind = SUBSTITUTION; for SAMPLE it states the bound
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

  -- Step 1b — PreComputation time guard. An exact PreComputation that would
  -- exceed max_precompute_time is never started; its approximation is a substitution.
  slow = [pc for pc in plan.pre_compute if estimated_time(pc) > budget.max_precompute_time]
  if slow ≠ []:
    a = approximate(plan, slow)                         -- NONE unless every pc in slow has an
                                                        -- approximate variant (Strategy 3 table)
    if a ≠ NONE and zone(a.cost_estimate, budget) ≤ DEFERRED:
      if APPROX_ALGORITHM ∈ budget.substitutions:
        return REWRITE(plan=a, substitution=APPROX_ALGORITHM,
                       warning=approximation_explanation(a), then=deliver(a, budget))
      return CLARIFY([option(a, SUBSTITUTION(APPROX_ALGORITHM))])
    return REJECT("PreComputation exceeds max_precompute_time and has no "
                  + "executable approximation. " + narrowing_hint(plan))

  -- Step 2 — cost-aware rewrite, before any delivery decision.
  -- Each candidate applies one rewrite rule and is one substitution.
  viable = []
  if z > SYNC:
    rewrites = [
      rewrite_strategy(plan, SHORTEST)    if has_ALL_PathPred(plan),          -- STRATEGY_SHORTEST
      rewrite_depth_limit(plan, depth=3)  if has_unbounded_TrailExpr(plan),   -- DEPTH_LIMIT
      rewrite_cone(plan, BOUND_FREE)      if has_FREE_FREE_scope(plan),       -- CONE_SCOPE
    ]
    viable  = [r for r in rewrites if zone(r.cost_estimate, budget) < z]
    allowed = [r for r in viable if r.substitution ∈ budget.substitutions]
    if allowed:
      r = best(allowed)
      return REWRITE(plan=r, substitution=r.substitution,
                     warning=rewrite_explanation(r), then=deliver(r, budget))

  -- Step 3 — over the executable ceiling and no allowed rewrite:
  -- an allowed sample, else options for the caller, else refuse.
  if z = OVER:
    s = sampling_plan(plan, budget)                    -- NONE unless S1–S4 hold
    if s ≠ NONE and SAMPLE ∈ budget.substitutions:
      return SAMPLE(fraction=s.fraction, method=RANDOM,
                    error_bound=SAMPLE_MAX_ERROR, confidence=1 − SAMPLE_DELTA,
                    then=deliver(s.plan, budget))
    options = clarification_options(plan, budget, viable, s)   -- [] unless C1–C3 admit one
    if options ≠ []:
      return CLARIFY(options)
    return REJECT("Query exceeds max_estimated_cost and admits no allowed or "
                  + "offerable reduction. " + narrowing_hint(plan))

  -- Step 4 — the submitted plan is executable; deliver it exactly.
  -- A viable rewrite that is not allowed is not offered here: the exact answer is available.
  return deliver(plan, budget)


deliver(plan: ExportPlan, budget: ExecutionBudget) → Delivery:
  -- Precondition: zone(plan.cost_estimate, budget) ≤ DEFERRED.
  -- Never rewrites, samples, clarifies or rejects.

  match zone(plan.cost_estimate, budget):
    DEFERRED:  return ASYNC(sink=default_sink, callback=user_endpoint, ttl=24h)      -- keeps the τ captured at submission (§5.1)
    STREAMING: return STREAM
    SYNC:
      -- Result size guard: the exact result, delivered in pages
      if estimated_rows(plan) > budget.max_result_rows:
        return PAGINATE(page_size=default_page_size)
      return PROCEED
```

**Rewrite precedence.** A rewrite is *viable* only when it moves the plan to a strictly cheaper zone, and *allowed* only when its substitution is in `budget.substitutions`. Only allowed rewrites are returned as `REWRITE`; viable rewrites that are not allowed matter only in the OVER zone, where they become `CLARIFY` options. This fixes every case:

- A rewrite that lowers cost without leaving the plan's zone is discarded. It would give up exactness and buy no better delivery, so the submitted plan is delivered as-is (Step 4) or, in the OVER zone, falls through to Step 3.
- A rewrite that still lands above a threshold is returned as `REWRITE` with the matching delivery: `then=STREAM` in the STREAMING zone, `then=ASYNC` in the DEFERRED zone. `STREAM` and `ASYNC` are returned for the submitted plan only after every rewrite candidate has been estimated and found non-viable.
- A viable rewrite always lands in DEFERRED or below (viable means `zone < z` and `z ≤ OVER`), so the precondition of `deliver` holds.
- `best(allowed)` selects the lowest zone; ties are broken by rule order in the `rewrites` list (strategy, depth limit, cone), then by lowest `cost_estimate`. The warning describes the rewrite actually returned.
- In the STREAMING and DEFERRED zones the submitted plan is executable, so a viable rewrite that is not allowed is dropped and the exact plan is delivered. The gate never trades exactness for speed without permission.
- A SYNC plan is never rewritten: no cheaper zone exists.

**SAMPLE branch.** `sampling_plan(plan, budget)` returns a sampled plan only when all of S1–S4 hold; otherwise it returns `NONE` and the gate moves on to CLARIFY. A sampled plan is returned as `SAMPLE` only when `SAMPLE ∈ budget.substitutions`; otherwise it is offered as a CLARIFY option.

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

**CLARIFY branch.** `clarification_options(plan, budget, viable, s)` returns the choices the gate puts to the caller when it may not reduce the plan itself.

```
C1  A REFINEMENT option is a single descent step in L(G_AB) produced by the recommender (§7)
    and is BDD-valid.
C2  A SUBSTITUTION option is a viable rewrite of Step 2, or the sampled plan s of Step 3,
    whose substitution is not in budget.substitutions. It carries the rewrite's warning,
    or for a sample the error bound and confidence of Strategy 3.
C3  Each option o is executable: zone(o.cost, budget) ≤ DEFERRED.

Options are ordered by zone, then cost ascending, then REFINEMENT before SUBSTITUTION,
then recommender rank or rule order, and truncated to MAX_CLARIFY_OPTIONS.
```

Data required: the recommender's refinement candidates and `GraphStatistics.path_cardinality` for their cost estimates. When no option satisfies C1–C3 the gate returns `REJECT`. A chosen REFINEMENT is a new question and is submitted to `gate()` as such. A chosen SUBSTITUTION is the caller's explicit permission for that one plan: the plan is executed under `deliver(o.plan, budget)` and its result carries the option's warning, exactly as if the gate had returned `REWRITE` or `SAMPLE` for it. By C3 every choice is executable, so the exchange ends in a delivery or a `REWRITE`.

**Decision precedence.** `REJECT` (hard ceiling) › PreComputation guard (`REWRITE`, `CLARIFY` or `REJECT`) › `REWRITE` › `SAMPLE` › `CLARIFY` › `REJECT` (no option) › `ASYNC` › `STREAM` › `PAGINATE` › `PROCEED`. Apart from the PreComputation guard, SAMPLE, CLARIFY and the no-option REJECT are reachable only in the OVER zone.

**Boundary cases.** Normative for any implementation of `gate()`. Budget: `streaming_threshold = 100`, `async_threshold = 1 000`, `max_estimated_cost = 10 000`, `HARD_CEILING_FACTOR = 2` (hard ceiling 20 000); rows within `max_result_rows` and PreComputations within `max_precompute_time` unless stated. T1–T15 take `substitutions` = all five kinds; T16–T20 take the default `substitutions = ∅`.

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
| T16 | 5 000 | SHORTEST → 80 | — | — | `ASYNC` — rewrite not allowed; the exact plan is delivered |
| T17 | 15 000 | cone → 900 | hold, sampled cost 40 | refinement 700 | `CLARIFY`: sample 40 (SUBSTITUTION), refinement 700, cone 900 (SUBSTITUTION) |
| T18 | 15 000 | none | fail | none | `REJECT` — as T13 |
| T19 | 50, one PreComputation over `max_precompute_time`, approximation cost 60 | — | — | — | `CLARIFY`: approximation 60 (SUBSTITUTION `APPROX_ALGORITHM`); with `APPROX_ALGORITHM` allowed, `REWRITE(then=PROCEED)` |
| T20 | 50, one PreComputation over `max_precompute_time`, no approximate variant | — | — | — | `REJECT` — never started, whatever `substitutions` holds |

Budgets rejected at construction: `streaming_threshold = 200, async_threshold = 100` (inverted); `streaming_threshold = −1` (negative); `async_threshold = 20 000, max_estimated_cost = 10 000` (above the ceiling); any threshold NaN or ∞. A budget with `streaming_threshold = async_threshold = 100` is valid: `est = 100` gives `PROCEED` and `est = 101` gives `ASYNC`.

**Four handling strategies.** Strategies 1 and 3 change the answer and run only under the matching `Substitution` (D40); strategies 2 and 4 deliver the exact answer.

**Strategy 1 — Cost-aware rewriting.** Moves the query down the strategy refinement order (§3.3). Each rule is a `Substitution` and applies only when the caller allows it. Rewrites are transparent — the result carries a note explaining what was approximated.

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

**Strategy 3 — Sampling with error bounds.** Applied at the `Anchor` step in Band 1, and only to plans that satisfy S1–S4 above.

*What is estimated.* The population is the finite set A of anchor nodes of the query, of size |A|. For an average `AVG(p)` (S3a) the random variable is `Xᵢ = p(aᵢ)`, the value of p at the i-th sampled anchor, and the schema declares that every value lies in `[lo, hi]`; the estimand is `μ = (1/|A|) Σ_{a∈A} p(a)` and the estimator is the sample mean `X̄`. For a proportion (S3b), `Xᵢ` is 1 if the i-th sampled anchor satisfies the Where predicate and 0 otherwise, so `[lo, hi] = [0, 1]`. Anchors are drawn uniformly without replacement. Hoeffding's inequality [15] then gives, for n draws,

```
P( |X̄ − μ| ≥ t ) ≤ 2 · exp( −2 n t² / (hi − lo)² )
```

and it holds for sampling without replacement as well as with replacement ([15], §6). Setting `t = ε · (hi − lo)` and requiring the right-hand side to be at most `δ/k` for each of k aggregates (union bound) gives the sample size below; each estimate then lies within `ε · (hi − lo)` of its true value, jointly for all k, with probability at least `1 − δ`. The sample size is derived from the target error via Hoeffding's inequality for the mean of range-normalised values, with `n = f · |A|` anchor nodes drawn uniformly without replacement.

```
Sampling (k aggregates, δ = SAMPLE_DELTA, ε = SAMPLE_MAX_ERROR):
  n*            = ceil(log(2k/δ) / (2 * ε²))           -- Hoeffding, union bound over k
  anchor_sample = SAMPLE(A, fraction = n*/|A|, method=RANDOM)
  error_bound   = sqrt(log(2k/δ) / (2 * n*))  ≤  ε
  -- k=1, δ=0.05, ε=0.05  →  n* = 738
  -- returned with: "Estimated (±{error_bound*100:.1f}% of range at 95% confidence)"
```

*Anchors are sampled, not paths.* Sampling keeps every path and tuple of a sampled anchor and drops every path of an unsampled one. S2 makes this exact for the covered aggregates: there is one tuple per anchor, so the sampled tuples are a uniform sample of the population being averaged. Nothing else inherits the bound. In particular:

- `SUM` and `COUNT` of rows are not covered directly. A COUNT can be recovered from a proportion as `|A| · X̄`, with error `|A| · ε`; a SUM would need `|A| · X̄` with a declared range and is excluded by S3.
- `MIN` and `MAX` have no Hoeffding bound: a sample mean does not estimate an extremum.
- With a fan-out join (S2 violated), tuples per anchor vary and the sample mean of tuples is not an unbiased estimate of the tuple mean; such plans are not sampled.
- Graph metrics computed on a sampled subgraph change with connectivity — removing anchors removes paths, communities and centralities — and receive no guarantee from this bound.
`GraphExpr` algorithms are not sampled by the gate. Their approximate variants are used only under `APPROX_ALGORITHM`, through the PreComputation guard (Step 1b) or the rewrite rule of Strategy 1, and carry the accuracy statement of the approximate algorithm — not the Hoeffding bound above, and not a `SAMPLE` decision. Without that permission the approximation is offered as a `CLARIFY` option.

```
GraphExpr approximations:
  BETWEENNESS_CENTRALITY  →  Monte Carlo approximation (k=200 samples)
  PAGE_RANK               →  power iteration with ε=0.01 convergence threshold
  SIGNATURE_ENTROPY       →  reservoir sample of path instances (k=1000)
```

**Strategy 4 — Async execution with materialisation.** In the DEFERRED zone (cost above `async_threshold`, at most `max_estimated_cost`), once no allowed rewrite applies, the gate returns ASYNC; the gate itself schedules nothing. The runtime's executor then submits the plan (§11.10 `enqueue_async`), and the query is materialised as a `PipelineArtifact` (§2.4) — the result is written to a `SinkTarget` and the query DAG node id is the cache key. The user receives an immediate acknowledgement; subsequent identical questions are answered from the cached result.

```
Async lifecycle:
  Missing (not yet run)
  → In-flight (executing)
  → Complete (result in sink, cached by result_key, §8.14)
  → Stale (after ttl expires)

WritePlan inferred from ADAPTIVE mode (PipelineArtifact Rule 10):
  First run: WritePlan(mode=APPEND)
  Re-run after stale: WritePlan(mode=MERGE)
```

**Clarification messages are lattice refinements or named substitutions.** When `CLARIFY` is returned, the options are not generic suggestions. Each is a specific BDD-valid descent step in `L(G_AB)`, or a substitution the caller has not allowed, stated with its warning; every option brings the query back within `max_estimated_cost` (conditions C1–C3), cheapest first:

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
SinkTarget  ::= DUCKDB|POSTGRESQL|MOTHERDUCK|PARQUET(URI)
              | DELTA(URI)                                     -- attached with TYPE delta; APPEND only
              | ICEBERG(catalog: K, schema: K, table: K)      -- an attached REST catalog; not a storage URI
```

Expressiveness constraints:

```
Construct                 DuckDB  PG(DD)  Parq  Delta  Ice  MDuck  Cypher   SPARQL
──────────────────────────────────────────────────────────────────────────────────
VerbHop⁻¹                 CTE     CTE     CTE   CTE    CTE  CTE    native   native
BoundPath/PathPred        CTE     CTE     CTE   CTE    CTE  CTE    partial  partial
Repeat (closure)          CTE     CTE     CTE   CTE    CTE  CTE    partial  partial
Strategy SHORTEST         CTE     CTE     CTE   CTE    CTE  CTE    partial  ✗
TemporalMode UNTIL/SINCE  CTE     CTE     CTE   CTE    CTE  CTE    partial  ✗
SignaturePred             CTE     CTE     CTE   CTE    CTE  CTE    partial  ✗
TemporalJoin (node)       native  native  nat   nat    nat  native native   partial
TemporalAgg               native  native  nat   nat    nat  native ✗        ✗
TrailExpr Bound→Bound     ext/py  ext/py  py    py     py   ext/py partial  partial
TrailExpr Bound→Free      py      py      py    py     py   py     partial  partial
TrailExpr Free→Bound      py      py      py    py     py   py     partial  partial
TrailExpr Free→Free       py      py      py    py     py   py     ✗        ✗
SIGNATURE_ENTROPY         py      py      py    py     py   py     ✗        ✗
REACHABLE_FROM/TO         CTE     CTE     CTE   CTE    CTE  CTE    partial  partial
QUALIFY                   native  via DD  nat   nat    nat  native ✗        ✗
Q_delta                   SQL     SQL     SQL   SQL    SQL  SQL    ✗        ✗
```

native=direct; SQL=standard SQL; CTE=recursive CTE; ext/py=extension or Python pre-computation; py=Python only; partial=expressible only for the restriction stated below, emitted with a warning; ✗=rejected at construction.

**Cypher and SPARQL cells.** Each classification below is reproducible from the cited documents: SPARQL 1.1 Query Language, W3C Recommendation, 21 March 2013 (https://www.w3.org/TR/sparql11-query/, sections 9 Property Paths, 11 Aggregates, 17.4.1.4 EXISTS) — the SPARQL 1.2 Query Language Working Draft of 19 November 2026 leaves section 9 unchanged in the parts read — and the Neo4j Cypher Manual for Cypher 25 (pages *Variable-length patterns*, *Shortest paths*, *Predicate functions*), read 2026-11-25. "Not documented" means the pages read do not describe the feature; it is not a claim that the engine lacks it.

| Construct | Cypher | SPARQL |
|---|---|---|
| VerbHop⁻¹ | **native** — a pattern traversed against the stored direction: `(d)<-[:PLACED]-(f)` | **native** — inverse path `^p`: `?f ^:placed ?d` |
| BoundPath / PathPred | **partial** — fixed patterns and quantified path patterns with per-iteration `WHERE`; EXISTS as an `EXISTS { … }` subquery. Uniqueness differs: Cypher's default is relationship-unique (trail), DGM's default SIMPLE needs the `ACYCLIC` mode (Neo4j 2026.03, Cypher 25). FORALL needs `NOT EXISTS { … WHERE NOT φ }`; that form is not documented in the pages read | **partial** — fixed-length BoundPaths with aliases are basic graph patterns; EXISTS/NOT EXISTS are `FILTER EXISTS { … }` / `FILTER NOT EXISTS { … }` (SPARQL 1.1 sec. 17.4.1.4). A path with repetition cannot bind intermediate nodes: "Variables can not be used as part of the path itself, only the ends" (SPARQL 1.1 sec. 9) |
| Repeat (closure) | **partial** — quantified path patterns `((a)-[:R]->(b)){m,n}`, `+`, with group variables per iteration; `depth_alias` via `size(relationships(p))`. SIMPLE needs `ACYCLIC` (see above) | **partial** — `p*`, `p+`, `p?` give the set of reachable endpoints without duplicates (SPARQL 1.1 sec. 9.4). No bounded repetition `{m,n}`, no access to intermediate nodes or length, no simple-path constraint: only `Repeat(p, 0|1, UNBOUNDED)` without body aliases is expressible, or bounded depth by unrolling into a UNION of fixed-length patterns |
| Strategy SHORTEST | **partial** — `SHORTEST 1` is documented as non-deterministic among ties ("Which one is returned is non-deterministic"); DGM's tie-break (§3.3) is emitted as `ALL SHORTEST` followed by `ORDER BY` the canonical key and `LIMIT 1` | **✗** — no shortest-path or path-length construct |
| TemporalMode UNTIL / SINCE | **partial** — `E(φ U ψ)` as a quantified path pattern whose iterations require φ on the source node, ending at a node satisfying ψ; SINCE is the same on a time-ordered path traversed backwards. A-forms need the undocumented NOT EXISTS form; match mode as above | **✗** — a per-position condition along a repeated path needs intermediate variables, which property paths do not bind |
| SignaturePred | **partial** — the label sequence is `[r IN relationships(p) \| type(r)]`; EXACT and PREFIX by list equality and slicing; CONTAINS and REGEX need conversion to a string | **✗** — the path cannot be bound, so its label sequence is not available |
| TemporalJoin (node) | **native** — validity and version resolution are predicates on node and relationship properties in `WHERE` | **partial** — `FILTER` on date literals expresses the validity predicates; selecting the version depends on how versions are encoded in RDF (separate resources or reification), which the exporter fixes per target |
| TemporalAgg | **✗** — rolling, trailing and to-date windows need per-row windowed aggregation; window functions are not documented in the pages read | **✗** — SPARQL 1.1 aggregates are `COUNT, SUM, MIN, MAX, AVG, GROUP_CONCAT, SAMPLE` (SPARQL 1.1 sec. 11); there are no window functions |
| TrailExpr Bound→Bound, Bound→Free, Free→Bound | **partial** — paths are enumerable as values and their type lists aggregated; exact SIMPLE semantics needs `ACYCLIC` | **partial** — only with finite `max_depth`, by unrolling into fixed-length patterns with a variable per predicate (`?a ?p1 ?x1 . ?x1 ?p2 ?b`), `FILTER` for distinct nodes, and `GROUP BY` the predicate variables. Unbounded depth is ✗ |
| TrailExpr Free→Free, SIGNATURE_ENTROPY | **✗** — enumeration of every path in the scope; not attempted by the exporter | **✗** — as above, and entropy needs a logarithm, which SPARQL 1.1's function library does not provide |
| REACHABLE_FROM / TO | **partial** — `(a)-[*]->(n)` or `(a)(()-->()){1,}(n)` yields the reachable node set, which equals `N_A*`; the edge cone `E_A*` (edges on a *simple* path from A) is not the set of traversed relationships and needs pre-computation | **partial** — `?a (:p1\|:p2)* ?n` yields the reachable node set `N_A*` (with `^` for the backward cone); `E_A*` is not expressible |
| QUALIFY | **✗** — window functions not documented in the pages read | **✗** — no window functions (SPARQL 1.1 sec. 11) |
| Q_delta | **✗** — the exporter does not encode snapshot versions in either target model | **✗** — same |

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
DELTA:              ATTACH … (TYPE delta); INSERT INTO the attached table — APPEND only (§6.2 Rule 8)
ICEBERG:            INSERT INTO / MERGE INTO catalog.schema.table on an attached REST catalog (§6.2 Rule 8)
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

Correlation candidates identified by DAG node sharing are essentially free — the shared subgraph is already materialised. Cross-session result caching uses `result_key` (§8.14), the hash of the canonical DAG with schema, parameters, snapshot and reference instant: same key, same result set (given same `G` snapshot). Cache invalidation is structural: a cached result for DAG node `q_id` is stale when any node or edge referenced by `q_id`'s leaf queries has changed in `G`.

### 7.3  Suggestion types per band

**B₁:** ScalarPred; PathPred including constellation paths; TrimJoin (incl. REACHABLE_FROM/TO); TemporalCompose; strategy refinement order; temporal property classes; trail space pivots.

**B₂:** GroupBy alternatives (hierarchy roll-ups, cross-role); TemporalAgg variants; PathAgg over unused bridges; Having calibration.

**B₃:** Community partition; K for K_SHORTEST; percentile thresholds; TARJAN_SCC partition; RelationshipSubgraph characterisation.

**Temporal pivots:** rolling window; period-over-period; Q_delta; temporal property class alternatives.

**Trail pivots:** "Other DISTINCT_SIGNATURES connecting these nodes." "Dominant path is X — restrict via SignaturePred." "Before fixing target: OUTGOING_SIGNATURES inventory from anchor."

### 7.4  Annotation-driven suggestion rules

| Annotation | Suppressed | Added |
|---|---|---|
| Degenerate(f, key) | — (nothing is suppressed because the attribute is degenerate) | GroupBy on `f.key` for per-transaction roll-ups; ScalarPred on `f.key` |
| Conformed(d,Fs) | — | Constellation paths; Bound→Free from d to confirm/extend Fs |
| Grain(PERIOD) / Additivity(SEMI_ADDITIVE({TIME})) | SUM across periods | TemporalAgg(SUM, …, LATEST); AVG; Q_delta |
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

**Suggesting is not validating.** The recommender decides what to *offer*; `Q_valid` decides what is *valid*. They are separate judgements:

```
valid(q)       q ∈ Q_valid(A, B): well-typed and consistent with Σ. Decided on the schema graph.
               A valid question is accepted whenever the user asks it, offered or not.

offered(q)     q is valid and ranks high enough to be shown. Decided by scoring (§7.1 Layer 3).
               Not offering q never makes q invalid.
```

A GroupBy key is withheld from suggestions when grouping by it would not summarise: when `distinct_count(key) / row_count` in `GraphStatistics.property_dist` exceeds `HIGH_CARDINALITY_RATIO`, each group holds about one tuple. The test is on cardinality and applies to every property alike. A degenerate attribute such as `order_number` on an order-line fact passes it — several lines share one order — and is offered; a per-row identifier fails it and is withheld, whether or not it is annotated Degenerate. In either case `GroupBy: [ s.order_number ]`, written explicitly, is valid and is planned.

Where a row above suppresses something that is semantically wrong — SUM across periods under `Grain(PERIOD)`, SUM/AVG/MIN/MAX under `FactlessFact` — the same exclusion is stated as a validity constraint in §8.11 or §8.15, and it is that constraint, not the suggestion rule, that makes the question invalid.

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
| Grain(PERIOD) / Additivity(SEMI_ADDITIVE({TIME})) | SUM across periods excluded from Π_agg | Use the LATEST window or AVG |
| FactlessFact | SUM/AVG/MIN/MAX excluded from Π_agg | Use COUNT |
| MedallionLayer(BRONZE) | BRONZE excluded from GroupBy in Π_agg | Filter to SILVER+ before GroupBy |
| WeightConstraint(ALLOC) | Unweighted PathAgg excluded | Use weighted form with explicit weight ref |

---

## 8  Theoretical Foundations

### 8.1  DGM as a labelled transition system

**Primary model (D36; ADR 0006).** The instance graph is a labelled transition system

```
T = (S, Act, →, ℓ)      S = N_I     Act = L × {fwd, rev}     ℓ(s) = P(s)
→ = {(a, (l, fwd), b) | (a, b, l) ∈ E_I}  ∪  {(b, (l, rev), a) | (a, b, l) ∈ E_I}
```

Edge labels are actions; reverse traversal is the action `(l, rev)`; property bags label states. In §8.1–§8.10, G, N and E denote G_I, N_I and E_I.

**Derived Kripke structure.** A PathPred with pattern `π` and sub-filter `φ` is evaluated on the product of `T` with the pattern's automaton `A_π`, which has one position per hop and a loop for each Repeat:

```
K_π = (S × positions(A_π),  {((n,i),(m,j)) | n —α→ m in T, i —α→ j in A_π},  L_π)
L_π((n, i)) = {p⁺ | atom p of φ is TRUE at n} ∪ {p⁻ | p is FALSE at n}     -- §6.1 encoding, aliases of position i
```

Paths of `K_π` from `(start, 0)` correspond one to one to path instances of `π`. CTL formulas (§4.1) are interpreted on `K_π` over maximal paths (D31); R is not assumed total. `SignaturePred` constrains the action word of a run of `T`, not the states of `K_π`.

Consequences: CTL/LTL model checking applies to `K_π`. The BDD layer is standard symbolic machinery (Bryant 1986; Clarke et al. 1986). PathPred(FORALL/EXISTS, ALL) are the CTL path quantifiers A/E over the paths of `K_π`, and TemporalMode provides the CTL operators of §4.1.

The transposed graph G^T is `T` with `fwd` and `rev` exchanged: it reverses adjacency. Backward-cone RelationshipSubgraph(Free,Bound(B)) is the set of paths leading to B in path order. It is the computation tree for past-temporal formulas about B only when restricted to time-ordered edge types (§2.3); over other edge types it is a structural predecessor set and says nothing about earlier moments. SINCE/ONCE/PREVIOUSLY TemporalMode execute as model checking on G^T under that restriction.

**RDF named graphs as multiple accessibility relations.** An RDF Dataset consists of a default graph and zero or more named graphs — each named graph is a distinct set of triples. In modal terms, an RDF Dataset with named graphs G₁, G₂, ..., Gₙ is a frame with one accessibility relation per named graph — an LTS whose actions are the graph names. DGM's computed `RelationshipSubgraph(A, B, strategy)` is a *restriction* of the transition relation of `T` to the subset of edges lying on simple paths between A and B. This is a structural distinction: RDF named graphs are asserted by the author; DGM's `G_AB` is derived from the schema graph. The question-space results of §8.11 apply to the derived scope.

### 8.2  Propositional completeness of the filter tree

**Theorem (propositional completeness).** Let A be a finite set of atoms (ScalarPred, PathPred and SignaturePred instances), each read as a two-valued BDD variable (§6.1). Every Boolean function f : {0,1}^A → {0,1} is expressed by some filter tree over A built with AND, OR and NOT.

Proof, by construction. If f is constantly 0, take `AND(a, NOT(a))` for any a ∈ A. Otherwise, for each assignment α with f(α) = 1 form the minterm m_α = AND over a ∈ A of (a if α(a) = 1, else NOT(a)); then OR over all such m_α is TRUE exactly on f⁻¹(1). ∎

**Scope of the claim.** The theorem is about Boolean combinations of a *fixed* set of atoms and nothing more:

- It says nothing about which atoms exist. A condition that is not a Boolean function of the chosen atoms cannot be expressed over them. With A = {`x > 5`}, the predicate `x > 3` is not expressible: at x = 4 it is TRUE while the only atom is FALSE, and at x = 0 both are FALSE — so it is not a function of the atom.
- It is not a claim of completeness relative to first-order logic, CTL, or the class of analytical queries. Each would need its own statement and proof; none is made in this specification.
- Under SQL's three-valued logic AND, OR and NOT are not complete for functions over {TRUE, FALSE, UNKNOWN}; the theorem holds over the two-valued encoding of §6.1, where each nullable atom contributes the variables p⁺ and p⁻.

### 8.3  Minterms, maxterms, canonical form

Terms are used in their strict sense, relative to a fixed variable set V (the BDD variables of §6.1):

```
literal   a variable or its negation
cube      a conjunction of literals over distinct variables       -- a term of a DNF
minterm   a cube containing every variable of V exactly once      -- one complete assignment
clause    a disjunction of literals over distinct variables       -- a term of a CNF
maxterm   a clause containing every variable of V exactly once
```

Over V = {p, q, r}, `p ∧ q` is a cube — a DNF term — but not a minterm: it covers the two minterms `p ∧ q ∧ r` and `p ∧ q ∧ ¬r`. A formula's minterm set is its truth table; two formulas are equivalent iff their minterm sets are equal. A BDD's paths to TRUE are disjoint cubes, not minterms.


Logical properties, over the BDD variables and relative to Δ (§6.1): equivalence — same minterm set; unsatisfiability — empty minterm set; tautology — full minterm set; implication — subset. These are properties of the formula. The number of minterms *realised* by the tuples of a join context C is at most |C|; that is a property of the data, and a logically satisfiable formula may be realised by no tuple at all.

### 8.4  PathPred as compact quantification over paths

A PathPred quantifies over the instances of a path pattern without listing them. `PathPred(EXISTS, ALL, φ)` over k instances is equivalent to a k-term disjunction of per-instance conditions, and FORALL to a k-clause conjunction (via NOT EXISTS NOT over path instances). The PathPred has size O(|pattern| + |φ|) whatever k is; the expanded formula has size Θ(k · |φ|).

The gap is exponential only for families of graphs in which k grows exponentially with the size of the graph. Such families exist:

```
Diamond chain D_n:  s = v₀, and for i = 1..n a diamond  v_{i-1} → aᵢ → vᵢ,  v_{i-1} → bᵢ → vᵢ.
                    |N| = 3n + 1,  |E| = 4n.
                    Simple paths from s to t = v_n:  2ⁿ  (an independent choice at each diamond).

PathPred(s, Repeat(BridgeHop(X, 'next', X), 1, UNBOUNDED), EXISTS, ALL, EVENTUALLY, φ)
                    has the same size for every n; its expansion over instances has 2ⁿ disjuncts.
```

On graphs with few paths per endpoint pair the gap is small or absent: in a tree there is exactly one path between two nodes, so k = 1 and the PathPred is no more compact than the single condition it stands for. "Compression" refers to the size of the representation only. Evaluating a PathPred under strategy ALL can still cost time proportional to the number of instances (§6.3 rewrites it to SHORTEST for that reason); compactness of the formula is not a claim about evaluation cost.

### 8.5  EXISTS/FORALL as disjunction/conjunction duality

EXISTS = disjunction over path instances (a DNF whose terms are the per-instance conditions). FORALL = conjunction over path instances (a CNF whose clauses are the per-instance conditions). These terms and clauses are not in general minterms or maxterms (§8.3). SHORTEST/MIN_WEIGHT collapse both to single term. This duality is over path instances; with a TemporalMode the dual pairs are (EF, AG), (EG, AF) and (EX, AX), so eliminating FORALL exchanges EVENTUALLY and GLOBALLY (§4.1).

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

### 8.8  Allen's interval algebra: domain of exhaustiveness

**Domain.** Let T be a totally ordered time domain (Date or Timestamp) extended with −∞ and +∞. An interval is `[s, e)` with `s < e`, where a NULL start is read as −∞ and a NULL end as +∞ (§3.2). This is the domain of the theorem; it includes intervals with open (NULL) bounds.

**Theorem.** For any two intervals `l = [a, b)` and `r = [c, d)` of the domain, exactly one of Allen's 13 relations, as defined in §3.2, holds.

Proof. The relation is decided by at most three comparisons, each of which has exactly one outcome in a total order:

```
a = c:  b < d → STARTS      b = d → EQUALS        b > d → STARTED_BY
a < c:  b < c → PRECEDES    b = c → MEETS
        b > c:  b < d → OVERLAPS    b = d → FINISHED_BY    b > d → CONTAINS
a > c:  a > d → PRECEDED_BY a = d → MET_BY
        a < d:  b < d → DURING      b = d → FINISHES       b > d → OVERLAPPED_BY
```

The branches are disjoint and cover every outcome, so at least one relation holds; each leaf condition is exactly the defining condition of its relation in §3.2, and the conditions of different leaves contradict each other, so at most one holds. Thirteen leaves, thirteen relations. ∎

**Outside the domain.** A degenerate interval, with `start ≥ end` — including `start = end` — is not in the domain: it satisfies no Allen relation (§3.2), so exhaustiveness does not hold for it and is not claimed. A comparison whose endpoint expression evaluates to NULL for reasons other than an open bound is UNKNOWN. A TimePoint is not an interval; point relations are a separate family. The theorem says nothing about intervals over different types, which TO4 rejects.

### 8.9  Temporal property class hierarchy

```
SAFETY, LIVENESS ∈ Π₁/Σ₁.  RESPONSE ∈ Π₂.  PERSISTENCE ∈ Σ₂.  RECURRENCE ∈ GF.
```

### 8.10  SIGNATURE_ENTROPY as information-theoretic measure

H = -Σ p(s) log₂ p(s). H=0: maximally focused. H=log₂(k): k equally frequent types.

Relationship to recommender routing: high H → Stage 1 characterisation (Free endpoint) before fixing target. Low H → proceed directly to Bound→Bound deep-dive. SIGNATURE_ENTROPY is the formal routing signal for the two-stage exploration flow.

### 8.11  Question completeness theorem

**Level.** This section is stated on the schema graph. `A` and `B` are node types in `N_S`; `G_AB`, `N_AB` and `E_AB` abbreviate the anchor-set subgraph `G_{A,B}` and its node and edge types (§3.4.6), which contains the directed `G_AB^S` of §3.4.5.

**Hypotheses.** The theorem holds under the following assumptions, and claims nothing outside them:

```
QH1  Finite schema       N_AB, E_AB and every property signature K_S(T) are finite.
QH2  Hop bound           path patterns have at most h hops (max_hops, §8.16.2); Repeat bounds
                         min and max are parameters, not part of the question.
QH3  Expression bound    SExpr and Expr nest to depth at most d (ArithExpr, Func, aggregate arguments).
QH4  Finite formalism    the operator vocabularies — ComparisonOp, AggFn, ArithOp, ScalarFn, TemporalMode,
                         Strategy kinds, TemporalWindow kinds, RankFn, StatFn, GraphAlgorithm kinds — are finite.
QH5  Structural question a question is a skeleton: every constant (threshold, literal, window length,
                         as_of, k) is a typed parameter slot of Π(q) (§8.15), not part of q.
QH6  Syntactic identity  two skeletons are the same question iff they have the same canonical form —
                         BDD canonical predicates (§6.1) and canonical query DAG (§8.14). Equivalence
                         that holds only on particular data is not used.
```

**Theorem (Question completeness).** Under QH1–QH6, for any two node types `A, B ∈ N_S`:

1. *Finiteness and enumeration.* `Q_valid(A, B)` is finite, and there is an algorithm that lists each of its elements exactly once, reading only `(G_AB, K_S, Σ|_AB, h, d)` and no instance data.
2. *Decidable membership.* Whether a given skeleton belongs to `Q_valid(A, B)` is decidable from the same inputs.
3. *Coverage.* For every instance graph typed over `G_S` and every concrete query `Q` whose join scope lies in `G_AB` and which satisfies the typing rules (§4.1, §5) and `Σ|_AB`, replacing the constants of `Q` by parameter slots yields a skeleton `q ∈ Q_valid(A, B)` and a parameter assignment `π ∈ Π_valid(q)` with `Q = q(π)`.

Part 1 alone would be close to trivial: any finite vocabulary has finitely many bounded combinations. The content of the theorem lies in parts 2 and 3 together with the instance independence of part 1. The question space is computed from the schema alone, membership can be checked, and every concrete question a user can pose over `G_AB` is a point of `Q_valid × Π_valid` — none falls outside it.

**Proof, by construction.**

*Enumeration.* A skeleton is assembled from a finite sequence of choices, each from a finite set:

```
(a) Join        a set of path patterns over G_AB of at most h hops each, connecting the anchors:
                at most Σ_{j≤h} (2|E_AB|)^j patterns per connection (§8.6)            — finite by QH1, QH2
(b) Atoms       structural atoms: a Ref in scope, a ComparisonOp and a typed parameter slot, or two
                Refs and a ComparisonOp; PathPred atoms over the patterns of (a) with a TemporalMode;
                SExpr operands of depth ≤ d                                             — finite: m atoms
(c) Where       a Boolean function of the m atoms, represented by its canonical BDD:
                at most 2^(2^m) functions                                               — finite by QH6
(d) Band 2      a GroupBy subset of the properties in scope, aggregates (AggFn × measure SExpr of
                depth ≤ d), and a Having function of the AggRef atoms                   — finite
(e) Band 3      window functions from the finite kinds over the Band 2 schema, and a Qualify function  — finite
```

The algorithm iterates over (a)–(e) in nested loops in a fixed order. It emits the canonical form of each combination that passes the typing rules of §4.1 and §5 and every annotation rule of `Σ|_AB` listed below, skipping a canonical form already emitted. Every loop ranges over a finite set computed from the inputs, so the algorithm terminates. QH6 makes the emitted list free of duplicates.

*Membership.* Given a skeleton, check that each component is drawn from the corresponding finite set, that it type-checks, and that no annotation rule rejects it. Each check is a finite computation on the inputs.

*Coverage.* Let `Q` be a concrete query as in part 3. Its Join uses patterns of at most h hops in `G_AB` (QH2), so (a) contains it. Its predicates are Boolean combinations of comparisons whose operands are Refs or constants; abstracting the constants gives atoms in (b) and a Boolean function in (c). The same holds for Bands 2 and 3 by QH3–QH4. The resulting skeleton passes the typing and annotation checks because `Q` does, so it is emitted, and the abstracted constants form `π`, which satisfies `Π_valid` because `Q` satisfies `Σ|_AB`. ∎

**Outside the claim.**
- *Parameters.* `Π(q)` is not claimed finite: thresholds, `as_of` and damping factors range over infinite or continuous domains (§8.15). Enumeration over parameters requires a declared discretisation (§8.16.6).
- *Data-dependent equivalence.* Two skeletons that happen to return the same result on the current data are distinct questions. The quotient by such equivalence depends on the instance and is not part of `Q_valid`.
- *Without QH2 or QH3.* Unbounded path length or expression depth makes the set of skeletons infinite. The bounds are hypotheses, not consequences.

**Corollary (Schema determinacy).** `Q_valid(A, B)` and its cardinality are functions of `(G_AB, K_S, Σ|_AB, k)` only. Two instance graphs typed over the same schema graph have the same `Q_valid(A, B)`; inserting or deleting instances changes query results, `GraphStatistics` and recommender scores (§8.12), never the question space. By the projection lemma (§3.4.5), every question in `Q_valid(A, B)` is well-typed for every pair of instances `(a, b)` with `h_N(a) = A`, `h_N(b) = B`.

**Anchor sets.** Everything in this section is stated for two anchors for readability and holds for any valid anchor set `𝒜` (§3.4.6), with `G_𝒜` in place of `G_AB`: `Q_space(𝒜)` is the set of valid queries whose Join binds every anchor alias and whose scope lies in `G_𝒜`; `Q_valid(𝒜)` removes those that violate `Σ|_𝒜`; `Q_valid(A, B)` is `Q_valid({A, B})`. The proof uses only the finiteness of `N_𝒜`, `E_𝒜` and the property signatures (QH1), so the theorem and the schema-determinacy corollary carry over unchanged. Two bounds depend on the number of anchors `k`:

```
Computing G_𝒜        k breadth-first searches of depth h over U(G_S):  O(k · (|N_S| + |E_S|))
Path patterns        at most Σ_{j=0}^{h} (2·|E_𝒜|)^j  — independent of k, since E_𝒜 ⊆ E_S
Join skeletons       a Join connecting k anchors uses at most k − 1 connecting paths of ≤ h hops:
                     at most ((2·|E_𝒜|)^h)^(k−1) skeletons — exponential in k
```

The last bound is why `k` is capped by `MAX_ANCHORS` (AS3) and why skeletons are never enumerated: the recommender moves locally, one "add an anchor", "drop an anchor" or "change a connecting path" step at a time, each with at most `|N_S|` or `|E_𝒜|` alternatives.

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
`FactlessFact(f)` removes SUM/AVG/MIN/MAX; `Additivity` removes the SUMs forbidden by AD1–AD4 (for `Grain(f, PERIOD)`, by default, cross-period SUM); `Degenerate(f, key)` removes nothing — `f.key` stays a valid GroupBy and filter key, and no path question can start from it because it is not a node type; `SCDType(d, SCD1)` removes temporal join questions on `d`.

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
Predicate strength: tautology → targeted conjunctions → contradiction
Aggregation grain:  no grouping → fine grouping → per-entity grain
```

The recommender (§7) is a **guided navigator of `L(G_AB)`** — using BDD feasibility filtering to rule out invalid or redundant points, and trail metrics to score remaining points by analytical value, without enumerating the full lattice.

**Size characterisation.** While `Q_valid(A, B)` is finite (above), its cardinality ranges from trivially small to exponential in the hop bound. §8.16 decomposes the full analytical space `A_full(A, B)` by vocabulary level, identifies the dominant growth factor (path combinatorics), and bounds the size of the neighbourhood generated by the recommender's local moves — see §8.16.6 for the bound, and what it does not claim, and §8.16.7 for the per-level navigation strategies.

### 8.12  Semantic scoring of the question lattice

The trail space metrics induce a **non-uniform distribution over `L(G_AB)`**, making certain questions analytically more valuable than others without requiring full enumeration.

**`SIGNATURE_ENTROPY(G_AB)` as lattice routing signal.** High entropy: many equally frequent path types — the most informative questions stratify by path type (`SignaturePred`, `GroupBy` dominant signature). Low entropy: one path type dominates — the most informative questions go deep on measures along that path (`TemporalAgg`, `PathAgg` with specific strategy). Single path type (`DISTINCT_SIGNATURES = 1`): the entire question space collapses to variations on one pattern.

**`BETWEENNESS_CENTRALITY` on `G_AB` as GroupBy pivot signal.** Nodes with high betweenness within `G_AB` are the most discriminating `GroupBy` keys — they partition the tuple space most finely. Questions that group by high-betweenness nodes are near the fine-grain end of the aggregation specificity dimension.

**`SIGNATURE_DIVERSITY(A)` as strategy signal.** Low diversity at `A`: relationship is focused — `Bound→Bound` queries with `SHORTEST` strategy cover most of the analytical value. High diversity at `A`: relationship is varied — `ALL` strategy or `K_SHORTEST` with a signature filter is needed to capture the full structure.

**`DOMINANT_SIGNATURE(A, B)` as predicate seed.** The dominant label sequence directly yields the most analytically productive `PathPred` pattern — it is the path type that appears in the highest-value predicates of `L(G_AB)`.

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

As a set of expressions `Q_algebra(A, B)` is infinite: compositions nest without bound. On a fixed finite instance each composed query has a finite result.

**Semiring structure.** For queries with the same result schema, the set operations form a semiring:

```
(Q_algebra(A, B), UNION, INTERSECT, ∅, Q_top)
```

where `∅` (empty result) is the additive identity and `Q_top` (all tuples in `G_AB`) is the multiplicative identity. This is the Boolean algebra of subsets of the result domain, read as a semiring; it says nothing about expressiveness, which is treated below.

**Three composition patterns in DGM:**

`Q₁ INTERSECT Q₂` — returns tuples satisfying both questions. For Band 1 queries, equivalent to `AND(Q₁.where, Q₂.where)` already handled by the BDD. For Band 2/3 compositions, intersection is not reducible to a single DGM query.

`Q₁ UNION Q₂` — returns tuples satisfying either. For Band 1 queries, equivalent to `OR(Q₁.where, Q₂.where)`, handled by the BDD's `UNION ALL` SQL strategy (§6.1). For Band 2/3 compositions, union has no single-query equivalent.

`Q₁ JOIN Q₂ ON key` — correlates the answers to two questions on a shared key. This is the most powerful composition: it asks "where do the answers to Q₁ and Q₂ agree on key?" Always a new point in `Q_algebra` not in `Q_valid`.

**Relation to Datalog.** The question algebra is compared below with a precisely defined Datalog class. The result is an inclusion for a stated fragment and a converse for a stated class of programs — not an equivalence.

*Datalog class.* `Datalog¬,≤` denotes Datalog with stratified negation and the built-in comparison predicates `=, ≠, <, ≤` over constants, with safe rules, evaluated under the stratified (perfect-model) semantics [13, 20]. The extensional database is the relational encoding of `G_I` given by the mapping of §2.6: one relation per node type (entity key and properties) and one binary relation per edge type.

*Fragment `F_D` of `Q_algebra`.* A composed query is in `F_D` iff:

```
D1  every leaf is a Band 1 query (no GroupBy, Agg, Having, Window or Qualify);
D2  every ScalarCond compares Refs and constants with =, ≠, <, ≤, >, ≥, IN, IsNull, IsNotNull —
    no ArithExpr and no Func (they create values outside the database);
D3  every PathPred is EXISTS with EVENTUALLY, NEXT or UNTIL, or the negation of one —
    i.e. EF, EX, E(·U·) and AG, AX; never EG, AF or A(·U·), whose duals require EG;
    no SignaturePred;
D4  every Repeat either has a finite max (any mode), or has UNBOUNDED max and a body of exactly one hop;
D5  every strategy is ALL (selecting a particular instance, as SHORTEST does with its tie-break,
    is not translated);
D6  compositions use JOIN, UNION, INTERSECT, EXCEPT and non-recursive WITH.
```

**Theorem 1 (F_D ⊆ Datalog¬,≤).** For every query in `F_D` there is a `Datalog¬,≤` program that computes the same result on every instance graph.

*Proof, by construction on the structure of the query.*
- A conjunction of conditions is the body of one rule; a disjunction is one rule per disjunct; a join of two questions on a key is a rule whose body contains both IDB atoms sharing the key variable.
- A comparison is a built-in literal. Three-valued semantics is respected by translating the T⁺ form of §6.1: a NULL property is an absent EDB fact, so a comparison on it fails, as in Kleene logic, and `IsNull` is negation of the property's existence.
- A fixed-length path is a conjunction of edge literals. Under SIMPLE or TRAIL mode, the distinctness of its nodes or edges is a conjunction of `≠` literals. A Repeat with finite max is the union of these conjunctions for each admitted length.
- A Repeat with UNBOUNDED max and a one-hop body `e` is the recursion `R(x) :- E_e(x, y), R(y)`, with the base and the per-position conditions of the mode:

  ```
  EF φ        Rφ(x)   :- φ(x).               Rφ(x)   :- E_e(x, y), Rφ(y).
  E(φ U ψ)    Rφψ(x)  :- ψ(x).               Rφψ(x)  :- φ(x), E_e(x, y), Rφψ(y).
  EX φ        Xφ(x)   :- E_e(x, y), φ(y).
  ```

  These rules compute walks. A walk witness can be shortened to a simple path by removing cycles. For a one-hop body this keeps every remaining step an `e`-edge, and the remaining positions are a subset of the walk's: the shortened path ends at the same φ- or ψ-node, and its earlier positions satisfied φ on the walk. Conversely every simple path is a walk. So SIMPLE, TRAIL and walk semantics agree for EF, EX and E(·U·) over a one-hop body. Any finite simple witness extends to a maximal trace, so the finite-trace semantics of §4.1 gives the same answer.
- A negated PathPred or EXCEPT is a negated IDB literal, placed in a stratum above the IDB it negates. Nesting depth bounds the number of strata, so the program is stratified. Safety holds because every variable of a negated literal is bound by the anchor or by a positive literal of the same rule.

∎

*Why D4 is needed.* For a body of two or more hops, cutting a cycle out of a walk can break the body's label sequence. Existence of a *simple* path whose labels match `(aa)*` is NP-complete [19], whereas walk existence is in PTIME. `Repeat(body, ·, UNBOUNDED)` with a multi-hop body under SIMPLE or TRAIL is therefore outside `F_D`; under WALK it is rejected by R4 (§3.2).

**Theorem 2 (a class of Datalog programs expressible in Q_algebra).** Let a *path-recursive unary program* be a `Datalog¬,≤` program in which:
- every IDB predicate is unary;
- every rule is either non-recursive — `P(x) :- C(x), L₁, …, L_m`, where `C(x)` is a Band 1 condition on `x` and each `Lᵢ` is `Q(x)` or `¬Q(x)` for an IDB `Q` of a lower stratum —
- or one of the right-linear pair `P(x) :- ψ(x).`, `P(x) :- θ(x), E_e(x, y), P(y).`, for a fixed edge type `e` traversed in one direction and Band 1 conditions `θ`, `ψ`.

Every IDB of such a program is expressed by a Band 1 query anchored on `x`.

*Proof, by induction on strata.* A non-recursive rule is a Where clause: AND of `C` and of the PathPreds or negations expressing the `Lᵢ`, which exist by induction. Several rules for one head are their OR. A right-linear pair defines `E(θ U ψ)` along walks of `e`-edges, which is `PathPred(x, Repeat(hop_e, 0, UNBOUNDED), EXISTS, ALL, UNTIL(ψ), θ)`. The base rule matches position 0 of the Repeat. The default SIMPLE mode gives the same answer as walks for a one-hop body, by the argument of Theorem 1. ∎

*Programs outside this class* — IDBs of arity two or more, mutual or non-linear recursion, recursion along a multi-hop pattern or a union of edge types such as `(a | b)*` — are not claimed to be expressible.

**Delimitation — the two sides are not equivalent.**
- *Q_algebra ⊄ Datalog¬,≤.* "Total revenue per customer", `GroupBy [c.id] Agg SUM(s.revenue)`, returns sums that in general are not values of the database. The least model of a safe Datalog program contains only constants of the program and the database [20], so no `Datalog¬,≤` program computes it. Aggregation (Band 2), windows (Band 3), `ArithExpr`, EG and the A-forms that need it, SignaturePred, the strategies other than ALL, TrailExpr and GraphExpr are all outside `F_D`.
- *Datalog¬,≤ ⊄ Q_algebra* is not established either way. The same-generation program `sg(x,y) :- flat(x,y).  sg(x,y) :- up(x,u), sg(u,v), down(v,y).` defines pairs connected by the non-regular label language `upⁿ flat downⁿ`. The path patterns of §3 describe regular label sequences, and no composition expressing `sg` is known; the converse of Theorem 1 is claimed only for the class of Theorem 2.

**Complexity.**

**Corollary (data complexity of F_D).** For a fixed query in `F_D`, evaluation is in polynomial time in the size of `G_I`, since stratified Datalog with comparisons has polynomial data complexity [20]. The bound uses walk semantics, which Theorem 1 shows equivalent to the modes admitted by D4.

The bound does not extend beyond `F_D`, and for some constructs a polynomial bound is not expected:

| Construct | Known complexity | Consequence |
|---|---|---|
| Simple path matching a regular pattern, e.g. `Repeat(a·a, 1, UNBOUNDED, SIMPLE)` | NP-complete [19] | no PTIME claim; outside `F_D` |
| Counting simple paths between two nodes: `DISTINCT_SIGNATURES`, `SIGNATURE_ENTROPY`, `COUNT(…, over=PATHS)` under ALL | #P-complete [18] | no PTIME claim; evaluated with `max_depth` and budget gate (§6.3) |
| Enumerating instances under ALL | output can be exponential (§8.4) | cost is output-sensitive |
| EG and the A-forms using it, over maximal simple paths | no bound established | no PTIME claim |

The Immerman–Vardi theorem [16, 17] states that on *ordered* structures least-fixpoint logic, and with it `Datalog¬` with a successor relation, expresses exactly the polynomial-time properties. `F_D` is a strict fragment of that language. The theorem therefore places `F_D` inside PTIME but does not make `Q_algebra` capture PTIME, and no such claim is made.


**Fixpoint semantics of recursive CTEs.** DGM already uses recursive CTEs for `BoundPath` traversal — path expansion is a least-fixpoint computation over the reachability relation. The question algebra generalises this: any CTE chain `Q_n` conditioned on `Q_{n-1}` is a fixpoint sequence. Question chains are **descent paths in `L(G_AB)`** — they trace routes from broad questions to specific answers, converging either at the empty result (bottom of `L`) or at a stable fixpoint (most specific achievable answer).

**Two-level predicate system.** The BDD covers single-query predicates. Cross-query predicate reasoning — "Q₂'s Where is implied by Q₁'s Agg result" — requires a second level:

```
Level 1: BDD over single-query predicates (current; §6.1)
Level 2: Inter-query containment over CTE result schemas
          -- decidable for conjunctive Band 1 queries (BDD implication check extended to result schema)
          -- harder for Band 2/3 compositions
```

For Band 1 queries, cross-query containment is decidable via the BDD's implication check extended to result schema `PropRef` sets. For Band 2/3 compositions, the problem reduces to query containment under aggregation — tractable for common cases (same GroupBy keys, monotone aggregates) but not in general.

**Generating set.** `Q_valid(A, B)` is a finite generating set for `Q_algebra(A, B)` by definition of the closure. Its expressiveness relative to Datalog and its complexity are those stated above for the fragment `F_D`; no equivalence with Datalog and no capture of PTIME is claimed. The question lattice `L(G_AB)` extends to the question algebra `A(G_AB)` with semiring structure. Question chains are descent paths in `A(G_AB)`.

### 8.14  Query DAG canonical form

**Motivation.** The BDD is a canonical form for predicate trees: two filters with the same BDD node id are equivalent. The converse holds only for the Boolean structure over the atoms: equivalence relative to the axioms Δ is decided by `equivalent` (§6.1), not by id, and filters whose equivalence depends on what the atoms mean beyond Δ get different ids. `Q_algebra(A, B)` has an analogous canonical form: the **query DAG**, a directed acyclic graph of relational operations whose leaf nodes are BDD-canonical `Q_valid` queries and whose internal nodes are semiring operations (`UNION`, `INTERSECT`, `JOIN`).

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
same id ⇒ equivalent  (O(1))      same id ⇒ equivalent  (O(1))
different id ⇒ not decided        different id ⇒ not decided
```

**Definition (query DAG).** A query DAG `D` over `Q_valid(A, B)` is a DAG where:
- **Leaf nodes** are elements of `Q_valid(A, B)`, each in BDD-canonical form.
- **Internal nodes** have one of three types: `UNION(left, right)`, `INTERSECT(left, right)`, `JOIN(left, right, key: PropRef)`.
- The DAG is **reduced** when no semiring elimination rule applies and no two nodes have identical `(type, left_id, right_id)` triples.
- A reduced query DAG is **canonical with respect to the rules applied** — the same composition, built in any order, reduces to the same DAG, unique up to the variable ordering on leaf BDD atoms. Two compositions that are equivalent but not related by those rules can reduce to different DAGs.

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

**Canonical form theorem (soundness).** If two composed queries reduce to the same query DAG node, they are equivalent: they return the same result on every instance graph.

*Proof.* By induction on construction. A leaf is identified with another leaf only when their BDD ids, anchors and patterns coincide, and equal BDD ids denote equal Boolean functions of the same atoms. Each elimination rule replaces a node by an equivalent one: the semiring identities hold for all instances, and a containment rule fires only when `contains` returns True, which it does only on a proof of containment (BDD implication for Band 1 leaves). The sharing rule merges nodes only when operator and children are identical. ∎

**Completeness is not claimed.** Equivalent queries can reduce to different nodes. The equivalence test therefore has two answers — EQUIVALENT (same node) and UNKNOWN (different nodes) — and never answers NOT EQUIVALENT:

```
Atoms outside the theory   Where x + 0 > 5      vs   Where x > 5
                           equivalent; ArithExpr is not normalised, so the atoms and the leaves differ.

Band 2 containment         Q₁ = GroupBy [c.id] Agg t := SUM(s.revenue) Having t > 100
                           Q₂ = same with Having t > 50
                           UNION(Q₁, Q₂) ≡ Q₂, but contains(Q₁, Q₂) returns False for Band 2 leaves,
                           so the UNION node is kept and differs from Q₂.
```

Cases that `contains` does not decide, including every Band 2 and Band 3 containment, are UNKNOWN, not equivalent.

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

**CTE count.** The number of nodes in the reduced DAG is the number of CTEs emitted, and it is minimal *within the canonical form*: no composition reachable from it by the rules above has fewer nodes. It is not a lower bound over all equivalent queries — the Band 2 example above has a two-node equivalent (`Q₂` alone) that the rules do not find. The topological sort of the DAG gives the canonical CTE ordering.

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

**Cross-session caching.** BDD node ids and query DAG node ids are **local handles**: integers issued by one manager instance, in creation order. Within that manager they give O(1) equality. Across managers — another process, another session, the same session after a restart — the same integer can denote a different structure, so a local id is never persisted, never sent to another process and never used as a cache key.

Persistent identity is a hash of a canonical serialisation:

```
dag_hash(n)     Merkle hash of a query DAG node: H(operator, payload, hashes of its children),
                children sorted by hash for commutative operators and in order otherwise.
                A leaf's payload is its canonical band query: anchor types, path patterns, strategies,
                and predicate_hash of each filter.

predicate_hash  Merkle hash of the filter's BDD built under the canonical variable order:
                bands in the order B₁, B₂, B₃; within a band, atoms ordered by the byte order of
                their canonical serialisation (§6.1). H(atom, hash(low), hash(high)).
                The selectivity-based order of §6.1 may be used to execute; it is never used to hash.

question_key  = H( canon_version,            -- version of these serialisation and normalisation rules
                   schema_version,           -- hash of G_S, Σ and the mapping M (§2.6)
                   dag_hash(root),
                   params )                  -- resolved π: Param bindings, strategies, Repeat bounds,
                                             -- TemporalContext fields other than the instant,
                                             -- sorted by name

result_key    = H( question_key,
                   W,                        -- watermarks of the plan's dependency sources (D34)
                   τ, time_zone )            -- the last two only for a τ-dependent query (§5.1)
```

`H` is SHA-256 over a length-prefixed, type-tagged encoding, so that no two distinct inputs share a byte string; keys are not truncated below 128 bits. `Now` is serialised as a symbol in `dag_hash`; its value enters only through τ in `result_key`.

```
cache[result_key]   = (result_set, computed_at, unversioned)   -- unversioned: deps(plan) has a source without a watermark
saved[question_key] = a saved question, independent of data and of time

CachePolicy(accept_stale: Duration | None = None)              -- supplied by the caller with each request
```

Consequences:

- Two sessions that build the same question over the same schema obtain the same `question_key`, whatever their local ids; over the same source versions and reference instant they obtain the same `result_key`.
- A change to the schema, annotations or mapping changes `schema_version`; a change to any resolved parameter changes `params`; a different `as_of` changes τ. Each changes the key, so an entry computed under other conditions is never returned.
- A change to the serialisation or normalisation rules changes `canon_version` and retires every earlier key.

**Invalidation by source watermark (D34; ADR 0004).** `deps(plan)` is the set of source relations, or partitions after pruning, that the plan can read: the NodeTables and RelationTables of every type in its patterns (including inside EXISTS and Repeat), the tables used for version resolution, and the base relations of Derived views (§2.6). Each source has a watermark `w(src)` that changes whenever a committed change to it becomes visible, such as a table version or snapshot id. `W` is the sorted list of `(src, w(src))` over `deps(plan)`.

- A committed change to any dependency advances its watermark, changes `W`, and therefore changes the key: the stale entry is never found again. A change to a source outside `deps(plan)` does not affect the entry.
- A source with no watermark is *unversioned*: no key change can reveal that it changed. An entry that depends on one is **not served by default** — the lookup misses and the plan runs (D41; ADR 0011). It is served only when the caller's `CachePolicy.accept_stale` is set and `now − computed_at ≤ accept_stale`; the result then carries `possibly_stale = true` and `computed_at`. No component sets `accept_stale` on the caller's behalf.
- A plan with sampling includes its seed among the parameters.

**Reading watermarks (D45).** A cached result is correct only if it was computed from exactly the source versions in its key. The runtime therefore reads `W` before executing the plan, and then either:

1. *pins* every read of a source to its version in `W`, where the backend can read at a version; or
2. reads `W` again after execution and stores the result only if `W` is unchanged. Otherwise the result is returned to the caller but not cached.

Where each backend's watermark comes from; whether a given backend version actually satisfies the requirement is recorded in the conformance matrix (#138):

| Backend | Watermark | Pinned read |
|---|---|---|
| Delta table | table version | yes — read at that version |
| Iceberg table (REST catalog) | current snapshot id | yes — read at that snapshot |
| DuckDB native table | none built in; a commit counter maintained by the loader, incremented in the same transaction as the change | no — rule 2 |
| PostgreSQL table | as DuckDB | no — rule 2 |
| Parquet or CSV files | none; file times are not trusted | — (unversioned) |

A watermark must advance on every committed change that becomes visible and must never move otherwise. A source whose watermark does not satisfy this is treated as unversioned.

Example: revenue by region for 2024 reads `fact_sale` partitions 2024-01 … 2024-12 and `dim_customer`. A load into partition 2025-01 leaves the entry valid; an SCD2 update of `dim_customer` invalidates it.

This enables semantic-level caching: whenever two syntactically different forms of a question reduce to the same canonical DAG, they share a `question_key` and a cached result. SQL text caching cannot achieve this. Forms the normalisation does not identify are cached separately; that costs a recomputation and never returns a wrong result.

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
                 per-hop TemporalOrdering                point | Allen

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
| Grain(f, PERIOD), Additivity(f, key, kind) | SUM excluded from Π_agg where AD1–AD4 forbid it |
| FactlessFact(f) | {SUM, AVG, MIN, MAX} excluded from Π_agg; only COUNT |
| SCDType(d, SCD1) | Temporal resolution parameters in Π_temporal irrelevant |
| Degenerate(f, key) | none — `f.key` remains selectable as a GroupBy key in Π_agg |
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
  ¬(Additivity forbids SUM by AD2/AD3 ∧ AggFn = SUM)        -- default for Grain(PERIOD): across TIME
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

The query DAG canonical form (§8.14) extends naturally: two parameterized composed queries `(q₁, π₁)` and `(q₂, π₂)` are equivalent if they reduce to the same DAG node with the same discrete parametric configuration on each leaf; the converse is not claimed. Continuous parameters (timestamps, thresholds) are not part of the canonical form — they affect result values but not query structure.

### 8.16  Vocabulary decomposition and analytical space characterisation

§8.11 proves that `Q_valid(A, B)` is finite under its hypotheses. §8.15 adds parameter space `Π`. This section decomposes the full analytical space `A_full(A, B)` into vocabulary levels, quantifies its size, and establishes bounds that make the recommender (§7) tractable.

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

Let `S` be the set of structural skeletons of `A_full(A, B)` — choices of Join and path patterns, §8.11 (a). The other factors are families indexed by the skeleton, since what is applicable depends on it:

```
f_predicate(s)  = |applicable_predicates(s)|  -- canonical Boolean functions over the atoms in scope of s
f_operator(s)   = |V_Op|^r(s)                 -- operator choices for the r(s) property references of s
f_parameter(s)  = |Π_disc_valid(s)|           -- valid parameter settings over the declared discretisation (§8.16.6)

|A_full(A, B)| = Σ_{s ∈ S} f_predicate(s) · f_operator(s) · f_parameter(s)
              ≤ f_structure × max_s f_predicate(s) × max_s f_operator(s) × max_s f_parameter(s),
  with f_structure = |S| = |paths(G_AB, max_hops)|
```

`f_parameter` is finite only over a discretised parameter domain; with a continuous threshold `|Π(s)|` is infinite and the estimate is not defined. The worked example (§8.16.8) uses the bound on the second line.

#### 8.16.3  Four-level vocabulary hierarchy

The factors fall into four asymptotic levels ordered by growth rate:

| Level | Factor | Growth | Dominant term |
|---|---|---|---|
| **Level 1 — Structure** | `f_structure` | Factorial / exponential in `|E_AB|` | Path combinatorics |
| **Level 2 — Predicate** | `f_predicate` | Exponential in predicate count | `2^|V_Pred|` subsets |
| **Level 3 — Parameter** | `f_parameter` | Polynomial in `|V_P|` and `|V_Par|` | Cartesian product of finite ranges |
| **Level 4 — Operator** | `f_operator` | Constant | `|V_Op|` is fixed by the formalism |

**Level 1 dominates.** The structural path space (Level 1) is the primary source of analytical space size. All other levels are multiplicative but grow strictly more slowly.

#### 8.16.4  Effect of a parameter and the effective analytical space

Not every valid configuration is analytically distinct: changing a parameter often leaves the result unchanged. The effective space keeps the configurations where some parameter visibly matters. Parameters are discrete or discretised, so "matters" is defined by comparing results, not by a derivative.

**Result comparison.** For `q ∈ Q_valid`, `π ∈ Π_valid(q)` and a snapshot `G_I(t)`, `res(q, π, t)` is the result as a multiset of rows. Two results are **equal within tolerance**, `r ≈_ε r'`, iff after sorting rows by all columns they have the same number of rows, the same non-numeric values, NULL in the same places, and numeric values with `|x − x'| ≤ ε_abs + ε_rel · max(|x|, |x'|)`. `ε_abs` and `ε_rel` are policy constants (default 0 and 10⁻⁹).

**Discretised domains.** Every parameter `p` of `q` has a finite domain `D_p`. For a finite type, `D_p` is the type itself; for a numeric or temporal parameter, `D_p` is a declared grid — quantiles of the property's distribution in `GraphStatistics`, or a fixed step. Two values of an ordered `D_p` are *adjacent* when no grid value lies between them.

**Observed effect.** A parameter `p` has an observed effect at `(q, π)` on snapshot `t` iff some `π'` that differs from `π` only in `p`, with `π'(p)` adjacent to `π(p)` in `D_p` (any other value, for an unordered `D_p`), satisfies `res(q, π, t) ≉_ε res(q, π', t)`.

**Three spaces**, which differ in what is needed to compute them:

| Space | Definition | Needs |
|---|---|---|
| `A_struct` | `{(q, π) : q ∈ Q_valid, π ∈ Π_disc_valid(q)}` | the schema only (§8.11) |
| `A_eff^obs(t)` | `{(q, π) ∈ A_struct : q has no parameter, or some parameter of q has an observed effect at (q, π) on G_I(t)}` | executing queries on snapshot `t` |
| `A_eff^est(t)` | the same test with results replaced by estimates from `GraphStatistics` | statistics only; a heuristic with no guarantee |

`A_eff^obs` is data-dependent: it changes with the snapshot. Computing it means executing `q` at `π` and at each neighbour of `π`, so its cost is that of those executions. The specification does not claim to compute `A_eff^obs` without executing queries; the recommender uses `A_eff^est` to prioritise and treats it as an estimate.

Example — `q = Where s.revenue > θ`, with `D_θ = {100, 200, 300, 400, 500, 600}` and the data `revenue ∈ {100, 600}`:

```
θ = 200:  rows with revenue > 200 → {600};  θ = 100 → {600};  θ = 300 → {600}   no observed effect at θ = 200
θ = 500:  → {600};  θ = 600 → {} (600 > 600 is false)                              observed effect at θ = 500
```

`(q, θ = 200)` is in `A_struct` but not in `A_eff^obs(t)`; `(q, θ = 500)` is in both. Adding a row with revenue 250 moves `θ = 200` into `A_eff^obs` — the dependence on data.

The ratio `|A_eff^obs(t)| / |A_struct|` is the **semantic density** of the model on snapshot `t`.

#### 8.16.5  Semantic basis

Define the **semantic basis** of a skeleton's predicates:

```
Basis(s) = minterms(s) × interesting_params(s)
```

where:

- `minterms(s)` are the minterms (§8.3) over the structural atoms in scope of skeleton `s` — the atoms of the Boolean algebra of its predicates, its minimal non-FALSE, ∨-irreducible elements;
- `interesting_params(s)` are the parameter settings at which some parameter has an observed effect (§8.16.4) for at least one minterm.

For Band 1 questions over a fixed skeleton, every Where predicate is the union of the minterms it contains, so UNION in `Q_algebra` (§8.13) generates every Band 1 predicate of `s` from `minterms(s)`. For Band 2 and Band 3 the basis is not claimed to generate the space. The recommender uses the basis as a starting set, not as the only questions it can reach.

#### 8.16.6  Neighbourhood size under local moves

The recommender moves from a point `(q, π)` by one of a fixed set of local moves. Let `m` be the number of structural atoms in scope of `q`'s skeleton, `L` the total number of hops in its path patterns, `a` its number of aggregates, `P_ord` its parameters with ordered discretised domains and `P_unord` those with unordered finite domains.

```
Move                                              Number of moves at (q, π)
─────────────────────────────────────────────────────────────────────────
add or remove one atom in Where                   ≤ m
replace one GroupBy key                           ≤ |V_P|
replace one hop of one path pattern               ≤ L · 2|E_AB|
replace the AggFn of one aggregate                ≤ a · |AggFn|
move one ordered parameter to an adjacent value   ≤ 2 · |P_ord|
change one unordered parameter                    ≤ Σ_{p ∈ P_unord} (|D_p| − 1)
```

**Proposition.** The neighbourhood generated by these moves has size at most `m + |V_P| + 2L·|E_AB| + a·|AggFn| + 2|P_ord| + Σ_{p∈P_unord} (|D_p| − 1)`. This is polynomial in the size of the schema and of `q`.

*Proof.* Each line counts the results of one kind of move, and the neighbourhood is their union. ∎

**The bound needs finite moves.** If a parameter may move to *any* value of an infinite domain — a threshold over ℝ — its line is infinite and so is the neighbourhood. The proposition holds only with discretised domains and adjacency moves.

**What the bound does not say.** It bounds the size of one step. It does not say that every point of `A_struct` is reachable from a given start by these moves, and it says nothing about how well the recommender ranks the neighbours (§8.12). Reachability and ranking quality are not claimed.

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

Estimate, with every factor and assumption stated:

| Factor | Full space | Assumed effective part | Basis of the assumption |
|---|---:|---:|---|
| `f_structure` | 5! = 120 | 50 | path patterns that satisfy Σ; illustrative, not measured |
| `f_predicate` | 2^10 = 1 024 | 50 | logically satisfiable predicate subsets (§6.1); illustrative |
| `f_parameter` | 20² = 400 | 40 | parameter settings to which the result is sensitive; illustrative |
| `f_operator` | 6 | 6 | all operators kept |

```
|A_full| ≤ 120 × 1 024 × 400 × 6 = 294 912 000 ≈ 2.9 × 10⁸
|A_eff^obs| ≈  50 ×    50 ×  40 × 6 =     600 000 = 6.0 × 10⁵
```

The full-space figure follows from the vocabulary sizes. The effective-space figure depends entirely on the three assumed reductions in the table, which are round numbers chosen to illustrate the order of magnitude; they were not computed from the dataset. The hierarchy used is the four-level one of §8.16.3 (Structure, Predicate, Parameter, Operator). Neighbourhood size under the moves of §8.16.6, with for illustration m = 10 atoms, |V_P| = 15, one path of L = 2 hops over |E_AB| = 5 edge types, a = 1 aggregate with |AggFn| = 6 and three ordered parameters: `10 + 15 + 2·2·5 + 6 + 2·3 = 57`. Every figure in this example is computed from the schema graph and its annotations: an eleventh reader, or ten thousand more, leaves all of them unchanged. An individual reader such as `Ana` is an element of `N_I` with `h_N(Ana) = Leitor`; she contributes to query results and to recommender scores, not to `V_N`.

---

## 9  Implementation Guide

### 9.1  Alignment with sqldim

Each architectural component of this specification, the section that defines it, and the module of the `sqldim` package where code for it was found. The status column is the result of a search of the package on 2026-11-25 for the component's defining names; it records whether a module exists, not whether that module conforms to this version of the specification. Conformance has not been audited.

| Component | Spec | sqldim module | Status |
|---|---|---|---|
| Schema / instance graphs, typing `h` | §2.1 | `core/graph/`, `core/kimball/` | Module present |
| Schema annotations Σ (Grain, SCDType, PipelineArtifact, MedallionLayer, …) | §2.4 | `core/query/dgm/annotations.py` | Module present |
| `Additivity`, `Provenance`, `Degenerate(f, key)` | §2.4 | — | No module found |
| SCD resolution AS_WAS / AS_IS | §2.5 | — | No module found |
| Relational mapping M, version and validity at an instant | §2.6 | — | No module found |
| Paths, BoundPath, VerbHop⁻¹, `Repeat` | §3.1–§3.2 | `core/query/dgm/core.py` | Module present; `Repeat` not found |
| TemporalOrdering (point, Allen) | §3.2 | `core/query/dgm/temporal/_ordering.py` | Module present |
| Strategies and refinement order | §3.3 | `core/query/dgm/core.py` | Module present |
| RelationshipSubgraph, TrimJoin, anchor sets | §3.4 | `core/query/dgm/graph/`, `planner/_core.py` | Module present; anchor sets not found |
| Predicate language: ScalarPred, PathPred, SignaturePred, TemporalMode, `on_empty` | §4.1 | `core/query/dgm/preds/` | Module present |
| Monitoring verdicts (`Monitor`) | §4.1 | — | No module found |
| Expression language, PathAgg, TemporalAgg, GraphExpr, TrailExpr | §4.2 | `core/query/dgm/temporal/_core.py`, `graph/_algs.py` | Module present |
| Bands, drill-across, Band 3 scope | §5 | `core/query/dgm/core.py` | Module present; drill-across not found |
| Q_delta | §5.1 | `core/query/dgm/temporal/` | Module present |
| BDD with three-valued encoding and order theory | §6.1 | `core/query/dgm/bdd.py` | Module present |
| Query planner, rules 1–12 | §6.2 | `core/query/dgm/planner/_core.py` | Module present |
| Execution budget and pre-execution gate | §6.3 | `core/query/dgm/planner/_gate.py` | Module present |
| Query exporter (SQL, Cypher, SPARQL, DGM_JSON/YAML), sinks | §6.4 | `core/query/dgm/exporters.py`, `sinks/` | Module present |
| Recommender | §7 | `core/query/dgm/recommender/` | Module present |
| Question algebra, query DAG, CSE, persistent keys | §8.13–§8.14 | `core/query/dgm/_dag.py`, `_cse.py` | Module present |
| Parameter space Π(q) | §8.15 | `core/query/dgm/parameters.py` | Module present |
| Natural language interface | §11 | `core/query/dgm/nl/` | Module present |
| Supervisor agent | §12 | `core/query/dgm/nl/supervisor/` | Module present |

### 9.2  Implementation roadmap

Each phase names the sections it implements and the result that shows it is done.

**Phase 1 — Schema layer (2–3 days)** · §2.1–§2.6
- Schema graph G_S with K_S and Σ; instance graph as a view through the relational mapping M (§2.6).
- Edge validity windows; reverse access paths I1–I5. Materialised reversed adjacency is optional (D18).
- *Done when:* the SCD2 and validity examples of §2.6 return the tabulated versions at each `as_of`.

**Phase 2 — Query DSL (1 week)** · §3, §5
- PathJoin / VerbHop / VerbHop⁻¹ / BridgeHop / Repeat.
- TemporalJoin, TemporalContext (reference instant, SCD resolution), TrimJoin (incl. REACHABLE_FROM, REACHABLE_TO).
- Full TemporalOrdering (point + Allen); UNTIL/SINCE as TemporalMode.
- PathPred with Quantifier, Strategy, TemporalMode, promote, on_empty.
- SignaturePred; SignatureRef; PathAgg; TemporalAgg; Qualify public; drill-across for constellations.
- *Done when:* Examples 1–7 of §10.1 construct without error and Example 1 returns the drill-across result of §5.2.2.

**Phase 3a — Filter tree typing (3–4 days)** · §4.1–§4.2
- ScalarCond and SExpr with typing and NULL rules; Ref kind enforcement.
- FORALL elimination by CTL duality (§4.1); TemporalMode SQL templates.
- *Done when:* every input of the "Rejected by validation" table of §4.1 is rejected.

**Phase 3b — BDD layer (~1.5 weeks)** · §6.1, §8.3
- Core BDD; DGMPredicateBDD compiler with the p⁺/p⁻ encoding and the order theory Δ_ord.
- Equivalence, logical satisfiability, implication; minterm enumeration.
- Logical optimisation pipeline (8 steps).
- *Done when:* every row of the short-circuit table of §6.1 gives the stated verdict.

**Phase 4 — Schema annotation layer (1 week)** · §2.4, §7.4
- SchemaAnnotation grammar, including Additivity, Provenance and PipelineArtifact transitions.
- Annotation consequences wired into planner, recommender, exporter.
- *Done when:* the AD1–AD4 examples of §2.4 are accepted or rejected as stated.

**Phase 5 — Graph algorithms (ongoing)** · §3.4, §4.2
- NodeExpr: PageRank, centrality, SCC, community, OUTGOING/INCOMING_SIGNATURES, SIGNATURE_DIVERSITY.
- PairExpr: shortest/min-weight, reachability, DISTINCT_SIGNATURES, DOMINANT_SIGNATURE, SIGNATURE_SIMILARITY.
- SubgraphExpr: max-flow, density, diameter, GLOBAL_SIGNATURE_COUNT, GLOBAL_DOMINANT_SIGNATURE, SIGNATURE_ENTROPY.
- RelationshipSubgraph all four endpoint cases; backward cones by reverse access path; cone containment (Rule 9).
- *Done when:* each algorithm returns the result type of the §4.2 table on the BookClub dataset.

**Phase 6 — Recommender (ongoing)** · §7
- Three-layer recommender; annotation-driven (§7.4) and TrailExpr-driven (§7.5) rules; validity separated from suggestion.
- Two-stage trail exploration flow; SIGNATURE_ENTROPY as routing signal.
- Temporal property class suggestions; Q_delta full implementation.
- *Done when:* no suggestion falls outside `Q_valid`, and an explicitly requested valid question is never refused for not being suggested.

**Phase 7 — Planner and exporter (~2 weeks)** · §6.2–§6.4, §8.14
- GraphStatistics with access paths, scc_sizes, temporal_density, causal_acyclic.
- Planning rules 1–12 (Rule 1a cone extension, Rule 3 TrailExpr, Rule 9 cone containment, Rule 10 PipelineArtifact, Rule 11 query DAG CSE and minimisation, Rule 12 drill-across).
- SQL emitter; Iceberg writes through an attached catalog; Cypher, SPARQL, DGM_JSON/YAML exporters.
- ExecutionBudget and pre-execution gate (§6.3): invariants, zones, rewrite before delivery, SAMPLE and CLARIFY branches.
- Persistent `question_key` and `result_key` (§8.14).
- *Done when:* every row of the boundary-case table T1–T15 of §6.3 returns the stated decision.

**Phase 8 — Natural language interface (~3 weeks)** · §11
- Implementation stack: Pydantic-AI (five specialist agents) + LangGraph (state graph with HIL checkpoints).
- DGMRuntime as Pydantic-AI dependency and LangGraph runtime context: schema, entity_registry, query_dag, recommender, gate, executor, budget. Not checkpointed.
- Pydantic output schemas derived from DGM formal types; one versioned OptionSet for confirmation and clarification.
- Five LLM-backed nodes (entity resolution, temporal classification, compositional detection, candidate ranking, explanation rendering) and nine deterministic nodes (candidate_generation, confirmation_loop, apply_refinement, dag_construction, budget_gate, apply_rewrite, enqueue_async, execution, clarification).
- Explicit LangGraph cycles: refinement restarts at entity resolution; clarification returns to confirmation, bounded by MAX_GATE_ROUNDS.
- Checkpointer for multi-turn state persistence across HIL pauses; state holds serialisable data only.
- *Done when:* a turn checkpointed after confirmation resumes in a new process and reaches the same decision.

**Phase 9 — Supervisor agent (scope to be fixed by #82–#87)** · §12
- Tool contracts and registry; supervisor graph with replan and abort routes; budget propagation; partial-failure handling; dataset isolation.
- The supervisor is part of this roadmap, but its contracts are still open in the review (issues #82–#87). Its estimate and completion criterion are set when they close.

---

## 10  Reference

### 10.1  Worked examples

**Example 1 — Constellation path**

```
-- Σ: Conformed(Customer, {Sale, Return})
Join: Anchor(Sale, sale)
      PathJoin(sale, Compose(VerbHop⁻¹(Sale,'placed',Customer,node_alias=customer),
                             VerbHop(Customer,'initiated',Return,node_alias=return_ev)), return_ev)
Where: AND(ScalarPred(PropRef(sale.occurred_on.year),(= 2024)),            -- Sale branch
           ScalarPred(PropRef(return_ev.occurred_on.year),(= 2024)))       -- Return branch
GroupBy: [ customer.region ]                                               -- conformance key
Agg: return_rate := COUNT(return_ev) / COUNT(sale)

-- Two fact branches, {sale} and {return_ev}, meet at the conformed dimension Customer.
-- Evaluated by drill-across (§5.2.2, Rule 12): sales and returns of 2024 are each counted per
-- region, the two results are aligned on region, and the ratio is taken afterwards.
-- A region with sales and no returns has return_rate = 0; with returns and no sales, NULL.
-- A predicate such as return_ev.occurred_on > sale.occurred_on would pair instances of the two
-- facts and is rejected (DA3).
```

**Example 2 — Forward cone: outgoing signature inventory (Stage 1)**

```
Join: Anchor(Customer, c)
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
Join: TrimJoin(Anchor(Sale, s), REACHABLE_TO(s))
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
    TrimJoin(Anchor(Customer, c) PathJoin(c,VerbHop(c,'placed',s),s),
             REACHABLE_FROM(c)),   -- forward cone of c
    REACHABLE_TO(s)                -- backward cone of s
  )
-- Planner Rule 9: REACHABLE_FROM(c) ∩ REACHABLE_TO(s) → REACHABLE_BETWEEN(c,s)
-- Single cone CTE emitted; strictly smaller subgraph
```

**Example 5 — Global signature entropy**

```
Join: Anchor(Customer, c) PathJoin(c,VerbHop(c,'placed',s),s)
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
--                   write_mode=ADAPTIVE, transitions=daily_rev_tr) ∈ Σ
-- Implies: Grain(daily_rev, ACCUMULATING), Grain(daily_rev_tr, EVENT),
--          (daily_rev_tr, daily_rev, 'transition_of'), (daily_rev_tr, daily_rev_tr, 'followed_by') CAUSAL
-- a.state is the current state; each state change is one daily_rev_tr instance

-- Step 1: Backfill — find Missing/Failed within horizon
Q_backfill:
  Anchor(daily_rev, a)
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
  Anchor(daily_rev, a)
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

-- Step 4: correctness assertions

-- 4a. State invariant — a property of the CURRENT state, evaluated at one instant.
--     No window older than the horizon is Missing now:
Anchor(daily_rev, a)
Where: NOT(AND(
         ScalarPred(PropRef(a.state), (= Missing)),
         ScalarPred(PropRef(a.window_end), (< ArithExpr(now, -, 90 days)))
       ))
-- This inspects a.state only. It says nothing about the states the artifact went through.

-- 4b. SAFETY over the trace — a property of the whole transition history.
--     trace(t₀): the 'followed_by' chain starting at the first transition of an artifact.
Anchor(daily_rev_tr, t0)
Where: ScalarPred(PropRef(t0.from_state), (= Missing))                -- first transition of its artifact
-- Below, `trace` abbreviates this BoundPath (positions: t0, then each successor):
--   Repeat(BridgeHop(daily_rev_tr, 'followed_by', daily_rev_tr, node_alias=t), min=0, max=UNBOUNDED)

PathPred(t0, trace, FORALL, ALL, GLOBALLY,
  NOT(ScalarPred(PropRef(t.to_state), (= Failed))))
-- AG(to_state ≠ Failed): the run never enters Failed.

-- The two differ on this recorded sequence for window w:
--   t₁ = (Missing,   scheduled, In-flight)      t₂ = (In-flight, failed,  Failed)
--   t₃ = (Failed,    retried,   In-flight)      t₁ —followed_by→ t₂ —followed_by→ t₃
--   Current state: a.state = In-flight. A check of the current state (4a-style, "state ≠ Failed")
--   passes — the initial state looked safe and so does the present one.
--   4b is FALSE at t₁: position t₂ has to_state = Failed. The trace entered Failed.

-- 4c. LIVENESS over the trace: every run eventually reaches Complete
PathPred(t0, trace, FORALL, ALL, EVENTUALLY,
  ScalarPred(PropRef(t.to_state), (= Complete)))
-- AF(Complete) on the recorded graph. FALSE means "not recorded as Complete", which for a window
-- still In-flight is not a violation; Monitor(LIVENESS(…), trace, deadline=…) (§4.1) tells a
-- pending window from a failed obligation.

-- 4d. RESPONSE: every Stale is eventually followed by Complete
RESPONSE(
  trigger  = ScalarPred(PropRef(t.to_state), (= Stale)),
  response = ScalarPred(PropRef(t.to_state), (= Complete))
)
-- AG(Stale → AF(Complete)) over trace

-- Step 5: Asymmetric failure — refresh vs backfill failure
PathPred(t0, trace, EXISTS, ALL, EVENTUALLY,
  AND(ScalarPred(PropRef(t.kind), (= failed)),
      ScalarPred(PropRef(t.to_state), (= Stale))))
-- EXISTS a failed transition that regressed to Stale → refresh failure (a prior Complete was kept).
-- A backfill failure has to_state = Failed instead (δ, §2.4), so no such position exists.

-- UNTIL over the same trace: the artifact stayed out of Failed until it first completed
PathPred(t0, trace, EXISTS, ALL,
  UNTIL(ScalarPred(PropRef(t.to_state), (= Complete))),
  NOT(ScalarPred(PropRef(t.to_state), (= Failed))))
-- E(to_state ≠ Failed  U  to_state = Complete)
```

**Example 7 — Full pipeline**

```
-- Schema: (Customer, Sale, 'placed') and (Product, Sale, 'included in') are verb edge types
--         (dim → fact); (Product, Store, 'promoted in') is a bridge edge type.
-- Σ: Conformed(Customer,{Sale,Return}), Grain(Sale,EVENT), SCDType(Customer,SCD2),
--    BridgeSemantics((Product,Store,'promoted in'),CAUSAL),
--    WeightConstraint((Product,Store,'promoted in'),ALLOCATIVE)

TemporalContext(default_as_of='2024-06-30', node_resolution=STRICT, edge_resolution=LAX)

Join:
  TrimJoin(
    Anchor(Customer, c)
    PathJoin(c, VerbHop(Customer,'placed',Sale), s)                              -- dim → fact
    PathJoin(s, VerbHop⁻¹(Sale,'included in',Product), d)                        -- fact → dim: reverse hop
    PathJoin(d, BridgeHop(Product,'promoted in',Store,edge_alias=promo), st),
    SINK_FREE
  )
Where:                                                    -- Band 1: PropRefs of C = {c, s, d, st, promo}
  AND(ScalarPred(PropRef(c.segment),(= "retail")),
      ScalarPred(PropRef(s.occurred_on.year),(= 2024)),
      PathPred(c,VerbHop(Customer,'placed',Sale,node_alias=fs),
               FORALL,ALL,EVENTUALLY, ScalarPred(PropRef(fs.revenue),(> 500))))
GroupBy: [ c.id, c.region ]
Agg:                                                      -- Band 2: aggregates over the tuples of each group
  weighted_rev  := PathAgg(d,BridgeHop(Product,'promoted in',Store,edge_alias=p2),
                           ALL,SUM,ArithExpr(PropRef(s.revenue),*,PropRef(p2.weight)))
  rev_30d       := TemporalAgg(SUM,s.revenue,s.occurred_on,ROLLING(30 days))
  out_diversity := GraphExpr(NodeExpr(SIGNATURE_DIVERSITY, c))
Having:
  AND(ScalarPred(AggRef(weighted_rev),(> 5000)),
      ScalarPred(AggRef(rev_30d),(> 1000)))
Window:                                                   -- Band 3: one row per (c.id, c.region);
                                                          -- in scope: c, c.id, c.region and the AggRefs
  scc_label   := GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC), c))
  rank_by_rev := RANK() OVER (PARTITION BY c.region ORDER BY weighted_rev DESC)
Qualify:                                                  -- top two customers of each region
  AND(ScalarPred(WinRef(scc_label),IS_NOT_NULL),
      ScalarPred(WinRef(rank_by_rev),(<= 2)))

-- The aliases s, d, st and promo are consumed by the aggregation and are not in scope in Band 3:
-- ranking by s.revenue there would be rejected (§5.2.3). The ranking uses the aggregate weighted_rev.
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
| Degenerate | fact property | Groupable and filterable; no dimension node, no join |
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

Semantics: maximal paths over a non-total transition relation; FORALL on an empty domain follows `on_empty` (§4.1, D31).

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

**Datalog and fixpoint query languages.** Datalog [13] is the query language whose semantics are least-fixpoint computations over relations; with stratified negation it is a fragment of first-order logic with least fixpoint (FO+LFP), which on ordered structures captures PTIME (Immerman–Vardi [16, 17]). §8.13 shows that a stated fragment `F_D` of DGM's question algebra translates into stratified Datalog with comparisons, and hence has polynomial data complexity, and that a class of path-recursive unary Datalog programs translates back. It does not establish an equivalence, and constructs outside `F_D` — aggregation, simple-path counting, simple paths under multi-hop repetition — are not PTIME-bounded by this argument.

**Ontologies, RDF 1.2, and knowledge representation.** RDF [10], RDFS, and OWL [11] constitute the standard ontological stack, currently evolving toward RDF 1.2 [14]. An RDF triple `(subject, predicate, object)` is structurally identical to DGM's verb edge `(dim, label, fact)` — both are typed directed labeled edges between entities. The `τ_N : N_S → {dim, fact}` kind function is a two-class hierarchy equivalent to `rdf:type` with `rdfs:subClassOf`. SPARQL 1.1 property paths (`+`, `*`, `|`, `^`) cover part of DGM's `BoundPath` traversal, and SPARQL's `FILTER EXISTS` / `FILTER NOT EXISTS` correspond to `PathPred(EXISTS)` / `PathPred(FORALL)`; §6.4 classifies the coverage construct by construct. The shared primitive is what makes SPARQL an export target.

**RDF 1.2 and triple terms.** RDF 1.2 introduces *triple terms* — the ability to use an RDF triple as a term (in the object position of another triple) — together with a reification mechanism via `rdf:reifies`. This narrows a historical gap with DGM: N-ary events, which previously required verbose standard reification or blank-node workarounds, can now be represented more cleanly in RDF. A triple term annotated via `rdf:reifies` is the RDF 1.2 approximation of DGM's fact node — a named resource that participates in multiple relationships simultaneously. However, the gap is narrowed, not closed: DGM's fact node is enforced by a structural invariant (`V ⊆ D×F×L`) that prevents fact nodes from being verb sources and ensures the dim/fact separation at the schema level. RDF 1.2 triple terms carry no enforced type discipline — any triple can be reified and any reifier can be used in any position. The two differ in enforcement: DGM states fact-as-node as a schema invariant checked at construction, while RDF 1.2 leaves it as a modelling convention, which is more flexible and less constrained.

Above the shared graph primitive, DGM and the RDF/OWL stack differ in scope. The six axes below compare capabilities and state the trade-off on each:

**(i) The dim/fact typed split.** OWL has no concept of a measurable event node distinct from a domain entity node. The structural invariant `V ⊆ D×F×L` — which enforces that verb edges connect dimensions to facts and not arbitrary node pairs — has no OWL equivalent. RDF 1.2 triple terms narrow the representation gap for N-ary events but do not enforce the type discipline. KG-OLAP [6] layers context dimensions on top of OWL without schema-level enforcement. Trade-off: the invariant also rules out models RDF accepts, such as an event that is the source of a verb edge.

**(ii) Aggregation and analytics.** OWL is a representation and reasoning language, not a query and analytics language. It has no `GroupBy`, `Agg`, `Having`, `Window`, or `Qualify`. SPARQL 1.1 and SPARQL 1.2 have limited aggregation but no window functions, no temporal aggregation, no path aggregation (`PathAgg`, `TemporalAgg`), and no graph algorithm expressions (`GraphExpr`). SPARQL covers Band 1 and the aggregates, GROUP BY and HAVING of Band 2, but not Band 3 (§6.4). Trade-off: SPARQL queries open-world data across vocabularies, which DGM's bands do not attempt.

**(iii) The schema annotation layer and operational vs assertional semantics.** OWL axioms are logical assertions that drive inference. RDF 1.2 reification (`rdf:reifies`) provides edge-level annotation — the equivalent of DGM's `P(e)` property bags — but these annotations are assertional with no enforced semantics. DGM's `Σ` carries *operational* metadata — `Grain`, `SCDType`, `BridgeSemantics`, `WeightConstraint`, `PipelineArtifact` — that drives execution planning, planner rule selection, and recommender behaviour. RDF annotations state facts that reasoners can use for entailment; DGM annotations select planner and recommender behaviour. Trade-off: `Σ` supports no inference — an annotation has exactly its listed consequences and implies nothing else.

**(iv) Temporal logic over paths.** OWL-Time [12] describes temporal relationships between intervals as ontological assertions. DGM's `TemporalMode` (AG, AF, UNTIL, SINCE, Allen's 13 interval relations in `TemporalOrdering`) is CTL model checking over Kripke structures derived from the graph (§8.1), a different apparatus from OWL inference. OWL-Time supports reasoning about interval relations under description-logic semantics; DGM evaluates temporal properties over recorded paths and infers no new facts.

**(v) Trail space analysis and question completeness.** The `RelationshipSubgraph` constructs, `TrailExpr` algorithms, `SIGNATURE_ENTROPY`, the question completeness theorem (§8.11), and the question algebra `Q_algebra(A, B)` with its translation into stratified Datalog for the fragment `F_D` (§8.13) are not part of the RDF/OWL standards. RDF named graphs are asserted scopes; DGM's `G_AB` is computed from the schema graph (§3.4.5–§3.4.6), and the results of §8.11 hold for it under their hypotheses. Ontologies describe what is true; DGM's question space describes what can be measured and asked of a typed schema.

**(vi) Reification and fact modelling — convergence in v1.2.** RDF 1.2 triple terms and DGM's fact nodes are independently derived solutions to the same problem: how to represent N-ary events as first-class participants in multiple relationships. RDF 1.2 achieves this through syntactic extension (triple terms + `rdf:reifies`); DGM achieves it through a typed schema invariant (`τ_N(f) = fact`, `V ⊆ D×F×L`). The solutions converge on the same representational pattern — a named node participating in multiple directed relationships — while differing in enforcement strictness (none in RDF 1.2; structural invariant in DGM) and operational consequence (none in RDF 1.2; planner rules 1b, 1c, 10 in DGM).

**Convergent research: SPARQL query relaxation.** Active research on SPARQL query relaxation — finding similar queries when exact matches fail — addresses a related problem to DGM's question lattice navigation and recommender architecture, but from different directions. SPARQL relaxation works bottom-up from query syntax via RDFS entailment and instance similarity; DGM works top-down from a formally defined question space `Q_valid(A, B)` using BDD feasibility filtering and trail metric scoring. The challenge is especially significant for statement-level annotations (reification models), where relaxation behaviour varies across standard reification, named graphs, n-ary relations, singleton properties, and RDF-Star. DGM's question algebra and canonical query DAG offer a related mechanism for the DGM-representable fragment; no formal comparison with SPARQL relaxation methods has been made.

The precise positioning: DGM is an **analytical graph model**, not a knowledge representation language. It borrows the graph primitive from RDF and the dimensional structure from Kimball-style data warehousing, adds a query language with CTL temporal predicates, an annotation-driven execution stack, and a question space that is complete under the hypotheses of §8.11. It is closest to KG-OLAP [6] at the intersection of graphs and OLAP. RDF 1.2's triple terms narrow the N-ary event representation gap. DGM is more constrained (a typed invariant instead of a convention) and standardises analytical constructs the RDF stack does not (three-band queries with windows, trail-space analysis); RDF is more general (open-world semantics, inference, cross-vocabulary integration). A DGM graph can be exported to SPARQL as one of its query targets, with the coverage shown in §6.4.

**Medallion architecture.** The Medallion architecture (Bronze/Silver/Gold) is a data design pattern describing pipeline quality progression — how data flows, is refined, and gains quality across layers. DGM and Medallion are complementary: Medallion describes the pipeline; DGM describes the semantic model. DGM sits at the Silver–Gold boundary and proposes a fourth layer above Gold, **Diamond**, as its own extension (D39) — it is not part of the standard pattern:

| Medallion layer | DGM construct | Role |
|---|---|---|
| Bronze | `Provenance(n, record_source, load_timestamp)` + `WritePlan(APPEND)` | Data substrate: raw, append-only, audit trail |
| Silver | `DGM = (G_S, G_I, h)` — τ_N typing + Σ annotation layer on `G_S`, instances in `G_I` | Semantic model: typed nodes, annotated edges, conformed dimensions, SCD2 history |
| Gold | `Q = B₁ ∘ B₂? ∘ B₃?` execution → `ExportPlan` → `WritePlan` | Query delivery: business-ready aggregates materialised to Delta/Iceberg |
| Diamond (DGM extension) | `Q_valid(A, B)` + recommender + NL interface (§11) + `ExecutionBudget` (§6.3) | Governed semantic layer: formal question space, NL access |

Silver is the main intersection: it is where DGM’s typed model `DGM = (G_S, G_I, h)` is constructed — entity matching/merging maps to hub node identity, schema conformation maps to `τ_N` assignment, SCD2 history maps to `SCDType(SCD2)`, relationship capture maps to `V ⊆ D×F×L`, and business rule encoding maps to the `Σ` annotation layer. Gold is DGM query execution materialising through `WritePlan` to DELTA (append only) and ICEBERG sinks (§6.2), alongside or in place of static star-schema views. Diamond, the DGM extension (D39), is the formal semantic layer — `Q_valid(A, B)` with the results of §8.11, `Q_algebra(A, B)` (§8.13), the recommender (§7), and the NL interface with bounded vocabulary and BDD-validated candidates (§11).

Pipeline lifecycle unification: each layer transition is modelled by a `PipelineArtifact` state machine (Bronze ingest `BACKFILL`, Silver construction `REFRESH`, Gold materialisation `ADAPTIVE`). SAFETY, LIVENESS and RESPONSE properties (§4.1) can be stated over the transition traces of these artifacts. What Medallion practice covers and DGM does not model: layer-specific access controls, Bronze schema-on-read flexibility, and CDC streaming ingestion mechanics. What DGM adds on top of the pattern, in its Diamond extension: the typed dim/fact split (`V ⊆ D×F×L`), the three-band query language with temporal predicates, the question space `Q_valid(A, B)` (§8.11), the question algebra with the fragment results of §8.13, and the NL interface with bounded vocabulary and BDD-validated candidates. The `MedallionLayer` annotation (§2.4) tags each node’s layer origin for governance, lineage, and planner quality signalling.

### 10.4  Design decisions

**D1** — G as 6-tuple with Σ (operational decision table, not metadata).
**D2** — VerbHop⁻¹ as query-time operation; V ⊆ D×F×L unchanged.
**D3** — TemporalMode realises CTL, interpreted over maximal paths (D31). The foundation is the established model-checking literature rather than a new formalism: Kripke structures come from the possible-worlds semantics of modal logic, developed in the late 1950s and early 1960s; CTL and its model-checking algorithms were introduced in 1981–1986 (Clarke, Emerson and Sistla [8]), and symbolic BDD-based model checking builds on Bryant [7]. DGM inherits these results; it does not extend their age.
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
**D17** — Free→Free scope restricted to Agg/Window. This is a semantic and operational decision, not a complexity result. A Where filter decides each tuple of C on its own; a Free→Free scope is a property of the whole subgraph, the same for every tuple, so as a row filter it either keeps every row or none and says nothing about the row. Evaluating it also requires traversing the whole scope for every query that filters on it. In Agg and Window it is computed once and broadcast, which is its intended use. No complexity class is claimed to follow from the restriction.
**D18** — G^T is logical: backward traversal (SINCE, ONCE, PREVIOUSLY, REACHABLE_TO, INCOMING_SIGNATURES) reads each edge relation by its target key and needs only the reverse access path of §2.6. Materialising reversed adjacency costs O(|E|) once and is amortised across queries; it is an optimisation for graphs that fit in memory, not a requirement.
**D19** — Cone containment (Rule 9) as structural theorem, not heuristic. Grounded in G_A* ∩ G_*B = G_AB. Always correct; fires whenever both constraints present on same Join branch.
**D20** — PipelineArtifact is syntactic sugar, not a new fact type, and it is declarative: planning produces write and transition plans, and only an authorised runtime executes them (D35). The backfill-incremental state machine is modelled entirely within existing constructs: Grain(ACCUMULATING), BridgeSemantics, WritePlan, Q_delta(CHANGED_PROPERTY), and CTL temporal properties. PipelineArtifact assembles these into one declaration and infers semantics from P(f).state at planning time (planner Rule 10). Follows the same pattern as TemporalProperty classes (D9).
**D21** — Backfill and refresh require different WritePlan modes by construction. Backfill (APPEND) has no prior artifact — failure leaves the window durably in Failed. Refresh (MERGE) preserves the prior Complete via atomic swap — failure regresses to Stale, not Failed. ADAPTIVE mode makes this automatic from P(f).state at planning time.
**D22** — DGM shares the graph primitive with RDF — the typed directed labelled edge — and differs above it: it adds the dim/fact typed split, the three-band analytical query structure, the annotation-driven planner, CTL temporal predicates, trail space analysis and the question space of §8.11. These are not part of the RDF/OWL standards; RDF in turn offers open-world semantics, inference and cross-vocabulary integration that DGM does not attempt.
**D23** — CTE composition extends `Q_valid` to `Q_algebra`. The BDD covers single-query predicate optimisation; cross-CTE CSE (Rule 11) extends it to multi-query scope. The fragment `F_D` translates into stratified Datalog with comparisons and has polynomial data complexity; the rest of `Q_algebra` carries no such guarantee (§8.13).
**D24** — The query DAG canonical form is the composition-level analogue of the BDD. The analogy is exact: variable ordering ↔ dependency partial order; sharing rule ↔ CSE; elimination rule ↔ semiring laws. Containment-based elimination is applied only for Band 1 leaf nodes (NP-complete but tractable in practice). For Band 2/3 nodes, only syntactic laws are applied — this is the conservative choice that preserves correctness at the cost of some missed simplifications. Caching by the canonical DAG's persistent key (§8.14) is the semantic-level generalisation of SQL text caching: it is sound — it never merges inequivalent questions — and incomplete, since some equivalent forms are not identified.
**D25** — The pre-execution gate produces `BudgetDecision` rather than raising errors or blocking. An executable plan is delivered exactly (`PROCEED`, `STREAM`, `ASYNC`, `PAGINATE`). A plan the budget does not admit is reduced only by a substitution the caller allowed (D40), and is otherwise answered with `CLARIFY` — refinements and the not-allowed substitutions, each named — or with `REJECT` when there is nothing to offer. Allowed rewriting precedes the choice of delivery mode; sampling is used or offered only where its error guarantee applies. The clarification path surfaces lattice refinements, not generic "simplify your query" messages — this is only possible because `Q_valid(A, B)` is formally defined.
**D26** — The NL interface does not generate queries from the LLM. It navigates a formally bounded question space. The LLM's five responsibilities are each a constrained mapping task — entity resolution against a closed vocabulary, candidate ranking over pre-validated options, pattern classification to a closed CTL vocabulary, compositional detection, and structured explanation rendering. This architecture prevents the two failure modes of SQL-generation NL interfaces: hallucinated schema references (prevented by closed PropRef vocabulary) and semantically invalid queries (prevented by BDD-validated candidates).
**D27** — LangGraph is chosen for the NL interface workflow and Pydantic-AI for each specialist agent. This pairing is structurally forced, not arbitrary. LangGraph handles the two properties that linear pipelines cannot: stateful cyclic workflows (the refinement and clarification cycles) and human-in-the-loop checkpoints with multi-turn state persistence. Pydantic-AI handles the two properties that unstructured LLM calls cannot: closed-vocabulary output enforcement (DGM formal types as Pydantic schemas) and bounded retry on validation failure. The majority of the LangGraph graph nodes (9 of 14) are deterministic DGM computation — only 5 nodes invoke LLMs, each with a formally bounded output type (§11.10). This ratio is intentional: the more computation that is formal and deterministic, the fewer guarantees depend on LLM reliability.
**D28** — The DGM/RDF relationship is one of convergence on the graph primitive and divergence on everything above it. RDF 1.2's triple terms narrow the N-ary event representation gap (axis vi), making the solutions more structurally similar than in RDF 1.1. DGM's position is that RDF leaves as conventions some constraints that analytical planning needs: DGM adds a type invariant where RDF has a convention, an operational annotation layer where RDF has assertions, window functions that SPARQL 1.1 does not have, and the question space of §8.11. As RDF 1.2 matures, the DGM SPARQL exporter target (§6.4) becomes a more faithful representation — triple terms enable cleaner encoding of fact nodes in SPARQL output. DGM is an analytical graph model; ontologies are knowledge representation languages. The relationship is complementary, not competitive: DGM can export to SPARQL as one execution target. The shared primitive and the divergence above it are stated in D22.

**D29** — DGM sits at the Silver–Gold boundary of the Medallion architecture and proposes Diamond as a fourth layer of its own (D39). Medallion's Bronze layer is the data substrate formally captured by DGM's `Provenance` annotation and `WritePlan(APPEND)`. The Silver layer is where DGM's typed model `DGM = (G_S, G_I, h)` is constructed (entity matching, SCD2 history, `Σ` encoding of business rules). Gold is `Q` execution producing materialised outputs via `ExportPlan`. Diamond is DGM's formal semantic layer — `Q_valid(A, B)` with the results of §8.11, `Q_algebra(A, B)` (§8.13), the recommender, and the NL interface with bounded vocabulary. The `MedallionLayer` annotation tags node and edge origin for governance, lineage, and planner quality signalling — following the same pattern as `PipelineArtifact` (D20): syntactic sugar over existing `Σ` constructs, not a new graph primitive.
**D30** — The parameter space `Π(q)` is structurally separated from the question space `Q_valid(A, B)`. Question space determines *what can be asked*; parameter space determines *how each question can be configured*. The separation preserves the BDD's role: structural validity is checked once per question skeleton; parametric validity is encoded as additional BDD variables over the discrete parameter dimensions (`BDD_full = BDD_structural × BDD_parametric`). The recommender navigates both dimensions independently — structural moves change the BDD; parametric moves change parametric BDD variables — with the same trail-metric scoring and annotation-constraint filtering. The `ExecutionBudget` gate evaluates `cost(q, π)` jointly, enabling parametric rewriting as a cost-reduction mechanism distinct from structural rewriting. The full analytical space is the dependent product `A(A, B) = Σ_{q ∈ Q_valid} Π_valid(q)` — a sigma type whose fibre over each question is the valid parameter space for that question.
**D31** — CTL is interpreted over maximal paths, not over a totalised transition relation. A path ends where the graph ends: a finite trace that reaches a terminal is complete, and EG/AG are decided on it without an implicit self-loop. Self-loops were rejected because they are edges no event produced — visible to hop counts, SHORTEST, signatures and PathAgg — and because they make NEXT at a terminal mean "now". Quantification over an empty domain is governed by `PathPred.on_empty`: FORALL is false by default (`REQUIRE_WITNESS`) and true only when `VACUOUS` is requested, so a universal condition never holds silently for a node with no path instance. Recorded in `docs/adr/0001-ctl-finite-trace-semantics.md`.
**D32** — PipelineArtifact transitions are facts. The artifact carries its current state as a property; every state change is an EVENT-grain transition fact linked to its artifact (`transition_of`) and to the next transition (`followed_by`, CAUSAL). State names are enumeration values, never nodes, and the state machine is a relation δ over those values. The current state is by definition the projection of the latest transition, so snapshot and history agree. Chosen over a mutable property (nothing to traverse) and over artifact versions (pipeline events entangled with attribute history). Recorded in `docs/adr/0002-pipeline-artifact-transition-facts.md`.
**D33** — Every query evaluation has one reference instant τ: `TemporalContext.default_as_of` when supplied, otherwise the planning service's clock captured once before planning and truncated to `REFERENCE_GRANULARITY`. `Now`, calendar and rolling windows, default version and validity resolution, TTL rules and monitor deadlines all resolve against τ; calendar boundaries use `TemporalContext.time_zone`. The query DAG is built with `Now` symbolic, so determinism of the DAG is preserved, and τ enters evaluation and cache identity instead. Dynamic `now` was rejected as incorrect (two expressions of one query could disagree; caches would serve the wrong instant) and a mandatory `as_of` as needing a default in practice. Recorded in `docs/adr/0003-reference-instant.md`.
**D34** — Result caches are invalidated by source watermarks. A cached result is keyed by the watermarks of the source relations its plan can read, so a committed change to any of them changes the key; an entry that depends on a source without a watermark is served only to a caller that accepts staleness explicitly (D41). Fine-grained provenance was not adopted because nothing records it; a TTL alone was rejected as unsound. Recorded in `docs/adr/0004-cache-invalidation-by-source-watermark.md`.
**D35** — PipelineArtifact is declarative. Evaluating annotations and planning queries has no side effect: the planner produces `WritePlan`s and `TransitionPlan`s, and an authorised runtime executes them and appends transition facts when their conditions hold. An active annotation was rejected because evaluating the model could then write; removing lifecycle from DGM was rejected because the planner needs its semantics. Recorded in `docs/adr/0005-pipeline-artifact-is-declarative.md`.
**D36** — The instance graph is a labelled transition system with actions `L × {fwd, rev}`; CTL is evaluated on a Kripke structure derived for each path predicate as the product of the system with the pattern automaton, with the atoms of the predicate as propositions. A pure Kripke encoding was rejected because it pushes edge labels into states; an action-based logic is not needed for the current operators. Recorded in `docs/adr/0006-lts-as-primary-model.md`.
**D37** — Retrieval distance `d_L` is the shortest path between skeletons in the graph of structural local moves (§8.16.6), with parameter differences as a tie-breaker; it is a metric on each connected component and gives a total, reproducible retrieval order. BDD-weighted and composite distances were not adopted. Recorded in `docs/adr/0007-retrieval-distance.md`.
**D38** — The supervisor runs independent steps concurrently up to `MAX_CONCURRENT_CALLS`, reserving each step's budget before dispatch, serialising knowledge-base writes, pausing dispatch for degrade and replan, and synthesising in plan order. Strict sequencing and unbounded parallelism were not adopted. Recorded in `docs/adr/0008-supervisor-bounded-concurrency.md`.
**D39** — Diamond is DGM's own extension of the Bronze/Silver/Gold Medallion pattern, labelled as such wherever it appears. Attributing it to another framework would need a source; removing it would lose a name for the semantic layer. Recorded in `docs/adr/0009-diamond-as-dgm-extension.md`.

**D40** — Every reduction the pre-execution gate can apply — the three rewrite rules, approximate algorithms and sampling — changes the answer, so each is a `Substitution` the caller must allow in `ExecutionBudget.substitutions`; the default allows none. A viable substitution that is not allowed is offered as a `CLARIFY` option and runs only when chosen. The PreComputation guard no longer swaps in an approximation silently. Applying substitutions automatically was rejected because it returned a different answer than the one asked for without the caller's consent. Recorded in `docs/adr/0010-gate-substitutions-are-opt-in.md`.

**D41** — A cached result that depends on a source without a watermark is not served unless the caller sets `CachePolicy.accept_stale`, and then only within that age and flagged `possibly_stale`. This replaces the TTL fallback of ADR 0004. Recorded in `docs/adr/0011-unversioned-cache-entries-are-opt-in.md`.

**D42** — The supervisor and the NL interface fail closed. `SUCCEEDED` means the contract was executed; an empty result is a success only after complete execution; missing capability, configuration or grant is reported as `NOT_IMPLEMENTED`, `UNSUPPORTED` or `UNAUTHORIZED`. The degrade route and `degrade_to` are removed: a REJECTED step is recovered only by a validated replan or ends the run. The NL graph runs only with all fourteen components and never with stub nodes. Recorded in `docs/adr/0012-supervisor-and-nl-fail-closed.md`.

**D43** — Each of the nine supervisor tools has a contract table entry (§12.3): what it executes, what a success holds, when an empty result is legitimate, and its typed failures. Enumeration is bounded by explicit arguments and reports `complete`; a sweep runs exactly the listed values; an invariant check reports `holds` only after complete evaluation. These entries are the conformance target of #129 and #132–#134.

**D44** — Authorization (§12.10) applies to every entry point that touches data — NL interface, query API, result cache and PipelineArtifact executor — not only to the supervisor. A call without an `AuthContext` is refused. Writes and transition appends require the new `WRITE` action.

**D45** — Watermarks are read before execution; a result is cached only if its reads were pinned to those versions or the watermarks are unchanged afterwards. The source of each backend's watermark is tabulated in §8.14, and a source whose watermark is not monotonic is unversioned.

**D46** — A PipelineArtifact plan is executed by a four-step protocol (validate, dispatch, write, record). The write and its terminal transition are atomic, either in one transaction or through a `write_id` that recovery can find. A sink with neither guarantee is unsupported for materialisation; there is no fallback sink. Extends ADR 0005.

**D47** — While the contracts of §11 and §12 are being implemented, the existing heuristic path (keyword planner, template or model-generated SQL, placeholder tools) survives only as an explicit, opt-in demo mode: selected by the caller per run, every result marked `demo = true`, never a contract success and never evidence of conformance. Without the selection, missing components fail closed; demo mode is never a fallback. Recorded in `docs/adr/0013-explicit-demo-mode.md`.

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

[11] McGuinness, D. L., van Harmelen, F. (2004). OWL Web Ontology Language Overview. W3C Recommendation. https://www.w3.org/TR/owl-features/

[12] Cox, S., Little, C. (2022). Time Ontology in OWL (OWL-Time). W3C Recommendation. https://www.w3.org/TR/owl-time/

[13] Ceri, S., Gottlob, G., Tanca, L. (1989). What You Always Wanted to Know About Datalog (And Never Dared to Ask). *IEEE Transactions on Knowledge and Data Engineering*, 1(1), 146–166. https://doi.org/10.1109/69.43410

[14] W3C (2026). RDF 1.2 Concepts and Abstract Data Model. W3C Candidate Recommendation Draft, 16 November 2026. https://www.w3.org/TR/2026/CRD-rdf12-concepts-20261116/ — not a W3C Recommendation; status checked 2026-11-25 at https://www.w3.org/standards/history/rdf12-concepts/ (Candidate Recommendation Snapshot of 7 April 2026, Candidate Recommendation Drafts from 11 November 2026). RDF 1.2 Semantics is likewise a Candidate Recommendation Draft (24 September 2026). Claims in this specification about RDF 1.2 refer to this draft and may change before Recommendation.

[15] Hoeffding, W. (1963). Probability Inequalities for Sums of Bounded Random Variables. *Journal of the American Statistical Association*, 58(301), 13–30. https://doi.org/10.1080/01621459.1963.10500830

[16] Immerman, N. (1986). Relational Queries Computable in Polynomial Time. *Information and Control*, 68(1–3), 86–104.

[17] Vardi, M. Y. (1982). The Complexity of Relational Query Languages. *Proceedings of the 14th ACM Symposium on Theory of Computing (STOC)*, 137–146.

[18] Valiant, L. G. (1979). The Complexity of Enumeration and Reliability Problems. *SIAM Journal on Computing*, 8(3), 410–421.

[19] Mendelzon, A. O., Wood, P. T. (1995). Finding Regular Simple Paths in Graph Databases. *SIAM Journal on Computing*, 24(6), 1235–1258.

[20] Abiteboul, S., Hull, R., Vianu, V. (1995). *Foundations of Databases*. Addison-Wesley. Chapters 12 (Datalog) and 15 (negation and stratification).


### 10.6  Changelog

Version history of this specification. Earlier versions than v0.16 are not recorded here, and no entry exists for v0.18, v0.21, v0.22.

**v0.16** — Endpoint type with Bound/Free variants; four-case RelationshipSubgraph endpoint lattice; forward cone via forward BFS; backward cone via BFS on G^T; full traversal space as SubgraphExpr; containment lattice theorem and cone intersection identity; REACHABLE_FROM/REACHABLE_TO as TrimCriterion; TrailExpr single-relaxation algorithms as NodeAlg; SIGNATURE_DIVERSITY; SIGNATURE_ENTROPY as Shannon entropy; global SubgraphAlg variants; granularity tier shift by endpoint case; Rule 9 cone containment optimisation; G^T at GraphStatistics load time; two-stage trail exploration flow; SIGNATURE_ENTROPY as recommender routing signal.

**v0.17** — `PipelineArtifact` composite annotation for backfill-incremental state machines; five-state artifact lifecycle modelled as `Grain(ACCUMULATING)` with implied `BridgeSemantics`; backfill/refresh asymmetry via `BridgeSemantics(SUPERSESSION)` vs `CAUSAL` on `In-flight → Failed` transition; `ADAPTIVE` write mode inferring `WritePlan` from `P(f).state` at planning time; planner Rule 10 for state-aware write planning and TTL expiry; scheduler loop expressed as composable DGM query pipeline (Q_backfill, Q_refresh, Q_delta monitoring); CTL LIVENESS/SAFETY/RESPONSE as formal scheduler correctness properties over the artifact graph; `UNTIL` operator distinguishing refresh failure (prior Complete preserved) from backfill failure (no prior artifact).

**v0.19** — *(The Datalog-equivalence and PTIME-capture claims of this entry are superseded by the fragment results of §8.13 in v0.27.)* Question algebra `Q_algebra(A, B)` as closure of `Q_valid` under CTE composition; semiring structure `(Q_algebra, UNION, INTERSECT, ∅, Q_top)`; Datalog equivalence theorem for `Q_algebra` under CTE composition; PTIME complexity bound via Immerman-Vardi; fixpoint semantics of recursive CTEs as descent paths in `L(G_AB)`; two-level predicate system (BDD for single-query, containment for cross-query); extended question completeness theorem (v0.19); planner Rule 11 cross-CTE CSE using BDD node id equality; compositional suggestion type (Correlation) in recommender; question chain suggestions as multi-step descent paths; Datalog reference [13] added; D23 design decision on CTE composition complexity.

**v0.20** — Query DAG as canonical form for `Q_algebra(A, B)`; BDD/query DAG parallel (variable ordering ↔ dependency partial order, sharing rule ↔ CSE, elimination rule ↔ semiring laws); canonical form theorem (two composed queries equivalent iff same DAG node id); two-level canonical form (BDD at leaves, query DAG at composition); construction algorithm with elimination + sharing rules; minimum CTE count as tight lower bound (= number of canonical DAG nodes); complexity stratification of containment checking (Band 1: NP-complete; monotone Band 2: polynomial; arbitrary Band 2: undecidable; recursive: Σ₁-hard); cross-session caching via DAG node id as cache key; query explanation via DAG decomposition; canonical-space recommender navigation (neighbourhood over DAG nodes); Rule 11 extended with full semiring elimination laws; D24 design decision on canonical form.

**v0.23** — §10.3 extended to six axes of RDF/DGM comparison; RDF 1.2 triple terms / reification analysis — convergent solutions to N-ary event representation with DGM's typed invariant remaining strictly more constrained; axis (iii) updated to distinguish RDF 1.2 assertional annotations from DGM's operational `Σ`; axis (vi) added on reification and fact modelling convergence; SPARQL query relaxation positioned as convergent research addressed by DGM's question lattice and question algebra; `G^T` as time-reversal of Kripke structure with named graph / multiple accessibility relation note added to §8.1; RDF 1.1 reference [10] updated to RDF 1.1 Concepts 2014; RDF 1.2 reference [14] added; D28 design decision on RDF 1.2 relationship. §11.10 Implementation architecture — LangGraph state graph with five Pydantic-AI specialist agents; DGMContext as Pydantic-AI dependency injection carrier; Pydantic output schemas as executable projection of DGM formal types (EntityResolutionResult, TemporalClassificationResult, CompositionalDetectionResult, CandidateRankingResult, ExplanationRenderResult); TemporalModeEnum / TemporalPropertyEnum / QAlgebraOpEnum as closed-set Pydantic validators; five LLM nodes + six deterministic non-LLM nodes in the graph; explicit refinement and clarification cycles; MemorySaver HIL state persistence; bounded retry for hallucination prevention; parallel DAG leaf execution in STREAM mode; ontology-to-Pydantic formal mapping table; Phase 8 roadmap updated with specific implementation artefacts; D27 design decision. `ExecutionBudget` type with `BudgetDecision` taxonomy (`PROCEED`, `STREAM`, `ASYNC`, `REWRITE`, `SAMPLE`, `PAGINATE`, `CLARIFY`, `REJECT`); pre-execution gate (§6.3) as formal decision procedure between planner and execution; four graceful handling strategies (cost-aware rewriting via strategy lattice, progressive streaming from DAG topology, sampling with Hoeffding error bounds, async execution via `PipelineArtifact` lifecycle); clarification messages grounded in recommender lattice refinements; §11 Natural Language Interface — five-layer architecture; LLM responsibilities narrowed to five tasks (entity resolution, candidate ranking, temporal classification, compositional detection, explanation rendering); `EntityRegistry` as entity/relationship/temporal vocabulary; closed `PropRef` vocabulary for hallucination prevention; BDD-validated candidate generation for LLM ranking; temporal intent classification to CTL operators; compositional intent detection to Q_algebra operations; confirmation/refinement loop as lattice navigation; ambiguity resolution grounded in question space positions; schema evolution handling via Q_delta; sample-parallel async execution pattern; D25 and D26 design decisions. Question completeness theorem (§8.11) — `Q_valid(A, B)` finite and enumerable from `G_AB` and `Σ|_AB`; question lattice `L(G_AB)` with tautological top, contradictory bottom, and three specificity dimensions; formal characterisation of recommender as guided navigator of `L(G_AB)`; semantic scoring theorem — trail metrics induce non-uniform distribution over `L(G_AB)` without enumeration; `BETWEENNESS_CENTRALITY` as GroupBy pivot signal; `DOMINANT_SIGNATURE` as predicate seed; ontology relationship paragraph positioning DGM against RDF/OWL/SPARQL/OWL-Time along five axes; references [10] RDF, [11] OWL, [12] OWL-Time; D22 design decision on DGM/ontology distinction.

**v0.24** — `MedallionLayer` annotation in §2.4 — formal capture of Medallion layer origin with three operational consequences (planner quality warning for BRONZE traversal, recommender BRONZE suppression, exporter minimum-layer tagging in `DGM_JSON` output). §1 Overview updated with Medallion architecture bullet. §9.1 alignment table updated with `MedallionLayer` row; duplicate `PipelineArtifact` row removed. §10.3 extended with Medallion architecture entry — four-layer formal correspondence table (Bronze=`Provenance`+`WritePlan(APPEND)`, Silver=`G` construction, Gold=`Q` execution, Diamond=`Q_valid`+recommender+NL+`ExecutionBudget`), Silver as DGM graph construction boundary, pipeline lifecycle unification via `PipelineArtifact`, genuine gaps (access controls, schema-on-read, CDC streaming) and DGM exclusivities (typed dim/fact split, CTL query language, question completeness, PTIME bound, bounded NL interface). D29 design decision on DGM/Medallion layering.

**v0.25** — Parameter space `Π(q)` formalization (§8.15) — six parameter families (temporal, strategy, filter, aggregation, window, algorithm) as dependent product over `Q_valid(A, B)`; annotation constraint projection onto parameter space; parameter completeness theorem; parameter type stratification (discrete finite / discrete infinite / continuous); finite parametric skeleton corollary; parameter equivalence and effective parameter space; parametric BDD extension (`BDD_full = BDD_structural × BDD_parametric`); full analytical space `A(A, B) = Σ_{q ∈ Q_valid} Π_valid(q)` as sigma type (dependent sum); `ExecutionBudget` cost sensitivity `cost(q, π)` and parametric rewriting; recommender navigation space `Nav(A, B) = L(G_AB) × Π` with structural/parametric/mixed moves; composed analytical space `A_composed`. §7.7 parametric suggestion types (temporal refinement, strategy shift, aggregation alternative, algorithm variant, threshold relaxation) with annotation-driven parametric rules and cost-aware parametric rewriting via `ExecutionBudget`. §10.2 updated with PipelineArtifact and MedallionLayer rows in schema annotations table; parameter families summary table added. D30 design decision on parameter space separation.

**v0.26** — §8.16 Vocabulary decomposition and analytical space characterisation — six vocabularies (`V_N`, `V_E`, `V_P`, `V_Pred`, `V_Op`, `V_Par`); factored size estimate via multiplicative decomposition; four-level hierarchy (see the superseded note at the end of this entry); semantic sparsity and effective analytical space `A_eff`; semantic basis as minimal generating set; polynomial neighbourhood bound; per-level navigation strategies for recommender and NL interface; worked example for BookClub `social_network` domain. *Superseded:* this entry originally described a hierarchy "term → phrase → sentence → paragraph" and gave a 14-element semantic basis and ~2.3 k effective questions; the body of v0.26 uses the Structure / Predicate / Parameter / Operator hierarchy of §8.16.3 and estimates ~6.0 × 10⁵ effective questions (§8.16.8), which are the figures in force. §8.11 cross-reference to §8.16.6–8.16.7 for size characterisation. §7.7 cross-reference to §8.16.5–8.16.6 for formal basis justification. §12 Supervisor Agent Architecture — six agent-safe properties; five-point tool contract (`BaseTool` ABC: `capability_schema`, `cost_estimate`, `execute`, `compose`, `explain`); nine core tools (`dgm_query`, `what_can_i_answer`, `correlate`, `explore_from`, `sensitivity_sweep`, `validate_invariant`, `store_finding`, `retrieve_related`, `map_analytical_space`); `TOOL_REGISTRY` dict; five-node LangGraph supervisor graph (plan → select_tool → execute_tool → synthesize → respond); budget propagation via `budget_fraction` on `ToolCallRequest`; six supervisor workflows (single-domain, cross-domain, hypothesis-driven, exhaustive-survey, validation, meta-analytical); knowledge base with lattice-adjacent retrieval keyed by `(A, B, q, π)`; meta-tool operationalising §8.11 question completeness; relationship to §7 recommender as supplier of `Q_valid`/lattice to supervisor navigation.

**v0.27 (in progress)** — Revision of v0.26 in response to the review tracked in issues #21–#96. Decisions are recorded as D31–D39 (§10.4) and in `docs/adr/0001`–`0009`; each change is linked to its issue in the pull request that made it. The conformance audit tracked in issue #141 adds the fail-closed rules: gate substitutions are opt-in (D40), unversioned cache entries are opt-in (D41), and the supervisor and NL interface fail closed without a degrade route (D42); `docs/adr/0010`–`0012`. It also closes specification gaps found by the audit: per-tool contracts (D43), authorization at every entry point (D44), watermark reading (D45) and the PipelineArtifact execution protocol (D46), and confines the existing heuristic path to an explicit demo mode (D47).

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

The LLM is responsible for five tasks only, each carried out by one agent in one node of the workflow (§11.10). Everything else in the interface is deterministic; the table after the list marks each component. All analytical guarantees are provided by the DGM formalism; the LLM cannot introduce invalid queries, hallucinated properties, or incorrect aggregation semantics because the question space is formally bounded before the LLM acts.

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

| Component | Section | Kind |
|---|---|---|
| Entity resolution | §11.3, Agent 1 | LLM-backed, output validated against the entity registry |
| Temporal intent classification | §11.4, Agent 2 | LLM-backed, closed enumeration |
| Compositional intent detection | §11.4, Agent 3 | LLM-backed, closed enumeration |
| Candidate ranking, including the choice among path signatures | §11.3, §11.5, Agent 4 | LLM-backed, may only reorder |
| Explanation rendering | §11.9, Agent 5 | LLM-backed, generation from structured facts |
| Entity registry construction and lookup | §11.3 | Deterministic |
| Path resolution | §11.3 | Deterministic |
| Band identification | §11.3 | Deterministic — lexicon lookup |
| Candidate generation, BDD validation, DAG construction | §11.5, §11.10 | Deterministic |
| Budget gate, rewrite, enqueue, execution, clarification options | §6.3, §11.9, §11.10 | Deterministic |
| Confirmation | §11.5 | Deterministic — human input |

### 11.3  Semantic grounding layer

**Entity resolution.** Grounded in `P(n)` property names, verb edge labels, and annotation vocabulary — not raw column names. The entity registry is built once from `G` and updated via `Q_delta` when `G` changes.

```
EntityRegistry = {
  prop_terms:    { NL term → PropRef }       -- "revenue"   → PropRef(s.revenue)
  node_terms:    { NL term → alias }         -- "customer"  → "c"            (key: the word; value: the alias)
  alias_types:   { alias → node type }       -- "c"         → Customer       (key: the alias; value: T ∈ N_S)
  verb_terms:    { NL term → VerbHop }       -- "purchased" → VerbHop(Customer,'placed',Sale)
  verb_labels:   { (node type, node type) → list[label] }   -- (Customer, Sale) → ['placed']
  temporal_terms:{ NL term → TemporalWindow } -- "Q3 2024" → PERIOD(...)
  agg_terms:     { NL term → AggFn }     -- "total" → SUM; "average" → AVG
}
```

A PropRef is produced from an alias, never from the NL term: term → alias by `node_terms`, alias → node type by `alias_types`, node type → property keys by `K_S`.

```
"customer"  →  node_terms  →  "c"  →  alias_types  →  Customer  →  K_S(Customer) = {id, region, segment}
            →  PropRefs  c.id, c.region, c.segment

get_valid_prop_refs("Customer") = ["c.id", "c.region", "c.segment"]
An alias absent from alias_types yields no PropRef; a term absent from node_terms is UNRESOLVED.
```

**Relationship resolution.** Trail metrics constrain path selection. When `DISTINCT_SIGNATURES(A, B) = 1`, the path is unambiguous and the LLM does not need to choose. When `SIGNATURE_ENTROPY` is high, the interface asks for clarification before resolving.

```
path_resolution(A: NodeRef, B: NodeRef) → BoundPath | AMBIGUOUS:        -- deterministic
  sigs = DISTINCT_SIGNATURES(A, B)
  if sigs == 1:
    return canonical_path(A, B)          -- unambiguous; determined by G_AB
  return AMBIGUOUS(candidates=top_k_signatures(A, B, k=3))
  -- An ambiguous path yields one query candidate per signature. Choosing among them by closeness
  -- to the utterance is part of candidate ranking (Agent 4), not a separate model call.
```

**Band identification.** A deterministic lexicon lookup maps intent signals to band structure. The lexicon is part of the entity registry and is grounded in DGM vocabulary — not SQL keywords. No model is called.

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

User selection triggers a descent step in `L(G_AB)` — the selected candidate becomes the current question, and the recommender's neighbourhood provides the next set of refinement suggestions.

The options are one ordered, versioned collection. The list rendered to the user is the list consulted when the answer arrives; each option has an id that does not depend on its position.

```
Shown (option_set_version = 2):
  c_9f2e  Total revenue from retail customers year-to-date
  c_41ab  Number of orders from retail customers year-to-date
  c_d07c  Rolling 30-day revenue trend for retail customers

Answer                                                   Outcome
{select, version 2, c_41ab}                              → c_41ab, wherever it sits in the list
{select, version 1, c_41ab}  (an older rendering)        → rejected; the list is shown again
{select, version 2, c_ffff}  (not in the list)           → rejected; the list is shown again
{refine, "wholesale customers this quarter"}             → new utterance; entity resolution runs again
{more}                                                   → same grounding; next page of candidates
```

A refinement replaces the utterance. Everything derived from the previous text — resolved entities, temporal and compositional classification, candidates and their ranking — is discarded and recomputed from entity resolution, so that a change of entity or anchor in the new text is reflected in the candidates. In the example, "wholesale" re-resolves `c.segment = 'wholesale'` and "this quarter" re-resolves the window to QTD before any candidate is generated.

### 11.6  Ambiguity resolution grounded in the question space

Ambiguity is not "the LLM is uncertain." It is "the utterance maps to multiple positions in `Q_valid(A, B)`." The positions are known; the task is to identify which one the user intended.

```
Anchor selection (deterministic, from the entity-resolution result):
  1. Every node alias resolved from the utterance is an anchor; a resolved property contributes
     the node type that owns it. Anchors are distinct (type, role) pairs.
  2. Order: for identity, anchors are sorted by (type name, role); for display, by first mention.
     The order never changes G_𝒜 — an anchor set is a set (§3.4.6).
  3. The set is checked against AS1–AS3. It is not assumed valid.

Anchor ambiguity:  the resolved anchors do not determine one question space
  Disconnected anchor (AS1 fails)        → ask which relationship was meant; the anchor is never dropped
  Role not stated ("by date", two Date roles; AS2)
                                         → one candidate per role
  Several fact types connect the anchors (Sale and Return both link Customer, Product, Date)
                                         → one candidate per connecting fact, plus the drill-across
                                           candidate when the facts are conformed on all anchors
  More than MAX_ANCHORS anchors (AS3)    → ask to narrow; no candidate is generated

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

Saved questions are stored by `question_key` (§8.14) together with their canonical serialisation. When the schema changes, `schema_version` changes and the stored key no longer matches; the interface re-derives the DAG from the stored serialisation against the new schema. If derivation fails, the question is invalidated. If derivation succeeds but produces a different DAG, the user is notified of the semantic change.

### 11.8  Hallucination prevention: what is guaranteed and what is not

The mechanisms below constrain *what the model can produce*. They guarantee that every query the interface executes refers only to declared schema elements and is structurally valid. They do not guarantee that it is the query the user meant.

**Guaranteed — closed vocabulary.** Entity resolution may only return `PropRef`s that `get_valid_prop_refs` lists for an alias with a node type (§11.3), i.e. elements of `{dom K_S(T) | T ∈ N_S}`. Agent output is validated against the entity registry in the runtime context (an output validator reading `RunContext`); an output naming anything else fails validation and is retried, and after the retry budget the node fails. A term with no match is returned as `UNRESOLVED`, which leads to a question, not a guess.

**Guaranteed — structural validity.** Candidates are generated by the recommender from `Q_valid` and pass the BDD feasibility filter (§7). The ranking agent may only reorder them; its output is checked to be a permutation of the ids it was given (§11.10). Every executed query is therefore a member of `Q_valid`: well-typed, consistent with Σ, and not provably empty.

**Not guaranteed — semantic correctness of a valid mapping.** A term can be mapped to a property that exists but is not the one meant: "revenue" to `s.gross_amount` when the user meant `s.net_amount`, or "customers" to the wrong role of a role-playing dimension. The output is in the vocabulary and the query is valid, so neither mechanism above detects it. This is the residual risk of the interface.

**Controls for the residual risk.** Two controls address it, neither of them formal:

- *Confirmation.* No candidate is executed before the user selects it from options rendered in plain language (§11.5); a misreading shows up as a description the user does not recognise.
- *Refinement and clarification.* The user can rephrase, which restarts resolution (§11.5), and ambiguous terms are surfaced as explicit choices (§11.6).

**Equivalent questions.** Syntactically different queries that reduce to the same canonical DAG share a `question_key` (§8.14), so they are treated as one question and share a cached result; equivalent forms the normalisation does not identify are treated as different questions. This removes duplicated work; it does not make an incorrect mapping correct.

### 11.9  Integration with the execution budget gate

The NL interface feeds directly into the pre-execution gate (§6.3). The interface passes the user's `ExecutionBudget` unchanged, including `substitutions`; it never widens that set. When the gate returns `CLARIFY`, the options are lattice refinements generated by the recommender, plus any substitution the user has not allowed, each with its warning — not generic "try a simpler query" messages. The NL interface renders them in natural language using the entity registry. A substitution runs only after the user picks it.

When the gate returns `ASYNC`, the interface first submits the job (`enqueue_async`, §11.10) and acknowledges only what the executor accepted. The reference given to the user is the job id of the returned ticket:

```
Job accepted:
"This analysis will run in the background.
 It is queued (job: j_7f3a2b, status: QUEUED).
 You'll be notified at the callback when it's ready."

Submission failed:
"I could not queue this analysis: <reason>. Nothing is running.
 You can retry, or narrow the question."
```

No acknowledgement mentions a queued job or a materialised result without a ticket. Submission is idempotent on the `result_key` (§8.14): re-submitting the same question over the same snapshot returns the existing job.

A preview may accompany the acknowledgement only when `SAMPLE ∈ substitutions` and the plan satisfies the sampling conditions S1–S4 of §6.3; it is then produced by Strategy 3 and carries the error bound defined there. For any other plan no approximate figure is shown.

When the gate returns `REWRITE`, the substitution was allowed by the user's budget. The interface runs the rewritten plan under the delivery the gate attached to it and shows the gate's warning; it does not rebuild the DAG or consult the gate again for that plan.

### 11.10  Implementation: LangGraph + Pydantic-AI

The NL interface is implemented as a LangGraph state graph of specialist Pydantic-AI agents. This pairing is structurally forced by two properties of the DGM NL interface: the workflow is stateful and cyclic (LangGraph), and the output vocabulary is formally bounded (Pydantic-AI).

**Why Pydantic-AI for each specialist agent.**

DGM's formal types are the Pydantic output schemas. Every agent output must be drawn from a closed set validated against the live graph. An agent that produces a `PropRef` outside the declared properties fails validation and is asked again — the output vocabulary is constrained at generation, not filtered afterwards.

**Reference versions.** The code below is written against Pydantic-AI 2.x (documentation as of v2.60.0, 2026-10-22; Output API: https://pydantic.dev/docs/ai/core-concepts/output/) and LangGraph 1.x (https://docs.langchain.com/oss/python/langgraph/graph-api). It is a specification sketch, not a module. The schema and agent blocks were executed once against `pydantic-ai-slim` 1.71.0 with Pydantic-AI's test model (construction of the five agents, the `get_valid_prop_refs` tool, an awaited `agent.run` and its `.output`); the names used exist from that version through the 2.x documentation. The graph block was checked for syntax only and was not run against LangGraph. The names it relies on:

| Library | Name used here | Note |
|---|---|---|
| Pydantic-AI | `Agent(..., output_type=...)`, `result.output` | `result_type` and `.data` were removed in v0.6.0 |
| Pydantic-AI | model id `'<provider>:<model>'` | a bare model name raises `UserError` from v2.0.0 |
| Pydantic-AI | `await agent.run(...)` | `run_sync` drives an event loop itself and is not called from async nodes |
| LangGraph | `StateGraph(State, context_schema=...)`, `Runtime[Context]`, `invoke/ainvoke(..., context=...)` | runtime dependencies stay out of the checkpointed state |
| LangGraph | `InMemorySaver`, `interrupt()`, `Command(resume=...)` | a checkpointer and a `thread_id` are required for interrupts |

**Identifiers.** Three kinds of identity appear, and they are different types:

```python
from typing import NewType

CandidateId = NewType("CandidateId", str)   # stable id of a proposed question, assigned before any DAG exists:
                                            # the hash of the candidate's canonical spec
QuestionKey = NewType("QuestionKey", str)   # persistent identity of a constructed question (§8.14)
DagNodeId   = NewType("DagNodeId", int)     # handle local to one QueryDAG manager; never stored in state
```

A candidate has a `CandidateId` from the moment it is generated. A `QuestionKey` exists only after `dag_construction`. A `DagNodeId` is obtained from the live manager inside a node and discarded when the node returns.

**Runtime context and state.** Live objects — graph handles, BDD and DAG managers, connections — are runtime context. The checkpointed state holds only serialisable data.

```python
from dataclasses import dataclass
from enum import Enum
from pydantic import BaseModel, Field, model_validator
from pydantic_ai import Agent, RunContext

MODEL = "anthropic:claude-sonnet-5-5"       # '<provider>:<model>'; configured per deployment
AGENT_TIMEOUT_S   = 30
MAX_GATE_ROUNDS   = 5                       # budget-gate evaluations per conversation turn
MAX_INVALID_PICKS = 3                       # consecutive invalid selections before giving up

@dataclass
class DGMRuntime:                           # NOT checkpointed; supplied on every invoke / resume
    schema:          object                 # G_S, Σ and mapping M
    entity_registry: object                 # EntityRegistry (§11.3)
    query_dag:       object                 # QueryDAG manager (local DagNodeIds)
    recommender:     object
    gate:            object                 # gate(plan, budget) of §6.3
    executor:        object                 # runs and enqueues plans
    budget:          object                 # ExecutionBudget

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
    prop:  str

class EntityResolutionResult(BaseModel):
    utterance_version: int
    resolved:     list[PropRefModel]
    unresolved:   list[str]                 # terms with no match: asked back to the user, never guessed
    node_aliases: dict[str, str]            # NL term → alias, e.g. "customer" → "c"

class TemporalClassificationResult(BaseModel):
    utterance_version: int
    mode:        TemporalModeEnum | None = None
    property_:   TemporalPropertyEnum | None = None
    window_type: str | None = None          # ROLLING|TRAILING|PERIOD|YTD|QTD|MTD|LATEST|EARLIEST
    confidence:  float

class CompositionalDetectionResult(BaseModel):
    utterance_version: int
    operation:  QAlgebraOpEnum | None = None
    join_key:   PropRefModel | None = None
    confidence: float

class QueryCandidate(BaseModel):            # exists BEFORE any DAG node: no dag id here
    candidate_id:  CandidateId
    spec:          dict                     # canonical, serialisable description of the question
    description:   str
    band_coverage: list[str]
    cost_estimate: float

class OptionSet(BaseModel):                 # the one collection shown to the user AND read back
    version: int
    kind:    str                            # "candidates" | "clarifications"
    items:   list[QueryCandidate]           # in display order

    def get(self, candidate_id: str) -> QueryCandidate | None:
        return next((c for c in self.items if c.candidate_id == candidate_id), None)

class CandidateRankingResult(BaseModel):
    order:      list[CandidateId]           # a permutation of the ids it was given
    confidence: list[float]

class Selection(BaseModel):                 # what the user sends back
    action:             str                 # "select" | "refine" | "more"
    option_set_version: int | None = None   # required for "select"
    candidate_id:       CandidateId | None = None
    text:               str | None = None   # required for "refine"

class PlanRef(BaseModel):                   # serialisable reference to a plan; the plan itself is rebuilt
    question_key:     QuestionKey           # from (question_key, applied_rewrites) by the runtime
    plan_version:     int = 0
    applied_rewrites: list[str] = []        # rewrite rule names, in order

class BudgetDecisionModel(BaseModel):
    type:     str                           # PROCEED|STREAM|ASYNC|REWRITE|SAMPLE|PAGINATE|CLARIFY|REJECT
    delivery: str | None = None             # `then` of REWRITE / SAMPLE: PROCEED|STREAM|ASYNC|PAGINATE
    rewrite_rule: str | None = None         # the Substitution of a REWRITE
    warning:  str | None = None
    reason:   str | None = None
    sink:     str | None = None
    callback: str | None = None
    ttl_s:    int | None = None

class JobTicket(BaseModel):
    job_id: str
    status: str                             # initial status reported by the executor, e.g. "QUEUED"

class ExplanationRenderResult(BaseModel):
    explanation:           str
    follow_up_suggestions: list[str]

class NLInterfaceState(BaseModel):          # every field is serialisable
    utterance:         str
    utterance_version: int = 0
    entity_result:     EntityResolutionResult | None = None
    temporal_result:   TemporalClassificationResult | None = None
    compositional:     CompositionalDetectionResult | None = None
    options:           OptionSet | None = None
    option_page:       int = 0
    selection:         Selection | None = None
    selection_error:   str | None = None
    invalid_picks:     int = 0
    selected:          QueryCandidate | None = None
    plan:              PlanRef | None = None
    gate_rounds:       int = 0
    budget_decision:   BudgetDecisionModel | None = None
    job:               JobTicket | None = None
    submission_error:  str | None = None
    final_result:      dict | None = None
    explanation:       str | None = None
```

**The five specialist agents.** Each agent takes `DGMRuntime` as its dependency type and returns a typed output.

```python
# Agent 1 — Entity Resolution
entity_agent = Agent(
    MODEL,
    deps_type=DGMRuntime,
    output_type=EntityResolutionResult,
    instructions=(
        "Map natural language terms to DGM PropRef values. "
        "Use get_valid_prop_refs to check what exists. "
        "Only return PropRef values the tool confirms. "
        "Mark anything not found as unresolved — do not guess."
    ),
)

@entity_agent.tool
def get_valid_prop_refs(ctx: RunContext[DGMRuntime], node_type: str) -> list[str]:
    """Returns every valid 'alias.prop' for aliases bound to the given node type."""
    registry = ctx.deps.entity_registry
    schema   = ctx.deps.schema
    return [
        f"{alias}.{prop}"
        for alias, alias_type in registry.alias_types.items()    # alias → node type
        if alias_type == node_type
        for prop in schema.property_keys(alias_type)             # dom K_S(alias_type)
    ]

@entity_agent.tool
def get_verb_labels(ctx: RunContext[DGMRuntime], from_type: str, to_type: str) -> list[str]:
    """Returns verb labels connecting two node types."""
    return ctx.deps.entity_registry.verb_labels.get((from_type, to_type), [])

# Agent 2 — Temporal Classifier
temporal_agent = Agent(
    MODEL, deps_type=DGMRuntime, output_type=TemporalClassificationResult,
    instructions=(
        "Classify temporal intent. Map to exactly one of: "
        "TemporalMode enum, TemporalProperty enum, or TemporalWindow type string. "
        "If no temporal intent, return all None with confidence=0."
    ),
)

# Agent 3 — Compositional Detector
compositional_agent = Agent(
    MODEL, deps_type=DGMRuntime, output_type=CompositionalDetectionResult,
    instructions=(
        "Detect multi-question composition. "
        "UNION='or'; INTERSECT='and both'; JOIN='compared to/alongside'; "
        "CHAIN='then/among those'. Return None for atomic questions."
    ),
)

# Agent 4 — Candidate Ranking
ranking_agent = Agent(
    MODEL, deps_type=DGMRuntime, output_type=CandidateRankingResult,
    instructions=(
        "Order the provided candidate ids by linguistic proximity to the utterance. "
        "Return every id exactly once. Do not add or remove ids. "
        "Assign confidence scores in [0,1]."
    ),
)

# Agent 5 — Explanation Renderer
explanation_agent = Agent(
    MODEL, deps_type=DGMRuntime, output_type=ExplanationRenderResult,
    instructions=(
        "Render the supplied query decomposition as natural language (2-4 sentences). "
        "State only what the supplied facts say: do not claim that a job was queued or "
        "a result materialised unless a job ticket is present."
    ),
)
```

**The LangGraph state graph.** Every node is `async def`. Agent calls are awaited under a timeout; blocking DGM work runs in a worker thread.

```python
import asyncio
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.runtime import Runtime
from langgraph.types import interrupt, Command

Ctx = Runtime[DGMRuntime]

async def run_agent(agent, prompt: str, ctx: DGMRuntime):
    result = await asyncio.wait_for(agent.run(prompt, deps=ctx), AGENT_TIMEOUT_S)
    return result.output

# ---- LLM-backed nodes (5) ----

async def entity_resolution_node(s: NLInterfaceState, rt: Ctx) -> dict:
    out = await run_agent(entity_agent, s.utterance, rt.context)
    return {"entity_result": out.model_copy(update={"utterance_version": s.utterance_version})}

async def temporal_classification_node(s: NLInterfaceState, rt: Ctx) -> dict:
    out = await run_agent(temporal_agent, s.utterance, rt.context)
    return {"temporal_result": out.model_copy(update={"utterance_version": s.utterance_version})}

async def compositional_detection_node(s: NLInterfaceState, rt: Ctx) -> dict:
    out = await run_agent(compositional_agent, s.utterance, rt.context)
    return {"compositional": out.model_copy(update={"utterance_version": s.utterance_version})}

async def candidate_ranking_node(s: NLInterfaceState, rt: Ctx) -> dict:
    shown  = {c.candidate_id: c.description for c in s.options.items}
    out    = await run_agent(ranking_agent, f"Utterance: {s.utterance}\nCandidates: {shown}", rt.context)
    by_id  = {c.candidate_id: c for c in s.options.items}
    if sorted(out.order) != sorted(by_id):                 # not a permutation: keep the recommender's order
        return {}
    return {"options": OptionSet(version=s.options.version + 1, kind=s.options.kind,
                                 items=[by_id[i] for i in out.order])}

async def explanation_rendering_node(s: NLInterfaceState, rt: Ctx) -> dict:
    facts = {
        "decision":         s.budget_decision.model_dump() if s.budget_decision else None,
        "job":              s.job.model_dump() if s.job else None,          # present only if accepted
        "submission_error": s.submission_error,
        "decomposition":    rt.context.query_dag.decompose_by_key(s.plan.question_key) if s.plan else None,
        "has_result":       s.final_result is not None,
    }
    out = await run_agent(explanation_agent, str(facts), rt.context)
    return {"explanation": out.explanation}

# ---- Deterministic nodes (9) ----

async def candidate_generation_node(s: NLInterfaceState, rt: Ctx) -> dict:
    v = s.utterance_version                                 # refuse results of an older utterance
    assert s.entity_result.utterance_version == v
    assert s.temporal_result.utterance_version == v and s.compositional.utterance_version == v
    items = rt.context.recommender.top_k_candidates(
        entity_result=s.entity_result, temporal_result=s.temporal_result,
        compositional=s.compositional, k=5, page=s.option_page)
    version = (s.options.version + 1) if s.options else 1
    return {"options": OptionSet(version=version, kind="candidates", items=items),
            "selection": None, "selection_error": None}

async def confirmation_loop_node(s: NLInterfaceState, rt: Ctx) -> dict:
    answer = interrupt({                                    # payload is JSON-serialisable
        "type": s.options.kind,
        "option_set_version": s.options.version,
        "options": [{"candidate_id": c.candidate_id, "description": c.description,
                     "cost_estimate": c.cost_estimate} for c in s.options.items],
        "error": s.selection_error,
        "prompt": "Pick one by candidate_id, send new text to refine, or ask for more.",
    })
    sel = Selection.model_validate(answer)
    if sel.action == "select":
        picked = s.options.get(sel.candidate_id) if sel.option_set_version == s.options.version else None
        if picked is None:                                  # stale version or unknown id: do not advance
            return {"selection": sel, "selected": None, "invalid_picks": s.invalid_picks + 1,
                    "selection_error": "That option is not in the list currently shown."}
        return {"selection": sel, "selected": picked, "selection_error": None, "invalid_picks": 0}
    return {"selection": sel, "selected": None, "selection_error": None, "invalid_picks": 0}

async def apply_refinement_node(s: NLInterfaceState, rt: Ctx) -> dict:
    if s.selection.action == "more":                        # same utterance: next page of candidates
        return {"option_page": s.option_page + 1}
    return {"utterance": s.selection.text,                  # new text: everything derived from the old
            "utterance_version": s.utterance_version + 1,   # utterance is invalidated
            "entity_result": None, "temporal_result": None, "compositional": None,
            "options": None, "option_page": 0, "selected": None, "plan": None}

async def dag_construction_node(s: NLInterfaceState, rt: Ctx) -> dict:
    dag = rt.context.query_dag
    node: DagNodeId = dag.make_from_spec(s.selected.spec)   # the DAG node is created HERE
    return {"plan": PlanRef(question_key=dag.question_key(node))}   # only the persistent key is stored

async def budget_gate_node(s: NLInterfaceState, rt: Ctx) -> dict:
    if s.gate_rounds >= MAX_GATE_ROUNDS:
        return {"budget_decision": BudgetDecisionModel(
            type="REJECT", reason="No executable form found after the allowed number of attempts.")}
    plan = rt.context.query_dag.plan_for(s.plan)            # rebuilt from the PlanRef
    d    = rt.context.gate(plan, rt.context.budget)         # §6.3: at most one REWRITE, already delivered
    return {"gate_rounds": s.gate_rounds + 1,
            "budget_decision": BudgetDecisionModel(
                type=d.type, delivery=getattr(d, "then_type", None),
                rewrite_rule=getattr(d, "rewrite_rule", None), warning=getattr(d, "warning", None),
                reason=getattr(d, "reason", None), sink=getattr(d, "sink", None),
                callback=getattr(d, "callback", None), ttl_s=getattr(d, "ttl_s", None))}

async def apply_rewrite_node(s: NLInterfaceState, rt: Ctx) -> dict:
    rule = s.budget_decision.rewrite_rule
    assert rule not in s.plan.applied_rewrites              # the same transformation is never reapplied
    return {"plan": s.plan.model_copy(update={
                "plan_version": s.plan.plan_version + 1,
                "applied_rewrites": s.plan.applied_rewrites + [rule]})}
    # The rewritten plan is NOT gated again: its delivery is budget_decision.delivery (§6.3).

async def enqueue_async_node(s: NLInterfaceState, rt: Ctx) -> dict:
    d = s.budget_decision
    try:
        ticket = await rt.context.executor.submit(          # idempotent on the result key (§8.14)
            plan=rt.context.query_dag.plan_for(s.plan),
            sink=d.sink, callback=d.callback, ttl_s=d.ttl_s,
            idempotency_key=rt.context.query_dag.result_key(s.plan))
    except Exception as e:                                  # submission failure is not an accepted job
        return {"job": None, "submission_error": str(e)}
    return {"job": JobTicket(job_id=ticket.job_id, status=ticket.status), "submission_error": None}

async def execution_node(s: NLInterfaceState, rt: Ctx) -> dict:
    d    = s.budget_decision
    mode = d.delivery if d.type in ("REWRITE", "SAMPLE") else d.type
    plan = rt.context.query_dag.plan_for(s.plan)
    result = await asyncio.to_thread(rt.context.executor.run, plan, mode)   # blocking work off the loop
    return {"final_result": result}

async def clarification_node(s: NLInterfaceState, rt: Ctx) -> dict:
    items = rt.context.recommender.clarification_options(s.plan, budget=rt.context.budget)
    return {"options": OptionSet(version=s.options.version + 1, kind="clarifications", items=items),
            "selection": None, "selection_error": None}

# ---- Routing ----

def route_after_confirmation(s: NLInterfaceState) -> str:
    if s.selection.action in ("refine", "more"):  return "revise"
    if s.selected is not None:                    return "confirmed"
    return "give_up" if s.invalid_picks >= MAX_INVALID_PICKS else "invalid"

def route_after_revision(s: NLInterfaceState) -> str:
    return "regenerate" if s.entity_result is not None else "reresolve"

def delivery_route(kind: str) -> str:
    return "enqueue" if kind == "ASYNC" else "execute"      # PROCEED, STREAM, PAGINATE run now

def route_budget(s: NLInterfaceState) -> str:
    t = s.budget_decision.type
    if t == "REWRITE":             return "rewrite"
    if t == "CLARIFY":             return "clarify"
    if t == "REJECT":              return "explain"
    if t == "SAMPLE":              return delivery_route(s.budget_decision.delivery)
    return delivery_route(t)

def route_after_rewrite(s: NLInterfaceState) -> str:
    return delivery_route(s.budget_decision.delivery)

# ---- Assembly ----

graph = StateGraph(NLInterfaceState, context_schema=DGMRuntime)

for name, fn in [
    ("entity_resolution", entity_resolution_node), ("temporal_classification", temporal_classification_node),
    ("compositional_detection", compositional_detection_node), ("candidate_ranking", candidate_ranking_node),
    ("explanation_rendering", explanation_rendering_node),
    ("candidate_generation", candidate_generation_node), ("confirmation_loop", confirmation_loop_node),
    ("apply_refinement", apply_refinement_node), ("dag_construction", dag_construction_node),
    ("budget_gate", budget_gate_node), ("apply_rewrite", apply_rewrite_node),
    ("enqueue_async", enqueue_async_node), ("execution", execution_node),
    ("clarification", clarification_node),
]:
    graph.add_node(name, fn)

graph.set_entry_point("entity_resolution")
graph.add_edge("entity_resolution",       "temporal_classification")
graph.add_edge("temporal_classification", "compositional_detection")
graph.add_edge("compositional_detection", "candidate_generation")
graph.add_edge("candidate_generation",    "candidate_ranking")
graph.add_edge("candidate_ranking",       "confirmation_loop")

graph.add_conditional_edges("confirmation_loop", route_after_confirmation,
    {"confirmed": "dag_construction",
     "revise":    "apply_refinement",
     "invalid":   "confirmation_loop",        # re-ask with the error; the workflow does not advance
     "give_up":   "explanation_rendering"})

graph.add_conditional_edges("apply_refinement", route_after_revision,
    {"reresolve":  "entity_resolution",       # new text: entity resolution runs again
     "regenerate": "candidate_generation"})   # "more": same grounding, next page

graph.add_edge("dag_construction", "budget_gate")

graph.add_conditional_edges("budget_gate", route_budget,
    {"execute": "execution", "enqueue": "enqueue_async",
     "rewrite": "apply_rewrite", "clarify": "clarification", "explain": "explanation_rendering"})

graph.add_conditional_edges("apply_rewrite", route_after_rewrite,
    {"execute": "execution", "enqueue": "enqueue_async"})   # never back to dag_construction or the gate

graph.add_edge("clarification",         "confirmation_loop")
graph.add_edge("enqueue_async",         "explanation_rendering")
graph.add_edge("execution",             "explanation_rendering")
graph.add_edge("explanation_rendering", END)

nl_app = graph.compile(checkpointer=InMemorySaver())

# One turn. The runtime context is passed on every call, including a resume:
#   config = {"configurable": {"thread_id": "t-1"}}
#   await nl_app.ainvoke({"utterance": "..."}, config=config, context=dgm_runtime)
#   await nl_app.ainvoke(Command(resume={"action": "select", "option_set_version": 2,
#                                        "candidate_id": "c_9f2e"}), config=config, context=dgm_runtime)
```

**Key structural properties:**

*Five LLM-backed nodes, nine deterministic nodes.* `entity_resolution`, `temporal_classification`, `compositional_detection`, `candidate_ranking` and `explanation_rendering` each make exactly one agent call. The other nine nodes call no model. The correspondence with §11.2 is one task, one agent, one node.

*Sync and async contract.* Every node is a coroutine. An agent is awaited with `agent.run` under `AGENT_TIMEOUT_S`; `run_sync` is never called inside a node, since it would drive an event loop from within one. Blocking DGM work (`executor.run`) is moved off the loop with `asyncio.to_thread`. A timeout raises in the node and fails the step; cancelling the graph task cancels the awaited agent call. Work already handed to a thread cannot be cancelled by the event loop, so `executor.run` must honour its own cancellation signal (for DuckDB, interrupting the connection).

*State is serialisable; context is not checkpointed.* `NLInterfaceState` contains only models, strings, numbers and lists. `DGMRuntime` is supplied through `context=` on every invocation and resume. After a resume in a new process the DAG manager is new: nodes rebuild what they need from `PlanRef` — the persistent `question_key` plus the ordered list of applied rewrites — and never from a local node id.

*One option set.* What is shown and what is read back is the same `OptionSet`. A selection names a `candidate_id` and the `option_set_version` it was shown under; reordering by the ranking agent creates a new version, so an index or an id from an older rendering cannot land on a different candidate. An unknown id or a stale version is rejected, the question is asked again, and after `MAX_INVALID_PICKS` the turn ends with an explanation.

*Refinement restarts at entity resolution.* New text increments `utterance_version` and clears every result derived from the previous utterance; `candidate_generation` refuses inputs whose version does not match. Asking for more options keeps the grounding and advances the page.

*Fail closed (D42; ADR 0012).* The graph is built only with all fourteen components: the five agents and the nine deterministic nodes above. A component that cannot be constructed — a missing model, agent, schema, checkpointer or backend — is a startup error naming the component. No node is replaced by a passthrough or stub, and the topology is never reduced to run without it. At run time, an agent that fails validation after its bounded retries ends the turn with a typed error in `explanation_rendering`; it does not route around the agent or rewrite the question by heuristic. An `ASYNC` decision is always enqueued; a failed submission is reported (§11.9) and never converted into synchronous execution.

*Rewrites make progress and terminate.* The gate returns at most one `REWRITE`, already carrying the delivery of the rewritten plan (§6.3). `apply_rewrite` records the rule in the `PlanRef`, increments `plan_version` and routes straight to delivery; the plan is not gated again and `dag_construction` is not revisited. A plan with no viable rewrite never yields `REWRITE`. Clarification can bring the conversation back to the gate; `MAX_GATE_ROUNDS` bounds that, after which the decision is `REJECT`.

*ASYNC means a submitted job.* The ASYNC route passes through `enqueue_async`, which submits the plan with the sink, callback and TTL of the decision and stores the returned `JobTicket`. If submission fails there is no ticket and `submission_error` is set; the explanation agent is given both and is instructed not to claim a queued job without a ticket.

*Bounded retry via Pydantic-AI.* If an agent produces output that fails validation, Pydantic-AI asks the model again up to its configured output-retry budget; exhausting it raises, and the node fails rather than returning unvalidated output.

**The ontology-to-Pydantic mapping (formal relationship):**

```
DGM formal constraint                  Enforcement
──────────────────────────────────────────────────────────────────────
PropRef alias ∈ EntityRegistry         get_valid_prop_refs lists only aliases with a node type
PropRef prop ∈ dom K_S(T)              get_valid_prop_refs reads schema.property_keys(T)
TemporalMode ∈ {EVENTUALLY,...}        TemporalModeEnum; no free strings permitted
QAlgebraOp ∈ {UNION,INTERSECT,...}     QAlgebraOpEnum; closed set
candidates pre-validated by BDD        generated by the recommender, not by the LLM
ranking only reorders                  CandidateRankingResult.order checked to be a permutation
selection ∈ options shown              candidate_id and option_set_version checked against OptionSet
DAG identity exists only after build   QueryCandidate has no DAG id; PlanRef holds the question_key
BudgetDecision variant ∈ fixed set     BudgetDecisionModel.type; delivery carried for REWRITE/SAMPLE
```

This is the machine-executable form of §11.8 (hallucination prevention): not a post-hoc filter, but a generation constraint baked into every agent's output schema. The DGM specification is the formal source; the Pydantic schema is its executable projection.

---

## 12  Supervisor Agent Architecture

The NL interface (§11) processes a single question against a single dataset per invocation. This section specifies a **supervisor agent** that orchestrates multiple DGM tool calls — across datasets, with cross-domain correlation, iterative refinement, and compound analytical synthesis.

### 12.1  DGM as a composable tool

Six formal properties make DGM **agent-safe** — suitable for autonomous use by a supervisor without human-in-the-loop per step. They concern the tools; who may call which tool on which data is governed separately by §12.10, and what happens when calls fail by §12.11:

| # | Property | Guarantee |
|---|---|---|
| 1 | **Closed output vocabulary** | Every tool output is typed; no free-text unless explicitly requested |
| 2 | **Formal question space** | Tools operate on `Q_valid(A, B)` — the supervisor cannot ask an invalid question |
| 3 | **Deterministic equivalence** | Same `(q, π)` input yields the same canonical DAG, and the same DAG implies equivalent results (§8.14); different DAGs are not proven inequivalent |
| 4 | **Formal failure modes** | Every call ends in a typed `ToolCallResult` with a status, a `ToolErrorKind` and, when the gate was reached, a `BudgetDecisionKind` (§12.2) — the supervisor reacts by rule, not by catching exceptions |
| 5 | **Cost estimation before execution** | Every tool provides `cost_estimate(args)` before `execute(args)` |
| 6 | **Composable via Q_algebra** | Tool outputs are joinable/unionable via `Q_algebra` operations (§8.13) |

### 12.2  Tool contract and types

Every symbol used by the supervisor is defined here. Types are written as Pydantic-style schemas; `CostEstimate` and `ExecutionBudget` are those of §6.3, and `DatasetId` is the identifier of a dataset as known to the host.

**Budget decisions.** `BudgetDecisionKind` is the variant tag of `BudgetDecision` (§6.3), not a separate taxonomy:

```
BudgetDecisionKind ::= PROCEED | STREAM | ASYNC | REWRITE | SAMPLE | PAGINATE | CLARIFY | REJECT
```

A child call that reaches the gate records the kind it received. `REJECT` makes the call `REJECTED` (§12.4). The supervisor runs without a human per step, so `CLARIFY` is treated as `REJECTED` with error `NEEDS_CLARIFICATION`; the options it carried are kept in the result for the final response.

**Authorization context** (§12.10). Issued by the host from the authenticated session; never produced or modified by a model.

```
AuthContext(
  principal_id: str,
  tenant_id:    str,
  grants:       frozenset[DatasetGrant])

DatasetGrant(dataset_id: DatasetId, actions: frozenset[READ | CORRELATE | WRITE_KB | WRITE])
```

**Capability schema.** Declared by each tool, fixed at startup.

```
ToolCapabilitySchema(
  name:            ToolName,                 -- key in TOOL_REGISTRY
  version:         str,
  description:     str,
  input_schema:    JSONSchema,               -- arguments are validated against it
  output_schema:   JSONSchema,               -- ToolCallResult.data is validated against it
  reads:           list[JSONPointer],        -- argument fields that name datasets the tool reads
  writes_kb:       bool,                     -- stores findings (§12.7)
  idempotent:      bool,                     -- re-executing with the same arguments has no further effect
  composable_with: list[ToolName],           -- tools whose output this tool may take as input
  available:       bool)                     -- false: registered but cannot execute in this deployment
```

`available` is computed at startup from the build and the host configuration — a tool whose implementation, model or backend is missing is registered with `available = false`. The planner sees only available tools, and dispatching an unavailable one returns `NOT_IMPLEMENTED` or `UNSUPPORTED` without running anything.

**Request, result and error.**

```
ToolCallRequest(
  call_id:          str,                     -- unique within the plan
  tool_name:        ToolName,
  arguments:        dict,                    -- valid against input_schema
  depends_on:       list[call_id],           -- results this call consumes; earlier steps only
  budget_fraction:  float ∈ [0, 1])
  -- derived by the supervisor, never by the planner model:
  datasets:         list[DatasetId]          -- read from `arguments` at the `reads` pointers
  idempotency_key:  str                      -- H(plan_id, call_id, tool_name, canonical(arguments))

ToolCallResult(
  call_id:      str,
  tool_name:    ToolName,
  status:       SUCCEEDED | FAILED | REJECTED | SKIPPED | CANCELLED,
  data:         Any | None,                  -- present iff SUCCEEDED; valid against output_schema
  decision:     BudgetDecisionKind | None,   -- kind returned by the child gate, if it was reached
  error:        ToolError | None,            -- present iff status ∉ {SUCCEEDED}
  datasets:     list[DatasetId],             -- provenance of `data`
  attempts:     int,                         -- ≥ 1 unless SKIPPED
  cost_actual:  CostEstimate,
  started_at:   Timestamp | None,
  finished_at:  Timestamp | None)

ToolError(kind: ToolErrorKind, message: str, retryable: bool,
          context: dict)                     -- structured detail: the violated rule, the offending argument,
                                             -- the missing component, work completed before the failure

ToolErrorKind ::= UNKNOWN_TOOL | INVALID_ARGUMENTS | UNAUTHORIZED
                | NOT_IMPLEMENTED                    -- the capability is not implemented or not configured here
                | UNSUPPORTED                        -- a valid request this dataset, backend or version cannot execute
                | BUDGET_REJECTED | NEEDS_CLARIFICATION
                | TIMEOUT | CANCELLED | TOOL_FAILURE | INVALID_OUTPUT
                | DEPENDENCY_NOT_MET | COMPOSITION_FAILURE
```

`retryable` is true only for `TIMEOUT` and for `TOOL_FAILURE` that the tool marks transient. All other kinds are final for that call.

**Fail closed (D42; ADR 0012).** A result reports what was executed, never what was intended:

- `SUCCEEDED` means the tool executed its whole contract. Its `data` is computed output, never a placeholder, default, synthetic identifier or stub marker.
- An empty collection is `SUCCEEDED` only when the operation ran to completion and observed nothing. Not executing, failing, lacking a capability and lacking a grant are each reported by their own status and `ToolErrorKind`, never as an empty result.
- A tool that cannot execute the request exactly as contracted returns FAILED with the matching kind. It does not substitute another tool, query, dataset, tenant, key, strategy or backend, and it does not widen the scope of a search to find something to return.
- Answer-changing reductions exist only as gate substitutions the caller allowed (§6.3, D40). No tool, router or recovery route introduces one.

**Demo mode (D47; ADR 0013).** A deployment may ship a heuristic path that does not implement these contracts — keyword planning, template or model-generated SQL, placeholder tools — for demonstration only. It runs only when the caller selects it explicitly for that run. Every result it produces carries `demo = true` and is never reported as a contract `SUCCEEDED` or used as evidence of conformance. Without that selection the components it stands in for are unavailable and fail closed as above. Demo mode is never entered because a component is missing.

**Plan.**

```
SupervisorPlan(
  plan_id:    str,
  version:    int,                           -- incremented by each replan
  goal:       str,
  steps:      list[ToolCallRequest],
  confidence: float ∈ [0, 1],
  reasoning:  str)
```

A plan is **valid** iff:

```
P1  call_ids are unique.
P2  every depends_on names an earlier step: the dependency relation is acyclic and respects step order.
P3  every tool_name is in TOOL_REGISTRY with available = true, and its arguments validate against the tool's input_schema.
P4  for every dependency d of step s: tool(s) ∈ composable_with(tool(d)).
P5  Σ budget_fraction ≤ 1.
P6  every dataset of every step is granted to the principal with the actions the tool needs (§12.10).
```

**Tool interface.** `BaseTool` is the single normative interface; "ToolContract" in earlier versions denotes the same thing.

```python
class BaseTool(ABC):
    capability_schema: ToolCapabilitySchema

    def cost_estimate(self, arguments: dict, ctx: "ToolContext") -> CostEstimate:
        """Estimate from statistics only: reads no data and has no side effect."""

    async def execute(self, request: ToolCallRequest, ctx: "ToolContext") -> ToolCallResult:
        """Run the call within ctx.budget and ctx.deadline; never raises for an expected failure,
        which is returned as a ToolCallResult with status and error set."""

    def compose(self, left: ToolCallResult, right: ToolCallResult) -> ToolCallResult:
        """Combine two SUCCEEDED results; defined only when right.tool_name is in composable_with."""

    def explain(self, arguments: dict) -> str:
        """Plain-language description of what the call will do."""

@dataclass
class ToolContext:                           # runtime context; never checkpointed, never shown to a model
    auth:     AuthContext
    budget:   ExecutionBudget                # the child budget (§12.5)
    deadline: Timestamp
    cancel:   CancelToken
    kb:       KnowledgeBaseHandle            # scoped to auth.tenant_id (§12.10)
    data:     DataAccess                     # opens only the datasets of the current request
```

**Registry.**

```
TOOL_REGISTRY : Mapping[ToolName, BaseTool]
```

- *Content.* Built at startup from the tools of §12.3 and immutable for the duration of a run.
- *Resolution.* Exact match on `ToolName`. There is no fuzzy or case-insensitive matching and no fallback to a default tool.
- *Validation.* Plan validation (P1–P6) uses the registry. A plan naming an unknown or unavailable tool is invalid: no step of it runs, and the supervisor replans with error `UNKNOWN_TOOL` or `NOT_IMPLEMENTED` (§12.5).
- *Execution.* `execute_tool` resolves the name again, so a tool removed between validation and execution yields `UNKNOWN_TOOL` for that call, never a call to another tool.

**`run_ask_from_source`.** The entry point used by `dgm_query`: one non-interactive turn of the NL interface (§11.10) on one dataset.

```
run_ask_from_source(source: DatasetId, utterance: str, ctx: ToolContext) → NLTurnResult

NLTurnResult(
  question_key: QuestionKey | None,          -- §8.14
  decision:     BudgetDecisionKind,
  rows:         list[dict] | None,
  job:          JobTicket | None,            -- ASYNC only (§11.10)
  explanation:  str,
  unresolved:   list[str])
```

Where the interactive interface would ask the user to confirm a candidate, the non-interactive turn takes the top-ranked candidate and records the others in `explanation`; this is the declared contract of the non-interactive mode, which the supervisor selects explicitly. Everything else follows the interactive graph: `run_ask_from_source` uses the full graph of §11.10, and if the NL interface is not available (§11.10, *Fail closed*) `dgm_query` is registered with `available = false`.

### 12.3  Nine core tools

| Tool | Purpose | Maps to | Reads | KB write | Idempotent |
|---|---|---|---|---|---|
| `dgm_query` | One NL question against a dataset | `run_ask_from_source` (§12.2, §11.10) | `/dataset` | no | yes |
| `dgm_what_can_i_answer` | List answerable questions from `Q_valid` | BDD enumeration (§8.3) | `/dataset` | no | yes |
| `dgm_correlate` | Cross-domain JOIN of two results | `Q_algebra.JOIN` (§8.13) | `/left/dataset`, `/right/dataset` | no | yes |
| `dgm_explore_from` | Navigate from a result to related questions | Recommender (§7) | `/dataset` | no | yes |
| `dgm_sensitivity_sweep` | Parameter sweep over `Π(q)` | §8.15 + §8.16.7 Level 3 strategy | `/dataset` | no | yes |
| `dgm_validate_invariant` | Check a formal model invariant | Schema validation (§2–§3) | `/dataset` | no | yes |
| `dgm_store_finding` | Persist an analytical finding | Knowledge base (§12.7) | `/finding/datasets/*` | yes | yes, by `idempotency_key` |
| `dgm_retrieve_related` | Retrieval from KB by move distance | **Not** vector similarity — `d_L` of §12.7 | `/dataset` | no | yes |
| `dgm_map_analytical_space` | Map vocabulary decomposition | §8.16 computation | `/dataset` | no | yes |

`dgm_store_finding` is idempotent only through its key: storing the same finding twice under one `idempotency_key` stores it once.

**Per-tool contracts (D43).** Arguments are validated against `input_schema` before anything runs; `data` is present only on SUCCEEDED (§12.2). For each tool: what it executes, what `data` holds, when an empty result is legitimate, and its tool-specific failures. A case not listed is a `TOOL_FAILURE`; no case is answered by running something else.

| Tool | Arguments | Executes | `data` on SUCCEEDED | Empty is legitimate when | Tool-specific failures |
|---|---|---|---|---|---|
| `dgm_query` | `dataset`, `utterance` | one non-interactive NL turn (`run_ask_from_source`) | `NLTurnResult`; for `ASYNC`, the accepted `job` and `rows = None` | the executed plan returned no row | utterance not grounded (§12.8) → `INVALID_ARGUMENTS`, unresolved terms in `context`; gate `REJECT`/`CLARIFY` → REJECTED |
| `dgm_what_can_i_answer` | `dataset`, `anchors` (required), `max_hops ≤ MAX_ENUM_HOPS`, `limit ≤ MAX_ENUM_QUESTIONS` | the enumeration of §8.11 (theorem, part 1) restricted to skeletons of `Q_valid(anchors)` whose connecting paths have at most `max_hops` edges; parameters are not enumerated | `questions` (`question_key` and a skeleton summary, in enumeration order), `complete` — true iff the bounded domain was exhausted before `limit` — and the bounds used | the bounded domain has no valid skeleton; then `complete = true` | `anchors` missing or not an anchor set (AS1–AS3) → `INVALID_ARGUMENTS`; no default anchor set is chosen and the domain is never widened |
| `dgm_map_analytical_space` | `dataset`, `utterance?` | the vocabulary decomposition of §8.16 from `G_S`, `K_S` and `Σ`; with an utterance, the grounding check of §12.8 | the six vocabularies with members and sizes (`V_Par` as parameter families and their domains), the anchor sets admissible under AS1–AS3 up to `MAX_ANCHORS`, and the §12.8 answer if an utterance was given | never: a dataset with a schema has non-empty `V_N` | — |
| `dgm_correlate` | `left`, `right` (call ids of SUCCEEDED `dgm_query` results), `keys` | a drill-across of the two results (§5.2.2, Rule 12): FULL OUTER JOIN on `keys` | the joined rows, with DA4 for a side that has no row | both inputs are empty | a key not declared `Conformed` for both results' fact types, a key that is not a GroupBy key of both, or different GroupBy key sets (grain) → `INVALID_ARGUMENTS` naming the DA rule; an input without rows (`ASYNC`) → `UNSUPPORTED`; no other key, grain or dataset is tried |
| `dgm_explore_from` | `source` (call id), `limit ≤ MAX_EXPLORE` | the structural moves of §8.16.6 from the source skeleton, each checked for membership in `Q_valid` | `suggestions`: question key, the move, its parameters and the reason, ordered by recommender score (§7) then `question_key` | no move from the source yields a valid skeleton | source not SUCCEEDED or without a `question_key` → `INVALID_ARGUMENTS` |
| `dgm_sensitivity_sweep` | `source` (call id), `parameter`, `values` (1…`MAX_SWEEP_POINTS`) | the source question at every listed value, in order; the gate is consulted once for the total cost before the first execution | `points`: (value, rows or job) for every value | never: each point is reported | a value outside `Π_valid(q)` → `INVALID_ARGUMENTS` naming it; total cost over the child budget → REJECTED, nothing executed; a point that fails → FAILED with the completed values in `context`. No value is skipped, sampled or chosen by the tool. Level 3 guidance (§8.16.7) may inform the planner's choice of `values`, which is then visible in the request. |
| `dgm_validate_invariant` | `dataset`, `invariant`, `max_witnesses ≤ MAX_WITNESSES` | evaluation of one invariant over every instance in its scope | `holds`, `checked` (instances examined), `violation_count`, `violations` (up to `max_witnesses` witnesses) | — | `invariant` outside `H1`–`H3`, `PARTITION` (the structural invariants of §2.1), `M1`–`M7`, `PA1`–`PA4` → `INVALID_ARGUMENTS`; `PA*` on a dataset with no PipelineArtifact type → `INVALID_ARGUMENTS`; evaluation not completed → FAILED or REJECTED. `holds` is never set without complete evaluation. |
| `dgm_store_finding` | `finding` with `(q, π)`, datasets and content | durable write to the tenant's knowledge base (§12.7) | `finding_id = H(tenant_id, idempotency_key)` | — | dataset not granted → `UNAUTHORIZED` (§12.10) |
| `dgm_retrieve_related` | `dataset`, `(q', π')`, `k` | the breadth-first retrieval of §12.7 | findings in the total order of §12.7, with `d_S` and `d_P` | no granted finding lies within `k` | — |

`MAX_ENUM_HOPS`, `MAX_ENUM_QUESTIONS`, `MAX_EXPLORE`, `MAX_SWEEP_POINTS` and `MAX_WITNESSES` are policy constants of the deployment. A request above one is `INVALID_ARGUMENTS`; it is not truncated silently.

### 12.4  Supervisor graph

```
plan → validate_plan ──invalid──→ replan
            │ valid
            ▼
       select_step ──none runnable──→ synthesize → respond → END
            │
            ▼
       execute_tool → route_result ─┬─ continue ─→ select_step
                                    ├─ retry ────→ execute_tool        (same call, same idempotency_key)
                                    ├─ replan ───→ validate_plan        (new plan version, §12.5)
                                    └─ abort ────→ respond → END
```

- **plan**: decompose the utterance into a `SupervisorPlan` (§12.2). The planner sees only the datasets granted to the principal (§12.10).
- **validate_plan**: check P1–P6. An invalid plan never runs a step.
- **select_step**: dispatch PENDING steps whose dependencies are all SUCCEEDED, in plan order, up to the concurrency limit and the budget reservation below. A PENDING step with a dependency that ended in any other status becomes SKIPPED with `DEPENDENCY_NOT_MET`. When no step is runnable, go to synthesize.
- **execute_tool**: authorize the request (§12.10), compute the child budget (§12.5), call `TOOL_REGISTRY[tool_name].execute`, validate `data` against the output schema (`INVALID_OUTPUT` otherwise), and record the result.
- **route_result**: apply the routing table below.
- **synthesize**: compose the SUCCEEDED results and state coverage (§12.11).
- **respond**: format the outcome: COMPLETE, PARTIAL, FAILED, ABORTED or CANCELLED (§12.11).

**Concurrency (D38; ADR 0008).** Runnable steps — those whose dependencies have all SUCCEEDED — are independent of one another and may run concurrently:

- *Limit.* At most `MAX_CONCURRENT_CALLS` (default 4) calls are RUNNING; free slots are filled with runnable steps in plan order. Two steps that write to the knowledge base never run at the same time.
- *Budget reservation.* A step's child budget (§12.5) is reserved before dispatch, and a step is dispatched only if `spent + reserved + its child budget ≤ B.max_estimated_cost`. On completion its reservation is released and its `cost_actual` added to `spent`.
- *Recovery barrier.* A result routed to replan stops new dispatches until every RUNNING call has finished; the recovery then sees a stable set of results. A retry re-dispatches only the failed call.
- *Deterministic synthesis.* Results are consumed in plan order, never in completion order.

`MAX_CONCURRENT_CALLS = 1` gives strictly sequential execution.

**Routing after a result.** Evaluated top to bottom; the first matching row applies.

| Result | Condition | Route |
|---|---|---|
| SUCCEEDED | — | continue |
| FAILED, `retryable` | tool is idempotent and `attempts < MAX_ATTEMPTS` | retry |
| REJECTED | `replans < MAX_REPLANS` | replan |
| REJECTED | otherwise | abort |
| FAILED, CANCELLED by the run | — | abort, outcome CANCELLED |
| FAILED, any other | — | continue (dependents become SKIPPED) |

`UNAUTHORIZED` is FAILED and not retryable: the call is not retried or replanned around, and its dependents are skipped. `NOT_IMPLEMENTED` and `UNSUPPORTED` are FAILED and not retryable in the same way; no other tool is substituted for the call (D42). A replan may not reintroduce a dataset the principal is not granted (P6).

**Abort** is terminal. It keeps the diagnostic of the step that caused it — `call_id`, tool, arguments, child budget, `decision`, `error` — and every result obtained so far. `respond` reports them; no synthesis is attempted and no partial result is presented as an answer.

### 12.5  Budget propagation and recovery

The supervisor's total budget `B` is an `ExecutionBudget` (§6.3). A step's child budget scales every cost field of `B` by the step's fraction and bounds time by the run's deadline:

```
child_budget(step) = B with
  max_estimated_cost, streaming_threshold, async_threshold  ×  step.budget_fraction
  max_result_rows, max_precompute_time, substitutions         unchanged
  max_wall_time                                               min(B.max_wall_time, deadline − now)
```

Scaling every cost field by the same non-negative factor preserves invariants I1–I3 of §6.3, so a child budget is always valid. Each child call passes through its own gate (§6.3) with this budget. `spent` is the sum of `cost_actual` over finished calls; `remaining = B.max_estimated_cost − spent − reserved`, where `reserved` is the sum of child budgets of RUNNING calls (§12.4).

When a step is REJECTED, the routing table of §12.4 chooses one of two recoveries. Neither replaces the rejected call by another tool on its own: a different call can only come from a new plan that is validated and recorded like the first (D42; ADR 0012).

1. **Replan.** Call the planner again with the goal, the current plan, every result so far and the rejection. The new plan increments `version` and must satisfy, besides P1–P6:
   - every SUCCEEDED call is kept with its `call_id`; its result is reused, not recomputed;
   - the rejected call does not reappear with the same tool, arguments and budget fraction — a replan has to change something;
   - the fractions of the new and still-pending steps sum to at most `remaining / B.max_estimated_cost`.
   A plan that violates these is invalid and counts as a further replan. At most `MAX_REPLANS` replans per run.
2. **Abort.** When replanning does not apply, the run ends ABORTED (§12.4).

`MAX_ATTEMPTS` and `MAX_REPLANS` are policy constants. Each recovery consumes a bounded counter, and none can re-run the same rejected call unchanged, so a run terminates.

### 12.6  Six supervisor workflows

| Workflow | Pattern | Tools used |
|---|---|---|
| **Analytical research** | Multi-dataset comparison | `dgm_query` × N → `dgm_correlate` → `dgm_explore_from` |
| **Schema monitoring** | Invariant checking across datasets | `dgm_validate_invariant` × N |
| **Report generation** | Comprehensive domain survey | `dgm_map_analytical_space` → `dgm_query` × K (basis elements) |
| **Contract validation** | Verify cross-domain consistency | `dgm_query` × 2 → `dgm_correlate` → `dgm_validate_invariant` |
| **Knowledge base** | Accumulate and retrieve findings | `dgm_query` → `dgm_store_finding` → ... → `dgm_retrieve_related` |
| **Multi-agent debate** | Synthesize conflicting analyses | `dgm_query` × N (different params) → `dgm_sensitivity_sweep` |

### 12.7  Knowledge base with move-distance retrieval

The `dgm_store_finding` / `dgm_retrieve_related` pair implements a **knowledge base** indexed by position in the question lattice `L(G_AB)`:

- **Storage**: Each finding is tagged with its `(q, π)` coordinates in `A_struct` (§8.16.4)
- **Distance** (D37; ADR 0007): `d_L((q, π), (q', π')) = d_S(q, q')`, the length of a shortest path between the two skeletons in the *move graph* — skeletons of `Q_valid` connected by one structural local move of §8.16.6 (add or remove a Where atom, replace a GroupBy key, replace a hop, replace an AggFn). Every move has an inverse, so the graph is undirected and `d_S` is a metric on each connected component; skeletons in different components or anchor sets are at distance ∞. `d_P(π, π')` counts the parameter slots that differ when `q = q'`.
- **Retrieval**: given `(q', π')` and radius `k` (default 2), breadth-first search from `q'` to depth `k` collects findings with `d_S ≤ k`, keeps those whose datasets are all granted (§12.10), and orders them by `d_S`, then `d_P`, then most recent, then finding id — a total, reproducible order.
- **Not vector similarity**: retrieval follows refinement moves, not embedding-space cosine similarity, so a retrieved finding is a bounded number of explicit edits away from the current question.

Example: from `revenue by region, Where segment = 'retail'`, a finding on `revenue by region` is at `d_S = 1`, one on `revenue by country, Where segment = 'retail'` at `d_S = 1`, and one on `order count by country` at `d_S = 3`.

### 12.8  Meta-tool: grounding check and question-space map

`dgm_map_analytical_space` reports the size and composition of the question space of a dataset (§8.16) and, given an utterance, says whether and how it maps into that space. The second function rests on a notion of grounding defined independently of `Q_valid` and `A_eff`.

**Definition (grounded utterance).** Relative to a dataset's schema `G_S`, entity registry (§11.3) and closed vocabularies (§11.2), an utterance `u` is **grounded** iff there is a reading of `u` in which:

```
GR1  every content term resolves through the entity registry: entity terms to node types,
     property terms to declared properties dom K_S(T) of those types, verb terms to edge types;
GR2  every aggregation, temporal and compositional term resolves to a member of the closed
     enumerations (AggFn, TemporalMode / TemporalProperty / TemporalWindow, Q_algebra operation);
GR3  the resolved node types form a valid anchor set (AS1–AS3, §3.4.6);
GR4  every constant in u has a type compatible, under §4.1, with the property it is compared with.
```

The definition mentions the registry, the signatures, the vocabularies and the typing rules — never `Q_valid` or `A_eff`.

**Proposition.** If `u` is grounded, the reading determines a skeleton `q` and constants `π`, and exactly one of the following holds:

1. `q ∈ Q_valid(𝒜)` and `π ∈ Π_valid(q)`, so `(q, π) ∈ A_struct` (§8.16.4); or
2. some annotation of `Σ|_𝒜` rejects `q` or `π`, and that annotation is identified.

*Proof.* GR1–GR3 give a Join within `G_𝒜` built from declared types and edges; GR2 fixes the operators; GR4 makes every comparison type-check. The result is a concrete query that satisfies the typing rules. If it satisfies `Σ|_𝒜`, part 3 of the question completeness theorem (§8.11) gives case 1. Otherwise the first annotation rule that fails is reported, which is case 2. The two cases are exclusive because membership in `Q_valid` is defined as passing every annotation rule. ∎

**The meta-tool's answers.** For an utterance the tool returns one of:

| Answer | When | Example |
|---|---|---|
| `NOT_GROUNDED(terms)` | some term fails GR1–GR4; the failing terms are listed | "What is the churn propensity of customers?" — no property resolves "churn propensity" |
| `VIOLATES(annotation)` | grounded, case 2 | "Total account balance over the year" — `SUM` of a balance across time is rejected by `Additivity` (AD2, §2.4) |
| `MAPS_TO(q, π)` | grounded, case 1 | "Total revenue by region in 2024" |

Membership in `A_eff^obs` is not reported: it depends on data and requires execution (§8.16.4).

### 12.9  Relationship to §7 recommender

The supervisor's `dgm_explore_from` tool wraps the recommender (§7) as a first-class tool. The key difference from direct recommender use:

- **§7 recommender**: Suggests next questions within a single session and dataset
- **§12 supervisor**: Uses recommender suggestions as **navigation signals** across datasets — a recommendation in dataset A may trigger a correlated query in dataset B

The polynomial neighbourhood bound (§8.16.6) guarantees that each `dgm_explore_from` call returns a manageable number of suggestions, preventing the supervisor from being overwhelmed by the combinatorial space.

### 12.10  Authorization and dataset isolation

**Scope (D44).** The rules of this section apply to every entry point that reads, enumerates, caches, persists or writes data — the NL interface of §11 used directly, the query API, the result cache of §8.14 and the PipelineArtifact executor of §2.4 — and not only to the supervisor. Each entry point receives an `AuthContext` from the host. A call without one is refused with `UNAUTHORIZED` before any data is touched; there is no anonymous or default context, and no component builds one from request arguments.

| Operation | Required on every dataset involved |
|---|---|
| execute a query, read a cached result | `READ` |
| enumerate or map the question space | `READ` |
| execute a `WritePlan` or append a transition fact (§2.4) | `WRITE` on the sink and on the dataset that holds the artifact's transitions |

**Identity.** Every run carries one `AuthContext` (§12.2), built by the host from the authenticated session before the supervisor starts. It lives in the runtime context: it is not part of the checkpointed state, not part of any model input or output, and not a field the planner can set. Planner output that names a principal, tenant or grant is ignored.

**Visibility.** The planner and `dgm_what_can_i_answer` see only datasets with a `READ` grant. A dataset that is not granted is not mentioned to a model, so a denial does not reveal that it exists.

**Check at execution time.** `execute_tool` checks the request against the grants immediately before calling the tool, even though plan validation (P6) already checked it, because grants can change during a run:

| Tool | Required on every dataset of the request |
|---|---|
| any tool that reads | `READ` |
| `dgm_correlate` | `READ` and `CORRELATE` on **both** datasets |
| `dgm_store_finding` | `WRITE_KB`, plus `READ` on every dataset the finding derives from |
| `dgm_retrieve_related` | `READ`; findings are returned only if every dataset they derive from is granted |

The datasets of a request are those named in its arguments at the tool's `reads` pointers (§12.2) — derived by the supervisor from validated arguments, never asserted by the model. A failed check returns `UNAUTHORIZED` with the message "not authorized or not found", reads no data and is not retried.

**Isolation.**

- *Data access.* `ToolContext.data` opens connections only to the datasets of the current request, for the duration of the call.
- *Results.* Every result records the datasets it derives from. A composition inherits the union, and `dgm_correlate` checks the grants of all of them.
- *Knowledge base.* Partitioned by `tenant_id`; one tenant's findings are never stored in or returned to another tenant.
- *State and caches.* Supervisor state and checkpoints are keyed by `(tenant_id, thread_id)`. Result caches (§8.14) are scoped by `tenant_id`, so a cached result computed for one tenant is never served to another.

**Credentials.** Tools never receive credentials in arguments, results or state. The runtime opens connections with the principal's own credentials, or with a service identity restricted to the principal's grants, and holds them in `ToolContext.data`. Credentials are never logged, checkpointed or passed to a model.

### 12.11  Partial failures and terminal outcomes

**State per call.** Each step has exactly one status at any time:

```
PENDING → RUNNING → SUCCEEDED | FAILED | REJECTED | CANCELLED
PENDING → SKIPPED                         -- a dependency ended in a status other than SUCCEEDED
RUNNING → RUNNING                         -- a retry; attempts increments
```

The plan's state is the status of each step; the supervisor never records success for the plan as a whole except as the outcome below.

**Retry, timeout, cancellation and idempotency.**

- *Retry.* Only `retryable` failures of idempotent tools are retried, at most `MAX_ATTEMPTS` times, with the same `idempotency_key`. A non-idempotent call is never retried.
- *Timeout.* Each call has the deadline `min(started_at + child max_wall_time, run deadline)`. On expiry the call's cancel token is set and the result is FAILED with `TIMEOUT`.
- *Cancellation.* A cancelled run sets the cancel token of every RUNNING call, marks them CANCELLED, marks every PENDING step CANCELLED, waits for the running calls to acknowledge, and ends with outcome CANCELLED. Abort cancels running calls in the same way. Results completed before the cancellation are kept in the diagnostic.
- *Idempotency.* A tool with a side effect (`dgm_store_finding`) performs it at most once per `idempotency_key`, so a retry after an ambiguous failure — a timeout after the write — does not duplicate it.

**Three kinds of failure are reported apart:**

- a *tool error*: FAILED, with the tool's `ToolErrorKind`;
- a *budget rejection*: REJECTED, with `decision = REJECT` (or CLARIFY);
- a *composition error*: `compose` or `synthesize` cannot combine results that each succeeded — `COMPOSITION_FAILURE`, recorded on the synthesis; the individual results are still reported.

**Synthesis and outcome.** `synthesize` uses SUCCEEDED results only and computes coverage:

```
coverage = (SUCCEEDED steps) / (steps of the final plan version)
missing  = [(call_id, tool, status, error.kind) for every step not SUCCEEDED]
```

| Outcome | Condition |
|---|---|
| COMPLETE | every step SUCCEEDED and synthesis succeeded |
| PARTIAL | at least one step SUCCEEDED, and some step did not or synthesis failed |
| FAILED | no step SUCCEEDED |
| ABORTED | routing chose abort (§12.4) |
| CANCELLED | the run was cancelled |

The response always states the outcome. A PARTIAL response states coverage and lists the missing steps with their reasons; it does not present the available results as the answer to the whole question.

Example — one dataset answers, the other times out:

```
Plan v1:  q_a = dgm_query(dataset=sales_eu)       q_b = dgm_query(dataset=sales_us)
          c   = dgm_correlate(left=q_a, right=q_b)          depends_on [q_a, q_b]

q_a  SUCCEEDED (1 attempt)
q_b  FAILED, TIMEOUT, retryable → retried → FAILED, TIMEOUT (attempts = MAX_ATTEMPTS = 2)
c    SKIPPED, DEPENDENCY_NOT_MET

Outcome PARTIAL, coverage 1/3.
Response: the result for sales_eu; "sales_us: not available (timeout after 2 attempts);
          the EU–US correlation was not computed."
```

---

*— end of specification —*