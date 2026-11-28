# DGM claims register

This register lists the theorems, propositions and stated bounds of the DGM
specification (`docs/motivation/dimensional_graph_modelling.md`, `docs/adr/`)
and the evidence behind each. It was built for issue #139 (AUD-11).

## Kinds of evidence

- **Exhaustive check on a bounded domain.** A test in
  `tests/conformance/test_claims.py` decides the statement for every input of
  the named domain (every directed graph on three nodes, every Boolean function
  of three atoms, …). It uses the reference semantics of
  `tests/conformance/oracle/` (see `dgm-conformance.md`) or code written in
  the test from the definitions; nothing imports `sqldim`. One check,
  `test_claim_question_enumeration`, compares sqldim's question lattice with
  an enumeration written from the spec in `tests/conformance/qvalid.py`: it
  reads `sqldim` only for the output under test.
- **Values stated by the spec, reproduced by the oracle.** Conformance cases
  whose expected value is a worked example or table of the specification.
- **Executable witness.** One fixture that exhibits a boundary or a
  counterexample.
- **Cited proof.** The claim rests on a published result; nothing here checks it.
- **Proof in the specification, not checked.** The specification gives a
  proof; no independent check exists.
- **Not independently evidenced.** Neither.

A check is evidence, not a proof. It shows the statement on its domain and on
nothing larger; the domain is stated with each claim. A counterexample is
recorded as found and is not fixed here: the specification decides. When
the specification is corrected, the claim states the corrected text and the
old counterexample stays as a witness of why it changed (§8.6 and §8.16.6,
corrected in #140).

Run the checks with `PYTHONPATH=. pytest tests/conformance/test_claims.py`.
The register below is generated from `tests/conformance/claims.py`; regenerate
it with `PYTHONPATH=. python -m tests.conformance.claims --write`. A test fails
when the committed text is stale, when a claim names a check or case that does
not exist, or when a check is not referenced by any claim.

## Register

<!-- BEGIN GENERATED: tests/conformance/claims.py -->

28 claims: 20 hold on their checked domain, 0 have a counterexample, 1 only cited, 7 are not independently evidenced.

| Claim | Clause | Evidence | Status | Artifacts |
|---|---|---|---|---|
| `CLM-3.3-ORDER` | §3.3 Refinement order | exhaustive check on a bounded domain; executable witness (a single fixture) | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_strategy_refinement_order`<br>`tests/conformance/test_claims.py::test_claim_min_weight_incomparable` |
| `CLM-3.4.2-CONE-SUBGRAPH` | §3.4.2 RelationshipSubgraph by reachability | exhaustive check on a bounded domain; values stated by the spec, reproduced by the oracle | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_cone_intersection_identity`<br>cases `CONE-*` |
| `CLM-3.4.5-PROJ` | §3.4.5 Projection lemma | proof in the specification, not checked | not evidenced | — |
| `CLM-4.1-DUAL` | §4.1 FORALL elimination; ADR 0001 Duality and construction | exhaustive check on a bounded domain; values stated by the spec, reproduced by the oracle | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_ctl_dualities`<br>`tests/conformance/test_claims.py::test_claim_dualities_need_nottrue`<br>cases `BDD-F-*` |
| `CLM-ADR1-VALUES` | ADR 0001 Worked values (Graphs A and B) | values stated by the spec, reproduced by the oracle | holds on the checked domain | cases `CTL-A-*`<br>cases `CTL-B-*` |
| `CLM-6.3-HOEFF` | §6.3 Strategy 3 — sampling with error bounds | cited proof from the literature; exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_hoeffding_sample_size`<br>`tests/conformance/test_claims.py::test_claim_hoeffding_exact` |
| `CLM-8.1-KRIPKE` | §8.1 Derived Kripke structure | not independently evidenced | not evidenced | — |
| `CLM-8.2-PC` | §8.2 Propositional completeness | exhaustive check on a bounded domain; values stated by the spec, reproduced by the oracle; proof in the specification, not checked | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_propositional_completeness`<br>cases `THM-PC-*` |
| `CLM-8.3-CUBE` | §8.3 Minterms, maxterms, canonical form | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_cubes_and_bdd_paths` |
| `CLM-8.4-DIAMOND` | §8.4 PathPred as compact quantification | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_diamond_chain_and_tree` |
| `CLM-8.5-COLLAPSE` | §8.5 EXISTS/FORALL duality | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_strategy_refinement_order` |
| `CLM-8.6-SIMPLE-BOUND` | §8.6 Finite traversal space | exhaustive check on a bounded domain; executable witness (a single fixture) | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_simple_path_bound_one_label`<br>`tests/conformance/test_claims.py::test_claim_simple_path_bound_parallel_edges`<br>`tests/conformance/test_claims.py::test_claim_simple_path_bound_counterexample` |
| `CLM-8.6-PATTERNS` | §8.6 Finite traversal space | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_pattern_count` |
| `CLM-8.7-CONE` | §8.7 Cone intersection theorem; §6.2 Rule 9 | exhaustive check on a bounded domain; values stated by the spec, reproduced by the oracle | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_cone_intersection_identity`<br>`tests/conformance/test_claims.py::test_claim_cone_old_counterexample_now_holds`<br>cases `CONE-*` |
| `CLM-8.8-ALLEN` | §8.8 Allen exhaustiveness | exhaustive check on a bounded domain; values stated by the spec, reproduced by the oracle; proof in the specification, not checked | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_allen_exhaustive`<br>cases `TO-THM-*` |
| `CLM-8.9-HIER` | §8.9 Temporal property class hierarchy | not independently evidenced | not evidenced | — |
| `CLM-8.10-ENTROPY` | §8.10 SIGNATURE_ENTROPY | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_entropy` |
| `CLM-8.11-QC` | §8.11 Question completeness theorem | proof in the specification, not checked | not evidenced | — |
| `CLM-8.11-ENUM` | §8.11 Question completeness, part 1; §12.3 `dgm_what_can_i_answer` enumeration | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_question_enumeration` |
| `CLM-8.13-WALK` | §8.13 Theorem 1 (key step) and Theorem 2 | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_walk_equals_simple_one_hop` |
| `CLM-8.13-D4` | §8.13 Why D4 is needed | executable witness (a single fixture); cited proof from the literature | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_two_hop_body_breaks_walk_reading` |
| `CLM-8.13-T1` | §8.13 Theorem 1 (F_D ⊆ Datalog¬,≤) | proof in the specification, not checked | not evidenced | — |
| `CLM-8.13-TRANSL` | §8.13 Theorem 1, the translation of its proof | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_datalog_translation` |
| `CLM-8.13-PTIME` | §8.13 Corollary (data complexity of F_D) and table | cited proof from the literature | rests on the citation | — |
| `CLM-8.14-CANON` | §8.14 Canonical form theorem (soundness) | proof in the specification, not checked | not evidenced | — |
| `CLM-8.15-PARAM` | §8.15 Parameter completeness; finite parametric skeleton | proof in the specification, not checked | not evidenced | — |
| `CLM-8.16.6-NBHD` | §8.16.6 Neighbourhood size under local moves | exhaustive check on a bounded domain; executable witness (a single fixture) | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_neighbourhood_bound`<br>`tests/conformance/test_claims.py::test_claim_neighbourhood_bound_counterexample` |
| `CLM-ADR7-METRIC` | ADR 0007 d_S | exhaustive check on a bounded domain | holds on the checked domain | `tests/conformance/test_claims.py::test_claim_shortest_path_metric`<br>`tests/conformance/test_claims.py::test_claim_move_graph` |

### Statements and checked domains

- **`CLM-3.3-ORDER`** — SHORTEST = K_SHORTEST(1) ⊑ K_SHORTEST(k) ⊑ K_SHORTEST(k') ⊑ ALL for 1 ≤ k ≤ k', and MIN_WEIGHT ⊑ ALL, where s ⊑ s' iff s(I) ⊆ s'(I); MIN_WEIGHT is incomparable with every K_SHORTEST(k).
  *Domain:* instance sets of every one-label graph on 3 nodes (self-loops included) under SIMPLE Repeat, k ≤ 4; incomparability by one weighted witness per k ≤ 3.
- **`CLM-3.4.2-CONE-SUBGRAPH`** — G_AB, G_A* and G_*B are subgraphs (every edge has both ends in the node set), and G_AB ⊆ G_A* ⊆ G_**, G_AB ⊆ G_*B ⊆ G_**.
  *Domain:* every directed graph on ≤ 3 nodes, self-loops included, every (A, B).
- **`CLM-3.4.5-PROJ`** — h maps G_ab^I into G_AB^S: h_N(N_ab^I) ⊆ N_AB^S and h_E(E_ab^I) ⊆ E_AB^S.
  *Note:* the proof was restated on reachability with the cones (#139); no check enumerates typed instance graphs.
- **`CLM-4.1-DUAL`** — Under on_empty = VACUOUS, AG φ = ¬EF ¬φ, AF φ = ¬EG ¬φ, AX φ = ¬EX ¬φ and A(φ U ψ) = ¬E(¬ψ U (¬φ ∧ ¬ψ)) ∧ ¬EG ¬ψ over maximal traces; under REQUIRE_WITNESS each FORALL form is its construction conjoined with EF TRUE (EX TRUE for NEXT). The inner ¬ must be NotTrue.
  *Domain:* every one-label graph on 3 nodes without self-loops, every two-valued labelling of φ and ψ, start node fixed by symmetry, patterns Repeat(0|1, UNBOUNDED, SIMPLE) and one hop; finite traces only (the lasso of ADR 0001 is not modelled).
  *Note:* with Kleene ¬ instead of NotTrue the AG duality fails at a node where φ is UNKNOWN — the reason §4.1 reads the inner NOT as NotTrue.
- **`CLM-ADR1-VALUES`** — The truth tables of ADR 0001 for EF, EG, EX, AF, AG, AX under both empty-domain policies.
  *Domain:* the two graphs of the ADR.
- **`CLM-6.3-HOEFF`** — For n anchors drawn uniformly without replacement, P(|X̄ − μ| ≥ t) ≤ 2·exp(−2nt²/(hi − lo)²); n* = ⌈log(2k/δ)/(2ε²)⌉ gives error_bound ≤ ε, and k = 1, δ = 0.05, ε = 0.05 gives n* = 738.
  *Domain:* the inequality by Hoeffding [15] (§6 for sampling without replacement); checked exactly (hypergeometric, rational arithmetic) for every population of N ≤ 16 values in {0, 1}, every n ≤ N and t on a grid; n* minimal for k ≤ 5, δ ∈ {0.01, 0.05, 0.1}, ε ∈ {0.01, 0.02, 0.05, 0.1}.
  *Note:* values in a general [lo, hi] rest on the citation; the union bound over k aggregates is elementary and not checked.
- **`CLM-8.1-KRIPKE`** — Paths of K_π from (start, 0) correspond one to one to path instances of π.
  *Note:* no product automaton is built by the oracle.
- **`CLM-8.2-PC`** — Every Boolean function of a finite atom set A is expressed by a filter tree over A with AND, OR, NOT; a condition that is not a function of the atoms (x > 3 over {x > 5}) is not.
  *Domain:* all 2^(2^m) functions for m ≤ 3 atoms, by the proof's minterm construction; the scope counterexample at x = 4 and x = 0.
- **`CLM-8.3-CUBE`** — A cube with j literals over |V| = m variables covers 2^(m−j) minterms; a BDD's paths to TRUE are pairwise disjoint cubes whose union is the formula's minterm set.
  *Domain:* every cube over 3 variables; every function of 3 variables, with a reduced decision diagram built in the check (not sqldim's BDD).
- **`CLM-8.4-DIAMOND`** — The diamond chain D_n has |N| = 3n + 1, |E| = 4n and 2^n simple paths from s to v_n; in a tree there is one path between two nodes, so no gain.
  *Domain:* D_n for n ≤ 6 (path instances by the oracle); a binary tree of depth 3.
- **`CLM-8.5-COLLAPSE`** — SHORTEST and MIN_WEIGHT select at most one instance, so EXISTS and FORALL collapse to a single term.
  *Domain:* as CLM-3.3-ORDER.
- **`CLM-8.6-SIMPLE-BOUND`** — The number of simple path instances in G_I is at most Σ_k |N_I|!/(|N_I|−k)! · m^k, where m ≥ 1 is the maximum number of parallel edges between an ordered pair of nodes (1 without edges).
  *Domain:* k read as the number of nodes, 1 ≤ k ≤ |N_I|; every one-label graph on ≤ 4 nodes (m = 1); every graph on ≤ 3 nodes with up to 2 parallel edges of distinct labels per ordered pair, and on 2 nodes with up to 3.
  *Note:* simple path instances are counted by §3.3 key, so parallel edges are distinct instances.
- **`CLM-8.6-PATTERNS`** — There are at most Σ_{j=0}^{k} (2·|E_S|)^j path patterns of at most k hops.
  *Domain:* |E_S| ≤ 3, k ≤ 4: the enumeration meets the bound with equality.
- **`CLM-8.7-CONE`** — G_A* ∩ G_*B = G_AB, for node and edge sets; Rule 9 is therefore exact.
  *Domain:* every directed graph on ≤ 3 nodes, self-loops included, every (A, B).
  *Note:* false under the simple-path reading of §3.4.2 before #139 (A→X, X→B, B→n, n→X); the spec now defines the cones by reachability.
- **`CLM-8.8-ALLEN`** — For two intervals of the domain (s < e, NULL bounds as ±∞) exactly one of the 13 relations holds, and the three-comparison tree of the proof picks it; a degenerate interval satisfies none.
  *Domain:* every pair of intervals with bounds in {0, …, 5, NULL}.
- **`CLM-8.9-HIER`** — SAFETY, LIVENESS ∈ Π₁/Σ₁; RESPONSE ∈ Π₂; PERSISTENCE ∈ Σ₂; RECURRENCE ∈ GF.
  *Note:* no proof and no citation in the spec (the classes are those of Manna and Pnueli's safety-progress hierarchy, which is not cited).
- **`CLM-8.10-ENTROPY`** — H = 0 iff one signature type occurs; H = log₂ k for k equally frequent types, the maximum over k types.
  *Domain:* every count vector of ≤ 4 types with counts ≤ 4.
- **`CLM-8.11-QC`** — Under QH1–QH6 Q_valid(A, B) is finite and enumerable from the schema, membership is decidable, and every concrete query over G_AB is q(π).
  *Note:* the theorem as a whole is not checked: CLM-8.11-ENUM checks that the §12.3 construction lists Q_valid on a bounded domain; membership (part 2) and coverage (part 3) have no independent check.
- **`CLM-8.11-ENUM`** — The construction (a)–(e) of §12.3, with its Σ rules, lists each skeleton of Q_valid(𝒜) exactly once and in non-decreasing grade.
  *Domain:* an enumeration written from §8.11, §12.3, §4.1, §4.2 and §5.2.3 in `tests/conformance/qvalid.py` (no `sqldim`), compared as a set and a count with the lattice's generator: every skeleton of grade ≤ 2 over two anchors and one edge type with h = 1 (one Join), for six schemas — no properties; a numeric dimension attribute; a String property on a BRONZE dimension and a Boolean; an ADDITIVE measure; a NON_ADDITIVE measure; a Degenerate attribute — 5,920 skeletons, none missing, none extra, none twice.
  *Note:* evidence that sqldim's enumeration meets the definition on this domain. Grade ≤ 2 keeps every filter to two atoms, where the reduced BDD size does not depend on the variable order that §8.14 leaves to the atoms' encoding. Outside the domain: properties of a FactlessFact, constellations (DA1–DA3), time-ordered bridges and Joins of more than one hop.
- **`CLM-8.13-WALK`** — For a one-hop body under Repeat(·, 0, UNBOUNDED), SIMPLE, TRAIL and walk semantics (the Datalog rules of Theorem 1, as a least fixpoint) agree on EF φ, EX φ and E(φ U ψ).
  *Domain:* every one-label graph on 3 nodes (self-loops included for EF and EX, excluded for E(φ U ψ)), every two-valued labelling of φ, ψ, start node fixed by symmetry.
- **`CLM-8.13-D4`** — For a body of two or more hops, walk and simple-path existence differ.
  *Domain:* one witness: (a·a)* from s to t on s→t, t→u, u→v, v→t; NP-completeness of the simple case is cited [19].
- **`CLM-8.13-T1`** — Every query in F_D has a stratified Datalog program computing it.
  *Note:* the translation is executed on a bounded sub-fragment (CLM-8.13-TRANSL) and its walk/simple step on its own (CLM-8.13-WALK); F_D as a whole — Joins over several anchors and PathJoins, IN, nested sub_filters, outer Refs, several edge labels, non-recursive WITH — is not.
- **`CLM-8.13-TRANSL`** — The proof's translation — conditions in the T⁺ form with NULL as an absent fact, a fixed-length path or finite-max Repeat as a union of edge conjunctions with ≠ literals for SIMPLE and TRAIL, the recursion for an UNBOUNDED one-hop Repeat, a negated PathPred and EXCEPT as a negated IDB literal — is a safe, stratified Datalog¬,≤ program whose perfect model is the query's result.
  *Domain:* unary queries anchored on one node type, against the reference semantics: 392 conditions (comparison with a constant and of two properties under all six operators, IsNull, NOT, AND, OR to depth 2) on a node with x, y ∈ {NULL, 0, 1, 2}; 35 queries — EXISTS with EVENTUALLY, UNTIL and NEXT over one hop, two hops, Repeat(e, 0|1, UNBOUNDED, SIMPLE|TRAIL) and Repeat(e, 0|1, 2, SIMPLE|TRAIL|WALK), with a sub_filter that is UNKNOWN on NULL, their negations, two mixed conditions, UNION, INTERSECT and EXCEPT — on every graph with one edge label and x ∈ {NULL, 0, 1}, on 3 nodes without self-loops and on 2 with them, up to isomorphism (406 graphs). The stratified evaluator is `tests/conformance/datalog.py`, which refuses an unsafe or unstratified program.
  *Note:* TRAIL distinctness of two edges is a disjunction (source or target differs), so it gives one conjunction of ≠ literals per choice — a union of conjunctions, still Datalog, where the proof writes 'a conjunction'.
- **`CLM-8.13-PTIME`** — F_D has polynomial data complexity; simple regular paths are NP-complete; counting simple paths is #P-complete; Immerman–Vardi does not make Q_algebra capture PTIME.
  *Note:* Abiteboul, Hull, Vianu [20]; Mendelzon and Wood [19]; Valiant [18]; Immerman [16], Vardi [17].
- **`CLM-8.14-CANON`** — Two composed queries that reduce to the same query DAG node are equivalent.
- **`CLM-8.15-PARAM`** — Π_valid(q) is constructively characterisable; its discrete part is finite.
  *Note:* proof sketch only.
- **`CLM-8.16.6-NBHD`** — The neighbourhood has at most m + g·(|V_P| − g) + 2L·|E_AB| + a·|AggFn| + 2|P_ord| + Σ(|D_p| − 1) elements, one line per move kind, where g is the number of GroupBy keys of q.
  *Domain:* the GroupBy line exactly, for every key set of every size g over |V_P| ≤ 6 properties; the other lines summed on a toy skeleton space; the §8.16.8 arithmetic (56).
- **`CLM-ADR7-METRIC`** — d_S, the unit-weight shortest-path distance in the undirected move graph M, is a metric on each connected component of M; BFS to depth k visits at most Σ_{j≤k} N^j skeletons; the ADR's example has d_S = 3.
  *Domain:* every undirected graph on ≤ 5 nodes; a toy move graph of 3 atoms × 3 keys × 2 hops × 3 AggFns.
  *Note:* the example reads 'order count' as COUNT over the same argument; a change of measure argument is not one of the four moves.

### Counterexamples

None open.

### Corrected after a counterexample

- **`CLM-8.6-SIMPLE-BOUND`** (§8.6 Finite traversal space): the bound was Σ_k |N_I|!/(|N_I|−k)!, which counts node sequences: nodes a, b and edges (a,b,x), (a,b,y), (a,b,z) give 5 simple path instances (2 of length 0, 3 of length 1) against 4; the corrected bound is 24.
- **`CLM-8.16.6-NBHD`** (§8.16.6 Neighbourhood size under local moves): the line read 'replace one GroupBy key ≤ |V_P|', which fails when q groups by g ≥ 2 keys: replacing one of them gives g·(|V_P| − g) neighbours, 6 > 5 for g = 2, |V_P| = 5. Polynomiality is unaffected.

<!-- END GENERATED -->

## The cone definitions (resolved in #139)

§3.4.2 defined `G_AB`, `G_A*` and `G_*B` by simple paths, while the proof of
the §8.7 cone intersection theorem used reachability. On the graph with edges
`A→X, X→B, B→n, n→X` the two disagree: `n` is in both cones but on no simple
path from `A` to `B`. The maintainer chose reachability: §3.4.2 now defines
`N_AB = {n | A →* n →* B}`, `N_A* = {n | A →* n}`, `N_*B = {n | n →* B}`,
with an edge in a cone when its source is reachable from `A` (or `B` is
reachable from its target, or both for `G_AB`). The oracle gained
`oracle/cones.py` and the cases `CONE-*` as a consequence of that change, and
`test_claim_cone_old_counterexample_now_holds` reproduces the old
counterexample.

## Independent review

No claim has been reviewed by a second person. Every check above was written
by the same process that wrote this register, and a check can share a
misreading with the text it checks. This table records an independent human
review of each claim: of the statement against the specification, of the
checked domain and of the check's code. A reviewer fills one row per claim
reviewed, with their name or handle, the date, a verdict (`agrees`,
`disagrees` or `partial`) and a note or link; `awaiting` means no one has. The
table is kept by hand and is not generated; `test_claims.py` fails when a
claim of the register is missing from it or listed twice.

| Claim | Review | Reviewer | Date | Verdict and note |
|---|---|---|---|---|
| `CLM-3.3-ORDER` | awaiting | | | |
| `CLM-3.4.2-CONE-SUBGRAPH` | awaiting | | | |
| `CLM-3.4.5-PROJ` | awaiting | | | |
| `CLM-4.1-DUAL` | awaiting | | | |
| `CLM-ADR1-VALUES` | awaiting | | | |
| `CLM-6.3-HOEFF` | awaiting | | | |
| `CLM-8.1-KRIPKE` | awaiting | | | |
| `CLM-8.2-PC` | awaiting | | | |
| `CLM-8.3-CUBE` | awaiting | | | |
| `CLM-8.4-DIAMOND` | awaiting | | | |
| `CLM-8.5-COLLAPSE` | awaiting | | | |
| `CLM-8.6-SIMPLE-BOUND` | awaiting | | | |
| `CLM-8.6-PATTERNS` | awaiting | | | |
| `CLM-8.7-CONE` | awaiting | | | |
| `CLM-8.8-ALLEN` | awaiting | | | |
| `CLM-8.9-HIER` | awaiting | | | |
| `CLM-8.10-ENTROPY` | awaiting | | | |
| `CLM-8.11-QC` | awaiting | | | |
| `CLM-8.11-ENUM` | awaiting | | | |
| `CLM-8.13-WALK` | awaiting | | | |
| `CLM-8.13-D4` | awaiting | | | |
| `CLM-8.13-T1` | awaiting | | | |
| `CLM-8.13-TRANSL` | awaiting | | | |
| `CLM-8.13-PTIME` | awaiting | | | |
| `CLM-8.14-CANON` | awaiting | | | |
| `CLM-8.15-PARAM` | awaiting | | | |
| `CLM-8.16.6-NBHD` | awaiting | | | |
| `CLM-ADR7-METRIC` | awaiting | | | |
