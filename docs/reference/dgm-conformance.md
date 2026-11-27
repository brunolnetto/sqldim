# DGM semantic conformance suite

`tests/conformance/` checks the DGM core against an executable reading of its
specification (`docs/motivation/dimensional_graph_modelling.md`, `docs/adr/`).
It was built for issue #137 (AUD-09) and is the common oracle for the
per-backend matrix (#138) and the theoretical claims (#139).

## How it is built

- **Oracle** (`tests/conformance/oracle/`). A small reference semantics in
  pure Python over tiny in-memory graphs, written from the definitions: maximal
  finite traces and the empty-domain policy (ADR 0001), Kleene logic and
  `NotTrue` (§4.1, §6.1), the Allen relations with open and degenerate
  intervals (§3.2, §8.8), `version`/`valid` and AS_WAS/AS_IS (§2.5, §2.6),
  AD1–AD4, fan-out and drill-across (§2.4, §4.2, §5.2.2), anchor sets by walk
  enumeration (§3.4.6), and the §6.1 BDD questions decided by enumerating the
  atoms' meaning. It imports nothing from `sqldim`; a test enforces this.
- **Cases** (`tests/conformance/cases/`). Data: id, clause, rule, fixture,
  inputs and, where the specification states one (a worked example or table),
  the expected value. `test_oracle.py` checks the oracle against every stated
  value. Other cases are derived from the definitions and decided by the oracle.
- **Backends** (`tests/conformance/runners/`). An adapter per implementation
  path: `DGMQuery.to_sql()` and `DGMPlanner.plan_query()` executed on DuckDB,
  `TemporalOrdering.to_sql()` on DuckDB, `DGMPredicateBDD`, `DGMModel` with the
  anchor-set functions, and `Grain`. A new backend (#138) is one `Backend` and
  its expectations; cases and oracle do not change.
- **Expectations** (`tests/conformance/expectations.py`). A disagreement is
  never fixed in the oracle. It is a strict `xfail` with its reason; a
  wholesale rejection by the implementation is *refused*; a missing construct
  is *not expressible*. All three are checked, so the matrix cannot go stale.

A passing case shows the observed value equals the specified one on that
fixture, and no more. Where a backend's code is wrong in general but happens to
give the right value on a fixture, the case passes; a sibling case exposes it
(for example `CTL-A-c-AF` for the FORALL templates of `to_sql()`).

Run it with `PYTHONPATH=. pytest tests/conformance -n 4`. Regenerate the
matrix below with `PYTHONPATH=. python -m tests.conformance.matrix --write`;
`test_matrix.py` fails when it is out of date.

## Traceability matrix

Cells count cases per status: *pass*, **non-conf.** (strict xfail), *refused*
(fail-closed rejection), *n/e* (no construct). `—`: the backend does not
implement the area.

<!-- BEGIN GENERATED: tests/conformance/matrix.py -->

353 cases; 174 carry a value stated by the specification, the others are derived by the oracle from the definitions. Every case is checked against the oracle.

| Clause | Cases | `duckdb-dgmquery` | `duckdb-plan-ir` | `duckdb-ordering` | `dgm-bdd` | `dgm-model` | `dgm-annotations` |
|---|---|---|---|---|---|---|---|
| ADR 0001 Empty-domain policy; §4.1 Empty domain | `CTL-A-c0-AF-REQ` … `CTL-B-a2-AX-VAC` (12) | 3 pass, 3 refused, 6 n/e | 6 refused, 6 n/e | — | — | — | — |
| ADR 0001 Truth conditions (UNTIL is strong) | `PP-UNTIL-01`, `PP-UNTIL-02` | 1 pass, 1 **non-conf.** | 2 refused | — | — | — | — |
| ADR 0001 Truth conditions (UNTIL is strong); §4.1 | `CTL-U-01` … `CTL-U-05` (5) | 5 refused | 5 refused | — | — | — | — |
| ADR 0001 Truth conditions; §4.1 Path semantics | `CTL-A-c-EF` … `CTL-B-a2-EX-VAC` (30) | 6 pass, 3 **non-conf.**, 15 refused, 6 n/e | 24 refused, 6 n/e | — | — | — | — |
| ADR 0003 / §5.1 one reference instant τ | `SCD-TAU-01` | 1 **non-conf.** | 1 refused | — | — | — | — |
| §2.1 Structural invariants (V_S ⊆ D_S × F_S) | `MOD-08`, `MOD-09`, `MOD-10`, `MOD-11` | — | — | — | — | 4 pass | — |
| §2.4 Additivity AD1–AD4 and grain defaults | `ADD-01` … `ADD-13` (13) | — | — | — | — | — | 3 pass, 4 **non-conf.**, 6 n/e |
| §2.5 SCD resolution: as-was and as-is | `SCD-01` … `SCD-05` (5) | 1 pass, 4 **non-conf.** | 1 pass, 4 **non-conf.** | — | — | — | — |
| §2.6 Declared model (well-formedness at construction); §2.4 argument kinds | `MOD-01` … `MOD-07` (7) | — | — | — | — | 7 pass | — |
| §2.6 Declared model: §2.1 invariants are not construction checks | `MOD-12` | — | — | — | — | 1 pass | — |
| §2.6 Version and validity at an instant (half-open windows) | `SCD-V-01` … `SCD-E-06` (13) | 9 pass, 4 **non-conf.** | 9 pass, 2 **non-conf.**, 2 refused | — | — | — | — |
| §3.2 Alias uniqueness (C ∩ L ≠ ∅ rejected) | `PP-ALIAS-04` | 1 **non-conf.** | 1 pass | — | — | — | — |
| §3.2 Repeat well-formedness | `CON-R2-01` … `CON-R-02` (7) | 7 pass | 7 pass | — | — | — | — |
| §3.2 Repetition and RepeatMode; PathJoin under ALL | `RJ-01` … `RJ-06` (6) | 6 refused | 3 pass, 3 refused | — | — | — | — |
| §3.2 TemporalOrdering semantics (Allen, half-open, NULL bounds) | `TO-PRECEDES-proper` … `TO-EQUALS-degenerate` (39) | — | — | 15 pass, 18 **non-conf.**, 6 n/e | — | — | — |
| §3.2 Typing of TemporalOrdering (TO1–TO4) | `TO-TYPE-01`, `TO-TYPE-02`, `TO-TYPE-03`, `TO-TYPE-04` | — | — | 3 **non-conf.**, 1 n/e | — | — | — |
| §3.2 point relations (NULL TimePoint is UNKNOWN) | `TO-BEFORE-pt`, `TO-AFTER-pt`, `TO-CONCURRENT-pt`, `TO-CONCURRENT-tol1` | — | — | 4 pass | — | — | — |
| §3.3 Strategy (deterministic ≺ order) | `PP-STR-01`, `PP-STR-02`, `PP-STR-03`, `PP-STR-04` | 2 pass, 2 **non-conf.** | 1 pass, 3 refused | — | — | — | — |
| §3.4.6 Anchor sets (AS1–AS3, N_𝒜, E_𝒜, Σ|_𝒜) | `AS-01` … `AS-GEN-11-edges` (50) | — | — | — | — | 49 pass, 1 **non-conf.** | — |
| §3.5 Evaluation contexts (Refs in C ∪ L) | `PP-ALIAS-01`, `PP-ALIAS-02`, `PP-ALIAS-03` | 3 pass | 3 pass | — | — | — | — |
| §3.5 L-promotion | `PP-PROM-01` | 1 **non-conf.** | 1 refused | — | — | — | — |
| §4.1 AF and AG are distinct; FORALL elimination | `CTL-G1-AF` … `CTL-G2-NOT-EF` (8) | 8 n/e | 8 n/e | — | — | — | — |
| §4.1 Band table; §5.2 cross-band Ref rules | `CON-B-01` … `CON-B-10` (10) | 10 pass | 10 pass | — | — | — | — |
| §4.1 Empty domain; ADR 0001 Empty-domain policy | `PP-EX-02`, `PP-FA-02`, `PP-FA-03`, `PP-NOT-03` | 3 pass, 1 n/e | 3 pass, 1 n/e | — | — | — | — |
| §4.1 FORALL elimination (CTL duality, NotTrue, REQUIRE_WITNESS guard) | `BDD-F-01`, `BDD-F-02`, `BDD-F-03` | — | — | — | 3 **non-conf.** | — | — |
| §4.1 FORALL with ONCE/SINCE; past-time modes need a time-ordered path | `PP-PAST-01`, `PP-PAST-02`, `PP-PAST-03` | 3 **non-conf.** | 3 refused | — | — | — | — |
| §4.1 NULL rules (NULL is not a literal) | `CON-N-01`, `CON-N-02` | 1 pass, 1 **non-conf.** | 1 pass, 1 **non-conf.** | — | — | — | — |
| §4.1 PathPred; FORALL elimination with NotTrue; ADR 0001 | `PP-EX-01` … `PP-NULL-04` (8) | 8 pass | 8 pass | — | — | — | — |
| §4.1 Recorded traces and monitoring verdicts | `MON-01` … `MON-15` (15) | — | — | — | — | — | — |
| §4.2 TemporalWindow LATEST / EARLIEST | `TAGG-01`, `TAGG-02` | — | — | — | — | — | — |
| §4.2 What an aggregate counts (ENTITIES / PATHS) | `AGG-OVER-01`, `AGG-OVER-02`, `AGG-OVER-03`, `AGG-OVER-04` | 2 pass, 2 **non-conf.** | 2 pass, 2 **non-conf.** | — | — | — | — |
| §5.2.2 Drill-across (steps 1–3, DA4) | `DA-01`, `DA-02` | 2 **non-conf.** | 2 **non-conf.** | — | — | — | — |
| §5.2.2 Well-formedness of a constellation (DA1–DA3) | `DA-WF-01`, `DA-WF-02`, `DA-WF-03`, `DA-WF-04` | — | — | — | — | — | — |
| §6.1 / §8.3 propositional equivalence, tautology, implication | `BDD-P-01` … `BDD-P-GEN-23` (31) | — | — | — | 31 pass | — | — |
| §6.1 Order theory of atoms (Δ_ord, O1–O6) | `BDD-O-01` … `BDD-O-08` (8) | — | — | — | 3 pass, 5 **non-conf.** | — | — |
| §6.1 Three-valued logic (T⁺/T⁻ encoding, Δ_excl, Δ_null) | `BDD-K-01` … `BDD-K-08` (8) | — | — | — | 4 pass, 3 **non-conf.**, 1 n/e | — | — |
| §6.1 Variables (canonical form of atoms) | `BDD-C-01`, `BDD-C-02`, `BDD-C-03` | — | — | — | 3 **non-conf.** | — | — |
| §8.2 Propositional completeness (proof by minterm construction) | `THM-PC-0000` … `THM-PC-1111` (16) | — | — | — | 16 pass | — | — |
| §8.3 A BDD's paths to TRUE are disjoint cubes covering its minterms | `THM-CUBE-00` … `THM-CUBE-07` (8) | — | — | — | 8 pass | — | — |
| §8.8 Allen exhaustiveness theorem | `TO-THM-01` | — | — | 1 n/e | — | — | — |
| §8.8 Outside the domain | `TO-THM-02` | — | — | 1 n/e | — | — | — |

### Non-conformances (strict xfail)

| Case | Backend | Observed vs. specified |
|---|---|---|
| `CTL-A-c-AF` | `duckdb-dgmquery` | to_sql() ignores the quantifier when a temporal mode is set: FORALL renders as the EXISTS template, so AF/AG/AX at c read 'some sale > 500' (true); expected false (s2 = 300) |
| `CTL-A-c-AG` | `duckdb-dgmquery` | to_sql() ignores the quantifier when a temporal mode is set: FORALL renders as the EXISTS template, so AF/AG/AX at c read 'some sale > 500' (true); expected false (s2 = 300) |
| `CTL-A-c-AX` | `duckdb-dgmquery` | to_sql() ignores the quantifier when a temporal mode is set: FORALL renders as the EXISTS template, so AF/AG/AX at c read 'some sale > 500' (true); expected false (s2 = 300) |
| `PP-STR-01` | `duckdb-dgmquery` | to_sql() ignores the strategy: SHORTEST/MIN_WEIGHT read every instance (s7 = 600 found; the ≺-least instance s6 = 300 expected) |
| `PP-STR-04` | `duckdb-dgmquery` | to_sql() ignores the strategy: SHORTEST/MIN_WEIGHT read every instance (s7 = 600 found; the ≺-least instance s6 = 300 expected) |
| `PP-ALIAS-04` | `duckdb-dgmquery` | a path alias equal to an alias of C is accepted and shadows it (rejected at construction by §3.2) |
| `PP-PROM-01` | `duckdb-dgmquery` | promote: true with ALL is accepted (§3.5 rejects it) |
| `PP-PAST-01` | `duckdb-dgmquery` | past-time modes are accepted on a non-time-ordered path, and FORALL with SINCE is accepted (§4.1 rejects both) |
| `PP-PAST-02` | `duckdb-dgmquery` | past-time modes are accepted on a non-time-ordered path, and FORALL with SINCE is accepted (§4.1 rejects both) |
| `PP-PAST-03` | `duckdb-dgmquery` | past-time modes are accepted on a non-time-ordered path, and FORALL with SINCE is accepted (§4.1 rejects both) |
| `PP-UNTIL-01` | `duckdb-dgmquery` | UNTIL(ψ) renders as EXISTS φ: ψ is never evaluated (strong UNTIL expected false when ψ never holds) |
| `CON-N-01` | `duckdb-dgmquery` | Compare(e, =, NULL) is accepted and emitted as `= NULL` (§4.1: NULL is not a literal) |
| `CON-N-01` | `duckdb-plan-ir` | Compare(e, =, NULL) is accepted and emitted as `= NULL` (§4.1: NULL is not a literal) |
| `AGG-OVER-01` | `duckdb-dgmquery` | aggregates count path fan-out: no `over`, so the ENTITIES default is not implemented (got the PATHS value) |
| `AGG-OVER-01` | `duckdb-plan-ir` | aggregates count path fan-out: no `over`, so the ENTITIES default is not implemented (got the PATHS value) |
| `AGG-OVER-03` | `duckdb-dgmquery` | aggregates count path fan-out: no `over`, so the ENTITIES default is not implemented (got the PATHS value) |
| `AGG-OVER-03` | `duckdb-plan-ir` | aggregates count path fan-out: no `over`, so the ENTITIES default is not implemented (got the PATHS value) |
| `TO-MEETS-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-MET_BY-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-OVERLAPS-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-OVERLAPPED_BY-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-STARTS-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-STARTS-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-STARTED_BY-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-STARTED_BY-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-DURING-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-DURING-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-CONTAINS-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-CONTAINS-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-FINISHES-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-FINISHES-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-FINISHED_BY-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-FINISHED_BY-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-EQUALS-open` | `duckdb-ordering` | a NULL bound is UNKNOWN in the emitted comparison instead of −∞/+∞ (§3.2), so the relation never holds on an open interval |
| `TO-EQUALS-degenerate` | `duckdb-ordering` | start ≥ end is not checked: the relation holds on degenerate intervals, which satisfy no Allen relation (§3.2) |
| `TO-TYPE-01` | `duckdb-ordering` | TemporalOrdering constructors take untyped SQL text and check nothing (TO1–TO3 are construction-time rejections) |
| `TO-TYPE-02` | `duckdb-ordering` | TemporalOrdering constructors take untyped SQL text and check nothing (TO1–TO3 are construction-time rejections) |
| `TO-TYPE-03` | `duckdb-ordering` | TemporalOrdering constructors take untyped SQL text and check nothing (TO1–TO3 are construction-time rejections) |
| `SCD-01` | `duckdb-dgmquery` | no AS_WAS: join_dim(...).as_of(τ) reads version(T, key, τ) for every fact; §2.5 makes AS_WAS the default (South 600, North 400 expected; got North 1 000) |
| `SCD-01` | `duckdb-plan-ir` | no AS_WAS: join_dim(...).as_of(τ) reads version(T, key, τ) for every fact; §2.5 makes AS_WAS the default (South 600, North 400 expected; got North 1 000) |
| `SCD-03` | `duckdb-dgmquery` | VERSION_KEY joined under as_of reads only the stored version valid at τ: S1 (version 101) loses its region under AS_WAS and AS_IS |
| `SCD-03` | `duckdb-plan-ir` | VERSION_KEY joined under as_of reads only the stored version valid at τ: S1 (version 101) loses its region under AS_WAS and AS_IS |
| `SCD-04` | `duckdb-dgmquery` | VERSION_KEY joined under as_of reads only the stored version valid at τ: S1 (version 101) loses its region under AS_WAS and AS_IS |
| `SCD-04` | `duckdb-plan-ir` | VERSION_KEY joined under as_of reads only the stored version valid at τ: S1 (version 101) loses its region under AS_WAS and AS_IS |
| `SCD-05` | `duckdb-dgmquery` | no AS_WAS: join_dim(...).as_of(τ) reads version(T, key, τ) for every fact; §2.5 makes AS_WAS the default (South 600, North 400 expected; got North 1 000) |
| `SCD-05` | `duckdb-plan-ir` | no AS_WAS: join_dim(...).as_of(τ) reads version(T, key, τ) for every fact; §2.5 makes AS_WAS the default (South 600, North 400 expected; got North 1 000) |
| `SCD-V-01` | `duckdb-dgmquery` | a missing version leaves the tuple with NULL attributes (LEFT JOIN): LAX should drop it; TemporalContext is ignored by to_sql() |
| `SCD-V-02` | `duckdb-dgmquery` | STRICT does not reject a missing version: TemporalContext is ignored by to_sql() (got the tuple with a NULL segment) |
| `SCD-V-06` | `duckdb-dgmquery` | join_dim compares valid_from/valid_to after ::DATE casts: a version that starts later on the same day is read at τ (wholesale instead of retail) |
| `SCD-V-06` | `duckdb-plan-ir` | join_dim compares valid_from/valid_to after ::DATE casts: a version that starts later on the same day is read at τ (wholesale instead of retail) |
| `SCD-E-06` | `duckdb-dgmquery` | a NULL valid_from is treated as UNKNOWN (`valid_from <= τ`), not as open (§2.6 valid(e, t)): E9 is missing |
| `SCD-E-06` | `duckdb-plan-ir` | a NULL valid_from is treated as UNKNOWN (`valid_from <= τ`), not as open (§2.6 valid(e, t)): E9 is missing |
| `SCD-TAU-01` | `duckdb-dgmquery` | TemporalContext.default_as_of is ignored by to_sql(): validity is not resolved at τ, so the expired edge E7 → M1 is followed |
| `ADD-02` | `dgm-annotations` | Grain.sum_invalid is true for every SUM over a PERIOD fact, without the AD2 exceptions (GroupBy period, pinned period, LATEST/EARLIEST) |
| `ADD-03` | `dgm-annotations` | Grain.sum_invalid is true for every SUM over a PERIOD fact, without the AD2 exceptions (GroupBy period, pinned period, LATEST/EARLIEST) |
| `ADD-11` | `dgm-annotations` | Grain.sum_invalid is true for every SUM over a PERIOD fact, without the AD2 exceptions (GroupBy period, pinned period, LATEST/EARLIEST) |
| `ADD-12` | `dgm-annotations` | Grain.sum_invalid is true for every SUM over a PERIOD fact, without the AD2 exceptions (GroupBy period, pinned period, LATEST/EARLIEST) |
| `DA-01` | `duckdb-dgmquery` | a two-branch Join is evaluated row to row, not by drill-across: C1 gives 6 sales, 900 revenue, 6 returns (expected 2, 300, 3); 1/0 yields inf, not NULL (§4.1) |
| `DA-01` | `duckdb-plan-ir` | a two-branch Join is evaluated row to row, not by drill-across: C1 gives 6 sales, 900 revenue, 6 returns (expected 2, 300, 3); 1/0 yields inf, not NULL (§4.1) |
| `DA-02` | `duckdb-dgmquery` | a two-branch Join is evaluated row to row, not by drill-across: C1 gives 6 sales, 900 revenue, 6 returns (expected 2, 300, 3); 1/0 yields inf, not NULL (§4.1) |
| `DA-02` | `duckdb-plan-ir` | a two-branch Join is evaluated row to row, not by drill-across: C1 gives 6 sales, 900 revenue, 6 returns (expected 2, 300, 3); 1/0 yields inf, not NULL (§4.1) |
| `AS-06` | `dgm-model` | build_space keeps in Σ|_𝒜 only the kinds the enumeration reads (Conformed, Grain, FactlessFact, MedallionLayer, BridgeSemantics); SCDType(Customer) is dropped although Customer ∈ N_𝒜 |
| `BDD-K-01` | `dgm-bdd` | the BDD is two-valued over every atom: no p⁺/p⁻ encoding for nullable atoms, no Δ_null |
| `BDD-K-06` | `dgm-bdd` | the BDD is two-valued over every atom: no p⁺/p⁻ encoding for nullable atoms, no Δ_null |
| `BDD-K-08` | `dgm-bdd` | the BDD is two-valued over every atom: no p⁺/p⁻ encoding for nullable atoms, no Δ_null |
| `BDD-O-01` | `dgm-bdd` | no order theory Δ_ord: atoms over one expression are unrelated |
| `BDD-O-02` | `dgm-bdd` | no order theory Δ_ord: atoms over one expression are unrelated |
| `BDD-O-06` | `dgm-bdd` | no order theory Δ_ord: atoms over one expression are unrelated |
| `BDD-O-07` | `dgm-bdd` | no order theory Δ_ord: atoms over one expression are unrelated |
| `BDD-O-08` | `dgm-bdd` | no order theory Δ_ord: atoms over one expression are unrelated |
| `BDD-C-01` | `dgm-bdd` | atoms are BDD variables by Python object identity, not by canonical form (§6.1 Variables), and comparisons are not normalised |
| `BDD-C-02` | `dgm-bdd` | atoms are BDD variables by Python object identity, not by canonical form (§6.1 Variables), and comparisons are not normalised |
| `BDD-C-03` | `dgm-bdd` | atoms are BDD variables by Python object identity, not by canonical form (§6.1 Variables), and comparisons are not normalised |
| `BDD-F-01` | `dgm-bdd` | FORALL is eliminated as NOT EXISTS(mode, NOT φ): the temporal mode is not dualised, NOT is Kleene NOT instead of NotTrue, and the REQUIRE_WITNESS guard is missing |
| `BDD-F-02` | `dgm-bdd` | FORALL is eliminated as NOT EXISTS(mode, NOT φ): the temporal mode is not dualised, NOT is Kleene NOT instead of NotTrue, and the REQUIRE_WITNESS guard is missing |
| `BDD-F-03` | `dgm-bdd` | FORALL is eliminated as NOT EXISTS(mode, NOT φ): the temporal mode is not dualised, NOT is Kleene NOT instead of NotTrue, and the REQUIRE_WITNESS guard is missing |

### Refused (fail-closed) and not expressible

| Case | Backend | Status | Reason |
|---|---|---|---|
| `CTL-A-c-EF` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c-EG` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c-EX` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c-AF` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c-AG` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c-AX` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-EF-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-EG-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-EX-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-AF-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-AG-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-AX-REQ` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `CTL-A-c0-EF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a0-EF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a0-EG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a0-EX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a0-AF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a0-AG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a0-AX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-EF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-EG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-EX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-AF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-AG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a1-AX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-EF-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EF-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-EG-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EG-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-EX-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EX-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-AF-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AF-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-AG-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AG-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-AX-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AX-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-B-a2-EF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-U-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-01` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-U-02` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-02` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-U-03` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-03` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-U-04` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-04` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `CTL-U-05` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-05` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct) |
| `PP-FA-03` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-FA-03` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-STR-01` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct) |
| `PP-STR-03` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct) |
| `PP-STR-04` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct) |
| `PP-PROM-01` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses promote=True for every strategy, not by the §3.5 rule |
| `PP-PAST-01` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses every temporal mode, not by the §4.1 rule |
| `PP-PAST-02` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses every temporal mode, not by the §4.1 rule |
| `PP-PAST-03` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses every temporal mode, not by the §4.1 rule |
| `PP-UNTIL-01` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `PP-UNTIL-02` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for every temporal mode (fail-closed; no SQL template) |
| `RJ-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-02` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-03` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-04` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-04` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-05` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-05` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-06` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-06` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `TO-PRECEDES-proper` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-PRECEDES-open` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-PRECEDES-degenerate` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-PRECEDED_BY-proper` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-PRECEDED_BY-open` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-PRECEDED_BY-degenerate` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-TYPE-04` | `duckdb-ordering` | n/e | arguments are untyped SQL text: Date and Timestamp cannot be told apart |
| `TO-THM-01` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `TO-THM-02` | `duckdb-ordering` | n/e | sqldim has no PRECEDES / PRECEDED_BY TemporalOrdering (§3.2 has 13) |
| `SCD-V-01` | `duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `SCD-V-02` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses TemporalContext, not because the version is missing |
| `SCD-TAU-01` | `duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `ADD-04` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `ADD-06` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `ADD-07` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `ADD-08` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `ADD-09` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `ADD-10` | `dgm-annotations` | n/e | sqldim has no Additivity annotation; Grain decides SUM only |
| `BDD-K-05` | `dgm-bdd` | n/e | sqldim has no NotTrue filter node |

### Not covered

| Clause | Why no executable case |
|---|---|
| §4.1 Monitor verdicts | sqldim has no Monitor construct; the cases `MON-*` run against the oracle only |
| §2.4 Additivity annotation, AD3/AD4 | sqldim has no Additivity annotation; only the grain default for SUM is checked (`dgm-annotations`) |
| §4.2 LATEST / EARLIEST | sqldim has no LATEST or EARLIEST TemporalWindow; `TAGG-*` run against the oracle only |
| §5.2.2 DA1–DA3 | constellation well-formedness is enforced only inside the question-lattice enumeration (`lattice/_context.py`), which has no query to submit; `DA-WF-*` run against the oracle only |
| ADR 0001 infinite traces (lasso) | the oracle does not model infinite traces and no backend executes a cyclic temporal PathPred; no case is written |
| §4.1 SINCE / ONCE / PREVIOUSLY evaluation | only their construction-time rejections are covered; no backend evaluates a past-time mode |
| §4.1 typing rules of ScalarPred (Int/String/Enum mixes) | PropRef and ScalarPred carry no property types; a typing error cannot be posed |
| §5.1 Q_delta, §2.5 SCD3/SCD6, TRAILING/ROLLING/YTD at τ | DeltaQuery and the windows render placeholders only; nothing is executable to compare |
| §3.3 K_SHORTEST / MIN_WEIGHT over weighted bridges | covered by the oracle on unweighted hops only; no backend executes a non-ALL strategy |
| §8.7 cone intersection theorem | deferred to #139; under the simple-path reading of §3.4.2 the identity needs a proof of its own (see the doc) |

<!-- END GENERATED -->

## Note for #139

§8.7 proves `G_A* ∩ G_*B = G_AB` by reachability, while §3.4.2 defines the
three subgraphs by *simple* paths. With edges `A→X, X→B, B→n, n→X`, the node
`n` is on a simple path from `A` and on a simple path to `B`, but on no simple
path `A →* n →* B`. The identity therefore needs either the reachability
reading or a separate argument; no case asserts it here.
