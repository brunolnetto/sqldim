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
  anchor-set functions, and `Grain`. A new backend is one `Backend` and its
  expectations; cases and oracle do not change. #138 added PostgreSQL through
  DuckDB, the refusals of the targets without an emitter, write planning
  (Rules 8 and 10) and the sinks. Their capability matrix, per target and
  sink, is `docs/reference/dgm-backend-matrix.md`.
- **Expectations** (`tests/conformance/expectations.py`). A disagreement is
  never fixed in the oracle. It is a strict `xfail` with its reason; a
  wholesale rejection by the implementation is *refused*; a missing construct
  is *not expressible*. All three are checked, so the matrix cannot go stale.

A passing case shows the observed value equals the specified one on that
fixture, and no more. Where a backend's code is wrong in general but happens to
give the right value on a fixture, the case passes; a sibling case exposes it
(before #173, `CTL-A-c-AF` exposed the FORALL templates that `to_sql()`
then rendered for a temporal mode; it now refuses every mode).

Run it with `PYTHONPATH=. pytest tests/conformance -n 4`. Regenerate the
matrix below with `PYTHONPATH=. python -m tests.conformance.matrix --write`;
`test_matrix.py` fails when it is out of date.

## Traceability matrix

Cells count cases per status: *pass*, **non-conf.** (strict xfail), *refused*
(fail-closed rejection), *n/e* (no construct). `—`: the backend does not
implement the area.

<!-- BEGIN GENERATED: tests/conformance/matrix.py -->

381 cases; 191 carry a value stated by the specification, the others are derived by the oracle from the definitions. Every case is checked against the oracle.

| Clause | Cases | `duckdb-dgmquery` | `duckdb-plan-ir` | `duckdb-ordering` | `dgm-bdd` | `dgm-model` | `dgm-annotations` | `pg-duckdb-plan-ir` | `planner-refusals` | `dgm-write-plan` | `sink-duckdb` | `sink-parquet` | `sink-postgresql` | `sink-motherduck` | `sink-delta` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ADR 0001 Empty-domain policy; §4.1 Empty domain | `CTL-A-c0-AF-REQ` … `CTL-B-a2-AX-VAC` (12) | 3 pass, 3 refused, 6 n/e | 3 pass, 3 refused, 6 n/e | — | — | — | — | 3 pass, 3 refused, 6 n/e | 6 refused, 6 n/e | — | — | — | — | — | — |
| ADR 0001 Truth conditions (UNTIL is strong) | `PP-UNTIL-01`, `PP-UNTIL-02` | 2 pass | 2 pass | — | — | — | — | 2 pass | 2 refused | — | — | — | — | — | — |
| ADR 0001 Truth conditions (UNTIL is strong); §4.1 | `CTL-U-01` … `CTL-U-05` (5) | 5 refused | 5 refused | — | — | — | — | 5 refused | 5 refused | — | — | — | — | — | — |
| ADR 0001 Truth conditions; §4.1 Path semantics | `CTL-A-c-EF` … `CTL-B-a2-EX-VAC` (30) | 9 pass, 15 refused, 6 n/e | 9 pass, 15 refused, 6 n/e | — | — | — | — | 9 pass, 15 refused, 6 n/e | 24 refused, 6 n/e | — | — | — | — | — | — |
| ADR 0003 / §5.1 one reference instant τ | `SCD-TAU-01` | 1 refused | 1 refused | — | — | — | — | 1 refused | 1 refused | — | — | — | — | — | — |
| §2.1 Structural invariants (V_S ⊆ D_S × F_S) | `MOD-08`, `MOD-09`, `MOD-10`, `MOD-11` | — | — | — | — | 4 pass | — | — | — | — | — | — | — | — | — |
| §2.4 Additivity AD1–AD4 and grain defaults | `ADD-01` … `ADD-13` (13) | — | — | — | — | — | 9 pass, 4 refused | — | — | — | — | — | — | — | — |
| §2.5 SCD resolution: as-was and as-is | `SCD-01` … `SCD-05` (5) | 5 pass | 5 pass | — | — | — | — | 5 pass | 5 refused | — | — | — | — | — | — |
| §2.5 SCD2 versions; §2.6 M2 (a closed version has valid_to set) | `SINK-CL-01`, `SINK-CL-02`, `SINK-CL-03`, `SINK-CL-04` | — | — | — | — | — | — | — | — | — | 4 pass | 4 **non-conf.** | 4 pass | — | — |
| §2.6 Declared model (well-formedness at construction); §2.4 argument kinds | `MOD-01` … `MOD-07` (7) | — | — | — | — | 7 pass | — | — | — | — | — | — | — | — | — |
| §2.6 Declared model: §2.1 invariants are not construction checks | `MOD-12` | — | — | — | — | 1 pass | — | — | — | — | — | — | — | — | — |
| §2.6 Version and validity at an instant (half-open windows) | `SCD-V-01` … `SCD-E-06` (13) | 11 pass, 2 refused | 11 pass, 2 refused | — | — | — | — | 11 pass, 2 refused | 13 refused | — | — | — | — | — | — |
| §3.2 Alias uniqueness (C ∩ L ≠ ∅ rejected) | `PP-ALIAS-04` | 1 pass | 1 pass | — | — | — | — | 1 pass | 1 refused | — | — | — | — | — | — |
| §3.2 Repeat well-formedness | `CON-R2-01` … `CON-R-02` (7) | 7 pass | 7 pass | — | — | — | — | — | — | — | — | — | — | — | — |
| §3.2 Repetition and RepeatMode; PathJoin under ALL | `RJ-01` … `RJ-06` (6) | 6 refused | 3 pass, 3 refused | — | — | — | — | 3 pass, 3 refused | 6 refused | — | — | — | — | — | — |
| §3.2 TemporalOrdering semantics (Allen, half-open, NULL bounds) | `TO-PRECEDES-proper` … `TO-EQUALS-degenerate` (39) | — | — | 39 pass | — | — | — | — | — | — | — | — | — | — | — |
| §3.2 Typing of TemporalOrdering (TO1–TO4) | `TO-TYPE-01`, `TO-TYPE-02`, `TO-TYPE-03`, `TO-TYPE-04` | — | — | 3 pass, 1 n/e | — | — | — | — | — | — | — | — | — | — | — |
| §3.2 point relations (NULL TimePoint is UNKNOWN) | `TO-BEFORE-pt`, `TO-AFTER-pt`, `TO-CONCURRENT-pt`, `TO-CONCURRENT-tol1` | — | — | 4 pass | — | — | — | — | — | — | — | — | — | — | — |
| §3.3 Strategy (deterministic ≺ order) | `PP-STR-01`, `PP-STR-02`, `PP-STR-03`, `PP-STR-04` | 1 pass, 3 refused | 1 pass, 3 refused | — | — | — | — | 1 pass, 3 refused | 4 refused | — | — | — | — | — | — |
| §3.4.6 Anchor sets (AS1–AS3, N_𝒜, E_𝒜, Σ|_𝒜) | `AS-01` … `AS-GEN-11-edges` (50) | — | — | — | — | 50 pass | — | — | — | — | — | — | — | — | — |
| §3.5 Evaluation contexts (Refs in C ∪ L) | `PP-ALIAS-01`, `PP-ALIAS-02`, `PP-ALIAS-03` | 3 pass | 3 pass | — | — | — | — | 3 pass | 3 refused | — | — | — | — | — | — |
| §3.5 L-promotion | `PP-PROM-01` | 1 pass | 1 pass | — | — | — | — | 1 pass | 1 n/e | — | — | — | — | — | — |
| §4.1 AF and AG are distinct; FORALL elimination | `CTL-G1-AF` … `CTL-G2-NOT-EF` (8) | 8 n/e | 8 n/e | — | — | — | — | 8 n/e | 8 n/e | — | — | — | — | — | — |
| §4.1 Band table (B3: window, QUALIFY); §4.1 NULL rules | `WIN-01`, `WIN-02`, `WIN-03` | 3 pass | 3 pass | — | — | — | — | 1 pass, 2 **non-conf.** | 3 refused | — | — | — | — | — | — |
| §4.1 Band table; §5.2 cross-band Ref rules | `CON-B-01` … `CON-B-10` (10) | 10 pass | 10 pass | — | — | — | — | — | — | — | — | — | — | — | — |
| §4.1 Empty domain; ADR 0001 Empty-domain policy | `PP-EX-02`, `PP-FA-02`, `PP-FA-03`, `PP-NOT-03` | 3 pass, 1 n/e | 3 pass, 1 n/e | — | — | — | — | 3 pass, 1 n/e | 3 refused, 1 n/e | — | — | — | — | — | — |
| §4.1 FORALL elimination (CTL duality, NotTrue, REQUIRE_WITNESS guard) | `BDD-F-01`, `BDD-F-02`, `BDD-F-03` | — | — | — | 3 pass | — | — | — | — | — | — | — | — | — | — |
| §4.1 FORALL with ONCE/SINCE; past-time modes need a time-ordered path | `PP-PAST-01`, `PP-PAST-02`, `PP-PAST-03` | 3 pass | 3 pass | — | — | — | — | 3 pass | 3 n/e | — | — | — | — | — | — |
| §4.1 NULL rules (NULL is not a literal) | `CON-N-01`, `CON-N-02` | 2 pass | 2 pass | — | — | — | — | — | — | — | — | — | — | — | — |
| §4.1 PathPred; FORALL elimination with NotTrue; ADR 0001 | `PP-EX-01` … `PP-NULL-04` (8) | 8 pass | 8 pass | — | — | — | — | 8 pass | 8 refused | — | — | — | — | — | — |
| §4.1 Recorded traces and monitoring verdicts | `MON-01` … `MON-15` (15) | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| §4.2 TemporalWindow LATEST / EARLIEST | `TAGG-01`, `TAGG-02` | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| §4.2 What an aggregate counts (ENTITIES / PATHS) | `AGG-OVER-01`, `AGG-OVER-02`, `AGG-OVER-03`, `AGG-OVER-04` | 4 pass | 4 pass | — | — | — | — | 4 pass | 4 refused | — | — | — | — | — | — |
| §5.2.2 Drill-across (steps 1–3, DA4) | `DA-01`, `DA-02` | 2 pass | 2 refused | — | — | — | — | 2 refused | 2 refused | — | — | — | — | — | — |
| §5.2.2 Well-formedness of a constellation (DA1–DA3) | `DA-WF-01`, `DA-WF-02`, `DA-WF-03`, `DA-WF-04` | 4 pass | 3 pass, 1 refused | — | — | — | — | — | — | — | — | — | — | — | — |
| §6.1 / §8.3 propositional equivalence, tautology, implication | `BDD-P-01` … `BDD-P-GEN-23` (31) | — | — | — | 31 pass | — | — | — | — | — | — | — | — | — | — |
| §6.1 Order theory of atoms (Δ_ord, O1–O6) | `BDD-O-01` … `BDD-O-08` (8) | — | — | — | 8 pass | — | — | — | — | — | — | — | — | — | — |
| §6.1 Three-valued logic (T⁺/T⁻ encoding, Δ_excl, Δ_null) | `BDD-K-01` … `BDD-K-08` (8) | — | — | — | 8 pass | — | — | — | — | — | — | — | — | — | — |
| §6.1 Variables (canonical form of atoms) | `BDD-C-01`, `BDD-C-02`, `BDD-C-03` | — | — | — | 3 pass | — | — | — | — | — | — | — | — | — | — |
| §6.2 Rule 10 with Delta and Iceberg writes (MERGE) | `WP-DELTA-merge`, `WP-ICEBERG-merge` | — | — | — | — | — | — | — | — | 1 pass, 1 **non-conf.** | — | — | — | — | — |
| §6.2 Rule 8 APPEND (INSERT INTO); §2.4 write_mode | `SINK-RT-01`, `SINK-RT-02`, `SINK-RT-03` | — | — | — | — | — | — | — | — | — | 3 pass | 3 **non-conf.** | 3 pass | — | — |
| §6.2 Rule 8 sink-aware write planning; Delta and Iceberg writes | `WP-DUCKDB-target` … `WP-ICEBERG-catalog` (14) | — | — | — | — | — | — | — | — | 11 pass, 3 **non-conf.** | — | — | — | — | — |
| §6.2 Rule 8, §6.4 sink write semantics: statements executed by DuckDB | `SINK-SQL-W`, `SINK-SQL-C` | — | — | — | — | — | — | — | — | — | 2 pass | 2 pass | 2 pass | 2 pass | 2 **non-conf.** |
| §8.2 Propositional completeness (proof by minterm construction) | `THM-PC-0000` … `THM-PC-1111` (16) | — | — | — | 16 pass | — | — | — | — | — | — | — | — | — | — |
| §8.3 A BDD's paths to TRUE are disjoint cubes covering its minterms | `THM-CUBE-00` … `THM-CUBE-07` (8) | — | — | — | 8 pass | — | — | — | — | — | — | — | — | — | — |
| §8.8 Allen exhaustiveness theorem | `TO-THM-01` | — | — | 1 pass | — | — | — | — | — | — | — | — | — | — | — |
| §8.8 Outside the domain | `TO-THM-02` | — | — | 1 pass | — | — | — | — | — | — | — | — | — | — | — |

### Non-conformances (strict xfail)

| Case | Backend | Observed vs. specified |
|---|---|---|
| `SINK-RT-01` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-RT-02` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and a second write() replaces the files of every partition it writes (COPY … OVERWRITE_OR_IGNORE), so the first write's rows there are lost |
| `SINK-RT-03` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-CL-01` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and close_versions() rewrites only the current rows into the partitions, overwriting the historical versions, which are lost |
| `SINK-CL-02` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and close_versions() rewrites only the current rows into the partitions, overwriting the historical versions, which are lost |
| `SINK-CL-03` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and when every current row is closed nothing is written to is_current=true, so the old files stay and each key is read back both open and closed |
| `SINK-CL-04` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-SQL-W` | `sink-delta` | DeltaLakeSink.write() sends `MERGE INTO delta.`<path>``, Spark SQL that DuckDB's parser rejects (and §6.2 allows a Delta write to APPEND only) |
| `SINK-SQL-C` | `sink-delta` | DeltaLakeSink.close_versions() is a no-op: it sends no statement and returns 0, so no version is closed |
| `WP-DELTA-target` | `dgm-write-plan` | Rule 8 plans `INSERT INTO delta_scan`; §6.2 Delta writes: delta_scan is read-only, the write goes to a Delta table attached with TYPE delta |
| `WP-ICEBERG-target` | `dgm-write-plan` | Rule 8 plans `INSERT INTO iceberg_scan`; §6.2 Iceberg writes: iceberg_scan never appears as a write target |
| `WP-ICEBERG-catalog` | `dgm-write-plan` | SinkTarget.ICEBERG carries no catalog, so Rule 8 cannot check that a REST catalog is attached and plans the write anyway (§6.2 Iceberg writes: rejected at planning time) |
| `WP-ICEBERG-merge` | `dgm-write-plan` | Rule 10 plans a MERGE into ICEBERG without the engine version or the table's merge-on-read mode; §6.2 rejects every Iceberg plan but APPEND on an engine version the capability table does not cover |
| `WIN-02` | `pg-duckdb-plan-ir` | DuckDB 1.5.0's postgres extension raises NotImplementedException (PostgresScanSerialize is not serializable) for a SUM window over a PostgreSQL table; the same SQL passes on DuckDB storage |
| `WIN-03` | `pg-duckdb-plan-ir` | DuckDB 1.5.0's postgres extension raises NotImplementedException (PostgresScanSerialize is not serializable) for a SUM window over a PostgreSQL table; the same SQL passes on DuckDB storage |

### Refused (fail-closed) and not expressible

| Case | Backend | Status | Reason |
|---|---|---|---|
| `CTL-A-c-EF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c-EG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c-EX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c-AF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c-AG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c-AX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-EF-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-EG-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-EX-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-AF-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-AG-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-AX-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-A-c0-EF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EF-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EF-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EG-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-EX-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AF-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AG-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-A-c0-AX-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a0-EF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EF` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a0-EG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EG` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a0-EX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-EX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EX` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-EX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a0-AF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AF` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a0-AG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AG` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a0-AX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a0-AX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AX` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a0-AX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-EF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EF` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-EG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EG` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-EX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-EX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EX` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-EX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-AF` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AF` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AF` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AF` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-AG` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AG` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AG` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AG` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a1-AX` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a1-AX` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AX` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a1-AX` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-EF-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EF-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EF-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EF-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-EG-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EG-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EG-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EG-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-EX-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-EX-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EX-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-EX-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-AF-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AF-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AF-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AF-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-AG-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AG-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AG-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AG-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-AX-REQ` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-B-a2-AX-REQ` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AX-REQ` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-B-a2-AX-REQ` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-B-a2-EF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EF-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EF-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EG-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-EX-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AF-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AG-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-B-a2-AX-VAC` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AF` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EG` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-AG` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G1-NOT-EF` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AF` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EG` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-AG` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-G2-NOT-EF` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `CTL-U-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-01` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-01` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-U-02` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-02` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-02` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-U-03` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-03` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-03` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-U-04` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-04` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-04` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `CTL-U-05` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `CTL-U-05` | `duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-05` | `pg-duckdb-plan-ir` | refused | a Repeat inside a PathPred is not emitted (UnsupportedConstruct): a trace has several evaluation positions, and SIMPLE needs node identities a hop does not declare |
| `CTL-U-05` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-EX-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-EX-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-FA-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-FA-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-FA-03` | `duckdb-dgmquery` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-FA-03` | `duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-FA-03` | `pg-duckdb-plan-ir` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-FA-03` | `planner-refusals` | n/e | PathPred has no `on_empty` field, so VACUOUS cannot be requested |
| `PP-NOT-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NOT-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NOT-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NULL-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NULL-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NULL-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-NULL-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-STR-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() renders only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-01` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-01` | `pg-duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-STR-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-STR-03` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() renders only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-03` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-03` | `pg-duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-STR-04` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() renders only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-04` | `duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-04` | `pg-duckdb-plan-ir` | refused | the plan IR executes only strategy ALL (UnsupportedConstruct): ≺ (§3.3) breaks ties by node keys a hop does not declare |
| `PP-STR-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-ALIAS-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-ALIAS-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-ALIAS-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-ALIAS-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-PROM-01` | `planner-refusals` | n/e | the query is rejected while it is built (§3.5, §4.1), before planning: it says nothing about the targets |
| `PP-PAST-01` | `planner-refusals` | n/e | the query is rejected while it is built (§3.5, §4.1), before planning: it says nothing about the targets |
| `PP-PAST-02` | `planner-refusals` | n/e | the query is rejected while it is built (§3.5, §4.1), before planning: it says nothing about the targets |
| `PP-PAST-03` | `planner-refusals` | n/e | the query is rejected while it is built (§3.5, §4.1), before planning: it says nothing about the targets |
| `PP-UNTIL-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `PP-UNTIL-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-02` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-03` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-04` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-04` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-04` | `pg-duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-05` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-05` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-05` | `pg-duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-05` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `RJ-06` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises for a Repeat (plan with DGMPlanner) |
| `RJ-06` | `duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-06` | `pg-duckdb-plan-ir` | refused | the plan IR emits only WALK with 1 ≤ min (SIMPLE/TRAIL need node identities; min = 0 not emitted) |
| `RJ-06` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `AGG-OVER-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `AGG-OVER-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `AGG-OVER-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `AGG-OVER-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `TO-TYPE-04` | `duckdb-ordering` | n/e | arguments are untyped SQL text: Date and Timestamp cannot be told apart |
| `SCD-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-05` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises UnsupportedConstruct for a TemporalContext (fail-closed, as the plan IR) |
| `SCD-V-01` | `duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `SCD-V-01` | `pg-duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `SCD-V-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-02` | `duckdb-dgmquery` | refused | rejected only because DGMQuery.to_sql() refuses TemporalContext, not because the version is missing |
| `SCD-V-02` | `duckdb-plan-ir` | refused | rejected only because the plan IR refuses TemporalContext, not because the version is missing |
| `SCD-V-02` | `pg-duckdb-plan-ir` | refused | rejected only because the plan IR refuses TemporalContext, not because the version is missing |
| `SCD-V-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-05` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-06` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-V-07` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-04` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-05` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-E-06` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `SCD-TAU-01` | `duckdb-dgmquery` | refused | DGMQuery.to_sql() raises UnsupportedConstruct for a TemporalContext (fail-closed, as the plan IR) |
| `SCD-TAU-01` | `duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `SCD-TAU-01` | `pg-duckdb-plan-ir` | refused | the plan IR refuses TemporalContext (node_resolution cannot be stated) |
| `SCD-TAU-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `ADD-04` | `dgm-annotations` | refused | the question lattice raises LatticeUnsupported for a SEMI_ADDITIVE measure: AD2 decides its SUM by the period or entity of each axis, which the enumeration does not decide (fail-closed) |
| `ADD-07` | `dgm-annotations` | refused | the question lattice does not offer AVG over a NON_ADDITIVE measure: AD3 accepts it only with a warning, and the enumeration offers no aggregate that needs one |
| `ADD-09` | `dgm-annotations` | refused | the question lattice raises LatticeUnsupported for a SEMI_ADDITIVE measure: AD2 decides its SUM by the period or entity of each axis, which the enumeration does not decide (fail-closed); rejected for that reason, not by AD2 |
| `ADD-10` | `dgm-annotations` | refused | the question lattice raises LatticeUnsupported for a SEMI_ADDITIVE measure: AD2 decides its SUM by the period or entity of each axis, which the enumeration does not decide (fail-closed) |
| `DA-01` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for a declared constellation: it has no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed) |
| `DA-01` | `pg-duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for a declared constellation: it has no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed) |
| `DA-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `DA-02` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for a declared constellation: it has no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed) |
| `DA-02` | `pg-duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for a declared constellation: it has no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed) |
| `DA-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `DA-WF-04` | `duckdb-plan-ir` | refused | the plan IR raises UnsupportedConstruct for a declared constellation: it has no node for per-branch aggregation aligned by FULL OUTER JOIN (fail-closed) |
| `WIN-01` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `WIN-02` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |
| `WIN-03` | `planner-refusals` | refused | every target without an emitter (SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON, DGM_YAML) raises UnsupportedCapability |

### Not covered

| Clause | Why no executable case |
|---|---|
| §4.1 Monitor verdicts | sqldim has no Monitor construct; the cases `MON-*` run against the oracle only |
| §2.4 AD2 over a declared SEMI_ADDITIVE axis | no implementation decides it: `Grain.rejects_sum` covers the grain default only, and the question lattice refuses every SEMI_ADDITIVE measure (`ADD-04`, `ADD-09`, `ADD-10` are refusals) |
| §4.2 LATEST / EARLIEST | sqldim has no LATEST or EARLIEST TemporalWindow; `TAGG-*` run against the oracle only |
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
