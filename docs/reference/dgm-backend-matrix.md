# DGM backend matrix

Which query targets, execution paths and sinks sqldim supports, and the
executed evidence for each claim. Built for issue #138 (AUD-10) on the
conformance suite of #137 (`docs/reference/dgm-conformance.md`): the same
cases and the same oracle run against every backend; nothing in the oracle
was changed to agree with an implementation.

## How it is built

- **Declarations** live in `sqldim/core/query/dgm/planner/_capabilities.py`.
  Each `QueryTarget` and `SinkTarget` has a `Profile`: per capability,
  *supported*, *partial* or *unsupported*, with a note, plus the emitter that
  produces its statements and the engine version the declaration was verified
  against. `DGMPlanner.plan_query` reads them: a target with no emitter raises
  `UnsupportedCapability` (a subclass of `UnsupportedConstruct`) and is never
  routed to another target. `CypherExporter` and `SPARQLExporter` raise the
  same error for a plan built for another target instead of relabelling its
  text. Two execution paths that are not planner targets have profiles of
  their own: `DGMQuery.to_sql()` and PG(DD).
- **Evidence** comes from the conformance backends (`tests/conformance/runners/`):
  - `pg-duckdb-plan-ir`: the plan IR's SQL executed by DuckDB over tables
    stored in PostgreSQL (DuckDB's postgres extension, the PG(DD) column of
    §6.4). Each case gets a fresh schema on a real server.
  - `planner-refusals`: each query planned for every target without an
    emitter. A case passes only if all of them raise `UnsupportedCapability`.
  - `dgm-write-plan`: planner Rules 8 and 10 per sink.
  - `sink-duckdb`, `sink-parquet`, `sink-postgresql`, `sink-delta`: each
    `SinkAdapter` writes the fixture, closes SCD2 versions and reads the rows
    back from real storage. A sink that refuses an operation with
    `SinkUnsupported` counts as a refusal.
  - `sink-*` `sink_sql` cases, which also run for MotherDuck: every
    statement a sink sends is captured by a recording connection and parsed by
    DuckDB's parser. That checks the statements, not their effect.
- **Consistency** is checked by `tests/conformance/test_backend_matrix.py`:
  - a *supported* capability has only passing cases;
  - an *unsupported* one has only refusals: a case passes there only when the
    specification itself rejects it;
  - every `QueryTarget` and `SinkTarget` has a row;
  - the committed matrix below is the generated one.

A capability with no case that ran is shown as *not executed* or *no case*,
never as passing. Cases of a backend that needs a PostgreSQL server are
skipped locally when `SQLDIM_PG_HOST` is unset, and are an error under CI
(`CI=true`), where the `postgres` service container provides the server.

Regenerate with `PYTHONPATH=. python -m tests.conformance.backend_matrix --write`.

## Matrix

<!-- BEGIN GENERATED: tests/conformance/backend_matrix.py -->

### Backends

| Backend | Declared by | Emitter | Version | Evidence | Executed in CI |
|---|---|---|---|---|---|
| DuckDB, plan IR | `SQL_DUCKDB` | plan IR → DuckDB SQL (#148) | DuckDB 1.5.0 | `duckdb-plan-ir` | yes |
| DuckDB, `DGMQuery.to_sql()` | `DGMQuery.to_sql()` | DGMQuery.to_sql() → DuckDB SQL | DuckDB 1.5.0 | `duckdb-dgmquery` | yes |
| PostgreSQL via DuckDB (PG(DD)) | `SQL_DUCKDB over PostgreSQL (PG(DD))` | plan IR → DuckDB SQL (#148), executed by DuckDB's postgres extension | DuckDB 1.5.0 postgres extension | `pg-duckdb-plan-ir` | yes, against the `postgres:19` service container (`SQLDIM_PG_HOST`); skipped locally without a server |
| MotherDuck | `SQL_MOTHERDUCK` | plan IR → DuckDB SQL (#148), the SQL_DUCKDB emitter | DuckDB 1.5.0 | — | **not executed** — needs a MotherDuck account and `MOTHERDUCK_TOKEN`; running its SQL on local DuckDB would be DuckDB evidence, not MotherDuck |
| PostgreSQL, native SQL | `SQL_POSTGRESQL` | **none** | — | `planner-refusals` | yes — the refusal of every query is executed |
| Cypher | `CYPHER` | **none** | — | `planner-refusals` | yes — the refusal of every query is executed |
| SPARQL | `SPARQL` | **none** | — | `planner-refusals` | yes — the refusal of every query is executed |
| DGM_JSON | `DGM_JSON` | **none** | — | `planner-refusals` | yes — the refusal of every query is executed |
| DGM_YAML | `DGM_YAML` | **none** | — | `planner-refusals` | yes — the refusal of every query is executed |
| DuckDB | `DUCKDB` | DuckDBSink; Rule 8: CREATE TABLE AS / INSERT INTO | DuckDB 1.5.0 | `sink-duckdb`, `dgm-write-plan` | yes — temporary file |
| PostgreSQL | `POSTGRESQL` | PostgreSQLSink through DuckDB's postgres extension | DuckDB 1.5.0 postgres extension | `sink-postgresql`, `dgm-write-plan` | yes, against the `postgres:19` service container (`SQLDIM_PG_HOST`); skipped locally without a server |
| Parquet | `PARQUET` | ParquetSink; Rule 8: COPY TO (FORMAT PARQUET) | DuckDB 1.5.0 | `sink-parquet`, `dgm-write-plan` | yes — temporary directory |
| MotherDuck | `MOTHERDUCK` | MotherDuckSink; Rule 8: CREATE TABLE AS / INSERT INTO | DuckDB 1.5.0 | `sink-motherduck`, `dgm-write-plan` | captured SQL and write plan only; storage **not executed** (needs `MOTHERDUCK_TOKEN`) |
| Delta | `DELTA` | DeltaLakeSink; Rule 8 write plan | DuckDB 1.5.0 delta extension | `sink-delta`, `dgm-write-plan` | yes — an empty Delta table built in a temporary directory (DuckDB 1.5.0 cannot create one), appended to through the `delta` extension |
| Iceberg | `ICEBERG` | IcebergSink (PyIceberg append); Rule 8 write plan | DuckDB 2.0 (documented, §6.2 Iceberg writes) | `dgm-write-plan` | write plan only; storage **not executed**: `IcebergSink` needs `pyiceberg` and an Iceberg REST catalog, and §6.2 writes need DuckDB 2.0; CI has neither (see *Reproducing* below) |

### Query capabilities

Cells: declared support · cases by status (*pass*, **non-conf.** = strict xfail, *refused* = fail-closed rejection, *n/e* = no construct).

| Backend | join | null | temporal | aggregation | cte | window | window_aggregate | pathpred |
|---|---|---|---|---|---|---|---|---|
| DuckDB, plan IR | partial · 6 pass | supported · 10 pass | partial · 16 pass, 3 refused | supported · 6 pass | partial · 6 pass | supported · 13 pass | supported · 2 pass | partial · 60 pass, 21 n/e |
| DuckDB, `DGMQuery.to_sql()` | partial · 6 pass | supported · 10 pass | partial · 16 pass, 3 refused | supported · 6 pass | unsupported · 6 refused | supported · 13 pass | supported · 2 pass | partial · 40 pass, 20 refused, 21 n/e |
| PostgreSQL via DuckDB (PG(DD)) | partial · 6 pass | supported · 8 pass | partial · 16 pass, 3 refused | supported · 6 pass | partial · 6 pass | partial · 1 pass, 2 refused | unsupported · 2 refused | partial · 60 pass, 21 n/e |
| MotherDuck | partial · *not executed* | supported · *not executed* | partial · *not executed* | supported · *not executed* | partial · *not executed* | supported · *not executed* | supported · *not executed* | partial · *not executed* |
| PostgreSQL, native SQL | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 2 refused | unsupported · 56 refused, 25 n/e |
| Cypher | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 2 refused | unsupported · 56 refused, 25 n/e |
| SPARQL | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 2 refused | unsupported · 56 refused, 25 n/e |
| DGM_JSON | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 2 refused | unsupported · 56 refused, 25 n/e |
| DGM_YAML | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 2 refused | unsupported · 56 refused, 25 n/e |

### Export contracts (sinks)

| Backend | append | scd2_close | merge |
|---|---|---|---|
| DuckDB | supported · 6 pass | supported · 5 pass | supported · *no case* |
| PostgreSQL | supported · 6 pass | supported · 5 pass | supported · *no case* |
| Parquet | supported · 6 pass | partial · 5 pass | partial · *no case* |
| MotherDuck | supported · 3 pass | supported · 1 pass | supported · *not executed* |
| Delta | partial · 7 pass | unsupported · 5 refused | unsupported · 1 pass |
| Iceberg | partial · 3 pass | partial · *not executed* | partial · 1 pass |

### Partial and unsupported declarations

| Declared | Where | Note |
|---|---|---|
| partial | `SQL_DUCKDB`: join; `DGMQuery.to_sql()`: join; `SQL_DUCKDB over PostgreSQL (PG(DD))`: join; `SQL_MOTHERDUCK`: join | PathJoin over verb and bridge hops; a constellation is evaluated by drill-across once its fact branches are declared (drill_across, §5.2.2): DGMQuery reads no model and cannot find them itself |
| partial | `SQL_DUCKDB`: temporal; `DGMQuery.to_sql()`: temporal; `SQL_DUCKDB over PostgreSQL (PG(DD))`: temporal; `SQL_MOTHERDUCK`: temporal | as_of with SCD resolution and half-open validity; TemporalContext raises UnsupportedConstruct |
| partial | `SQL_DUCKDB`: cte; `SQL_DUCKDB over PostgreSQL (PG(DD))`: cte; `SQL_MOTHERDUCK`: cte | Repeat over a one-hop body: WALK with 1 ≤ min ≤ max ≤ 8 unrolled; SIMPLE, TRAIL, min = 0 and other bounds as WITH RECURSIVE over the node identities of a declared model (DGMPlanner(model=…)), with MAX_REPEAT_DEPTH reported; a multi-hop body and depth_alias are refused |
| partial | `SQL_DUCKDB`: pathpred; `SQL_DUCKDB over PostgreSQL (PG(DD))`: pathpred; `SQL_MOTHERDUCK`: pathpred | EXISTS / FORALL; strategy ALL, and SHORTEST, K_SHORTEST and MIN_WEIGHT (unweighted hops) over the entity keys of a declared model; EVENTUALLY, GLOBALLY and UNTIL over one evaluation position per instance, or over the iterations of one Repeat with a declared model; NEXT over the first hop; several or no positions on a Repeat-free path, past-time modes, a mode with a strategy and on_empty are refused |
| unsupported | `DGMQuery.to_sql()`: cte | a Repeat raises UnsupportedConstruct; plan it with DGMPlanner |
| partial | `DGMQuery.to_sql()`: pathpred | EXISTS / FORALL, strategy ALL; EVENTUALLY, GLOBALLY and UNTIL over one evaluation position per instance; NEXT over the first hop; it reads no model, so a mode over a Repeat's iterations and other strategies are refused, as are past-time modes and on_empty |
| partial | `SQL_DUCKDB over PostgreSQL (PG(DD))`: window | ROW_NUMBER with QUALIFY is verified; RANK and DENSE_RANK are not |
| unsupported | `SQL_DUCKDB over PostgreSQL (PG(DD))`: window_aggregate | a SUM window over a PostgreSQL scan raises NotImplementedException (PostgresScanSerialize) in DuckDB 1.5.0's postgres extension; DGMPlanner(execution_profile=POSTGRESQL_VIA_DUCKDB) refuses it before execution |
| unsupported | `SQL_POSTGRESQL`: join, null, temporal, aggregation, cte, window, window_aggregate, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; PostgreSQL data is queried with SQL_DUCKDB over a database attached with DuckDB's postgres extension (PG(DD), §6.4) |
| unsupported | `CYPHER`: join, null, temporal, aggregation, cte, window, window_aggregate, pathpred; `SPARQL`: join, null, temporal, aggregation, cte, window, window_aggregate, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability (§6.4 cells unbuilt) |
| unsupported | `DGM_JSON`: join, null, temporal, aggregation, cte, window, window_aggregate, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; DGMJSONExporter serialises a plan |
| unsupported | `DGM_YAML`: join, null, temporal, aggregation, cte, window, window_aggregate, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; DGMYAMLExporter serialises a plan |
| partial | `PARQUET`: scd2_close | the whole table is rewritten into a staging directory that replaces it, keeping every version; local filesystem only (a remote URI raises SinkUnsupported), and not atomic for a concurrent reader |
| partial | `PARQUET`: merge | Rule 10 plans a MERGE WritePlan |
| partial | `DELTA`: append | §6.2: APPEND only, by ATTACH … (TYPE delta) and INSERT INTO the attached table; DuckDB 1.5.0 cannot create the table |
| unsupported | `DELTA`: scd2_close | §6.2: the delta extension rejects UPDATE; DeltaLakeSink.close_versions raises SinkUnsupported |
| unsupported | `DELTA`: merge | §6.2 Delta writes: MERGE is rejected (Rule 10) |
| partial | `ICEBERG`: append | §6.2: INSERT INTO catalog.schema.table on the attached REST catalog declared with IcebergCatalog; without one Rule 8 rejects the plan |
| partial | `ICEBERG`: scd2_close | partition overwrite via PyIceberg |
| partial | `ICEBERG`: merge | §6.2: Rule 10 plans it only for a merge-on-read table on a documented engine version (DuckDB 2.0, 2.0.1) |

### Divergences found by the matrix (strict xfail)

Those of the DuckDB paths are listed in `dgm-conformance.md`.

| Case | Backend | Observed vs. specified |
|---|---|---|

<!-- END GENERATED -->

## Reproducing the backends CI does not run

- **PostgreSQL** (run in CI). Set `SQLDIM_PG_HOST`, `SQLDIM_PG_PORT`,
  `SQLDIM_PG_USER`, `SQLDIM_PG_PASSWORD` and optionally `SQLDIM_PG_DBNAME`
  (default `postgres`), then run `PYTHONPATH=. pytest tests/conformance -k
  "pg-duckdb or sink-postgresql"`. The runner needs to create and drop
  schemas.
- **MotherDuck.** It needs an account and `MOTHERDUCK_TOKEN`. The query
  target uses the SQL_DUCKDB emitter, so the reproducer would be the
  `duckdb-plan-ir` backend with a session that attaches `md:<db>`. Running
  the same SQL on local DuckDB is DuckDB evidence and is not counted for
  MotherDuck.
- **Delta.** DuckDB 1.5.0 cannot create a Delta table ("Delta tables do not
  support creating tables", §6.2 *Delta writes*), and `deltalake` is an
  optional extra. The `sink-delta` harness therefore writes an empty table by
  hand (one empty Parquet file and its first commit in `_delta_log`). It then
  appends through the `delta` extension, which CI installs with
  `INSTALL delta`.
- **Iceberg.** It needs `pyiceberg` and an Iceberg REST catalog. §6.2
  *Iceberg writes* states the precondition: a catalog attached with
  `ATTACH 'warehouse' AS … (TYPE iceberg, SECRET …, URI …)`, writes by
  three-part name, and its capability table was read for DuckDB 2.0. A
  reproducer runs a REST catalog, for example the `apache/iceberg-rest-fixture`
  image, attaches it from DuckDB 2.0 and runs the `sink_roundtrip` and
  `sink_close` cases. Until then `IcebergSink` is evidenced only by its write
  plan.
