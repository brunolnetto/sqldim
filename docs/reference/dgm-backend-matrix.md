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
  - `sink-duckdb`, `sink-parquet`, `sink-postgresql`: each `SinkAdapter`
    writes the fixture, closes SCD2 versions and reads the rows back from real
    storage.
  - `sink-*` `sink_sql` cases, which also run for MotherDuck and Delta: every
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
| Delta | `DELTA` | DeltaLakeSink; Rule 8 write plan | DuckDB 1.5.0 delta extension | `sink-delta`, `dgm-write-plan` | captured SQL and write plan only; storage **not executed**: DuckDB 1.5.0 cannot create a Delta table and `deltalake` is an optional extra, so CI has no Delta table to write to |
| Iceberg | `ICEBERG` | IcebergSink (PyIceberg append); Rule 8 write plan | DuckDB 2.0 (documented, §6.2 Iceberg writes) | `dgm-write-plan` | write plan only; storage **not executed**: `IcebergSink` needs `pyiceberg` and an Iceberg REST catalog, and §6.2 writes need DuckDB 2.0; CI has neither (see *Reproducing* below) |

### Query capabilities

Cells: declared support · cases by status (*pass*, **non-conf.** = strict xfail, *refused* = fail-closed rejection, *n/e* = no construct).

| Backend | join | null | temporal | aggregation | cte | window | pathpred |
|---|---|---|---|---|---|---|---|
| DuckDB, plan IR | partial · 5 pass, 1 **non-conf.** | supported · 10 pass | partial · 16 pass, 3 refused | partial · 5 pass, 1 **non-conf.** | partial · 3 pass, 3 refused | supported · 13 pass | partial · 20 pass, 40 refused, 21 n/e |
| DuckDB, `DGMQuery.to_sql()` | partial · 5 pass, 1 **non-conf.** | supported · 10 pass | partial · 16 pass, 3 refused | partial · 5 pass, 1 **non-conf.** | unsupported · 6 refused | supported · 13 pass | partial · 20 pass, 40 refused, 21 n/e |
| PostgreSQL via DuckDB (PG(DD)) | partial · 5 pass, 1 **non-conf.** | supported · 8 pass | partial · 16 pass, 3 refused | partial · 5 pass, 1 **non-conf.** | partial · 3 pass, 3 refused | partial · 1 pass, 2 **non-conf.** | partial · 20 pass, 40 refused, 21 n/e |
| MotherDuck | partial · *not executed* | supported · *not executed* | partial · *not executed* | partial · *not executed* | partial · *not executed* | supported · *not executed* | partial · *not executed* |
| PostgreSQL, native SQL | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 56 refused, 25 n/e |
| Cypher | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 56 refused, 25 n/e |
| SPARQL | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 56 refused, 25 n/e |
| DGM_JSON | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 56 refused, 25 n/e |
| DGM_YAML | unsupported · 6 refused | unsupported · 8 refused | unsupported · 19 refused | unsupported · 6 refused | unsupported · 6 refused | unsupported · 3 refused | unsupported · 56 refused, 25 n/e |

### Export contracts (sinks)

| Backend | append | scd2_close | merge |
|---|---|---|---|
| DuckDB | supported · 6 pass | supported · 5 pass | supported · *no case* |
| PostgreSQL | supported · 6 pass | supported · 5 pass | supported · *no case* |
| Parquet | partial · 3 pass, 3 **non-conf.** | partial · 1 pass, 4 **non-conf.** | partial · *no case* |
| MotherDuck | supported · 3 pass | supported · 1 pass | supported · *not executed* |
| Delta | partial · 2 pass, 2 **non-conf.** | partial · 1 **non-conf.** | unsupported · 1 pass |
| Iceberg | partial · 1 pass, 2 **non-conf.** | partial · *not executed* | partial · 1 **non-conf.** |

### Partial and unsupported declarations

| Declared | Where | Note |
|---|---|---|
| partial | `SQL_DUCKDB`: join; `DGMQuery.to_sql()`: join; `SQL_DUCKDB over PostgreSQL (PG(DD))`: join; `SQL_MOTHERDUCK`: join | PathJoin over verb and bridge hops; a two-branch Join is evaluated row to row, not by drill-across (§5.2.2) |
| partial | `SQL_DUCKDB`: temporal; `DGMQuery.to_sql()`: temporal; `SQL_DUCKDB over PostgreSQL (PG(DD))`: temporal; `SQL_MOTHERDUCK`: temporal | as_of with SCD resolution and half-open validity; TemporalContext raises UnsupportedConstruct |
| partial | `SQL_DUCKDB`: aggregation; `DGMQuery.to_sql()`: aggregation; `SQL_DUCKDB over PostgreSQL (PG(DD))`: aggregation; `SQL_MOTHERDUCK`: aggregation | over = ENTITIES / PATHS; drill-across is not implemented (DA-01) |
| partial | `SQL_DUCKDB`: cte; `SQL_DUCKDB over PostgreSQL (PG(DD))`: cte; `SQL_MOTHERDUCK`: cte | Repeat in WALK mode with 1 ≤ min; SIMPLE, TRAIL and min = 0 are refused |
| partial | `SQL_DUCKDB`: pathpred; `DGMQuery.to_sql()`: pathpred; `SQL_DUCKDB over PostgreSQL (PG(DD))`: pathpred; `SQL_MOTHERDUCK`: pathpred | EXISTS / FORALL without a temporal mode, strategy ALL; temporal modes, other strategies and on_empty are refused |
| unsupported | `DGMQuery.to_sql()`: cte | a Repeat raises UnsupportedConstruct; plan it with DGMPlanner |
| partial | `SQL_DUCKDB over PostgreSQL (PG(DD))`: window | ROW_NUMBER with QUALIFY works; a SUM window over a PostgreSQL scan raises NotImplementedException in DuckDB 1.5.0's postgres extension |
| unsupported | `SQL_POSTGRESQL`: join, null, temporal, aggregation, cte, window, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; PostgreSQL data is queried with SQL_DUCKDB over a database attached with DuckDB's postgres extension (PG(DD), §6.4) |
| unsupported | `CYPHER`: join, null, temporal, aggregation, cte, window, pathpred; `SPARQL`: join, null, temporal, aggregation, cte, window, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability (§6.4 cells unbuilt) |
| unsupported | `DGM_JSON`: join, null, temporal, aggregation, cte, window, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; DGMJSONExporter serialises a plan |
| unsupported | `DGM_YAML`: join, null, temporal, aggregation, cte, window, pathpred | no emitter: DGMPlanner.plan_query raises UnsupportedCapability; DGMYAMLExporter serialises a plan |
| partial | `PARQUET`: append | COPY TO with OVERWRITE_OR_IGNORE: a later write replaces the partitions it writes; is_current reads back as VARCHAR |
| partial | `PARQUET`: scd2_close | rewrites the current rows only; historical versions are lost |
| partial | `PARQUET`: merge | Rule 10 plans a MERGE WritePlan |
| partial | `DELTA`: append | §6.2: APPEND only, by INSERT INTO an attached Delta table; Rule 8 plans INSERT INTO delta_scan and DeltaLakeSink.write sends a Spark MERGE INTO |
| partial | `DELTA`: scd2_close | DeltaLakeSink.close_versions is a no-op |
| unsupported | `DELTA`: merge | §6.2 Delta writes: MERGE is rejected (Rule 10) |
| partial | `ICEBERG`: append | §6.2: INSERT INTO catalog.schema.table on an attached REST catalog; Rule 8 plans INSERT INTO iceberg_scan and SinkTarget.ICEBERG names no catalog |
| partial | `ICEBERG`: scd2_close | partition overwrite via PyIceberg |
| partial | `ICEBERG`: merge | §6.2: merge-on-read tables only, DuckDB 2.0 documented |

### Divergences found by the matrix (strict xfail)

Those of the DuckDB paths are listed in `dgm-conformance.md`.

| Case | Backend | Observed vs. specified |
|---|---|---|
| `DA-01` | `pg-duckdb-plan-ir` | a two-branch Join is evaluated row to row, not by drill-across; under the ENTITIES default the per-branch counts and sums agree on this fixture, but 1/0 yields inf, not NULL (§4.1) |
| `WIN-02` | `pg-duckdb-plan-ir` | DuckDB 1.5.0's postgres extension raises NotImplementedException (PostgresScanSerialize is not serializable) for a SUM window over a PostgreSQL table; the same SQL passes on DuckDB storage |
| `WIN-03` | `pg-duckdb-plan-ir` | DuckDB 1.5.0's postgres extension raises NotImplementedException (PostgresScanSerialize is not serializable) for a SUM window over a PostgreSQL table; the same SQL passes on DuckDB storage |
| `WP-DELTA-target` | `dgm-write-plan` | Rule 8 plans `INSERT INTO delta_scan`; §6.2 Delta writes: delta_scan is read-only, the write goes to a Delta table attached with TYPE delta |
| `WP-ICEBERG-target` | `dgm-write-plan` | Rule 8 plans `INSERT INTO iceberg_scan`; §6.2 Iceberg writes: iceberg_scan never appears as a write target |
| `WP-ICEBERG-catalog` | `dgm-write-plan` | SinkTarget.ICEBERG carries no catalog, so Rule 8 cannot check that a REST catalog is attached and plans the write anyway (§6.2 Iceberg writes: rejected at planning time) |
| `WP-ICEBERG-merge` | `dgm-write-plan` | Rule 10 plans a MERGE into ICEBERG without the engine version or the table's merge-on-read mode; §6.2 rejects every Iceberg plan but APPEND on an engine version the capability table does not cover |
| `SINK-RT-01` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-RT-02` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and a second write() replaces the files of every partition it writes (COPY … OVERWRITE_OR_IGNORE), so the first write's rows there are lost |
| `SINK-RT-03` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-CL-01` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and close_versions() rewrites only the current rows into the partitions, overwriting the historical versions, which are lost |
| `SINK-CL-02` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and close_versions() rewrites only the current rows into the partitions, overwriting the historical versions, which are lost |
| `SINK-CL-03` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written; and when every current row is closed nothing is written to is_current=true, so the old files stay and each key is read back both open and closed |
| `SINK-CL-04` | `sink-parquet` | is_current, ParquetSink's default hive partition column, is read back as VARCHAR ('true'/'false'), not as the BOOLEAN written |
| `SINK-SQL-W` | `sink-delta` | DeltaLakeSink.write() sends `MERGE INTO delta.`<path>``, Spark SQL that DuckDB's parser rejects (and §6.2 allows a Delta write to APPEND only) |
| `SINK-SQL-C` | `sink-delta` | DeltaLakeSink.close_versions() is a no-op: it sends no statement and returns 0, so no version is closed |

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
- **Delta.** It needs a Delta table to write to. DuckDB 1.5.0 cannot create
  one ("Delta tables do not support creating tables", §6.2 *Delta writes*),
  and `deltalake` is an optional extra. `DeltaLakeSink.write` would fail
  anyway: its statement does not parse (`SINK-SQL-W`).
- **Iceberg.** It needs `pyiceberg` and an Iceberg REST catalog. §6.2
  *Iceberg writes* states the precondition: a catalog attached with
  `ATTACH 'warehouse' AS … (TYPE iceberg, SECRET …, URI …)`, writes by
  three-part name, and its capability table was read for DuckDB 2.0. A
  reproducer runs a REST catalog, for example the `apache/iceberg-rest-fixture`
  image, attaches it from DuckDB 2.0 and runs the `sink_roundtrip` and
  `sink_close` cases. Until then `IcebergSink` is evidenced only by its write
  plan.
