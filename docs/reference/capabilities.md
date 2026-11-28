# What sqldim implements

This page says what the DGM part of sqldim can do today. It is generated
from the declarations the code acts on, not kept by hand:

- the natural-language (NL) interface: `build_nl_graph` and the `sqldim nl`
  commands;
- the supervisor tool registry: `build_registry` in
  `sqldim/core/query/dgm/nl/supervisor/_tools.py`;
- the query-target and sink profiles: `sqldim/core/query/dgm/planner/_capabilities.py`;
- the claims register: `tests/conformance/claims.py`.

It was built for issue #140 (AUD-12). A declaration is not evidence. The
evidence is on three other pages, and each one is also generated and checked
for staleness:

| Question | Page |
|---|---|
| Does the core follow the specification, case by case? | [DGM semantic conformance](dgm-conformance.md) |
| Which backends and sinks were executed, and which cases passed? | [DGM backend matrix](dgm-backend-matrix.md) |
| What backs each theorem or bound of the specification? | [DGM claims register](dgm-claims.md) |

## How to read it

- **available / supported**: implemented, with a contract and tests. A
  *supported* backend capability has only passing conformance cases.
- **partial**: implemented for part of the construct. Whatever is outside
  that part is refused with a typed error (`UnsupportedConstruct`,
  `UnsupportedCapability`). It is never approximated. The backend matrix
  says, per cell, which part that is.
- **demo only**: a demonstration, not a capability (D47, ADR 0013). It runs
  only when the caller selects it for that run (`--demo` or `demo=True`).
  Every result it produces carries `demo = true` and is never a contract
  result. Nothing selects it automatically.
- **unavailable / unsupported**: not implemented. The call fails closed with
  `NOT_IMPLEMENTED` or `UNSUPPORTED` and names what is missing. It never
  returns a placeholder or an empty result (ADR 0012).
- **Theoretical claims** carry the strength of their evidence. *Holds on the
  checked domain* means an exhaustive check on a bounded domain, not a proof.
  *Rests on the citation* means a published result that nothing here checks.
  *Not evidenced* means the specification states the claim, and may give a
  proof, but nothing checks it independently.

### The NL interface is a demonstration

`sqldim nl ask`, `sqldim nl chat`, `sqldim nl showtime` and the eval suites
under `sqldim nl evals` answer a data question only through the heuristic
demo path. That path is a keyword planner, template or LLM-written SQL and
nodes that confirm automatically, and it is not the §11.10 pipeline, which is
tracked in #143. Without `--demo`, `nl ask` reports `NOT_IMPLEMENTED`.
`nl chat` and `nl ask --mode supervisor` with a model plan only over the
contract tools listed as available below; `dgm_query` is not one of them.
`showtime` and the eval suites always run in demo mode. Eval scores are not
conformance evidence.

## Generated

Regenerate with `PYTHONPATH=. python -m tests.conformance.capabilities --write`.
`tests/conformance/test_capabilities.py` fails when this block is stale.

<!-- BEGIN GENERATED: tests/conformance/capabilities.py -->

### Natural-language interface (§11)

**Demo only.** `build_nl_graph()` raises `NLPipelineUnavailable` (`NOT_IMPLEMENTED`) unless the caller selects the demo mode (`demo=True`, `--demo`). The §11.10 pipeline is missing:

- dag_construction by DGM synthesis over the question lattice (D26; the current node writes template or LLM-authored SQL)
- the three §11.10 deterministic nodes absent from this graph (#143)
- human confirmation in confirmation_loop and clarification (the current nodes auto-confirm)
- gate cost estimates from the planner (§6.3; the current gate uses the SQL text length)

| Command | Help |
|---|---|
| `sqldim nl ask` | Ask a natural-language question — demo only: answering it needs --demo; without it the NL pipeline reports NOT_IMPLEMENTED |
| `sqldim nl chat` | Interactive REPL over the supervisor — demo only: answering a data question needs --demo (dgm_query is NOT_IMPLEMENTED without it) |
| `sqldim nl showtime` | Guided demo walkthrough of the DGM Supervisor; always runs in demo mode (D47) |
| `sqldim nl evals` | NL interface evaluation suite — runs the heuristic NL graph in demo mode; its scores are not conformance evidence (D47) |

### Supervisor tools (§12.3)

Columns are `build_registry()` with the default configuration, with a knowledge base (`kb=`), with a knowledge base and a move graph (`kb=`, `moves=`), and with `demo=True`. A tool that is unavailable returns `NOT_IMPLEMENTED` without running.

| Tool | Default | With a knowledge base | With a knowledge base and a move graph | Demo mode | Implementation |
|---|---|---|---|---|---|
| `dgm_query` | **unavailable** | **unavailable** | **unavailable** | demo only | missing: run_ask_from_source: the non-interactive NL turn of §11.10 |
| `dgm_what_can_i_answer` | available | available | available | available | `sqldim.core.query.dgm.nl.supervisor._tools_lattice.WhatCanIAnswerTool` |
| `dgm_correlate` | **unavailable** | **unavailable** | **unavailable** | **unavailable** | missing: drill-across over conformed keys (§5.2.2, Rule 12) |
| `dgm_explore_from` | **unavailable** | **unavailable** | **unavailable** | **unavailable** | missing: structural moves from a question key (§8.16.6) |
| `dgm_sensitivity_sweep` | **unavailable** | **unavailable** | **unavailable** | **unavailable** | missing: contract dgm_query execution of the source question (#153), to run it over Π(q) (§8.15) |
| `dgm_validate_invariant` | available | available | available | available | `sqldim.core.query.dgm.nl.supervisor._tools_invariants.ValidateInvariantTool` |
| `dgm_store_finding` | **unavailable** | available | available | **unavailable** | `sqldim.core.query.dgm.nl.supervisor._tools_kb.StoreFindingTool` |
| `dgm_retrieve_related` | **unavailable** | **unavailable** | available | **unavailable** | `sqldim.core.query.dgm.nl.supervisor._tools_kb.RetrieveRelatedTool` |
| `dgm_map_analytical_space` | available | available | available | available | `sqldim.core.query.dgm.nl.supervisor._tools_space.MapAnalyticalSpaceTool` |

### Query targets (§6.4)

Declared support per capability, from the profiles the planner reads. A target with no emitter raises `UnsupportedCapability` and is never routed to another target. *Partial* is explained per cell, with the cases that ran, in `dgm-backend-matrix.md`.

| Backend | Declared by | Emitter | join | null | temporal | aggregation | cte | window | window_aggregate | pathpred | Executed in CI |
|---|---|---|---|---|---|---|---|---|---|---|---|
| DuckDB, plan IR | `SQL_DUCKDB` | plan IR → DuckDB SQL (#148) | partial | supported | partial | supported | partial | supported | supported | partial | yes |
| DuckDB, `DGMQuery.to_sql()` | `DGMQuery.to_sql()` | DGMQuery.to_sql() → DuckDB SQL | partial | supported | partial | supported | unsupported | supported | supported | partial | yes |
| PostgreSQL via DuckDB (PG(DD)) | `SQL_DUCKDB over PostgreSQL (PG(DD))` | plan IR → DuckDB SQL (#148), executed by DuckDB's postgres extension | partial | supported | partial | supported | partial | partial | unsupported | partial | yes, against the `postgres:19` service container (`SQLDIM_PG_HOST`); skipped locally without a server |
| MotherDuck | `SQL_MOTHERDUCK` | plan IR → DuckDB SQL (#148), the SQL_DUCKDB emitter | partial | supported | partial | supported | partial | supported | supported | partial | **not executed** — needs a MotherDuck account and `MOTHERDUCK_TOKEN`; running its SQL on local DuckDB would be DuckDB evidence, not MotherDuck |
| PostgreSQL, native SQL | `SQL_POSTGRESQL` | **none** | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | yes — the refusal of every query is executed |
| Cypher | `CYPHER` | **none** | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | yes — the refusal of every query is executed |
| SPARQL | `SPARQL` | **none** | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | yes — the refusal of every query is executed |
| DGM_JSON | `DGM_JSON` | **none** | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | yes — the refusal of every query is executed |
| DGM_YAML | `DGM_YAML` | **none** | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | yes — the refusal of every query is executed |

### Write sinks (§6.2 Rule 8)

| Backend | Declared by | Emitter | append | scd2_close | merge | Executed in CI |
|---|---|---|---|---|---|---|
| DuckDB | `DUCKDB` | DuckDBSink; Rule 8: CREATE TABLE AS / INSERT INTO | supported | supported | supported | yes — temporary file |
| PostgreSQL | `POSTGRESQL` | PostgreSQLSink through DuckDB's postgres extension | supported | supported | supported | yes, against the `postgres:19` service container (`SQLDIM_PG_HOST`); skipped locally without a server |
| Parquet | `PARQUET` | ParquetSink; Rule 8: COPY TO (FORMAT PARQUET) | supported | partial | partial | yes — temporary directory |
| MotherDuck | `MOTHERDUCK` | MotherDuckSink; Rule 8: CREATE TABLE AS / INSERT INTO | supported | supported | supported | captured SQL and write plan only; storage **not executed** (needs `MOTHERDUCK_TOKEN`) |
| Delta | `DELTA` | DeltaLakeSink; Rule 8 write plan | partial | unsupported | unsupported | yes — an empty Delta table built in a temporary directory (DuckDB 1.5.0 cannot create one), appended to through the `delta` extension |
| Iceberg | `ICEBERG` | IcebergSink (PyIceberg append); Rule 8 write plan | partial | partial | partial | write plan only; storage **not executed**: `IcebergSink` needs `pyiceberg` and an Iceberg REST catalog, and §6.2 writes need DuckDB 2.0; CI has neither (see *Reproducing* in `dgm-backend-matrix.md`) |

### Theoretical claims

| Status | Claims |
|---|---|
| holds on the checked domain | `CLM-3.3-ORDER`, `CLM-3.4.2-CONE-SUBGRAPH`, `CLM-4.1-DUAL`, `CLM-ADR1-VALUES`, `CLM-6.3-HOEFF`, `CLM-8.2-PC`, `CLM-8.3-CUBE`, `CLM-8.4-DIAMOND`, `CLM-8.5-COLLAPSE`, `CLM-8.6-SIMPLE-BOUND`, `CLM-8.6-PATTERNS`, `CLM-8.7-CONE`, `CLM-8.8-ALLEN`, `CLM-8.10-ENTROPY`, `CLM-8.11-ENUM`, `CLM-8.13-WALK`, `CLM-8.13-D4`, `CLM-8.13-TRANSL`, `CLM-8.16.6-NBHD`, `CLM-ADR7-METRIC` |
| **counterexample** | — |
| rests on the citation | `CLM-8.13-PTIME` |
| not evidenced | `CLM-3.4.5-PROJ`, `CLM-8.1-KRIPKE`, `CLM-8.9-HIER`, `CLM-8.11-QC`, `CLM-8.13-T1`, `CLM-8.14-CANON`, `CLM-8.15-PARAM` |

<!-- END GENERATED -->
