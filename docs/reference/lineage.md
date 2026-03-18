# Lineage Reference

Structured lineage event emission for pipeline runs. Opt-in — zero dependencies by default, with an optional OpenLineage backend.

## Installation

```bash
# Zero-dep: console emitter only (always available)
pip install sqldim

# OpenLineage integration
pip install sqldim[lineage]
```

## Event Model

### LineageEvent

```python
from sqldim.lineage import LineageEvent, RunState, DatasetRef

event = LineageEvent(
    run_id="run-abc-123",
    job_name="bronze_to_silver",
    inputs=[DatasetRef(name="bronze.raw_orders", namespace="sqldim")],
    outputs=[DatasetRef(name="silver.orders", namespace="sqldim")],
    state=RunState.COMPLETE,
    facets={"processing_time_ms": 340},
)
```

| Field | Type | Description |
|---|---|---|
| `run_id` | `str` | Unique identifier for this run |
| `job_name` | `str` | Logical job name (e.g., gate ID or loader model name) |
| `inputs` | `list[DatasetRef]` | Upstream datasets consumed |
| `outputs` | `list[DatasetRef]` | Downstream datasets produced |
| `state` | `RunState` | Current lifecycle state |
| `facets` | `dict` | Arbitrary metadata (timing, row counts, etc.) |

### RunState

| State | When emitted |
|---|---|
| `START` | Gate or loader begins processing |
| `RUNNING` | Intermediate progress (optional) |
| `COMPLETE` | Processing finished successfully |
| `FAIL` | Processing failed (exception or gate violation) |
| `ABORT` | Processing was cancelled |

### DatasetRef

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Dataset name (table, topic, file) |
| `namespace` | `str \| None` | Logical namespace (default: `None`) |
| `facets` | `dict` | Dataset-level metadata (schema, partition info, etc.) |

## Emitters

### ConsoleLineageEmitter

Zero-dependency. Writes each event as a JSON line to stderr.

```python
from sqldim.lineage import ConsoleLineageEmitter, LineageEvent, RunState

emitter = ConsoleLineageEmitter()
emitter.emit(LineageEvent(
    run_id="run-1",
    job_name="test_job",
    inputs=[],
    outputs=[],
    state=RunState.COMPLETE,
))
```

### OpenLineageEmitter

Requires `sqldim[lineage]`. Translates internal `LineageEvent` to the official [OpenLineage `RunEvent`](https://openlineage.io/docs/spec/running/) protocol and POSTs to a lineage server (e.g., Marquez, DataHub).

```python
from sqldim.lineage import OpenLineageEmitter

emitter = OpenLineageEmitter(
    url="http://marquez:5000",
    namespace="sqldim",         # default namespace for DatasetRefs without one
    api_key=None,               # optional auth header
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `url` | `str` | *(required)* | Lineage server base URL |
| `namespace` | `str` | `"default"` | Default namespace for datasets |
| `api_key` | `str \| None` | `None` | If set, sent as `Authorization: Bearer <api_key>` |

**Error handling**: If the HTTP request fails, `OpenLineageEmitter` logs a warning but does **not** raise — lineage emission is non-blocking to avoid halting pipelines.

## Integration Points

### QualityGate

Pass `lineage_emitter` to `QualityGate.run()`:

```python
from sqldim.lineage import ConsoleLineageEmitter, DatasetRef
from sqldim.contracts import QualityGate
from sqldim.contracts.layer import Layer

gate = QualityGate("bronze_to_silver", Layer.BRONZE, Layer.SILVER)
result = gate.run(
    view="bronze.raw_orders",
    lineage_emitter=ConsoleLineageEmitter(),
    job_name="bronze_to_silver",
    inputs=[DatasetRef(name="bronze.raw_orders")],
    outputs=[DatasetRef(name="silver.orders")],
)
# Automatically emits: START → COMPLETE (or FAIL on violation)
```

### DimensionalLoader

Pass `lineage_emitter` to `DimensionalLoader.run()` for per-model pipeline tracking:

```python
from sqldim.lineage import ConsoleLineageEmitter, DatasetRef
from sqldim.loader import DimensionalLoader

loader = DimensionalLoader(con)
loader.run(
    model=dim_customer,
    source=view,
    lineage_emitter=ConsoleLineageEmitter(),
    inputs=[DatasetRef(name="bronze.customers")],
    outputs=[DatasetRef(name="silver.dim_customer")],
)
```

## See Also

- [Data Contracts](../features/data_contracts.md) — lineage integration with quality gates
- [Observability](../features/observability.md) — tracing and metrics exporters
- [Config Reference](config.md) — `SqldimConfig` global settings
