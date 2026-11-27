# CLI Reference

The `sqldim` command has three groups:

- `exercise` runs the bundled examples and the benchmark suite.
- `nl` holds the natural-language commands. They are demonstrations, not
  a capability (see [below](#nl--natural-language-demonstrations-only)).
- `ops` prints migration and schema guidance. It does not inspect or
  change your schema.

Run `sqldim` with no arguments, or `sqldim <group> --help`, for the help
summary. The help text is the reference for options; this page describes what
each command does.

```
sqldim <group> <command> [subcommand] [options]
```

---

## `exercise` — examples and benchmarks

### `exercise example list`

Lists the real-world pipelines and feature showcases. They are discovered from
`sqldim.application.examples.real_world.*` and
`sqldim.application.examples.features.*` through their `EXAMPLE_METADATA`.

```bash
sqldim exercise example list
```

### `exercise example run <name>`

Runs one example by the name `example list` prints, for example `ecommerce`,
`nba`, `saas` or `dgm`. The exit code is `1` when the name is unknown.

```bash
sqldim exercise example run ecommerce
```

### `exercise bench list` / `exercise bench run`

Lists or runs the application benchmark groups (A–T). The options of
`bench run` are `--max-tier`, `--report`, `--out-dir`, `--source`,
`--compare-last`, `--fail-on-regression` and `--fail-on-breach`; see
`sqldim exercise bench run --help` and the
[benchmark suite design](../architecture/benchmark-suite.md).

---

## `nl` — natural-language demonstrations only

The §11.10 NL pipeline is not implemented yet (#143). Every `nl` command
answers a data question only through the heuristic demo path: a keyword
planner, template or LLM-written SQL and nodes that confirm automatically.
That path is a demonstration, not a capability (D47,
[ADR 0013](../adr/0013-explicit-demo-mode.md)). It runs only when selected,
and every result is marked demo and is never a contract result. The
[capability page](capabilities.md) lists, from the code, what is missing and
which supervisor tools are available.

| Command | Without `--demo` | With `--demo` |
|---|---|---|
| `nl ask "<q>" --dataset DS` | reports `NOT_IMPLEMENTED` | the heuristic NL graph |
| `nl ask "<q>" --mode supervisor` | with a model, the supervisor plans over the available contract tools; `dgm_query` is `NOT_IMPLEMENTED` | keyword planner and the demo `dgm_query` |
| `nl chat` | as `--mode supervisor`, interactively | as `--mode supervisor`, interactively |
| `nl showtime` | — (always demo) | guided walkthrough of curated questions |
| `nl evals run`, `nl evals drift` | — (always demo) | eval suites over the heuristic NL graph; the scores are not conformance evidence |
| `nl evals run --tool T …` | lists the per-tool eval cases; they are not executed | — |
| `nl evals list` | lists the eval cases | — |

The model comes from `--model-provider` (`ollama`, `openai`, `anthropic`,
`gemini`, `groq`, `mistral`) and `--model`. A provider reads its API key from
its environment variable. Nothing selects a model or the demo path on your
behalf.

---

## `ops` — guidance only

| Command | What it does |
|---|---|
| `ops migrations init [--dir DIR]` | creates `DIR` (default `migrations`) with an empty `__init__.py` |
| `ops migrations generate "message"` | prints how to call `generate_migration(models, current_state, message)`; generates nothing |
| `ops migrations show` | prints guidance; does not inspect the schema |
| `ops schema graph` | prints how to render a `SchemaGraph` (`.to_dict()`, `.to_mermaid()`); renders nothing |

To diff a schema, call `generate_migration` from your project. To render a
schema graph, build a `SchemaGraph` from your models (see the
[schema graph reference](schema_graph.md)).

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | No command given, an unknown example name, a failed eval case, or an NL turn or supervisor run that did not complete |
| `2` | `nl ask` in single-dataset mode without `--demo`: the NL pipeline is `NOT_IMPLEMENTED` |

## See also

- [Capabilities](capabilities.md) — what is implemented, generated from the code
- [Getting Started](../getting-started.md) — end-to-end walkthrough
- [Example datasets](datasets.md) — the datasets the examples and `nl` commands use
