# CLI Reference

The `sqldim` command-line tool provides migration management, schema introspection, example pipelines, and a big-data capability overview.

## Installation

The CLI is installed automatically with sqldim:

```bash
pip install sqldim
```

## Usage

```
sqldim <command> [subcommand] [options]
```

Run `sqldim` with no arguments to print the help summary.

---

## `migrations` — Migration Management

### `migrations init`

Initialize a migration directory in your project.

```bash
sqldim migrations init
sqldim migrations init --dir custom_migrations
```

| Option | Default | Description |
|---|---|---|
| `--dir` | `migrations` | Path to the migration directory |

Creates the target directory (if absent) and an `__init__.py` so it is importable by Alembic or the sqldim migration runner.

### `migrations generate "message"`

Generate a migration from a schema diff.

```bash
sqldim migrations generate "add customer dimension"
```

| Argument | Description |
|---|---|
| `message` | Human-readable migration message |

> **Note:** This command prints guidance. In practice, call `generate_migration(models, current_state, message)` directly from your project's migration script for actual diff detection.

### `migrations show`

Show pending migration info.

```bash
sqldim migrations show
```

Prints a summary of any schema changes detected since the last applied migration.

---

## `schema` — Schema Introspection

### `schema graph`

Print the schema graph metadata.

```bash
sqldim schema graph
```

> **Note:** This prints a guidance hint. In practice, build a `SchemaGraph` from your models and call `.to_dict()` or `.to_mermaid()` to render the dimensional model as JSON or a Mermaid ER diagram.

---

## `example` — Example Pipeline Runner

### `example list`

List all available real-world examples.

```bash
sqldim example list
```

Output:

```
[sqldim] Available examples (run with: sqldim example run <name>):

  nba              NBA Analytics
                   Cumulative arrays + SCD Type 2 + dual-paradigm graph projection

  saas             SaaS Growth
                   Vectorised SCD (Narwhals) + referral graph + bitmask retention

  user-activity    User Activity
                   Bitmask datelist encoding + L7/L28 retention metrics
```

### `example run <name>`

Run a named real-world example showcase.

```bash
sqldim example run nba
sqldim example run saas
sqldim example run user-activity
```

| Argument | Description |
|---|---|
| `name` | Example name: `nba`, `saas`, or `user-activity` |

Dynamically imports and invokes the showcase function, supporting both async and sync entry-points. Returns exit code `1` if the name is unrecognised, `0` on success.

---

## `bigdata` — Big-Data Capabilities

### `bigdata features`

Print a summary of sqldim's big-data architecture.

```bash
sqldim bigdata features
```

Outputs a four-layer overview:

1. **Sources** — CSV, Parquet, Delta Lake, DuckDB, PostgreSQL
2. **Processors** — Lazy SCD processors (Types 1–6) and Narwhals vectorised SCD
3. **Loaders** — Accumulating, Cumulative, Bitmask, ArrayMetric, Snapshot (all batch-aware)
4. **Sinks** — DuckDB, MotherDuck, Parquet, Delta Lake, Iceberg, PostgreSQL

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Unknown subcommand or unrecognised example name |

## See Also

- [Getting Started](../getting-started.md) — end-to-end walkthrough
- [Fact Types Reference](fact_types.md) — fact table patterns referenced by `schema graph`
- [Lazy Loaders](../guides/lazy_loaders.md) — DuckDB-native loaders listed under `bigdata features`
- [Sinks Reference](sinks.md) — sink details listed under `bigdata features`
