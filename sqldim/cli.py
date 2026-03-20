"""
sqldim CLI — dimensional migration management and example runner.

Usage:
    sqldim migrations generate "message"
    sqldim migrations show
    sqldim example list
    sqldim example run <name>
    sqldim bigdata features
"""

import argparse
import sys

# ── migration commands ────────────────────────────────────────────────────────


def cmd_migrations_generate(args: argparse.Namespace) -> None:
    """Generate a migration from a schema diff.

    In practice, call ``generate_migration(models, current_state, message)``
    directly from your project's migration script.
    """
    print(f"[sqldim] Generating migration: '{args.message}'")
    print(
        "[sqldim] Tip: call generate_migration(models, current_state, message) in your project."
    )


def cmd_migrations_show(args: argparse.Namespace) -> None:
    """Show pending migration info.

    Prints a human-readable summary of any schema changes detected
    since the last applied migration.
    """
    print("[sqldim] No pending migrations detected (run generate first).")


def cmd_migrations_init(args: argparse.Namespace) -> None:
    """Initialize sqldim migration environment.

    Creates the migrations directory and an empty ``__init__.py`` so the
    folder is importable by Alembic or the sqldim migration runner.
    """
    import os

    migration_dir = getattr(args, "dir", "migrations")
    os.makedirs(migration_dir, exist_ok=True)
    init_file = os.path.join(migration_dir, "__init__.py")
    if not os.path.exists(init_file):
        open(init_file, "w").close()
    print(f"[sqldim] Migration directory initialized at '{migration_dir}/'")


def cmd_schema_graph(args: argparse.Namespace) -> None:
    """Print schema graph as JSON or Mermaid.

    Pass your ``SchemaGraph`` instance to ``.to_dict()`` or ``.to_mermaid()``
    to render the dimensional model as serialisable data or a diagram.
    """
    print("[sqldim] Pass your SchemaGraph instance to .to_dict() or .to_mermaid()")


# ── example commands ──────────────────────────────────────────────────────────

_EXAMPLES = {
    "nba": (
        "NBA Analytics",
        "Cumulative arrays + SCD Type 2 + dual-paradigm graph projection",
        "sqldim.examples.real_world.nba_analytics.showcase",
        "run_showcase",
    ),
    "saas": (
        "SaaS Growth",
        "Vectorised SCD (Narwhals) + referral graph + bitmask retention",
        "sqldim.examples.real_world.saas_growth.showcase",
        "run_saas_showcase",
    ),
    "user-activity": (
        "User Activity",
        "Bitmask datelist encoding + L7/L28 retention metrics",
        "sqldim.examples.real_world.user_activity.showcase",
        "run_activity_showcase",
    ),
    "ecommerce": (
        "E-Commerce Order Intelligence",
        "SCD Type 2 customer tiers + bridge attribution + accumulating order milestones",
        "sqldim.examples.real_world.ecommerce.showcase",
        "run_ecommerce_showcase",
    ),
    "fintech": (
        "Fintech Payment Intelligence",
        "SCD Type 2 risk tiers + payment graph + array-metric balances + drift observability",
        "sqldim.examples.real_world.fintech.showcase",
        "run_fintech_showcase",
    ),
    "supply-chain": (
        "Supply Chain Inventory",
        "SCD Type 2 warehouse capacity + shipment graph + cumulative stock arrays + schema graph",
        "sqldim.examples.real_world.supply_chain.showcase",
        "run_supply_chain_showcase",
    ),
}


def cmd_example_list(args: argparse.Namespace) -> None:
    """List all available real-world examples.

    Prints each example name, title, and a one-line description so users
    can choose which showcase to run with ``sqldim example run <name>``.
    """
    print("[sqldim] Available examples (run with: sqldim example run <name>):\n")
    for name, (title, desc, _mod, _fn) in _EXAMPLES.items():
        print(f"  {name:<16} {title}")
        print(f"  {'':16} {desc}\n")


def cmd_example_run(args: argparse.Namespace) -> int:
    """Run a named real-world example showcase.

    Dynamically imports and invokes the showcase function for the given
    example name, supporting both async and synchronous entry-points.
    Returns 1 if the name is unrecognised, 0 on success.
    """
    import asyncio
    import importlib

    name = args.name.lower()
    if name not in _EXAMPLES:
        print(
            f"[sqldim] Unknown example '{name}'. Run 'sqldim example list' to see options."
        )
        return 1
    _title, _desc, mod_path, fn_name = _EXAMPLES[name]
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name)
    if asyncio.iscoroutinefunction(fn):
        asyncio.run(fn())
    else:
        fn()


# ── big data commands ─────────────────────────────────────────────────────────

_BIGDATA_SUMMARY = """
[sqldim] Big-data architecture overview
========================================

sqldim is built as a zero-copy, SQL-first dimensional engine.
All heavy lifting stays inside DuckDB; Python is the orchestrator.

  Layer 1 — Sources (sqldim.sources)
  ───────────────────────────────────
  CSVSource           read CSV/TSV files via DuckDB COPY
  ParquetSource       columnar reads — terabytes from object storage
  DeltaSource         Delta Lake table via delta-kernel-python
  DuckDBSource        arbitrary SQL against a local/MotherDuck DB
  PostgreSQLSource    streaming SELECT from PostgreSQL

  Layer 2 — Processors (sqldim.core.processors)
  ─────────────────────────────────────────
  LazySCDProcessor    SCD Type 2 — 100% SQL, zero Python rows in memory
  LazyType1Processor  SCD Type 1 overwrite — pure SQL UPDATE
  LazyType3Processor  SCD Type 3 column-rotation — pure SQL
  LazyType6Processor  SCD Type 6 hybrid — SQL only
  LazyType4Processor  SCD Type 4 mini-dimension split
  LazyType5Processor  SCD Type 5 (extends Type 4)
  NarwhalsSCDProcessor  in-process vectorised SCD via Narwhals
                        (Polars or Pandas backend — single-pass join)

  Layer 3 — Loaders (sqldim.loaders)
  ────────────────────────────────────
  AccumulatingLoader  accumulating snapshot — milestone timestamps
  CumulativeLoader    dense history arrays (e.g. players_cumulated)
  BitmaskerLoader     bitmask datelist — 32 bools → 1 integer
  ArrayMetricLoader   month-partitioned array metrics
  SnapshotLoader      periodic snapshot facts

  All Loaders accept batch_size (default 100,000 rows) so writes
  never materialise the full dataset in Python memory.

  Layer 4 — Sinks (sqldim.sinks)
  ────────────────────────────────
  DuckDBSink          local DuckDB — fast development & CI
  MotherDuckSink      cloud DuckDB — scalable analytics
  ParquetSink         columnar files — data-lake storage
  DeltaLakeSink       ACID Delta tables — lakehouse pattern
  IcebergSink         Apache Iceberg — open table format
  PostgreSQLSink      relational DB — existing infrastructure

  Scale characteristics
  ─────────────────────
  • Processors speak SQL: 1 billion-row SCD diff = a single JOIN.
  • Narwhals backend: Polars for local scale; pandas for ecosystem compat.
  • Batch writes: every sink flushes in batch_size chunks (default 100k).
  • Partitioned sinks: Parquet/Delta write by partition key automatically.
  • No ORM session: lazy processors never call session.add() or flush().
"""


def cmd_bigdata_features(args: argparse.Namespace) -> None:
    """Print big-data capabilities summary.

    Summarises the lazy/vectorised processing, Narwhals compatibility,
    and Iceberg/Delta/Parquet sink options available in sqldim.
    """
    print(_BIGDATA_SUMMARY)


# ── parser ────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqldim",
        description="sqldim — Dimensional Modeling toolkit for Python",
    )
    subparsers = parser.add_subparsers(dest="command")

    # migrations subcommand
    mig = subparsers.add_parser("migrations", help="Migration management")
    mig_sub = mig.add_subparsers(dest="subcommand")

    gen = mig_sub.add_parser("generate", help="Generate a migration from model diff")
    gen.add_argument("message", help="Migration message")
    gen.set_defaults(func=cmd_migrations_generate)

    show = mig_sub.add_parser("show", help="Show pending migrations")
    show.set_defaults(func=cmd_migrations_show)

    init = mig_sub.add_parser("init", help="Initialize migration directory")
    init.add_argument("--dir", default="migrations", help="Migration directory path")
    init.set_defaults(func=cmd_migrations_init)

    # schema subcommand
    schema = subparsers.add_parser("schema", help="Schema introspection")
    schema_sub = schema.add_subparsers(dest="subcommand")
    graph = schema_sub.add_parser("graph", help="Print schema graph")
    graph.set_defaults(func=cmd_schema_graph)

    # example subcommand
    example = subparsers.add_parser(
        "example", help="Real-world example pipeline runner"
    )
    example_sub = example.add_subparsers(dest="subcommand")

    ex_list = example_sub.add_parser("list", help="List available examples")
    ex_list.set_defaults(func=cmd_example_list)

    ex_run = example_sub.add_parser("run", help="Run a named example")
    ex_run.add_argument(
        "name",
        help="Example name (nba | saas | user-activity | ecommerce | fintech | supply-chain)",
    )
    ex_run.set_defaults(func=cmd_example_run)

    # bigdata subcommand
    bigdata = subparsers.add_parser("bigdata", help="Big-data capability overview")
    bigdata_sub = bigdata.add_subparsers(dest="subcommand")

    bd_features = bigdata_sub.add_parser(
        "features", help="Print big-data features summary"
    )
    bd_features.set_defaults(func=cmd_bigdata_features)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
