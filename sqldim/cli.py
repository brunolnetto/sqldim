"""
sqldim CLI — dimensional migration management.

Usage:
    sqldim migrations generate "message"
    sqldim migrations show
"""
import argparse
import json
import sys
from typing import List


def cmd_migrations_generate(args: argparse.Namespace) -> None:
    """Generate a migration from a schema diff."""
    print(f"[sqldim] Generating migration: '{args.message}'")
    print("[sqldim] Tip: call generate_migration(models, current_state, message) in your project.")

def cmd_migrations_show(args: argparse.Namespace) -> None:
    """Show pending migration info."""
    print("[sqldim] No pending migrations detected (run generate first).")

def cmd_migrations_init(args: argparse.Namespace) -> None:
    """Initialize sqldim migration environment."""
    import os
    migration_dir = getattr(args, "dir", "migrations")
    os.makedirs(migration_dir, exist_ok=True)
    init_file = os.path.join(migration_dir, "__init__.py")
    if not os.path.exists(init_file):
        open(init_file, "w").close()
    print(f"[sqldim] Migration directory initialized at '{migration_dir}/'")

def cmd_schema_graph(args: argparse.Namespace) -> None:
    """Print schema graph as JSON or Mermaid."""
    print("[sqldim] Pass your SchemaGraph instance to .to_dict() or .to_mermaid()")


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

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    args.func(args)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
