# Contributing to sqldim

Thank you for your interest in contributing! This guide will help you get set up and ship a PR.

## Prerequisites

- **Python 3.11+**
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Setup

```bash
# Clone the repo
git clone https://github.com/brunolnetto/sqldim.git
cd sqldim

# Install with dev dependencies (using uv)
uv sync --group dev

# Or with pip
pip install -e ".[all]"
pip install -r <(cat pyproject.toml | grep -A20 '\[dependency-groups\]' | grep '"' | sed 's/.*"\(.*\)".*/\1/')
```

## Running Tests

```bash
# All tests
uv run pytest

# With coverage
uv run pytest --cov=sqldim --cov-report=term-missing

# Specific module
uv run pytest tests/core/test_scd.py -v

# Async tests (auto-handled by pytest-asyncio)
uv run pytest tests/ -k "async"
```

## Project Structure

```
sqldim/
├── core/
│   ├── kimball/       # Dimensions, Facts, SCD, Fields
│   ├── graph/         # Vertex/Edge models, TraversalEngine
│   └── loaders/       # DimensionalLoader, Cumulative, Bitmask, etc.
├── sources/           # CSV, Parquet, Kafka readers
├── sinks/             # DuckDB, PostgreSQL, Iceberg, Delta writers
├── contracts/         # Data quality & SLA enforcement
├── medallion/         # Bronze/Silver/Gold orchestration
├── migrations/        # Schema evolution (Alembic)
└── session.py         # AsyncDimensionalSession
```

## Coding Standards

- **Type hints** on all public functions and methods
- **Docstrings** for public APIs — use Google-style (Args, Returns, Raises)
- **Imports**: stdlib → third-party → local, grouped with blank lines
- **No unused imports or dead code** — remove before committing
- **Existing comments**: never modify or remove comments in code you're not changing
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes

## PR Workflow

1. **Branch**: Create a feature branch from `main` (`feat/...`, `fix/...`, `docs/...`)
2. **Develop**: Make changes, write tests for new behavior
3. **Test**: Run the full test suite locally — all tests must pass
4. **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat: add bitmask loader for L7/L28 tracking`
   - `fix: resolve SK race condition in concurrent loads`
   - `docs: add getting-started tutorial`
   - `refactor: extract hash strategy from SCD processor`
5. **Push & PR**: Open a PR against `main`, describe what changed and why

## Reporting Issues

When filing an issue, please include:
- **What** you tried to do
- **What** happened (error message, unexpected behavior)
- **Minimal reproduction** (code snippet or data sample)
- **Environment**: Python version, OS, backend (DuckDB/Postgres/etc.)

## Questions?

Feel free to open a GitHub Discussion for questions that aren't bug reports or feature requests. We're happy to help!
