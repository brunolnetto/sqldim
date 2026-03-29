"""Session-scoped fixtures shared across the entire test suite.

Pre-import heavy AI SDK modules once at session start.

The pydantic-ai model factories in ``sqldim.core.query.dgm.nl._agents``
use lazy imports (``from pydantic_ai.models.X import XModel`` inside the
factory body) to avoid loading AI SDKs at package import time.  The
downside is that the first test in the session to exercise each factory
pays the full cold-import cost:

  * ``pydantic_ai.models.openai``  +  ``openai`` SDK     ≈  25–30 s
  * ``pydantic_ai.models.anthropic`` + ``anthropic`` SDK  ≈  30–35 s
  * ``pydantic_ai.models.google``  + ``google.genai``     ≈  90–100 s
  * ``pydantic_ai.models.groq``    + ``groq`` SDK         ≈  8–12 s

``tests/cli/test_pipeline_sources.py::TestMakeModel`` is the first
collector to exercise these factories (``tests/cli/`` sorts before
``tests/query/``), so those tests appear as extreme outliers.

The ``_preimport_ai_sdks`` fixture below is session-scoped and runs
automatically before any test is collected or executed.  The import cost
is paid once during pytest session setup and is not attributed to any
individual test, removing the outliers entirely.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", autouse=True)
def _preimport_ai_sdks() -> None:
    """Pre-import heavy pydantic-ai model modules once at session start.

    Each ``try/except ImportError`` guard ensures the fixture is safe even
    when optional AI extras are not installed in the test environment.
    """
    # openai / Ollama-compatible provider
    try:
        from pydantic_ai.models.openai import OpenAIChatModel  # noqa: F401
        from pydantic_ai.providers.openai import OpenAIProvider  # noqa: F401
    except ImportError:
        pass

    # Anthropic
    try:
        from pydantic_ai.models.anthropic import AnthropicModel  # noqa: F401
    except ImportError:
        pass

    # Google Gemini (google.genai — the heaviest import by far)
    try:
        from pydantic_ai.models.google import GoogleModel  # noqa: F401
    except ImportError:
        pass

    # Groq
    try:
        from pydantic_ai.models.groq import GroqModel  # noqa: F401
    except ImportError:
        pass

    # Mistral (optional extra — skip silently when not installed)
    try:
        from pydantic_ai.models.mistral import MistralModel  # noqa: F401
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# Session-scoped dataset pipeline cache
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _dataset_pipeline_cache(tmp_path_factory):
    """Build each named dataset pipeline once and cache as a ``.duckdb`` file.

    Pipelines are built on an **in-memory** DuckDB connection (avoiding slow
    WAL/checkpoint writes during data generation) and then exported to a
    temporary file via ``ATTACH + CREATE TABLE AS SELECT``.  This is ~5× faster
    than building directly against a file-backed connection.

    Returns a dict mapping dataset name → path to its cached ``.duckdb`` file.
    Used by :func:`_patch_dataset_pipeline_source` to skip repeated Faker builds.
    The ``observability`` domain is excluded here and built lazily by
    :func:`domain_pipeline_cache` using patched (fast) builders.
    """
    import importlib
    import duckdb

    datasets_to_cache = [
        # Standard cached datasets (used broadly across the test suite)
        "ecommerce",
        "nba_analytics",
        "fintech",
        "supply_chain",
        "saas_growth",
        "user_activity",
        # Drift-suite-only domains — pre-built here so domain_pipeline_cache
        # can build observability quickly (patching these fast builders).
        # observability is intentionally excluded: it rebuilds all 12 domains
        # internally and is best handled lazily in domain_pipeline_cache.
        "retail",
        "enterprise",
    ]

    cache: dict[str, str] = {}
    tmpdir = tmp_path_factory.mktemp("dataset_pipeline_cache")

    # Pre-import Faker once in the main thread so domain builders' lazy
    # ``from faker import Faker`` calls are a no-op (avoids import lock wait).
    try:
        import faker as _  # noqa: F401
    except ImportError:
        pass

    def _build_one(ds_name: str) -> tuple[str, str] | None:
        try:
            mod = importlib.import_module(
                f"sqldim.application.datasets.domains.{ds_name}.pipeline.builder"
            )
            db_path = str(tmpdir / f"{ds_name}.duckdb")
            # Build in-memory to avoid WAL write overhead, then export once.
            # For the retail domain, temporarily reduce data-generation constants
            # to cut the build from ~12s (200 customers × 90 days) to ~6s.
            # Structural tests only check table/column existence, not row counts.
            _saved: dict[str, int] = {}
            if ds_name == "retail":
                for attr, val in [("_N_CUSTOMERS", 50), ("_N_DAYS", 30)]:
                    if hasattr(mod, attr):
                        _saved[attr] = getattr(mod, attr)
                        setattr(mod, attr, val)
            con = duckdb.connect(":memory:")
            try:
                mod.build_pipeline(con)
                con.execute(f"ATTACH '{db_path}' AS _out")
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_catalog = 'memory' AND table_schema = 'main'"
                ).fetchall()
                for (tbl,) in tables:
                    con.execute(
                        f"CREATE TABLE _out.main.{tbl} AS SELECT * FROM main.{tbl}"
                    )
                con.execute("CHECKPOINT _out")
                con.execute("DETACH _out")
            finally:
                con.close()
                for attr, orig in _saved.items():
                    setattr(mod, attr, orig)
            return ds_name, db_path
        except Exception:  # noqa: BLE001
            return None  # domain not available or build failed — skip caching

    for ds_name in datasets_to_cache:
        result = _build_one(ds_name)
        if result is not None:
            name, db_path = result
            cache[name] = db_path

    return cache


@pytest.fixture(scope="session", autouse=True)
def _patch_dataset_pipeline_source(_dataset_pipeline_cache):
    """Patch DatasetPipelineSource.setup/teardown to use cached pipeline DBs.

    For any dataset whose name matches a key in *_dataset_pipeline_cache*,
    ``setup()`` attaches the pre-built file and copies tables to a fresh
    ``:memory:`` connection instead of running the full Faker-heavy builder.
    Teardown simply closes the connection.  Tests that need the live builder
    (e.g. pipeline integration tests) should explicitly skip or override.
    """
    import duckdb
    from unittest import mock
    from sqldim.application._pipeline_sources import DatasetPipelineSource

    cache = _dataset_pipeline_cache

    def _fast_setup(self: DatasetPipelineSource) -> None:
        # Identify dataset by class name prefix to find cache key
        ds_name = getattr(self._dataset, "__class__", None)
        module = getattr(ds_name, "__module__", "") if ds_name else ""
        # Extract domain name from module path
        # e.g. "sqldim.application.datasets.domains.ecommerce.dataset"
        matched_key: str | None = None
        for key in cache:
            if key in module:
                matched_key = key
                break

        if matched_key is None:
            # Not cached — fall back to real setup
            self._con = duckdb.connect(":memory:")
            self._dataset.setup(self._con)
            for src, table in self._dataset.sources:
                try:
                    already_loaded = self._con.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0] > 0
                    if already_loaded:
                        continue
                    snap_sql = src.snapshot().as_sql(self._con)
                    self._con.execute(
                        f"INSERT INTO {table} BY NAME SELECT * FROM ({snap_sql})"
                    )
                except NotImplementedError:
                    pass
            return

        db_path = cache[matched_key]
        self._con = duckdb.connect(":memory:")
        self._con.execute(f"ATTACH '{db_path}' AS _ds_cache (READ_ONLY)")
        tables = self._con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = '_ds_cache' AND table_schema = 'main'"
        ).fetchall()
        for (tbl,) in tables:
            self._con.execute(
                f"CREATE TABLE {tbl} AS SELECT * FROM _ds_cache.main.{tbl}"
            )
        self._con.execute("DETACH _ds_cache")

    def _fast_teardown(self: DatasetPipelineSource) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    with mock.patch.object(DatasetPipelineSource, "setup", _fast_setup):
        with mock.patch.object(DatasetPipelineSource, "teardown", _fast_teardown):
            yield
