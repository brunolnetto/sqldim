"""Session-scoped fixtures for eval tests."""

from __future__ import annotations

import importlib
import unittest.mock as mock
from typing import Generator

import duckdb
import pytest


@pytest.fixture(scope="session")
def domain_pipeline_cache(
    _dataset_pipeline_cache: dict[str, str],
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[dict[str, str], None, None]:
    """Return the pre-built domain pipeline cache for drift structural tests.

    Eight of the nine drift-suite domains come directly from the session-wide
    ``_dataset_pipeline_cache``.  The ``observability`` domain is built here
    lazily, using *patched* build_pipeline functions so that the 12-domain
    telemetry collection run reuses cached file snapshots instead of
    re-running expensive Faker builds.  This reduces the observability build
    from ~44s to ~3s.
    """
    cache = dict(_dataset_pipeline_cache)

    if "observability" not in cache:
        tmpdir = tmp_path_factory.mktemp("domain_pipeline_obs")
        db_path = str(tmpdir / "observability.duckdb")

        # For every domain already in the cache, replace its build_pipeline
        # with a fast version that restores from the cached .duckdb file.
        def _make_fast_builder(cached_db_path: str):
            def _fast_build(con: duckdb.DuckDBPyConnection, *, seed: int = 42) -> None:
                con.execute(f"ATTACH '{cached_db_path}' AS _c (READ_ONLY)")
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_catalog = '_c' AND table_schema = 'main'"
                ).fetchall()
                for (tbl,) in tables:
                    con.execute(f"CREATE TABLE {tbl} AS SELECT * FROM _c.main.{tbl}")
                con.execute("DETACH _c")

            return _fast_build

        patches: list[mock._patch] = []  # type: ignore[type-arg]
        for domain, cached_path in cache.items():
            mod_path = (
                f"sqldim.application.datasets.domains.{domain}.pipeline.builder"
            )
            try:
                mod = importlib.import_module(mod_path)
                p = mock.patch.object(mod, "build_pipeline", _make_fast_builder(cached_path))
                patches.append(p)
                p.start()
            except (ImportError, AttributeError):
                pass

        try:
            obs_mod = importlib.import_module(
                "sqldim.application.datasets.domains.observability.pipeline.builder"
            )
            con = duckdb.connect(":memory:")
            try:
                obs_mod.build_pipeline(con)
                con.execute(f"ATTACH '{db_path}' AS _out")
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_catalog = 'memory' AND table_schema = 'main'"
                ).fetchall()
                for (tbl,) in tables:
                    con.execute(f"CREATE TABLE _out.main.{tbl} AS SELECT * FROM main.{tbl}")
                con.execute("CHECKPOINT _out")
                con.execute("DETACH _out")
            finally:
                con.close()
        finally:
            for p in patches:
                p.stop()

        cache["observability"] = db_path

    yield cache

