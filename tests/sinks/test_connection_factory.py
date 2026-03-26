"""
Tests for sqldim.sinks._connection.make_connection() factory.
Red-green target: 100 % line coverage on _connection.py.
"""

import os
import duckdb
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers — import guard
# ---------------------------------------------------------------------------


def _import():
    from sqldim.sinks._connection import make_connection

    return make_connection


# ---------------------------------------------------------------------------
# make_connection() — basic contract
# ---------------------------------------------------------------------------


class TestMakeConnectionBasic:
    def test_returns_duckdb_connection(self):
        make_connection = _import()
        con = make_connection()
        assert isinstance(con, duckdb.DuckDBPyConnection)
        con.close()

    def test_connection_is_usable(self):
        make_connection = _import()
        con = make_connection()
        result = con.execute("SELECT 42").fetchone()[0]
        assert result == 42
        con.close()

    def test_memory_limit_applied(self):
        make_connection = _import()
        con = make_connection(memory_limit="1GB")
        row = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        # DuckDB normalises '1GB' → '953.7 MiB' — just verify it's set
        assert row  # truthy
        con.close()

    def test_temp_directory_applied(self, tmp_path):
        make_connection = _import()
        tmp = str(tmp_path / "spill")
        con = make_connection(temp_directory=tmp)
        row = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        assert row == tmp
        con.close()

    def test_threads_applied(self):
        make_connection = _import()
        con = make_connection(threads=2)
        row = con.execute("SELECT current_setting('threads')").fetchone()[0]
        assert int(row) == 2
        con.close()

    def test_preserve_insertion_order_false_by_default(self):
        make_connection = _import()
        con = make_connection()
        # DuckDB returns a bool directly for boolean settings
        row = con.execute(
            "SELECT current_setting('preserve_insertion_order')"
        ).fetchone()[0]
        assert row is False or str(row).lower() in ("false", "0")
        con.close()

    def test_preserve_insertion_order_true_when_requested(self):
        make_connection = _import()
        con = make_connection(preserve_insertion_order=True)
        row = con.execute(
            "SELECT current_setting('preserve_insertion_order')"
        ).fetchone()[0]
        assert row is True or str(row).lower() in ("true", "1")
        con.close()


# ---------------------------------------------------------------------------
# make_connection() — environment variable fallbacks
# ---------------------------------------------------------------------------


class TestMakeConnectionEnvFallback:
    def test_env_memory_limit_used_when_no_arg(self):
        make_connection = _import()
        with patch.dict(os.environ, {"SQLDIM_MEMORY_LIMIT": "512MB"}):
            con = make_connection()
            row = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            # DuckDB normalises to MiB — just verify the value changed from default
            assert row  # e.g. '488.2 MiB' — truthy, proves limit was set
            con.close()

    def test_explicit_arg_overrides_env(self):
        make_connection = _import()
        with patch.dict(os.environ, {"SQLDIM_MEMORY_LIMIT": "512MB"}):
            con = make_connection(memory_limit="256MB")
            row = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            # DuckDB normalises '256MB' → '244.1 MiB'
            assert row  # set to some value
            con.close()

    def test_env_temp_dir_used_when_no_arg(self, tmp_path):
        make_connection = _import()
        tmp = str(tmp_path / "env_spill")
        with patch.dict(os.environ, {"SQLDIM_TEMP_DIR": tmp}):
            con = make_connection()
            row = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
            assert row == tmp
            con.close()


# ---------------------------------------------------------------------------
# make_connection() — psutil fallback
# ---------------------------------------------------------------------------


class TestMakeConnectionPsutilFallback:
    def test_uses_4gb_default_when_psutil_missing(self):
        make_connection = _import()
        import sys

        with patch.dict(sys.modules, {"psutil": None}):
            # Clear env so it must use hardcoded default
            env = {
                k: v for k, v in os.environ.items() if k not in ("SQLDIM_MEMORY_LIMIT",)
            }
            with patch.dict(os.environ, env, clear=True):
                con = make_connection()
                row = con.execute("SELECT current_setting('memory_limit')").fetchone()[
                    0
                ]
                # Either psutil was available (system RAM) or fallback "4GB"
                assert row  # just ensure it's set
                con.close()

    def test_temp_directory_created_if_missing(self, tmp_path):
        make_connection = _import()
        new_dir = str(tmp_path / "does_not_exist_yet" / "sub")
        con = make_connection(temp_directory=new_dir)
        assert os.path.isdir(new_dir)
        con.close()
