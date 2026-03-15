"""Tests for sqldim/examples/utils.py."""
import os
import duckdb
import pytest

from sqldim.examples.utils import (
    tmp_db,
    make_tmp_db,
    setup_dim,
    teardown_dim,
    section,
    banner,
    print_rows,
    show_provider,
)


# ── tmp_db ──────────────────────────────────────────────────────────────────

def test_tmp_db_yields_valid_path():
    with tmp_db() as path:
        assert path.endswith(".duckdb")
        con = duckdb.connect(path)
        con.close()


def test_tmp_db_cleans_up_after_exit():
    with tmp_db() as path:
        con = duckdb.connect(path)
        con.close()
    assert not os.path.exists(path)


def test_tmp_db_cleans_up_wal(tmp_path, monkeypatch):
    """WAL file is removed when it exists."""
    import tempfile
    with tmp_db() as path:
        # Simulate a WAL file
        wal = path + ".wal"
        open(wal, "w").close()
    assert not os.path.exists(wal)


# ── make_tmp_db ──────────────────────────────────────────────────────────────

def test_make_tmp_db_returns_string():
    path = make_tmp_db()
    assert isinstance(path, str)
    assert path.endswith(".duckdb")
    # File must not exist yet (DuckDB creates it on connect)
    assert not os.path.exists(path)
    # Clean up
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def test_make_tmp_db_is_connectable():
    path = make_tmp_db()
    try:
        con = duckdb.connect(path)
        con.close()
    finally:
        for p in (path, path + ".wal"):
            if os.path.exists(p):
                os.unlink(p)


# ── setup_dim / teardown_dim ─────────────────────────────────────────────────

class _FakeSrc:
    def __init__(self):
        self.setup_called = False
        self.teardown_called = False

    def setup(self, con, table):
        self.setup_called = True

    def teardown(self, con, table):
        self.teardown_called = True


class _NoMethodSrc:
    pass


def test_setup_dim_calls_setup_when_present():
    src = _FakeSrc()
    con = duckdb.connect()
    setup_dim(con, src, "t")
    assert src.setup_called


def test_setup_dim_noop_when_no_setup():
    src = _NoMethodSrc()
    con = duckdb.connect()
    setup_dim(con, src, "t")  # must not raise


def test_teardown_dim_calls_teardown_when_present():
    src = _FakeSrc()
    con = duckdb.connect()
    teardown_dim(con, src, "t")
    assert src.teardown_called


def test_teardown_dim_drops_table_when_no_method():
    src = _NoMethodSrc()
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id INTEGER)")
    teardown_dim(con, src, "t")
    # table should be gone
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    assert "t" not in tables


def test_teardown_dim_noop_when_table_does_not_exist():
    src = _NoMethodSrc()
    con = duckdb.connect()
    teardown_dim(con, src, "nonexistent_table")  # must not raise


# ── section context manager ──────────────────────────────────────────────────

def test_section_prints_header(capsys):
    with section("My Section"):
        pass
    out = capsys.readouterr().out
    assert "My Section" in out


def test_section_returns_self():
    ctx = section("Test")
    with ctx as s:
        assert s is ctx


# ── banner ───────────────────────────────────────────────────────────────────

def test_banner_prints_title(capsys):
    banner("Test Banner")
    out = capsys.readouterr().out
    assert "Test Banner" in out


def test_banner_with_subtitle(capsys):
    banner("Title", "Subtitle text")
    out = capsys.readouterr().out
    assert "Title" in out
    assert "Subtitle text" in out


def test_banner_without_subtitle(capsys):
    banner("Only Title", "")
    out = capsys.readouterr().out
    assert "Only Title" in out


# ── print_rows ───────────────────────────────────────────────────────────────

def test_print_rows_no_headers(capsys):
    print_rows([(1, "Alice"), (2, "Bob")])
    out = capsys.readouterr().out
    assert "Alice" in out
    assert "Bob" in out


def test_print_rows_with_headers(capsys):
    print_rows([(1, "Alice")], headers=["id", "name"])
    out = capsys.readouterr().out
    assert "id" in out
    assert "name" in out
    assert "Alice" in out


def test_print_rows_empty(capsys):
    print_rows([])
    out = capsys.readouterr().out
    assert out == ""


# ── show_provider ────────────────────────────────────────────────────────────

class _WithDescribeProvider:
    def describe_provider(self):
        return "described!"


class _WithProvider:
    class _P:
        def describe(self):
            return "from provider"
    provider = _P()


class _WithNoneProvider:
    provider = None


class _WithNothing:
    pass


def test_show_provider_calls_describe_provider(capsys):
    show_provider(_WithDescribeProvider())
    out = capsys.readouterr().out
    assert "described!" in out


def test_show_provider_falls_back_to_provider(capsys):
    show_provider(_WithProvider())
    out = capsys.readouterr().out
    assert "from provider" in out


def test_show_provider_noop_when_provider_is_none(capsys):
    show_provider(_WithNoneProvider())
    out = capsys.readouterr().out
    assert out == ""


def test_show_provider_noop_when_no_method(capsys):
    show_provider(_WithNothing())
    out = capsys.readouterr().out
    assert out == ""


# ── OSError silenced in tmp_db cleanup ───────────────────────────────────────

def test_tmp_db_cleanup_silences_oserror(monkeypatch):
    """Verify that OSError during tmp file removal is silenced."""
    import duckdb
    import os

    from sqldim.examples.utils import tmp_db

    orig_unlink = os.unlink
    call_count = [0]

    def fake_unlink(p):
        call_count[0] += 1
        if call_count[0] <= 1:  # first call (pre-yield) passes through
            return orig_unlink(p)
        raise OSError("permission denied")  # cleanup calls raise

    monkeypatch.setattr(os, "unlink", fake_unlink)
    # Should NOT raise even though cleanup os.unlink raises OSError
    with tmp_db() as path:
        con = duckdb.connect(path)
        con.close()
    # cleanup unlink was attempted and OSError was silenced
    assert call_count[0] >= 2
