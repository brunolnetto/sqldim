import pytest
import os
from sqldim.cli import main, build_parser

def test_no_subcommand_returns_1():
    rc = main([])
    assert rc == 1

def test_migrations_init(tmp_path):
    target = str(tmp_path / "migs")
    rc = main(["migrations", "init", "--dir", target])
    assert rc == 0
    assert os.path.isdir(target)
    assert os.path.isfile(os.path.join(target, "__init__.py"))

def test_migrations_init_idempotent(tmp_path):
    target = str(tmp_path / "migs2")
    main(["migrations", "init", "--dir", target])
    rc = main(["migrations", "init", "--dir", target])
    assert rc == 0

def test_migrations_generate(capsys):
    rc = main(["migrations", "generate", "add segment column"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "add segment column" in out

def test_migrations_show(capsys):
    rc = main(["migrations", "show"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "sqldim" in out

def test_schema_graph(capsys):
    rc = main(["schema", "graph"])
    assert rc == 0

def test_build_parser():
    parser = build_parser()
    assert parser.prog == "sqldim"

def test_main_as_module(capsys):
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "-m", "sqldim.cli"],
        capture_output=True, text=True
    )
    # No subcommand → help printed, exit 1
    assert result.returncode == 1


# ---------------------------------------------------------------------------
# example list / run
# ---------------------------------------------------------------------------

def test_example_list(capsys):
    rc = main(["example", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "nba" in out
    assert "saas" in out
    assert "user-activity" in out


def test_example_run_unknown_returns_1(capsys):
    rc = main(["example", "run", "no-such-demo"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Unknown example" in out
    assert "no-such-demo" in out


def test_example_run_nba(capsys):
    rc = main(["example", "run", "nba"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Michael Jordan" in out or "Showcase" in out


def test_example_run_saas(capsys):
    rc = main(["example", "run", "saas"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "SaaS" in out or "complete" in out.lower()


def test_example_run_user_activity(capsys):
    rc = main(["example", "run", "user-activity"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "User" in out or "Bitmask" in out or "Activity" in out


def test_example_run_sync_branch(capsys, monkeypatch):
    """Cover the sync (non-coroutine) code path in cmd_example_run."""
    import sqldim.cli as cli_mod

    called = []

    def _sync_showcase():
        called.append(True)

    monkeypatch.setitem(cli_mod._EXAMPLES, "sync_test", (
        "Sync Test", "desc",
        "sqldim.examples.real_world.nba_analytics.showcase",
        "run_showcase",   # will be replaced by monkeypatch below
    ))
    import sqldim.examples.real_world.nba_analytics.showcase as nba_mod
    monkeypatch.setattr(nba_mod, "run_showcase", _sync_showcase)
    # Temporarily override iscoroutinefunction so the else-branch is taken
    import asyncio
    monkeypatch.setattr(asyncio, "iscoroutinefunction", lambda fn: False)

    rc = main(["example", "run", "sync_test"])
    assert rc == 0
    assert called


# ---------------------------------------------------------------------------
# bigdata features
# ---------------------------------------------------------------------------

def test_bigdata_features(capsys):
    rc = main(["bigdata", "features"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "DuckDB" in out
    assert "Parquet" in out
    assert "Layer" in out
