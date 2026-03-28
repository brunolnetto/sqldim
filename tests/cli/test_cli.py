import pytest
import os
from unittest import mock
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


def test_main_as_module(monkeypatch):
    import runpy
    import sys

    monkeypatch.setattr(sys, "argv", ["sqldim.cli"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("sqldim.cli.__main__", run_name="__main__")
    # No subcommand → help printed, exit 1
    assert exc.value.code == 1


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
    assert "NBA" in out or "complete" in out.lower()


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
    import sqldim.cli.examples as cli_mod
    import asyncio

    called = []

    def _sync_showcase():
        called.append(True)

    def _fake_discover():
        return {
            "sync_test": (
                "Sync Test",
                "desc",
                "sqldim.application.examples.real_world.nba_analytics.showcase",
                "run_showcase",
                "real_world",
            )
        }

    monkeypatch.setattr(cli_mod, "_discover_examples", _fake_discover)
    import sqldim.application.examples.real_world.nba_analytics.showcase as nba_mod

    monkeypatch.setattr(nba_mod, "run_showcase", _sync_showcase)
    # Temporarily override iscoroutinefunction so the else-branch is taken
    monkeypatch.setattr(asyncio, "iscoroutinefunction", lambda fn: False)

    rc = main(["example", "run", "sync_test"])
    assert rc == 0
    assert called


def test_discover_examples_import_error_skips_pkg(capsys):
    """Lines 89-90: ImportError on the package itself causes silent skip."""
    import sqldim.cli.examples as cli_mod

    import importlib

    real_import = importlib.import_module

    def _fake_import(name, *a, **kw):
        if name == "sqldim.application.examples.real_world":
            raise ImportError("simulated missing package")
        return real_import(name, *a, **kw)

    with mock.patch("importlib.import_module", side_effect=_fake_import):
        result = cli_mod._discover_examples()
    # real_world pkg skipped; features pkg still works
    assert isinstance(result, dict)


def test_discover_examples_showcase_import_error_skips_module():
    """Line 101: ImportError on the showcase module itself causes silent skip."""
    import sqldim.cli.examples as cli_mod
    import importlib

    real_import = importlib.import_module

    def _fake_import(name, *a, **kw):
        if name.endswith(".showcase") and "nba_analytics" in name:
            raise ImportError("simulated missing showcase")
        return real_import(name, *a, **kw)

    with mock.patch("importlib.import_module", side_effect=_fake_import):
        result = cli_mod._discover_examples()
    # nba showcase absent; other showcases intact
    assert "nba" not in result
    assert len(result) > 0


def test_example_list_no_examples(capsys, monkeypatch):
    """Lines 120-121: empty discovery returns early with a helpful message."""
    import sqldim.cli.examples as cli_mod

    monkeypatch.setattr(cli_mod, "_discover_examples", lambda: {})
    rc = main(["example", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No examples found" in out


def test_example_list_single_kind_skips_empty_group(capsys, monkeypatch):
    """Line 131: when features group is empty it is skipped silently."""
    import sqldim.cli.examples as cli_mod

    monkeypatch.setattr(
        cli_mod,
        "_discover_examples",
        lambda: {
            "nba": ("NBA", "desc", "mod", "run", "real_world"),
        },
    )
    rc = main(["example", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Real-world" in out
    assert "Feature showcases" not in out


def test_discover_examples_non_package_modules_skipped():
    """Lines 97-98: non-package entries in iter_modules are silently skipped."""
    import sqldim.cli.examples as cli_mod
    import pkgutil

    real_iter = pkgutil.iter_modules

    def _fake_iter(path):
        # Yield a non-package entry first, then the real entries
        yield (None, "_not_a_pkg", False)
        yield from real_iter(path)

    with mock.patch("pkgutil.iter_modules", side_effect=_fake_iter):
        result = cli_mod._discover_examples()
    # Non-package entry silently skipped; real examples still discovered
    assert isinstance(result, dict)
    assert len(result) > 0


def test_discover_examples_none_metadata_skips_module():
    """Line 101: showcase module that returns EXAMPLE_METADATA=None is silently skipped."""
    import importlib
    import types
    import sqldim.cli.examples as cli_mod

    real_import = importlib.import_module

    def _fake_import(name, *a, **kw):
        if name == "sqldim.application.examples.real_world.nba_analytics.showcase":
            # Module imports fine but has no EXAMPLE_METADATA → getattr returns None
            return types.ModuleType(name)
        return real_import(name, *a, **kw)

    with mock.patch("importlib.import_module", side_effect=_fake_import):
        result = cli_mod._discover_examples()
    # nba_analytics showcase skipped; other showcases still discovered
    assert "nba" not in result
    assert len(result) > 0
