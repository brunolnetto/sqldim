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
