"""D44 / §12.10: the entry points outside the supervisor fail closed.

Covers the NL interface used directly (:func:`run_ask`), the local host that
the CLI and the eval harness use, and an inventory proving that no module
outside the host opens a dataset by identifier.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from sqldim.application.ask import KNOWN_DATASETS, run_ask
from sqldim.application.supervisor_host import (
    local_data_access,
    open_local_dataset,
)
from sqldim.core.query.dgm.nl.supervisor._auth import (
    NOT_AUTHORIZED_OR_NOT_FOUND,
    AccessDenied,
    Action,
    DataAccess,
)
from tests.supervisor._fakes import Opener, auth

_ROOT = Path(__file__).resolve().parents[2] / "sqldim"

_HOST = {
    _ROOT / "application" / "supervisor_host.py",  # the only opener by identifier
    _ROOT / "application" / "ask.py",  # defines load_dataset
}

_OPENERS_BY_ID = {"load_dataset", "DatasetPipelineSource"}


def _boom(*_: object, **__: object) -> None:
    raise AssertionError("the NL graph must not run")


@pytest.fixture
def no_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqldim.application.ask.run_ask_from_source", _boom)


class TestRunAsk:
    def test_without_context_nothing_is_opened(
        self, no_graph: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert run_ask("count orders", "commerce", data=None) == 1
        assert "UNAUTHORIZED: no authorization context" in capsys.readouterr().out

    def test_context_is_mandatory(self) -> None:
        with pytest.raises(TypeError):
            run_ask("count orders", "commerce")  # type: ignore[call-arg]

    def test_ungranted_dataset_is_refused_before_opening(
        self, no_graph: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        opener = Opener()
        data = DataAccess(auth({"growth": {Action.READ}}), opener)
        assert run_ask("count orders", "commerce", data=data) == 1
        assert opener.opened == []
        assert NOT_AUTHORIZED_OR_NOT_FOUND in capsys.readouterr().out

    def test_grant_without_read_is_refused(self, no_graph: None) -> None:
        opener = Opener()
        data = DataAccess(auth({"commerce": {Action.WRITE_KB}}), opener)
        assert run_ask("count orders", "commerce", data=data) == 1
        assert opener.opened == []

    def test_unknown_and_ungranted_look_the_same(
        self, no_graph: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data = DataAccess(auth({"growth": {Action.READ}}), Opener())
        outputs = []
        for name in ("commerce", "no_such_dataset"):
            run_ask("q", name, data=data)
            outputs.append(capsys.readouterr().out.replace(name, "X"))
        assert outputs[0] == outputs[1]

    def test_granted_dataset_is_opened_once_through_data_access(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[object] = []
        monkeypatch.setattr(
            "sqldim.application.ask.run_ask_from_source",
            lambda utterance, source, **kw: seen.append(source) or 0,
        )
        opener = Opener()
        data = DataAccess(auth({"commerce": {Action.READ}}), opener)
        assert run_ask("count orders", "commerce", data=data) == 0
        assert opener.opened == ["commerce"]
        assert seen == ["handle:commerce"]


class TestLocalHost:
    def test_grants_only_the_selected_datasets(self) -> None:
        data = local_data_access(["commerce"])
        assert data.auth.readable_datasets() == ["commerce"]
        assert data.auth.actions_on("commerce") == frozenset({Action.READ})

    def test_unknown_name_is_refused_like_an_ungranted_one(self) -> None:
        with pytest.raises(AccessDenied) as exc:
            open_local_dataset("no_such_dataset")
        assert str(exc.value) == NOT_AUTHORIZED_OR_NOT_FOUND

    def test_local_context_cannot_be_widened_by_scope(self) -> None:
        data = local_data_access(["commerce"])
        other = next(d for d in sorted(KNOWN_DATASETS) if d != "commerce")
        with pytest.raises(AccessDenied):
            data.scoped([other]).open(other)

    def test_cli_dataset_source_goes_through_the_host(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse

        from sqldim.cli.ask import _make_dataset_source

        assert _make_dataset_source(argparse.Namespace(dataset="no_such")) is None
        assert "UNAUTHORIZED" in capsys.readouterr().out


def _calls(tree: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", "")
            if name in _OPENERS_BY_ID:
                names.add(name)
    return names


def test_inventory_no_module_outside_the_host_opens_a_dataset_by_id() -> None:
    """§12.10 audit: opening by identifier happens only in the host."""
    offenders = {}
    for path in sorted(_ROOT.rglob("*.py")):
        if path in _HOST:
            continue
        found = _calls(ast.parse(path.read_text(encoding="utf-8")))
        if found:
            offenders[str(path.relative_to(_ROOT))] = sorted(found)
    assert offenders == {}
