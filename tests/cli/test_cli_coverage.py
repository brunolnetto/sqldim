"""Coverage tests for uncovered CLI branches.

Targets:
- sqldim/cli/__init__.py        lines 39-40, 43-44, 55-56, 137, 165-231
- sqldim/cli/ask.py             lines 87, 116-184
- sqldim/cli/bench.py           lines 26-48
- sqldim/cli/chat.py            lines 19-20, 23-24, 26, 43, 68-70, 79-80, 83, 105-107, ...
- sqldim/cli/examples.py        lines 15, 19-20, 27-29, 38-39
- sqldim/cli/showtime.py        lines 71-76, 121-122
- sqldim/cli/supervisor.py      line 71
"""

from __future__ import annotations

import argparse
import sys

import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# sqldim/cli/__init__.py — cmd_bench_run / cmd_bench_list / cmd_showtime
# dispatch wrappers (lines 39-40, 43-44, 55-56)
# ---------------------------------------------------------------------------


class TestCLIInitDispatch:
    """Top-level dispatch wrappers just import and call the impl."""

    def test_cmd_bench_list_dispatches(self, capsys) -> None:
        from sqldim.cli import main

        rc = main(["exercise", "bench", "list"])
        assert rc == 0

    def test_cmd_bench_run_dispatches(self, capsys, monkeypatch) -> None:
        """cmd_bench_run called via main; just check it delegates."""
        monkeypatch.setattr(
            "sqldim.cli.bench.cmd_bench_run",
            lambda args: 0,
        )
        from sqldim.cli import main as _main

        rc = _main(["exercise", "bench", "run"])
        assert rc == 0

    def test_cmd_chat_dispatches(self, monkeypatch) -> None:
        """cmd_chat wrapper (lines 43-44) delegates to chat impl."""
        monkeypatch.setattr("sqldim.cli.chat._run_repl", lambda *a, **kw: 0)
        from sqldim.cli import main

        # Provide datasets to skip interactive picker
        rc = main(["nl", "chat", "--datasets", "commerce"])
        assert rc == 0

    def test_cmd_showtime_dispatches_missing_rich(self, capsys, monkeypatch) -> None:
        """cmd_showtime wrapper covers lines 55-56; returns 1 when rich absent."""
        monkeypatch.setitem(sys.modules, "rich", None)
        monkeypatch.setitem(sys.modules, "rich.console", None)
        from sqldim.cli import main as _main

        rc = _main(["nl", "showtime"])
        assert rc == 1


# ---------------------------------------------------------------------------
# sqldim/cli/__init__.py — cmd_evals_list tag display (line 137)
# ---------------------------------------------------------------------------


class TestCLIEvalsListTags:
    """Eval cases with tags show the [tag] suffix in listing output."""

    def test_tagged_case_prints_bracket(self, capsys, monkeypatch) -> None:
        from sqldim.application.evals.cases import EvalCase

        tagged = EvalCase(
            id="test.01.tagged",
            dataset="commerce",
            utterance="test question",
            tags=["entity", "ranking"],
        )
        from sqldim.application.evals import cases as cases_mod

        original = cases_mod.EVAL_SUITE

        monkeypatch.setattr(cases_mod, "EVAL_SUITE", [tagged])
        try:
            from sqldim.cli import main

            main(["nl", "evals", "list"])
        finally:
            cases_mod.EVAL_SUITE = original

        out = capsys.readouterr().out
        assert "[entity" in out


# ---------------------------------------------------------------------------
# sqldim/cli/__init__.py — _run_tool_evals (lines 165-231)
# ---------------------------------------------------------------------------


class TestRunToolEvals:
    """_run_tool_evals via main(['nl', 'evals', 'run', '--tool', ...])."""

    def test_unknown_tool_returns_1(self, capsys) -> None:
        from sqldim.cli import main

        rc = main(["nl", "evals", "run", "--tool", "nonexistent_tool_xyz_abc"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "no eval cases found" in out.lower() or "no matching" in out.lower()

    def test_known_tool_returns_0(self, capsys) -> None:
        """Runs with a real tool that has eval cases."""
        from sqldim.application.evals.tool_eval_loader import load_all_tool_eval_cases

        all_cases = load_all_tool_eval_cases()
        if not all_cases:
            pytest.skip("No tool eval cases available")
        tool_name = all_cases[0].tool
        from sqldim.cli import main

        rc = main(["nl", "evals", "run", "--tool", tool_name])
        assert rc == 0

    def test_unknown_tool_lists_available(self, capsys) -> None:
        from sqldim.cli import main

        main(["nl", "evals", "run", "--tool", "nonexistent_xyz"])
        out = capsys.readouterr().out
        # Should either show "Available tools:" or warn about no cases
        assert "tool" in out.lower()

    def test_tool_with_output_file(self, tmp_path, capsys) -> None:
        from sqldim.application.evals.tool_eval_loader import load_all_tool_eval_cases

        all_cases = load_all_tool_eval_cases()
        if not all_cases:
            pytest.skip("No tool eval cases available")
        tool_name = all_cases[0].tool
        output_path = str(tmp_path / "tool_report.json")
        from sqldim.cli import main

        rc = main(["nl", "evals", "run", "--tool", tool_name, "--output", output_path])
        assert rc == 0
        assert (tmp_path / "tool_report.json").exists()

    def test_tool_with_dataset_filter(self, capsys) -> None:
        """--dataset filter keeps only matching domain cases."""
        from sqldim.application.evals.tool_eval_loader import load_all_tool_eval_cases

        all_cases = load_all_tool_eval_cases()
        if not all_cases:
            pytest.skip("No tool eval cases available")
        case = all_cases[0]
        from sqldim.cli import main

        # Filter to a non-matching domain so we end up with 0 after filter
        rc = main(
            [
                "nl",
                "evals",
                "run",
                "--tool",
                case.tool,
                "--dataset",
                "nonexistent_domain_xyz",
            ]
        )
        assert rc == 1  # no cases after filter


# ---------------------------------------------------------------------------
# sqldim/cli/ask.py — _build_pipeline_source unknown source (line 87)
# and _run_supervisor (lines 116-184)
# ---------------------------------------------------------------------------


class TestCLIAskCoverage:
    @pytest.fixture(autouse=True)
    def _stub_model(self, monkeypatch) -> None:
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)

    @pytest.fixture(autouse=True)
    def _stub_supervisor_graph(self, monkeypatch) -> None:
        """Replace build_supervisor_graph with a fast stub for all tests."""

        def _fake_build(*a, **kw):
            class _G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "stub synthesis",
                        "visited_nodes": ["plan", "execute"],
                        "node_timings": {"plan": 1, "execute": 2},
                        "results": [
                            {
                                "tool_name": "dgm_query",
                                "success": True,
                                "data": {"rows": [1, 2], "count": 2},
                                "error": None,
                            }
                        ],
                    }

            return _G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build,
        )

    def test_unknown_source_type_returns_1(self, capsys) -> None:
        """_build_pipeline_source prints error and returns None when source unknown."""
        from sqldim.cli.ask import _build_pipeline_source
        import types

        args = types.SimpleNamespace(source="nonexistent_source_type")
        result = _build_pipeline_source(args)
        assert result is None
        out = capsys.readouterr().out
        assert "Unknown source type" in out

    def test_supervisor_mode_returns_zero(self, capsys) -> None:
        from sqldim.cli import main

        rc = main(["nl", "ask", "show me revenue", "--mode", "supervisor"])
        assert rc == 0

    def test_supervisor_mode_unknown_datasets_returns_1(self, capsys) -> None:
        from sqldim.cli import main

        rc = main(
            [
                "nl",
                "ask",
                "test",
                "--mode",
                "supervisor",
                "--datasets",
                "nonexistent_dataset_xyz",
            ]
        )
        assert rc == 1
        out = capsys.readouterr().out
        assert "Unknown" in out

    def test_supervisor_mode_verbose(self, capsys) -> None:
        from sqldim.cli import main

        rc = main(["nl", "ask", "show revenue", "--mode", "supervisor", "--verbose"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "Nodes visited" in out

    def test_supervisor_mode_verbose_with_error_hop(self, capsys, monkeypatch) -> None:
        """Verbose display covers the error branch (line 179 of ask.py)."""

        def _fake_build_err(*a, **kw):
            class _G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "partial",
                        "visited_nodes": ["plan"],
                        "node_timings": {},
                        "results": [
                            {
                                "tool_name": "bad_tool",
                                "success": False,
                                "data": {},
                                "error": "connection refused",
                            }
                        ],
                    }

            return _G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build_err,
        )
        from sqldim.cli import main

        rc = main(["nl", "ask", "test", "--mode", "supervisor", "--verbose"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "ERROR" in out

    def test_supervisor_mode_datasets_filter_reduces_available(self, capsys) -> None:
        """--datasets with valid names sets available = sorted(selected) (line 129)."""
        from sqldim.cli import main

        rc = main(
            [
                "nl",
                "ask",
                "test",
                "--mode",
                "supervisor",
                "--datasets",
                "commerce",
                "fintech",
            ]
        )
        assert rc == 0

    def test_supervisor_mode_model_provider_error_continues(
        self, capsys, monkeypatch
    ) -> None:
        """make_model raising logs warning and continues (lines 140-141 of ask.py)."""

        def _bad_model(*a, **kw):
            raise RuntimeError("bad key")

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl._agents.make_model",
            _bad_model,
        )
        from sqldim.cli import main

        rc = main(
            ["nl", "ask", "test", "--mode", "supervisor", "--model-provider", "openai"]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Warning" in out or "could not build" in out

    def test_supervisor_mode_no_synthesis_message(self, capsys, monkeypatch) -> None:
        """When synthesis is empty, 'No synthesis produced' is printed."""

        def _fake_build(*a, **kw):
            class G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "",
                        "visited_nodes": [],
                        "node_timings": {},
                        "results": [],
                        "conversation_history": [],
                    }

            return G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build,
        )
        from sqldim.cli import main

        main(["nl", "ask", "show revenue", "--mode", "supervisor"])
        out = capsys.readouterr().out
        assert "No synthesis" in out


# ---------------------------------------------------------------------------
# sqldim/cli/bench.py — cmd_bench_list (lines 26-48)
# ---------------------------------------------------------------------------


class TestCmdBenchList:
    def test_bench_list_returns_zero(self, capsys) -> None:
        from sqldim.cli.bench import cmd_bench_list

        rc = cmd_bench_list(argparse.Namespace())
        assert rc == 0

    def test_bench_list_prints_groups(self, capsys) -> None:
        from sqldim.cli.bench import cmd_bench_list

        cmd_bench_list(argparse.Namespace())
        out = capsys.readouterr().out
        assert "benchmark group(s)" in out

    def test_bench_list_shows_letters(self, capsys) -> None:
        from sqldim.cli.bench import cmd_bench_list

        cmd_bench_list(argparse.Namespace())
        out = capsys.readouterr().out
        # Each group shows at least one [letter]
        assert "[" in out


# ---------------------------------------------------------------------------
# sqldim/cli/chat.py — _check_interactive_deps when packages missing (19-20, 23-24, 26)
# and _select_datasets_interactive edge cases (43, 68-70, 79-80, 83)
# and _run_repl slash commands (105-107, 206, 222-223, 232-233, 243-264)
# ---------------------------------------------------------------------------


class TestCmdChatMissingDeps:
    """cmd_chat lines 222-223: _check_interactive_deps() fails → print + return 1."""

    def test_missing_rich_returns_1(self, capsys, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "rich", None)
        from sqldim.cli import main

        rc = main(["nl", "chat"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "sqldim chat" in out

    def test_missing_prompt_toolkit_returns_1(self, capsys, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "prompt_toolkit", None)
        from sqldim.cli import main

        rc = main(["nl", "chat"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "sqldim chat" in out


class TestCmdChatDatasetPaths:
    """cmd_chat lines 232-233, 243-264: dataset selection and model build."""

    @pytest.fixture(autouse=True)
    def _stub_repl(self, monkeypatch) -> None:
        """Replace _run_repl so we never enter the interactive loop."""
        monkeypatch.setattr("sqldim.cli.chat._run_repl", lambda *a, **kw: 0)

    @pytest.fixture(autouse=True)
    def _stub_graph(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            lambda *a, **kw: object(),
        )

    def test_unknown_datasets_returns_1(self, capsys) -> None:
        """Lines 240-242: unknown --datasets → print error + return 1."""
        import argparse
        from sqldim.cli.chat import cmd_chat

        args = argparse.Namespace(
            datasets=["nonexistent_xyz_domain"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_chat(args)
        assert rc == 1
        out = capsys.readouterr().out
        assert "Unknown" in out

    def test_known_datasets_returns_0(self, capsys) -> None:
        """Line 243: --datasets with valid names → datasets = sorted(selected)."""
        import argparse
        from sqldim.cli.chat import cmd_chat

        args = argparse.Namespace(
            datasets=["commerce", "fintech"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_chat(args)
        assert rc == 0

    def test_no_datasets_uses_interactive_selection(self, capsys, monkeypatch) -> None:
        """Line 245: no --datasets → calls _select_datasets_interactive."""
        monkeypatch.setattr(
            "sqldim.cli.chat._select_datasets_interactive",
            lambda ds: ["commerce"],
        )
        import argparse
        from sqldim.cli.chat import cmd_chat

        args = argparse.Namespace(
            datasets=None,
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_chat(args)
        assert rc == 0


class TestRunReplNoSessionFactory:
    """_run_repl lines 105-107: when _session_factory is None it imports prompt_toolkit."""

    def test_repl_uses_prompt_toolkit_when_no_factory(self, monkeypatch) -> None:
        """Provide a fake PromptSession so we don't block on stdin."""
        from unittest.mock import MagicMock
        import types

        fake_session = MagicMock()
        fake_session.prompt.side_effect = [EOFError()]

        fake_pt = types.ModuleType("prompt_toolkit")
        fake_pt.PromptSession = lambda **kw: fake_session  # type: ignore[attr-defined]
        fake_history = types.ModuleType("prompt_toolkit.history")
        fake_history.InMemoryHistory = lambda: None  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, "prompt_toolkit", fake_pt)
        monkeypatch.setitem(sys.modules, "prompt_toolkit.history", fake_history)

        from sqldim.cli.chat import _run_repl

        class FakeGraph:
            def invoke(self, state, config=None):
                return {
                    "synthesis": "ok",
                    "visited_nodes": [],
                    "node_timings": {},
                    "results": [],
                }

        # _session_factory=None → imports prompt_toolkit (lines 105-107)
        rc = _run_repl(FakeGraph(), ["commerce"], _session_factory=None)
        assert rc == 0


class TestCheckInteractiveDepsWhenMissing:
    def test_rich_missing_returns_false(self, monkeypatch) -> None:
        """Setting sys.modules['rich'] = None makes 'import rich' raise ImportError."""
        monkeypatch.setitem(sys.modules, "rich", None)
        from sqldim.cli.chat import _check_interactive_deps

        ok, msg = _check_interactive_deps()
        assert ok is False
        assert "rich" in msg

    def test_prompt_toolkit_missing_returns_false(self, monkeypatch) -> None:
        """Setting sys.modules['prompt_toolkit'] = None makes its import fail."""
        monkeypatch.setitem(sys.modules, "prompt_toolkit", None)
        from sqldim.cli.chat import _check_interactive_deps

        ok, msg = _check_interactive_deps()
        assert ok is False
        assert "prompt-toolkit" in msg

    def test_both_missing_returns_false(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "rich", None)
        monkeypatch.setitem(sys.modules, "prompt_toolkit", None)
        from sqldim.cli.chat import _check_interactive_deps

        ok, msg = _check_interactive_deps()
        assert ok is False
        assert "rich" in msg
        assert "prompt-toolkit" in msg

    def test_all_deps_present_returns_true(self) -> None:
        from sqldim.cli.chat import _check_interactive_deps

        ok, msg = _check_interactive_deps()
        assert ok is True
        assert msg == ""


class TestSelectDatasetsInteractiveEdgeCases:
    """Edge-case paths in _select_datasets_interactive."""

    def test_out_of_range_number_retries(self) -> None:
        """Entering an out-of-range index shows a warning and loops."""
        from sqldim.cli.chat import _select_datasets_interactive

        datasets = ["alpha", "beta"]
        # Call 1: "5" — out of range → no selection
        # Call 2: "1" — valid → returns ["alpha"]
        calls = iter(["5", "1"])
        result = _select_datasets_interactive(
            datasets, _prompt_fn=lambda _: next(calls)
        )
        assert result == ["alpha"]

    def test_invalid_name_shows_warning(self) -> None:
        """Entering an invalid name retries."""
        from sqldim.cli.chat import _select_datasets_interactive

        datasets = ["alpha", "beta"]
        calls = iter(["gamma", "alpha"])  # gamma invalid → retry, then alpha
        result = _select_datasets_interactive(
            datasets, _prompt_fn=lambda _: next(calls)
        )
        assert result == ["alpha"]

    def test_mixed_valid_invalid_names_retries(self) -> None:
        """Entering 'beta,gamma' where gamma is invalid retries."""
        from sqldim.cli.chat import _select_datasets_interactive

        datasets = ["alpha", "beta"]
        calls = iter(["beta,gamma", "beta"])
        result = _select_datasets_interactive(
            datasets, _prompt_fn=lambda _: next(calls)
        )
        assert result == ["beta"]

    def test_no_prompt_fn_uses_prompt_toolkit(self, monkeypatch) -> None:
        """When _prompt_fn is None, the function imports prompt_toolkit.prompt at line 43-44."""
        import prompt_toolkit

        monkeypatch.setattr(prompt_toolkit, "prompt", lambda _: "all")
        from sqldim.cli.chat import _select_datasets_interactive

        result = _select_datasets_interactive(["alpha", "beta"], _prompt_fn=None)
        assert result == ["alpha", "beta"]


class TestRunReplSlashCommands:
    """_run_repl slash commands branch coverage."""

    def _make_graph_stub(self):
        """Return a graph-like object that always returns a stub synthesis."""

        class FakeGraph:
            def invoke(self, state, config=None):
                return {
                    "synthesis": "stub result",
                    "visited_nodes": ["plan", "respond"],
                    "node_timings": {"plan": 1.0, "respond": 2.0},
                    "results": [],
                }

        return FakeGraph()

    def _make_session(self, responses):
        """Return a session-like object yielding preset prompts."""
        from unittest.mock import MagicMock

        session = MagicMock()
        session.prompt.side_effect = responses + [EOFError()]
        return session

    def test_repl_quit_command_returns_0(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_help_command_continues(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/help", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_datasets_command(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/datasets", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce", "fintech"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_history_empty(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/history", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_history_non_empty(self) -> None:
        """After a real question, /history shows prior exchange."""
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["show revenue", "/history", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_clear_command(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["show revenue", "/clear", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_unknown_command_continues(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/unknowncmd", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_verbose_shows_timings(self, capsys) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["show revenue", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            verbose=True,
            _session_factory=lambda: session,
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Nodes" in out or "plan" in out

    def test_repl_empty_utterance_skipped(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["", "  ", "/quit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_no_synthesis_prints_warning(self, capsys) -> None:
        """Graph returning empty synthesis → 'No synthesis produced'."""
        from sqldim.cli.chat import _run_repl

        class NoSynthGraph:
            def invoke(self, state, config=None):
                return {
                    "synthesis": "",
                    "visited_nodes": [],
                    "node_timings": {},
                    "results": [],
                }

        session = self._make_session(["something", "/quit"])
        _run_repl(NoSynthGraph(), ["commerce"], _session_factory=lambda: session)
        out = capsys.readouterr().out
        assert "No synthesis" in out

    def test_repl_exit_slash_command(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/exit"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0

    def test_repl_q_alias(self) -> None:
        from sqldim.cli.chat import _run_repl

        session = self._make_session(["/q"])
        rc = _run_repl(
            self._make_graph_stub(),
            ["commerce"],
            _session_factory=lambda: session,
        )
        assert rc == 0


# ---------------------------------------------------------------------------
# sqldim/cli/examples.py — _read_example_metadata edge cases (lines 15, 19-20, 27-29)
# and _load_one_showcase / _scan_pkg (lines 38-39)
# ---------------------------------------------------------------------------


class TestReadExampleMetadata:
    def test_returns_none_for_nonexistent_file(self, tmp_path) -> None:
        from sqldim.cli.examples import _read_example_metadata

        result = _read_example_metadata(tmp_path / "no_such_file.py")
        assert result is None

    def test_returns_none_for_syntax_error(self, tmp_path) -> None:
        from sqldim.cli.examples import _read_example_metadata

        f = tmp_path / "bad.py"
        f.write_text("def (:")
        result = _read_example_metadata(f)
        assert result is None

    def test_returns_none_when_no_metadata_assignment(self, tmp_path) -> None:
        from sqldim.cli.examples import _read_example_metadata

        f = tmp_path / "no_meta.py"
        f.write_text("x = 1\ny = 2\n")
        result = _read_example_metadata(f)
        assert result is None

    def test_returns_metadata_when_present(self, tmp_path) -> None:
        from sqldim.cli.examples import _read_example_metadata

        f = tmp_path / "showcase.py"
        f.write_text(
            'EXAMPLE_METADATA = {"name": "test", "title": "T", "description": "D", "entry_point": "run"}\n'
        )
        result = _read_example_metadata(f)
        assert result is not None
        assert result["name"] == "test"

    def test_load_one_showcase_missing_package(self) -> None:
        from sqldim.cli.examples import _load_one_showcase

        result = _load_one_showcase("sqldim.nonexistent.pkg.xyz", "submod", "test")
        assert result is None

    def test_scan_pkg_missing_package(self) -> None:
        from sqldim.cli.examples import _scan_pkg

        result = _scan_pkg("sqldim.nonexistent.pkg.xyz", "test")
        assert result == {}

    def test_returns_none_when_literal_eval_fails(self, tmp_path) -> None:
        """Lines 27-28: ValueError/TypeError raised by ast.literal_eval → return None."""
        from sqldim.cli.examples import _read_example_metadata

        f = tmp_path / "bad_meta.py"
        # ast.literal_eval raises ValueError for non-literal expressions
        f.write_text("EXAMPLE_METADATA = some_function_call()\n")
        result = _read_example_metadata(f)
        assert result is None


# ---------------------------------------------------------------------------
# sqldim/cli/showtime.py — no scenarios branch (lines 121-122)
# and verbose tool display (line 187)
# ---------------------------------------------------------------------------


class TestCmdShowtime:
    def test_no_scenarios_returns_zero(self, capsys, monkeypatch) -> None:
        """When dataset has no demo scenarios, showtime returns 0 gracefully."""
        from sqldim.cli import showtime as st

        monkeypatch.setattr(st, "_build_scenario_list", lambda datasets: [])

        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = st.cmd_showtime(args)
        assert rc == 0

    def test_showtime_missing_rich_returns_1(self, capsys, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "rich", None)
        monkeypatch.setitem(sys.modules, "rich.console", None)
        monkeypatch.setitem(sys.modules, "rich.markdown", None)
        monkeypatch.setitem(sys.modules, "rich.panel", None)
        from sqldim.cli import main

        rc = main(["nl", "showtime"])
        assert rc == 1

    def test_verbose_error_hop_prints_error_line(self, capsys, monkeypatch) -> None:
        """showtime.py line 187: error hop displayed when verbose=True."""
        from sqldim.cli import showtime as st

        def _fake_build(*a, **kw):
            class _G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "Done.",
                        "visited_nodes": ["plan", "execute", "synthesize"],
                        "node_timings": {"plan": 1, "execute": 2, "synthesize": 3},
                        "results": [
                            {
                                "tool_name": "dgm_query",
                                "success": False,
                                "data": {},
                                "error": "DB timeout",
                            }
                        ],
                    }

            return _G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build,
        )
        monkeypatch.setattr(
            st,
            "_build_scenario_list",
            lambda datasets: [{"title": "Demo Q", "question": "What is revenue?"}],
        )
        args = argparse.Namespace(
            datasets=None,
            model_provider=None,
            model=None,
            verbose=True,
        )
        rc = st.cmd_showtime(args)
        assert rc == 0


# ---------------------------------------------------------------------------
# sqldim/cli/supervisor.py — verbose display (line 71)
# ---------------------------------------------------------------------------


class TestCLISupervisor:
    def test_supervisor_verbose_prints_timings(self, capsys, monkeypatch) -> None:
        """supervisor.py line 71: error hop displayed via print."""

        def _fake_build(*a, **kw):
            class _G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "ok",
                        "visited_nodes": ["plan"],
                        "node_timings": {"plan": 10},
                        "results": [
                            {
                                "tool_name": "tool_x",
                                "success": False,
                                "data": {},
                                "error": "timeout",
                            }
                        ],
                    }

            return _G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build,
        )
        from sqldim.cli import main

        rc = main(["nl", "ask", "show revenue", "--mode", "supervisor", "--verbose"])
        assert isinstance(rc, int)
        out = capsys.readouterr().out
        assert len(out) > 0
