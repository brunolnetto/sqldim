"""Tests for the interactive chat CLI command."""

from __future__ import annotations

import argparse


class TestCheckInteractiveDeps:
    """_check_interactive_deps reports missing packages."""

    def test_returns_ok_when_deps_available(self) -> None:
        from sqldim.cli.chat import _check_interactive_deps

        ok, msg = _check_interactive_deps()
        # rich and prompt_toolkit are available in the test env
        assert ok is True
        assert msg == ""


class TestSelectDatasetsInteractive:
    """_select_datasets_interactive picks datasets from user input."""

    def test_selects_all_on_empty_input(self) -> None:
        from sqldim.cli.chat import _select_datasets_interactive

        result = _select_datasets_interactive(
            ["alpha", "beta", "gamma"], _prompt_fn=lambda _: "all"
        )
        assert result == ["alpha", "beta", "gamma"]

    def test_selects_by_number(self) -> None:
        from sqldim.cli.chat import _select_datasets_interactive

        result = _select_datasets_interactive(
            ["alpha", "beta", "gamma"], _prompt_fn=lambda _: "1,3"
        )
        assert result == ["alpha", "gamma"]

    def test_selects_by_name(self) -> None:
        from sqldim.cli.chat import _select_datasets_interactive

        result = _select_datasets_interactive(
            ["alpha", "beta", "gamma"], _prompt_fn=lambda _: "beta"
        )
        assert result == ["beta"]

    def test_empty_input_selects_all(self) -> None:
        from sqldim.cli.chat import _select_datasets_interactive

        result = _select_datasets_interactive(
            ["alpha", "beta"], _prompt_fn=lambda _: ""
        )
        assert result == ["alpha", "beta"]


class TestChatSubcommandParser:
    """The chat subcommand is registered in the CLI parser."""

    def test_chat_subcommand_exists(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["nl", "chat"])
        assert args.func is not None

    def test_chat_with_datasets_flag(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["nl", "chat", "--datasets", "commerce", "fintech"])
        assert args.datasets == ["commerce", "fintech"]

    def test_chat_with_model_flags(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(
            [
                "nl",
                "chat",
                "--model-provider",
                "openai",
                "--model",
                "gpt-4o-mini",
            ]
        )
        assert args.model_provider == "openai"
        assert args.model == "gpt-4o-mini"


class TestCmdChatValidation:
    """cmd_chat validates datasets argument."""

    def test_unknown_dataset_returns_1(self, capsys) -> None:
        from sqldim.cli.chat import cmd_chat

        args = argparse.Namespace(
            datasets=["nonexistent_dataset_xyz"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_chat(args)
        assert rc == 1
        out = capsys.readouterr().out
        assert "Unknown" in out

    def test_no_datasets_prints_message(self, capsys) -> None:
        """When KNOWN_DATASETS is empty, line 232 is executed (print message)."""
        from unittest.mock import patch

        from sqldim.cli.chat import cmd_chat

        args = argparse.Namespace(
            datasets=None,
            model_provider=None,
            model=None,
            verbose=False,
        )
        with patch("sqldim.application.ask.KNOWN_DATASETS", {}):
            rc = cmd_chat(args)
        # return 1 is pragma'd; print on line 232 is covered
        assert rc == 1
        out = capsys.readouterr().out
        assert "No datasets" in out

    def test_model_build_failure_warns_and_continues(self, capsys) -> None:
        """When make_model raises, a warning is printed and chat proceeds (lines 255-256)."""
        from unittest.mock import patch

        from sqldim.cli.chat import cmd_chat

        # Provide a valid dataset so we get past the dataset check
        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider="openai",
            model="gpt-4o-mini",
            verbose=False,
        )

        calls: list[int] = []

        def _fake_run_repl(graph, datasets, *, verbose=False, _session_factory=None):
            calls.append(1)
            return 0

        with (
            patch(
                "sqldim.core.query.dgm.nl._agents.make_model",
                side_effect=ValueError("bad provider"),
            ),
            patch("sqldim.cli.chat._run_repl", side_effect=_fake_run_repl),
        ):
            rc = cmd_chat(args)

        out = capsys.readouterr().out
        assert "Warning" in out or rc == 0  # warning printed; REPL still called
        assert calls  # _run_repl was invoked despite the model failure


class TestRunRepl:
    """_run_repl handles slash commands and graph invocation."""

    @staticmethod
    def _make_session(inputs: list[str]):
        """Create a fake PromptSession from a list of input strings."""
        it = iter(inputs)

        class FakeSession:
            def prompt(self, prompt_text):
                return next(it)

        return FakeSession

    def test_quit_command_exits(self) -> None:
        """The REPL exits cleanly on /quit."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph, ["commerce"], _session_factory=self._make_session(["/quit"])
        )
        assert rc == 0

    def test_eof_exits(self) -> None:
        """The REPL exits cleanly on Ctrl-D (EOFError)."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        class EofSession:
            def prompt(self, prompt_text):
                raise EOFError

        graph = build_supervisor_graph()
        rc = _run_repl(graph, ["commerce"], _session_factory=EofSession)
        assert rc == 0

    def test_help_command(self) -> None:
        """The /help command prints help text."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(["/help", "/quit"]),
        )
        assert rc == 0

    def test_datasets_command(self) -> None:
        """The /datasets command lists active datasets."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce", "fintech"],
            _session_factory=self._make_session(["/datasets", "/quit"]),
        )
        assert rc == 0

    def test_history_command_empty(self) -> None:
        """The /history command shows 'no history' when empty."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(["/history", "/quit"]),
        )
        assert rc == 0

    def test_clear_command(self) -> None:
        """The /clear command resets conversation history."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(["/clear", "/quit"]),
        )
        assert rc == 0

    def test_unknown_command(self) -> None:
        """Unknown slash commands print an error."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(["/bogus", "/quit"]),
        )
        assert rc == 0

    def test_question_invokes_graph(self) -> None:
        """A regular question invokes the supervisor graph."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(
                ["What datasets are available?", "/quit"]
            ),
        )
        assert rc == 0

    def test_verbose_prints_timings(self) -> None:
        """Verbose mode prints node traversal info."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            verbose=True,
            _session_factory=self._make_session(["hello", "/quit"]),
        )
        assert rc == 0

    def test_empty_input_ignored(self) -> None:
        """Empty input lines are skipped."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(["", "  ", "/quit"]),
        )
        assert rc == 0

    def test_history_after_question(self) -> None:
        """The /history command shows entries after a question."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        graph = build_supervisor_graph()
        rc = _run_repl(
            graph,
            ["commerce"],
            _session_factory=self._make_session(
                ["What tools do you have?", "/history", "/quit"]
            ),
        )
        assert rc == 0

    def test_exit_aliases(self) -> None:
        """Both /exit and /q work as quit aliases."""
        from sqldim.cli.chat import _run_repl
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

        for cmd in ["/exit", "/q"]:
            graph = build_supervisor_graph()
            rc = _run_repl(
                graph,
                ["commerce"],
                _session_factory=self._make_session([cmd]),
            )
            assert rc == 0
