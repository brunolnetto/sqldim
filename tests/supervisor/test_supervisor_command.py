"""Tests for the supervisor CLI command."""

from __future__ import annotations


class TestSupervisorCommandDiscovery:
    """The supervisor mode must be available via CLI parser."""

    def test_supervisor_mode_exists(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        # Parse a minimal supervisor invocation via ask --mode supervisor
        args = parser.parse_args(["nl", "ask", "test question", "--mode", "supervisor"])
        assert args.func is not None
        assert args.question == "test question"
        assert args.mode == "supervisor"

    def test_supervisor_lists_datasets(self) -> None:
        from sqldim.application.ask import KNOWN_DATASETS

        # Canonical domain names exposed by the consolidated registry
        assert "entertainment" in KNOWN_DATASETS
        assert "commerce" in KNOWN_DATASETS


class TestCmdSupervisorExecution:
    """cmd_supervisor runs the complete stub graph pipeline."""

    def test_basic_invocation_returns_zero(self, capsys) -> None:
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        args = argparse.Namespace(
            question="Which datasets are available?",
            model=None,
            model_provider=None,
            datasets=None,
            verbose=False,
        )
        rc = cmd_supervisor(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert out.strip()  # synthesis was printed

    def test_verbose_mode_prints_node_info(self, capsys) -> None:
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        args = argparse.Namespace(
            question="What tools do you have?",
            model=None,
            model_provider=None,
            datasets=None,
            verbose=True,
        )
        rc = cmd_supervisor(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Nodes visited" in out

    def test_invalid_model_provider_prints_warning(self, capsys) -> None:
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        args = argparse.Namespace(
            question="test",
            model=None,
            model_provider="invalid_provider",
            datasets=None,
            verbose=False,
        )
        rc = cmd_supervisor(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Warning" in out

    def test_datasets_flag_restricts_available(self, capsys) -> None:
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        args = argparse.Namespace(
            question="What can I ask?",
            model=None,
            model_provider=None,
            datasets=["commerce"],
            verbose=False,
        )
        rc = cmd_supervisor(args)
        assert rc == 0

    def test_unknown_dataset_returns_1(self, capsys) -> None:
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        args = argparse.Namespace(
            question="test",
            model=None,
            model_provider=None,
            datasets=["nonexistent_xyz"],
            verbose=False,
        )
        rc = cmd_supervisor(args)
        assert rc == 1
        out = capsys.readouterr().out
        assert "Unknown" in out

    def test_datasets_flag_in_parser(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(
            [
                "nl",
                "ask",
                "question",
                "--mode",
                "supervisor",
                "--datasets",
                "ecommerce",
                "fintech",
            ]
        )
        assert args.datasets == ["ecommerce", "fintech"]

    def test_verbose_with_error_hop_prints_error(self, capsys, monkeypatch) -> None:
        """Verbose mode with a failed tool hop covers supervisor.py line 71."""
        import argparse
        from sqldim.cli.supervisor import cmd_supervisor

        def _fake_build(*a, **kw):
            class _G:
                def invoke(self, state, config=None):
                    return {
                        "synthesis": "partial",
                        "visited_nodes": ["plan"],
                        "node_timings": {"plan": 5},
                        "results": [
                            {
                                "tool_name": "bad_tool",
                                "success": False,
                                "data": {},
                                "error": "oops",
                            }
                        ],
                    }

            return _G()

        monkeypatch.setattr(
            "sqldim.core.query.dgm.nl.supervisor._graph.build_supervisor_graph",
            _fake_build,
        )

        args = argparse.Namespace(
            question="test",
            model=None,
            model_provider=None,
            datasets=None,
            verbose=True,
        )
        rc = cmd_supervisor(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "ERROR" in out and "oops" in out
