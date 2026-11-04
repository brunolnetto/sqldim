"""Tests for the showtime CLI command."""

from __future__ import annotations

import argparse


class TestShowtimeSubcommandParser:
    """The showtime subcommand is registered in the CLI parser."""

    def test_showtime_subcommand_exists(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["nl", "showtime"])
        assert args.func is not None

    def test_showtime_with_datasets_flag(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["nl", "showtime", "--datasets", "commerce"])
        assert args.datasets == ["commerce"]

    def test_showtime_with_verbose(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["nl", "showtime", "--verbose"])
        assert args.verbose is True


class TestDemoScenarios:
    """_build_scenario_list generates the right set of scenarios."""

    def test_cross_dataset_always_included(self) -> None:
        from sqldim.cli.showtime import _build_scenario_list

        scenarios = _build_scenario_list([])
        assert len(scenarios) >= 2  # cross-dataset scenarios
        assert scenarios[0]["title"] == "Discovery"

    def test_commerce_scenarios_included(self) -> None:
        from sqldim.cli.showtime import _build_scenario_list

        scenarios = _build_scenario_list(["commerce"])
        titles = [s["title"] for s in scenarios]
        assert "Revenue query (commerce)" in titles
        assert "Customer analysis (commerce)" in titles

    def test_unknown_dataset_returns_only_cross(self) -> None:
        from sqldim.cli.showtime import _build_scenario_list

        scenarios = _build_scenario_list(["unknown_ds"])
        # Only cross-dataset scenarios
        assert all(
            s["title"] in ("Discovery", "Cross-dataset exploration") for s in scenarios
        )


class TestCmdShowtimeValidation:
    """cmd_showtime validates datasets and runs scenarios."""

    def test_unknown_dataset_returns_1(self) -> None:
        from sqldim.cli.showtime import cmd_showtime

        args = argparse.Namespace(
            datasets=["nonexistent_xyz"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_showtime(args)
        assert rc == 1

    def test_runs_with_all_datasets(self, monkeypatch) -> None:
        """Showtime completes with all datasets (auto-pressing Enter)."""
        from sqldim.cli.showtime import cmd_showtime

        # Auto-press Enter for the "Press Enter" prompts
        monkeypatch.setattr("rich.console.Console.input", lambda self, *a, **kw: "")

        args = argparse.Namespace(
            datasets=None,  # all datasets
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_showtime(args)
        assert rc == 0

    def test_runs_with_specific_dataset(self, monkeypatch) -> None:
        """Showtime works with a single dataset selection."""
        monkeypatch.setattr("rich.console.Console.input", lambda self, *a, **kw: "")

        from sqldim.cli.showtime import cmd_showtime

        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_showtime(args)
        assert rc == 0

    def test_verbose_mode(self, monkeypatch) -> None:
        """Verbose mode prints node details."""
        monkeypatch.setattr("rich.console.Console.input", lambda self, *a, **kw: "")

        from sqldim.cli.showtime import cmd_showtime

        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider=None,
            model=None,
            verbose=True,
        )
        rc = cmd_showtime(args)
        assert rc == 0

    def test_ctrl_c_stops_early(self, monkeypatch) -> None:
        """Ctrl-C during 'Press Enter' stops the demo gracefully."""

        def _raise_keyboard_interrupt(self, *a, **kw):
            raise KeyboardInterrupt

        monkeypatch.setattr("rich.console.Console.input", _raise_keyboard_interrupt)

        from sqldim.cli.showtime import cmd_showtime

        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider=None,
            model=None,
            verbose=False,
        )
        rc = cmd_showtime(args)
        assert rc == 0

    def test_invalid_model_provider(self, monkeypatch) -> None:
        """An invalid model provider prints a warning but continues."""
        monkeypatch.setattr("rich.console.Console.input", lambda self, *a, **kw: "")

        from sqldim.cli.showtime import cmd_showtime

        args = argparse.Namespace(
            datasets=["commerce"],
            model_provider="invalid_provider",
            model=None,
            verbose=False,
        )
        rc = cmd_showtime(args)
        assert rc == 0
