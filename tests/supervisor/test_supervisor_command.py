"""Tests for the supervisor CLI command."""

from __future__ import annotations


class TestSupervisorCommandDiscovery:
    """The supervisor command must be available via CLI parser."""

    def test_supervisor_subcommand_exists(self) -> None:
        from sqldim.cli import build_parser

        parser = build_parser()
        # Parse a minimal supervisor invocation
        args = parser.parse_args(["supervisor", "test question"])
        assert args.func is not None
        assert args.question == "test question"

    def test_supervisor_lists_datasets(self) -> None:
        from sqldim.application.ask import KNOWN_DATASETS

        # At least social_network and ecommerce should be available
        assert "social_network" in KNOWN_DATASETS
        assert "ecommerce" in KNOWN_DATASETS
