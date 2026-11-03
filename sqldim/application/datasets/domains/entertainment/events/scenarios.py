"""Scenario re-exports for the entertainment domain."""

from sqldim.application.datasets.domains.entertainment.events.movies import (
    apply_new_release,
)

__all__ = ["apply_new_release"]
