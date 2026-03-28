"""GitHub-issues-source OLTP drift events for the devops domain."""

from __future__ import annotations

import duckdb


def apply_issue_resolution(con: duckdb.DuckDBPyConnection) -> None:
    """Close all currently-open GitHub issues.

    **OLTP mutation** — sets ``github_issues.state = 'closed'`` for every row
    where ``state = 'open'``.  Open/closed issue count queries will
    show the shift immediately.
    """
    con.execute(
        "UPDATE github_issues SET state = 'closed' WHERE state = 'open'"
    )


__all__ = ["apply_issue_resolution"]
