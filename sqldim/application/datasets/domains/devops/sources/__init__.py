"""sources sub-package for the devops domain — exposes all source classes."""

from sqldim.application.datasets.domains.devops.sources.github_issues import (
    GitHubIssuesSource,
    _GITHUB_SPEC,
)

__all__ = [
    "GitHubIssuesSource",
    "_GITHUB_SPEC",
]
