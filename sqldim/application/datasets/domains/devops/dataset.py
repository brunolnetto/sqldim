"""
devops domain Dataset — FK-ordered collection of all OLTP sources.

Instantiate and use with a DuckDB connection:

    import duckdb
    from sqldim.application.datasets.domains.devops.dataset import devops_dataset

    con = duckdb.connect()
    devops_dataset.setup(con)

    for table, rows in devops_dataset.snapshots().items():
        print(f"{table}: {len(rows)} rows")

    devops_dataset.teardown(con)
"""
from sqldim.application.datasets.dataset import Dataset
from sqldim.application.datasets.domains.devops.sources import (
    GitHubIssuesSource,
)

devops_dataset = Dataset(
    "devops",
    [
        (GitHubIssuesSource(), "github_issues"),
    ],
)

__all__ = ["devops_dataset"]
