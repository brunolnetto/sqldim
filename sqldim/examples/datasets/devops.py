"""
sqldim/examples/datasets/devops.py
=====================================
DevOps / developer-tooling domain: GitHub Issues.

``GitHubIssuesSource`` simulates the DuckDB staging area produced by a
`dlt <https://dlthub.com>`_ pipeline that pulls issues from the GitHub API.
sqldim then consumes the staging table via ``_DatasetSource`` and processes
it into a SCD Type 2 dimension.

OLTP → SCD2 pipeline::

    dlt pipeline  ──►  staging DB (github_staging.issues)
                  ──►  _DatasetSource
                  ──►  LazySCDProcessor
                  ──►  dim_github_issue (SCD2)
"""
from __future__ import annotations

import random
from typing import Any

import duckdb
from faker import Faker

from sqldim.examples.datasets.base import (
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.examples.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)


# ── Vocabulary ────────────────────────────────────────────────────────────────

_AUTHORS = ["alice", "bob", "carol", "dave", "eve", "frank", "grace"]
_LABEL_POOLS = ["bug", "enhancement", "docs", "ci", "refactor", "test", "question"]

def _esc(s: str) -> str:
    return s.replace("'", "''")


def _make_title(fake: Faker) -> str:
    templates = [
        "Fix {component} {noun} handling",
        "Add {component} support to {noun}",
        "Refactor {noun} to use {component}",
        "{component}: improve {noun} performance",
        "CI: pin {noun} version",
        "{component}: {noun} migration",
        "Add {noun} support to {component}",
    ]
    components = ["DuckDB", "Narwhals", "scd_engine", "dimensions", "loaders",
                  "graph", "sources", "query", "migrations", "bitmask"]
    nouns = ["loader", "adapter", "handler", "engine", "strategy",
             "pipeline", "schema", "resolver", "processor", "backfill"]
    return random.choice(templates).format(
        component=random.choice(components),
        noun=random.choice(nouns),
    )


# ── Schema + event declaration ────────────────────────────────────────────────

_GITHUB_SPEC = DatasetSpec(
    name="github_issue",
    schemas={
        "source": EntitySchema(
            name="github_issue",
            fields=[
                FieldSpec("issue_id", "INTEGER", kind="seq"),
                FieldSpec(
                    "title", "VARCHAR", kind="computed",
                    fn=lambda fake, i: _make_title(fake),
                ),
                FieldSpec("state",  "VARCHAR", kind="choices", choices=["open", "closed"]),
                FieldSpec("author", "VARCHAR", kind="choices", choices=_AUTHORS),
                FieldSpec("labels", "VARCHAR", kind="choices", choices=_LABEL_POOLS),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "state",
                condition=lambda i, r: r["state"] == "open" and i % 2 == 0,
                mutate=lambda v, r, fake: "closed",
            ),
        ],
        # Append one brand-new issue to the event batch (simulates a newly opened issue)
        new_rows_fn=lambda rows, fake: [
            {
                "issue_id": max(r["issue_id"] for r in rows) + 1,
                "title":    _make_title(fake),
                "state":    "open",
                "author":   random.choice(_AUTHORS),
                "labels":   random.choice(_LABEL_POOLS),
            }
        ],
    ),
)


@DatasetFactory.register("github_issues")
class GitHubIssuesSource(SchematicSource):
    """
    OLTP GitHub issue tracker — simulates a dlt pipeline staging database.

    Schema is declared via ``_GITHUB_SCHEMA`` (``EntitySchema``).  Events are
    driven by ``_GITHUB_EVENTS`` (``EventSpec``):
      * Open issues at even indices get closed.
      * One brand-new issue is appended to the event batch via ``new_rows_fn``.

    Use ``seed_staging(path, batch)`` to write the staging DuckDB file that
    ``_DatasetSource`` reads.  Use ``setup(con, table)`` to create the empty
    SCD2 warehouse target.

    Staging schema (dlt output)::

        issue_id    INTEGER
        title       VARCHAR
        state       VARCHAR   -- 'open' | 'closed'
        author      VARCHAR
        labels      VARCHAR

    sqldim output (DIM_DDL)::

        + valid_from  VARCHAR
        + valid_to    VARCHAR
        + is_current  BOOLEAN
        + checksum    VARCHAR
    """

    _spec = _GITHUB_SPEC

    provider = SourceProvider(
        name="GitHub Issues API",
        description="Issue tracker data loaded via dlt GitHub source pipeline.",
        url="https://dlthub.com/docs/dlt-ecosystem/verified-sources/github",
        auth_required=True,
        requires=["dlt", "dlt[github]"],
    )

    @property
    def STAGING_DDL(self) -> str:  # type: ignore[override]
        """DDL for the dlt-managed staging table (github_staging.issues)."""
        return self._spec.source.oltp_ddl("github_staging.issues")

    # ── Staging DB helpers ────────────────────────────────────────────────

    def seed_staging(self, path: str, batch: str = "initial") -> None:
        """
        Populate a DuckDB file with issue rows in the ``github_staging``
        schema, simulating what a dlt pipeline produces.

        batch="initial" → initial snapshot
        batch="updated" → event batch (some issues closed + new issue)
        """
        rows = self._initial if batch == "initial" else self._events1
        con  = duckdb.connect(path)
        con.execute("CREATE SCHEMA IF NOT EXISTS github_staging")
        con.execute("DROP TABLE IF EXISTS github_staging.issues")
        con.execute(self.STAGING_DDL)
        self._insert_rows(con, "github_staging.issues", rows)
        con.close()

    def _insert_rows(
        self,
        con: duckdb.DuckDBPyConnection,
        table: str,
        rows: list[dict],
    ) -> None:
        values = ", ".join(
            f"({r['issue_id']}, '{_esc(r['title'])}', '{r['state']}', "
            f"'{r['author']}', '{r['labels']}')"
            for r in rows
        )
        con.execute(f"INSERT INTO {table} VALUES {values}")
