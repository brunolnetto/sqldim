"""ContractEngine — executes all rules in a single DuckDB UNION ALL round-trip."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import duckdb

from sqldim.contracts.report import ContractReport, ContractViolation
from sqldim.contracts.schema import ColumnSpec

if TYPE_CHECKING:
    from sqldim.contracts.composite import Contract


# ---------------------------------------------------------------------------
# Schema evolution report
# ---------------------------------------------------------------------------


@dataclass
class EvolutionChange:
    """Describes a single column-level schema change."""

    change_type: (
        str  # "added", "widened", "narrowed", "renamed", "removed", "type_changed"
    )
    column: str
    detail: str = ""


@dataclass
class EvolutionReport:
    """Structured result of :meth:`ContractEngine.check_evolution_safety`.

    Changes are buckered into three tiers matching the Schema Evolution Policy ADR:

    * **safe_changes** — deploy without migration (nullable adds, metadata-only changes).
    * **additive_changes** — require explicit migration before deploy (NOT NULL adds, widening).
    * **breaking_changes** — require backfill + rewrite (type narrowing, renames, removals).
    * **required_migrations** — suggested DDL snippets for *additive_changes*.
    """

    safe_changes: list[EvolutionChange] = field(default_factory=list)
    additive_changes: list[EvolutionChange] = field(default_factory=list)
    breaking_changes: list[EvolutionChange] = field(default_factory=list)
    required_migrations: list[str] = field(default_factory=list)

    @property
    def is_safe(self) -> bool:
        """True when no additive or breaking changes require operator action."""
        return not self.additive_changes and not self.breaking_changes

    def summary(self) -> str:
        def _tier(label: str, changes: list) -> str:
            return f"{label} ({len(changes)}): " + ", ".join(c.column for c in changes)

        lines = [
            _tier(label, changes)
            for label, changes in (
                ("Safe", self.safe_changes),
                ("Additive", self.additive_changes),
                ("Breaking", self.breaking_changes),
            )
            if changes
        ]
        return "\n".join(lines) if lines else "No schema changes detected."


# Integer type widening order (higher index = wider type)
_INT_HIERARCHY = ["tinyint", "smallint", "int", "integer", "bigint"]
_FLOAT_HIERARCHY = ["float", "real", "double", "decimal"]


def _is_widening(old_dtype: str, new_dtype: str) -> bool:
    """Return True when *new_dtype* is a safe widening of *old_dtype*."""
    for hierarchy in (_INT_HIERARCHY, _FLOAT_HIERARCHY):
        if old_dtype.lower() in hierarchy and new_dtype.lower() in hierarchy:
            return hierarchy.index(old_dtype.lower()) < hierarchy.index(
                new_dtype.lower()
            )
    return False


def _classify_type_change(old: ColumnSpec, new: ColumnSpec) -> EvolutionChange:
    if _is_widening(old.dtype, new.dtype):
        return EvolutionChange(
            change_type="widened",
            column=old.name,
            detail=f"{old.dtype} \u2192 {new.dtype} (safe widening)",
        )
    return EvolutionChange(
        change_type="type_changed",
        column=old.name,
        detail=f"{old.dtype} \u2192 {new.dtype}",
    )


def _process_removed_col(name: str, report: "EvolutionReport") -> None:
    report.breaking_changes.append(
        EvolutionChange(
            change_type="removed",
            column=name,
            detail=f"Column '{name}' removed — data loss risk",
        )
    )


def _check_added(
    old_map: dict, new_map: dict, backend: str, report: "EvolutionReport"
) -> None:
    for name, new_col in new_map.items():
        if name not in old_map:
            _process_added_col(name, new_col, backend, report)


def _check_removed(old_map: dict, new_map: dict, report: "EvolutionReport") -> None:
    for name in old_map:
        if name not in new_map:
            _process_removed_col(name, report)


def _check_modified(old_map: dict, new_map: dict, report: "EvolutionReport") -> None:
    for name, old_col in old_map.items():
        if name in new_map:
            _process_modified_col(name, old_col, new_map[name], report)


def _check_schema_maps(
    old_map: dict, new_map: dict, backend: str, report: "EvolutionReport"
) -> None:
    _check_added(old_map, new_map, backend, report)
    _check_removed(old_map, new_map, report)
    _check_modified(old_map, new_map, report)


def _process_added_col(
    name: str, new_col: "ColumnSpec", backend: str, report: "EvolutionReport"
) -> None:
    """Classify a newly added column and append to *report*."""
    if new_col.nullable and backend == "parquet":
        report.safe_changes.append(
            EvolutionChange(
                change_type="added",
                column=name,
                detail=f"Nullable column '{name}' ({new_col.dtype})",
            )
        )
    elif new_col.nullable:
        report.additive_changes.append(
            EvolutionChange(
                change_type="added",
                column=name,
                detail=f"Nullable column '{name}' ({new_col.dtype})",
            )
        )
        report.required_migrations.append(
            f"ALTER TABLE <table> ADD COLUMN {name} {new_col.dtype};"
        )
    else:
        report.additive_changes.append(
            EvolutionChange(
                change_type="added",
                column=name,
                detail=f"NOT NULL column '{name}' ({new_col.dtype}) \u2014 backfill required",
            )
        )
        report.required_migrations.append(
            f"ALTER TABLE <table> ADD COLUMN {name} {new_col.dtype} DEFAULT <value>;"
        )


def _process_modified_col(
    name: str, old_col: "ColumnSpec", new_col: "ColumnSpec", report: "EvolutionReport"
) -> None:
    """Classify a modified column and append to *report*."""
    if old_col.dtype != new_col.dtype:
        change = _classify_type_change(old_col, new_col)
        if change.change_type == "widened":
            report.additive_changes.append(change)
            report.required_migrations.append(
                f"ALTER TABLE <table> ALTER COLUMN {name} TYPE {new_col.dtype};"
            )
        else:
            report.breaking_changes.append(change)
    elif old_col.nullable and not new_col.nullable:
        report.breaking_changes.append(
            EvolutionChange(
                change_type="narrowed",
                column=name,
                detail=f"'{name}' nullable \u2192 NOT NULL \u2014 backfill NULLs first",
            )
        )


class ContractEngine:
    """Stateless engine that validates a view against a :class:`Contract`.

    All rules are UNION ALL'd into one DuckDB round-trip so validation
    overhead is O(1 scan) regardless of rule count.
    """

    @staticmethod
    def _build_violations(rows: list) -> list:
        """Convert raw DuckDB result rows into ContractViolation objects."""
        return [
            ContractViolation(rule=row[0], severity=row[1], count=row[2], detail=row[3])
            for row in rows
        ]

    def validate(
        self,
        con: duckdb.DuckDBPyConnection,
        view: str,
        contract: "Contract | None",
    ) -> ContractReport:
        """Return a :class:`ContractReport` for *view* against *contract*.

        Returns an empty report when *contract* is ``None`` or has no rules.
        """
        if contract is None or not getattr(contract, "rules", []):
            return ContractReport.empty()

        sqls = [f"SELECT * FROM ({r.as_sql(view)})" for r in contract.rules]
        union_sql = " UNION ALL ".join(sqls)

        t0 = time.perf_counter()
        rows = con.execute(union_sql).fetchall()
        elapsed = time.perf_counter() - t0

        return ContractReport(
            violations=self._build_violations(rows), view=view, elapsed_s=elapsed
        )

    @staticmethod
    def check_evolution_safety(
        old_schema: list[ColumnSpec],
        new_schema: list[ColumnSpec],
        backend: str = "parquet",
    ) -> EvolutionReport:
        """Compare two lists of :class:`ColumnSpec` and classify every change.

        Parameters
        ----------
        old_schema:
            The previously deployed column specifications.
        new_schema:
            The proposed (new model) column specifications.
        backend:
            One of ``"parquet"``, ``"delta"``, or ``"postgres"``.  Some changes
            are safe on Parquet (``union_by_name``) but additive on Postgres.

        Returns
        -------
        EvolutionReport
            Structured report with safe / additive / breaking tiers and
            suggested DDL migration snippets.
        """
        report = EvolutionReport()
        old_map = {c.name: c for c in old_schema}
        new_map = {c.name: c for c in new_schema}
        _check_schema_maps(old_map, new_map, backend, report)
        return report
