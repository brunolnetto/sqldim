"""ContractEngine — executes all rules in a single DuckDB UNION ALL round-trip."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

import duckdb

from sqldim.contracts.report import ContractReport, ContractViolation

if TYPE_CHECKING:
    from sqldim.contracts.composite import Contract


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

        sqls = [
            f"SELECT * FROM ({r.as_sql(view)})"
            for r in contract.rules
        ]
        union_sql = " UNION ALL ".join(sqls)

        t0 = time.perf_counter()
        rows = con.execute(union_sql).fetchall()
        elapsed = time.perf_counter() - t0

        return ContractReport(
            violations=self._build_violations(rows), view=view, elapsed_s=elapsed
        )
