"""Showcase: accumulating, snapshot, and transaction fact table patterns.

Demonstrates how :class:`AccumulatingFact`, :class:`PeriodicSnapshotFact`,
and :class:`TransactionFact` are loaded against a live DuckDB instance.
"""

from sqldim.examples.features.fact_patterns.showcase import run_showcase

__all__ = ["run_showcase"]
