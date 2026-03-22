"""Tests for the SQL-validation contracts engine — red phase.

Covers: Severity, ContractViolation, ContractReport, ContractViolationError,
Rule ABC, all built-in column/table rules, ContractEngine,
SourceContract / StateContract / OutputContract, and SCD2 rules.
"""
from __future__ import annotations

import duckdb
import pytest

from sqldim.contracts.reporting.report import (
    ContractViolation,
    ContractReport,
    Severity,
)
from sqldim.contracts.exceptions import ContractViolationError
from sqldim.contracts.validation.rules import (
    Rule,
    NotNull,
    NoDuplicates,
    NullRate,
    TypeMatch,
    ColumnExists,
    RowCount,
    ValueRange,
    RegexMatch,
)
from sqldim.contracts.engine import ContractEngine
from sqldim.contracts.reporting.composite import (
    SourceContract,
    StateContract,
    OutputContract,
)
from sqldim.contracts.validation.scd_rules import (
    SCD2Invariants,
    NoOrphanVersions,
    MonotonicValidFrom,
    NoGapPeriods,
    HashConsistency,
)
from sqldim.contracts.validation.freshness import Freshness, RowCountDelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _con_with(sql: str) -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB connection with the given CREATE/INSERT SQL."""
    con = duckdb.connect()
    con.execute(sql)
    return con


def _view(con, view_sql: str, name: str = "v") -> str:
    con.execute(f"CREATE OR REPLACE VIEW {name} AS {view_sql}")
    return name


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------

class TestSeverity:
    def test_error_and_warning_and_info_exist(self):
        assert Severity.ERROR
        assert Severity.WARNING
        assert Severity.INFO

    def test_values_are_strings(self):
        assert Severity.ERROR.value == "error"
        assert Severity.WARNING.value == "warning"
        assert Severity.INFO.value == "info"


# ---------------------------------------------------------------------------
# ContractViolation
# ---------------------------------------------------------------------------

class TestContractViolation:
    def test_fields(self):
        v = ContractViolation(rule="NOT_NULL", severity="error", count=5, detail="col=x")
        assert v.rule == "NOT_NULL"
        assert v.severity == "error"
        assert v.count == 5
        assert v.detail == "col=x"


# ---------------------------------------------------------------------------
# ContractReport
# ---------------------------------------------------------------------------

class TestContractReport:
    def test_empty_factory(self):
        r = ContractReport.empty()
        assert r.violations == []
        assert r.view == ""
        assert r.elapsed_s == 0.0

    def test_has_errors_true(self):
        r = ContractReport(
            violations=[ContractViolation("R", "error", 1, "")],
            view="v",
        )
        assert r.has_errors() is True

    def test_has_errors_false_when_only_warnings(self):
        r = ContractReport(
            violations=[ContractViolation("R", "warning", 1, "")],
            view="v",
        )
        assert r.has_errors() is False

    def test_has_warnings(self):
        r = ContractReport(
            violations=[ContractViolation("R", "warning", 2, "")],
            view="v",
        )
        assert r.has_warnings() is True

    def test_summary_contains_rule_name(self):
        r = ContractReport(
            violations=[ContractViolation("NOT_NULL", "error", 10, "col=nk")],
            view="incoming",
        )
        s = r.summary()
        assert "NOT_NULL" in s
        assert "10" in s

    def test_to_dict_round_trips(self):
        import json
        r = ContractReport(
            violations=[ContractViolation("NOT_NULL", "error", 3, "detail")],
            view="v",
            elapsed_s=0.05,
        )
        d = r.to_dict()
        assert json.dumps(d)  # no precision loss / serialisation error
        assert d["violations"][0]["rule"] == "NOT_NULL"
        assert d["elapsed_s"] == 0.05

    def test_log_does_not_raise(self):
        r = ContractReport(
            violations=[
                ContractViolation("A", "error", 1, ""),
                ContractViolation("B", "warning", 2, ""),
                ContractViolation("C", "info", 3, ""),
            ],
            view="v",
        )
        r.log()  # must not raise


# ---------------------------------------------------------------------------
# ContractViolationError
# ---------------------------------------------------------------------------

class TestContractViolationError:
    def test_carries_report(self):
        report = ContractReport(
            violations=[ContractViolation("NOT_NULL", "error", 5, "col=nk")],
            view="incoming",
        )
        exc = ContractViolationError(report)
        assert exc.report is report

    def test_str_contains_violations(self):
        report = ContractReport(
            violations=[ContractViolation("NOT_NULL", "error", 5, "col=nk")],
            view="incoming",
        )
        exc = ContractViolationError(report)
        assert "NOT_NULL" in str(exc)

    def test_is_exception(self):
        report = ContractReport.empty()
        exc = ContractViolationError(report)
        assert isinstance(exc, Exception)


# ---------------------------------------------------------------------------
# Rule ABC
# ---------------------------------------------------------------------------

class TestRuleABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Rule()  # type: ignore

    def test_must_implement_as_sql(self):
        class Partial(Rule):
            name = "partial"
            severity = Severity.ERROR

        with pytest.raises(TypeError):
            Partial()  # type: ignore

    def test_concrete_returns_string(self):
        class MyRule(Rule):
            name = "my_rule"
            severity = Severity.ERROR

            def as_sql(self, view: str) -> str:
                return "SELECT 'my_rule' AS rule, 'error' AS severity, 0 AS violations, '' AS detail"

        r = MyRule()
        sql = r.as_sql("some_view")
        assert isinstance(sql, str)
        assert "my_rule" in sql


# ---------------------------------------------------------------------------
# NotNull rule
# ---------------------------------------------------------------------------

class TestNotNull:
    def test_passes_on_no_nulls(self):
        con = _con_with("CREATE TABLE t AS SELECT 1 AS nk UNION ALL SELECT 2 AS nk")
        v = _view(con, "SELECT * FROM t")
        engine = ContractEngine()
        from sqldim.contracts.reporting.composite import SourceContract
        sc = SourceContract(rules=[NotNull("nk")])
        report = engine.validate(con, v, sc)
        assert not report.has_errors()

    def test_fails_on_null(self):
        con = _con_with("CREATE TABLE t AS SELECT NULL::INT AS nk")
        v = _view(con, "SELECT * FROM t")
        engine = ContractEngine()
        sc = SourceContract(rules=[NotNull("nk")])
        report = engine.validate(con, v, sc)
        assert report.has_errors()
        assert any(r.rule == "NOT_NULL" for r in report.violations)

    def test_default_severity_error(self):
        r = NotNull("col")
        assert r.severity == Severity.ERROR


# ---------------------------------------------------------------------------
# NoDuplicates rule
# ---------------------------------------------------------------------------

class TestNoDuplicates:
    def test_passes_unique(self):
        con = _con_with("CREATE TABLE t AS SELECT 'a' AS nk UNION ALL SELECT 'b' AS nk")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[NoDuplicates("nk")]))
        assert not report.has_errors()

    def test_fails_duplicate(self):
        con = _con_with("CREATE TABLE t AS SELECT 'a' AS nk UNION ALL SELECT 'a' AS nk")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[NoDuplicates("nk")]))
        assert report.has_errors()
        assert any(r.rule == "NO_DUPLICATES" for r in report.violations)

    def test_default_severity_error(self):
        assert NoDuplicates("x").severity == Severity.ERROR


# ---------------------------------------------------------------------------
# NullRate rule
# ---------------------------------------------------------------------------

class TestNullRate:
    def test_passes_under_threshold(self):
        con = _con_with(
            "CREATE TABLE t AS SELECT * FROM (VALUES ('a'), ('b'), (NULL)) AS x(col)"
        )
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[NullRate("col", max_pct=0.5)]))
        assert not report.has_errors()

    def test_fails_over_threshold(self):
        con = _con_with(
            "CREATE TABLE t AS SELECT * FROM (VALUES ('a'), (NULL), (NULL)) AS x(col)"
        )
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[NullRate("col", max_pct=0.1)]))
        assert report.has_warnings()

    def test_default_severity_warning(self):
        assert NullRate("x", max_pct=0.1).severity == Severity.WARNING


# ---------------------------------------------------------------------------
# ColumnExists rule
# ---------------------------------------------------------------------------

class TestColumnExists:
    def test_passes_when_column_present(self):
        con = _con_with("CREATE TABLE t AS SELECT 1 AS id, 'x' AS name")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[ColumnExists("id")]))
        assert not report.has_errors()

    def test_fails_when_column_missing(self):
        con = _con_with("CREATE TABLE t AS SELECT 1 AS id")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[ColumnExists("foobar")]))
        assert report.has_errors()

    def test_default_severity_error(self):
        assert ColumnExists("x").severity == Severity.ERROR


# ---------------------------------------------------------------------------
# RowCount rule
# ---------------------------------------------------------------------------

class TestRowCount:
    def test_passes_within_range(self):
        con = _con_with("CREATE TABLE t AS SELECT unnest(range(100)) AS x")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[RowCount(min_rows=50, max_rows=200)]))
        assert not report.has_errors()

    def test_fails_below_min(self):
        con = _con_with("CREATE TABLE t AS SELECT 1 AS x")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[RowCount(min_rows=100)]))
        assert report.has_warnings() or report.has_errors()

    def test_fails_above_max(self):
        con = _con_with("CREATE TABLE t AS SELECT unnest(range(1000)) AS x")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[RowCount(max_rows=5)]))
        assert report.has_warnings() or report.has_errors()

    def test_default_severity_warning(self):
        assert RowCount(min_rows=1).severity == Severity.WARNING


# ---------------------------------------------------------------------------
# ValueRange rule
# ---------------------------------------------------------------------------

class TestValueRange:
    def test_passes_within_range(self):
        con = _con_with("CREATE TABLE t AS SELECT 50 AS v")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[ValueRange("v", min_val=0, max_val=100)]))
        assert not report.has_errors()

    def test_fails_below_min(self):
        con = _con_with("CREATE TABLE t AS SELECT -5 AS v")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[ValueRange("v", min_val=0)]))
        assert report.has_warnings()

    def test_default_severity_warning(self):
        assert ValueRange("x", min_val=0).severity == Severity.WARNING


# ---------------------------------------------------------------------------
# RegexMatch rule
# ---------------------------------------------------------------------------

class TestRegexMatch:
    def test_passes_matching(self):
        con = _con_with("CREATE TABLE t AS SELECT '2024-01-01' AS dt")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[RegexMatch("dt", r"^\d{4}-\d{2}-\d{2}$")])
        )
        assert not report.has_errors()

    def test_fails_non_matching(self):
        con = _con_with("CREATE TABLE t AS SELECT 'not-a-date' AS dt")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[RegexMatch("dt", r"^\d{4}-\d{2}-\d{2}$")])
        )
        assert report.has_warnings()

    def test_default_severity_warning(self):
        assert RegexMatch("col", r"\d+").severity == Severity.WARNING


# ---------------------------------------------------------------------------
# TypeMatch rule
# ---------------------------------------------------------------------------

class TestTypeMatch:
    def test_passes_correct_type(self):
        con = _con_with("CREATE TABLE t AS SELECT 42 AS amount")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[TypeMatch("amount", "INTEGER")])
        )
        assert not report.has_errors()

    def test_fails_wrong_type(self):
        con = _con_with("CREATE TABLE t AS SELECT 'abc' AS amount")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[TypeMatch("amount", "INTEGER")])
        )
        assert report.has_errors()

    def test_default_severity_error(self):
        assert TypeMatch("col", "DOUBLE").severity == Severity.ERROR


# ---------------------------------------------------------------------------
# ContractEngine
# ---------------------------------------------------------------------------

class TestContractEngine:
    def test_empty_contract_returns_ok(self):
        con = duckdb.connect()
        report = ContractEngine().validate(con, "v", SourceContract(rules=[]))
        assert not report.has_errors()
        assert report.violations == []

    def test_none_contract_returns_empty(self):
        con = duckdb.connect()
        report = ContractEngine().validate(con, "v", None)
        assert isinstance(report, ContractReport)
        assert not report.has_errors()

    def test_multiple_rules_union(self):
        con = _con_with(
            "CREATE TABLE t AS SELECT NULL::VARCHAR AS nk UNION ALL SELECT NULL::VARCHAR AS nk"
        )
        v = _view(con, "SELECT * FROM t")
        sc = SourceContract(rules=[NotNull("nk"), NoDuplicates("nk")])
        report = ContractEngine().validate(con, v, sc)
        # Both rules fire
        rule_names = {r.rule for r in report.violations}
        assert "NOT_NULL" in rule_names

    def test_elapsed_s_populated(self):
        con = _con_with("CREATE TABLE t AS SELECT 1 AS nk")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(con, v, SourceContract(rules=[NotNull("nk")]))
        assert report.elapsed_s >= 0.0


# ---------------------------------------------------------------------------
# Composite contracts (SourceContract / StateContract / OutputContract)
# ---------------------------------------------------------------------------

class TestCompositeContracts:
    def test_source_contract_label(self):
        sc = SourceContract(rules=[])
        assert sc.label == "source"

    def test_state_contract_label(self):
        sc = StateContract(rules=[])
        assert sc.label == "state"

    def test_output_contract_label(self):
        sc = OutputContract(rules=[])
        assert sc.label == "output"

    def test_rules_attribute(self):
        r = NotNull("nk")
        sc = SourceContract(rules=[r])
        assert sc.rules == [r]


# ---------------------------------------------------------------------------
# SCD2 rules
# ---------------------------------------------------------------------------

def _scd2_view(con, rows: list[dict], name: str = "dim") -> str:
    """Seed a DuckDB view with SCD2-shaped rows for rule tests."""
    cols = ["nk", "name", "is_current", "valid_from", "valid_to"]
    val_parts = []
    for r in rows:
        vc = r.get("valid_to", "NULL")
        val_parts.append(
            f"('{r['nk']}', '{r['name']}', {str(r['is_current']).upper()}, "
            f"'{r['valid_from']}', {vc if vc == 'NULL' else repr(vc)})"
        )
    values = ", ".join(val_parts)
    con.execute(
        f"CREATE OR REPLACE VIEW {name} AS "
        f"SELECT * FROM (VALUES {values}) AS x({', '.join(cols)})"
    )
    return name


class TestSCD2Invariants:
    def test_passes_valid_dimension(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "Alice", "is_current": True, "valid_from": "2024-01-01", "valid_to": "NULL"},
            {"nk": "NK2", "name": "Bob", "is_current": True, "valid_from": "2024-01-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[SCD2Invariants("nk")])
        )
        assert not report.has_errors()

    def test_fails_duplicate_current(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "Alice", "is_current": True, "valid_from": "2024-01-01", "valid_to": "NULL"},
            {"nk": "NK1", "name": "Alice2", "is_current": True, "valid_from": "2024-06-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[SCD2Invariants("nk")])
        )
        assert report.has_errors()

    def test_fails_null_valid_from_on_current(self):
        con = duckdb.connect()
        con.execute(
            "CREATE OR REPLACE VIEW dim AS "
            "SELECT * FROM (VALUES ('NK1', 'Alice', TRUE, NULL::VARCHAR, NULL::VARCHAR)) "
            "AS x(nk, name, is_current, valid_from, valid_to)"
        )
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[SCD2Invariants("nk")])
        )
        assert report.has_errors()

    def test_default_severity_error(self):
        assert SCD2Invariants("nk").severity == Severity.ERROR


class TestNoOrphanVersions:
    def test_passes_with_successor(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "Old", "is_current": False, "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
            {"nk": "NK1", "name": "New", "is_current": True, "valid_from": "2024-06-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[NoOrphanVersions("nk")])
        )
        assert not report.has_errors()

    def test_fails_orphan_historical(self):
        con = duckdb.connect()
        # Historical row for NK1 but no current row for NK1
        _scd2_view(con, [
            {"nk": "NK1", "name": "Old", "is_current": False, "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
            {"nk": "NK2", "name": "Bob", "is_current": True, "valid_from": "2024-01-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[NoOrphanVersions("nk")])
        )
        assert report.has_errors()

    def test_default_severity_error(self):
        assert NoOrphanVersions("nk").severity == Severity.ERROR


class TestMonotonicValidFrom:
    def test_passes_strictly_increasing(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "v1", "is_current": False, "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
            {"nk": "NK1", "name": "v2", "is_current": True, "valid_from": "2024-06-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[MonotonicValidFrom("nk")])
        )
        assert not report.has_errors()

    def test_fails_non_monotonic(self):
        con = duckdb.connect()
        # v2 has earlier valid_from than v1 — wrong order
        _scd2_view(con, [
            {"nk": "NK1", "name": "v1", "is_current": False, "valid_from": "2024-06-01", "valid_to": "2024-12-01"},
            {"nk": "NK1", "name": "v2", "is_current": True, "valid_from": "2024-01-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[MonotonicValidFrom("nk")])
        )
        assert report.has_errors()

    def test_default_severity_error(self):
        assert MonotonicValidFrom("nk").severity == Severity.ERROR


class TestNoGapPeriods:
    def test_passes_contiguous(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "v1", "is_current": False, "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
            {"nk": "NK1", "name": "v2", "is_current": True, "valid_from": "2024-06-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[NoGapPeriods("nk")])
        )
        assert not report.has_warnings()

    def test_fails_gap(self):
        con = duckdb.connect()
        _scd2_view(con, [
            {"nk": "NK1", "name": "v1", "is_current": False, "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
            # Gap: valid_from is 2024-07-01, but valid_to of prev was 2024-06-01
            {"nk": "NK1", "name": "v2", "is_current": True, "valid_from": "2024-07-01", "valid_to": "NULL"},
        ])
        report = ContractEngine().validate(
            con, "dim", StateContract(rules=[NoGapPeriods("nk")])
        )
        assert report.has_warnings() or report.has_errors()

    def test_default_severity_warning(self):
        assert NoGapPeriods("nk").severity == Severity.WARNING


# ---------------------------------------------------------------------------
# Freshness rule
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_passes_recent_timestamp(self):
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t AS SELECT TIMESTAMP '{now}' AS loaded_at")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[Freshness("loaded_at", max_age_hours=1)])
        )
        assert not report.has_errors()

    def test_fails_stale_timestamp(self):
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE t AS SELECT TIMESTAMP '2000-01-01 00:00:00' AS loaded_at"
        )
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, SourceContract(rules=[Freshness("loaded_at", max_age_hours=1)])
        )
        assert report.has_warnings() or report.has_errors()

    def test_default_severity_warning(self):
        assert Freshness("ts", max_age_hours=24).severity == Severity.WARNING


class TestRowCountDelta:
    def test_passes_within_tolerance(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE t AS SELECT unnest([1,2,3,4,5]) AS x")
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, StateContract(rules=[RowCountDelta(baseline=5, max_pct=0.2)])
        )
        assert not report.has_warnings() and not report.has_errors()

    def test_fails_large_delta(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE t AS SELECT unnest([1,2,3,4,5,6,7,8,9,10]) AS x")
        v = _view(con, "SELECT * FROM t")
        # baseline=5, actual=10 → 100 % change > 20 %
        report = ContractEngine().validate(
            con, v, StateContract(rules=[RowCountDelta(baseline=5, max_pct=0.2)])
        )
        assert report.has_warnings() or report.has_errors()

    def test_default_severity_warning(self):
        assert RowCountDelta(baseline=100, max_pct=0.1).severity == Severity.WARNING

    def test_name(self):
        assert RowCountDelta(baseline=1, max_pct=0.05).name == "ROW_COUNT_DELTA"


class TestHashConsistency:
    def test_passes_correct_hashes(self):
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE t AS SELECT 'A' AS name, md5('A' || '|' || '1') AS row_hash, 1 AS score"
        )
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, StateContract(rules=[HashConsistency("row_hash", ["name", "score"])])
        )
        assert not report.has_errors() and not report.has_warnings()

    def test_fails_corrupted_hash(self):
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE t AS SELECT 'A' AS name, 'bad_hash' AS row_hash, 1 AS score"
        )
        v = _view(con, "SELECT * FROM t")
        report = ContractEngine().validate(
            con, v, StateContract(rules=[HashConsistency("row_hash", ["name", "score"])])
        )
        assert report.has_warnings() or report.has_errors()

    def test_default_severity_warning(self):
        assert HashConsistency("h", ["a"]).severity == Severity.WARNING

    def test_name(self):
        assert HashConsistency("h", ["a", "b"]).name == "HASH_CONSISTENCY"
