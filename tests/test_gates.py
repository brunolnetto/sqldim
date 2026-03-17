"""Tests for Quality Gates — checked in RED, implemented to GREEN."""
import pytest

from sqldim.medallion import Layer
from sqldim.contracts.gates import (
    CheckResult,
    GateResult,
    QualityGate,
)


# ---------------------------------------------------------------------------
# CheckResult
# ---------------------------------------------------------------------------

class TestCheckResult:
    def test_passed_check(self):
        r = CheckResult(name="null_check", passed=True)
        assert r.name == "null_check"
        assert r.passed is True
        assert r.detail == ""

    def test_failed_check_with_detail(self):
        r = CheckResult(name="freshness", passed=False, detail="stale by 45m")
        assert r.passed is False
        assert r.detail == "stale by 45m"

    def test_check_result_is_immutable(self):
        r = CheckResult(name="x", passed=True)
        with pytest.raises((AttributeError, TypeError)):
            r.passed = False  # type: ignore


# ---------------------------------------------------------------------------
# GateResult
# ---------------------------------------------------------------------------

class TestGateResult:
    def _make(self, checks):
        return GateResult(results=checks)

    def test_all_pass(self):
        results = [CheckResult("a", True), CheckResult("b", True)]
        g = self._make(results)
        assert g.ok is True

    def test_any_fail(self):
        results = [CheckResult("a", True), CheckResult("b", False, "oops")]
        g = self._make(results)
        assert g.ok is False

    def test_failing_property(self):
        results = [
            CheckResult("a", True),
            CheckResult("b", False, "err1"),
            CheckResult("c", False, "err2"),
        ]
        g = self._make(results)
        failing = g.failing
        assert len(failing) == 2
        assert all(not r.passed for r in failing)

    def test_failing_empty_when_all_pass(self):
        results = [CheckResult("a", True), CheckResult("b", True)]
        g = self._make(results)
        assert g.failing == []

    def test_empty_results_is_ok(self):
        g = self._make([])
        assert g.ok is True
        assert g.failing == []


# ---------------------------------------------------------------------------
# QualityGate
# ---------------------------------------------------------------------------

class TestQualityGate:
    def test_basic_attributes(self):
        gate = QualityGate("bronze_to_silver", Layer.BRONZE, Layer.SILVER)
        assert gate.name == "bronze_to_silver"
        assert gate.layer_from is Layer.BRONZE
        assert gate.layer_to is Layer.SILVER

    def test_add_check_returns_self_for_chaining(self):
        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        returned = gate.add_check(lambda: CheckResult("dummy", True))
        assert returned is gate

    def test_run_no_checks_returns_ok(self):
        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        result = gate.run()
        assert isinstance(result, GateResult)
        assert result.ok is True

    def test_run_single_passing_check(self):
        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        gate.add_check(lambda: CheckResult("null_check", True, "all good"))
        result = gate.run()
        assert result.ok is True
        assert len(result.results) == 1
        assert result.results[0].name == "null_check"

    def test_run_single_failing_check(self):
        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        gate.add_check(lambda: CheckResult("completeness", False, "missing 5%"))
        result = gate.run()
        assert result.ok is False
        assert len(result.failing) == 1

    def test_run_multiple_checks_all_pass(self):
        gate = QualityGate("g", Layer.SILVER, Layer.GOLD)
        gate.add_check(lambda: CheckResult("schema", True))
        gate.add_check(lambda: CheckResult("freshness", True))
        gate.add_check(lambda: CheckResult("completeness", True))
        result = gate.run()
        assert result.ok is True
        assert len(result.results) == 3

    def test_run_multiple_checks_one_fails(self):
        gate = QualityGate("g", Layer.SILVER, Layer.GOLD)
        gate.add_check(lambda: CheckResult("schema", True))
        gate.add_check(lambda: CheckResult("freshness", False, "stale"))
        result = gate.run()
        assert result.ok is False
        assert len(result.failing) == 1
        assert result.failing[0].name == "freshness"

    def test_check_receives_kwargs(self):
        received = {}

        def my_check(threshold=0.95):
            received["threshold"] = threshold
            return CheckResult("completeness", True)

        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        gate.add_check(my_check)
        gate.run(threshold=0.99)
        assert received["threshold"] == 0.99

    def test_check_raises_counts_as_failure(self):
        def bad_check():
            raise RuntimeError("database down")

        gate = QualityGate("g", Layer.BRONZE, Layer.SILVER)
        gate.add_check(bad_check)
        result = gate.run()
        assert result.ok is False
        assert "database down" in result.failing[0].detail

    def test_invalid_promotion_raises(self):
        """A gate that crosses non-adjacent layers should raise ValueError."""
        with pytest.raises(ValueError):
            QualityGate("skip", Layer.BRONZE, Layer.GOLD)
