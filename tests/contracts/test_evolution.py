"""Tests for ContractEngine.check_evolution_safety and related helpers."""
from __future__ import annotations


from sqldim.contracts.engine import (
    EvolutionChange,
    EvolutionReport,
    ContractEngine,
    _is_widening,
    _classify_type_change,
)
from sqldim.contracts.validation.schema import ColumnSpec


# ---------------------------------------------------------------------------
# EvolutionChange
# ---------------------------------------------------------------------------

class TestEvolutionChange:
    def test_fields(self):
        ec = EvolutionChange("added", "my_col", "detail text")
        assert ec.change_type == "added"
        assert ec.column == "my_col"
        assert ec.detail == "detail text"

    def test_default_detail(self):
        ec = EvolutionChange("removed", "col_x")
        assert ec.detail == ""


# ---------------------------------------------------------------------------
# EvolutionReport
# ---------------------------------------------------------------------------

class TestEvolutionReport:
    def test_is_safe_empty(self):
        r = EvolutionReport()
        assert r.is_safe is True

    def test_is_safe_only_safe_changes(self):
        r = EvolutionReport(safe_changes=[EvolutionChange("added", "x")])
        assert r.is_safe is True

    def test_not_safe_with_additive(self):
        r = EvolutionReport(additive_changes=[EvolutionChange("added", "x")])
        assert r.is_safe is False

    def test_not_safe_with_breaking(self):
        r = EvolutionReport(breaking_changes=[EvolutionChange("removed", "x")])
        assert r.is_safe is False

    def test_summary_no_changes(self):
        assert EvolutionReport().summary() == "No schema changes detected."

    def test_summary_with_safe(self):
        r = EvolutionReport(safe_changes=[EvolutionChange("added", "col_a")])
        assert "Safe" in r.summary()
        assert "col_a" in r.summary()

    def test_summary_with_additive(self):
        r = EvolutionReport(additive_changes=[EvolutionChange("widened", "price")])
        assert "Additive" in r.summary()
        assert "price" in r.summary()

    def test_summary_with_breaking(self):
        r = EvolutionReport(breaking_changes=[EvolutionChange("removed", "old_col")])
        assert "Breaking" in r.summary()
        assert "old_col" in r.summary()

    def test_summary_all_tiers(self):
        r = EvolutionReport(
            safe_changes=[EvolutionChange("added", "a")],
            additive_changes=[EvolutionChange("widened", "b")],
            breaking_changes=[EvolutionChange("removed", "c")],
        )
        summary = r.summary()
        assert "Safe" in summary
        assert "Additive" in summary
        assert "Breaking" in summary


# ---------------------------------------------------------------------------
# _is_widening
# ---------------------------------------------------------------------------

class TestIsWidening:
    def test_int_widening(self):
        assert _is_widening("int", "bigint") is True

    def test_int_narrowing(self):
        assert _is_widening("bigint", "int") is False

    def test_same_type_not_widening(self):
        assert _is_widening("int", "int") is False

    def test_tinyint_to_bigint(self):
        assert _is_widening("tinyint", "bigint") is True

    def test_smallint_to_integer(self):
        assert _is_widening("smallint", "integer") is True

    def test_float_widening(self):
        assert _is_widening("float", "double") is True

    def test_float_narrowing(self):
        assert _is_widening("double", "float") is False

    def test_real_to_decimal(self):
        assert _is_widening("real", "decimal") is True

    def test_cross_hierarchy_int_float(self):
        # int and float are in different hierarchies — not a widening
        assert _is_widening("int", "float") is False

    def test_unknown_types(self):
        assert _is_widening("varchar", "text") is False


# ---------------------------------------------------------------------------
# _classify_type_change
# ---------------------------------------------------------------------------

class TestClassifyTypeChange:
    def test_widening_classified_as_widened(self):
        old = ColumnSpec(name="qty", dtype="int", nullable=True)
        new = ColumnSpec(name="qty", dtype="bigint", nullable=True)
        result = _classify_type_change(old, new)
        assert result.change_type == "widened"
        assert "int" in result.detail
        assert "bigint" in result.detail

    def test_narrowing_classified_as_type_changed(self):
        old = ColumnSpec(name="qty", dtype="bigint", nullable=True)
        new = ColumnSpec(name="qty", dtype="int", nullable=True)
        result = _classify_type_change(old, new)
        assert result.change_type == "type_changed"

    def test_incompatible_types_classified_as_type_changed(self):
        old = ColumnSpec(name="col", dtype="varchar", nullable=True)
        new = ColumnSpec(name="col", dtype="integer", nullable=True)
        result = _classify_type_change(old, new)
        assert result.change_type == "type_changed"


# ---------------------------------------------------------------------------
# ContractEngine.check_evolution_safety
# ---------------------------------------------------------------------------

class TestCheckEvolutionSafety:

    # -- Nullable column added ----------------------------------------

    def test_nullable_add_on_parquet_is_safe(self):
        old = [ColumnSpec(name="id", dtype="int", nullable=False)]
        new = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="extra", dtype="varchar", nullable=True),
        ]
        report = ContractEngine.check_evolution_safety(old, new, backend="parquet")
        assert report.is_safe
        assert len(report.safe_changes) == 1
        assert report.safe_changes[0].column == "extra"
        assert not report.additive_changes
        assert not report.required_migrations

    def test_nullable_add_on_postgres_is_additive(self):
        old = [ColumnSpec(name="id", dtype="int", nullable=False)]
        new = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="extra", dtype="varchar", nullable=True),
        ]
        report = ContractEngine.check_evolution_safety(old, new, backend="postgres")
        assert not report.is_safe
        assert len(report.additive_changes) == 1
        assert report.additive_changes[0].column == "extra"
        assert len(report.required_migrations) == 1
        assert "ALTER TABLE" in report.required_migrations[0]

    def test_nullable_add_on_delta_is_additive(self):
        old = [ColumnSpec(name="id", dtype="int", nullable=False)]
        new = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="extra", dtype="varchar", nullable=True),
        ]
        report = ContractEngine.check_evolution_safety(old, new, backend="delta")
        assert not report.is_safe
        assert len(report.additive_changes) == 1

    # -- NOT NULL column added -----------------------------------------

    def test_not_null_add_is_always_additive(self):
        old = [ColumnSpec(name="id", dtype="int", nullable=False)]
        new = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="required_col", dtype="varchar", nullable=False),
        ]
        for backend in ("parquet", "postgres", "delta"):
            report = ContractEngine.check_evolution_safety(old, new, backend=backend)
            assert not report.is_safe, f"Expected additive for backend={backend}"
            assert len(report.additive_changes) == 1
            assert "backfill" in report.additive_changes[0].detail
            assert len(report.required_migrations) == 1
            assert "DEFAULT" in report.required_migrations[0]

    # -- Column removed -----------------------------------------------

    def test_column_removed_is_breaking(self):
        old = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="old_col", dtype="varchar", nullable=True),
        ]
        new = [ColumnSpec(name="id", dtype="int", nullable=False)]
        report = ContractEngine.check_evolution_safety(old, new)
        assert not report.is_safe
        assert len(report.breaking_changes) == 1
        assert report.breaking_changes[0].change_type == "removed"
        assert report.breaking_changes[0].column == "old_col"

    # -- Type widening ------------------------------------------------

    def test_type_widening_is_additive(self):
        old = [ColumnSpec(name="amount", dtype="int", nullable=True)]
        new = [ColumnSpec(name="amount", dtype="bigint", nullable=True)]
        report = ContractEngine.check_evolution_safety(old, new)
        assert not report.is_safe
        assert len(report.additive_changes) == 1
        assert report.additive_changes[0].change_type == "widened"
        assert len(report.required_migrations) == 1
        assert "ALTER COLUMN" in report.required_migrations[0]

    # -- Type narrowing -----------------------------------------------

    def test_type_narrowing_is_breaking(self):
        old = [ColumnSpec(name="amount", dtype="bigint", nullable=True)]
        new = [ColumnSpec(name="amount", dtype="int", nullable=True)]
        report = ContractEngine.check_evolution_safety(old, new)
        assert not report.is_safe
        assert len(report.breaking_changes) == 1
        assert report.breaking_changes[0].change_type == "type_changed"

    # -- Nullable → NOT NULL ------------------------------------------

    def test_nullable_to_not_null_is_breaking(self):
        old = [ColumnSpec(name="email", dtype="varchar", nullable=True)]
        new = [ColumnSpec(name="email", dtype="varchar", nullable=False)]
        report = ContractEngine.check_evolution_safety(old, new)
        assert not report.is_safe
        assert len(report.breaking_changes) == 1
        assert report.breaking_changes[0].change_type == "narrowed"
        assert "NOT NULL" in report.breaking_changes[0].detail

    # -- NOT NULL → nullable ------------------------------------------

    def test_not_null_to_nullable_no_change(self):
        # Relaxing a NOT NULL constraint: dtype unchanged, nullability relaxed
        # This is neither a type change nor a nullability narrowing → no change entry
        old = [ColumnSpec(name="email", dtype="varchar", nullable=False)]
        new = [ColumnSpec(name="email", dtype="varchar", nullable=True)]
        report = ContractEngine.check_evolution_safety(old, new)
        assert report.is_safe
        assert not report.breaking_changes
        assert not report.additive_changes

    # -- No changes ---------------------------------------------------

    def test_identical_schemas_no_changes(self):
        schema = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="name", dtype="varchar", nullable=True),
        ]
        report = ContractEngine.check_evolution_safety(schema, schema[:])
        assert report.is_safe
        assert report.summary() == "No schema changes detected."

    # -- Mixed changes ------------------------------------------------

    def test_mixed_changes(self):
        old = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="legacy_col", dtype="varchar", nullable=True),
            ColumnSpec(name="amount", dtype="int", nullable=True),
        ]
        new = [
            ColumnSpec(name="id", dtype="int", nullable=False),
            ColumnSpec(name="amount", dtype="bigint", nullable=True),  # widened
            ColumnSpec(name="new_nullable", dtype="varchar", nullable=True),  # added safe
        ]
        report = ContractEngine.check_evolution_safety(old, new, backend="parquet")
        # legacy_col removed → breaking
        assert any(c.column == "legacy_col" for c in report.breaking_changes)
        # amount widened → additive
        assert any(c.column == "amount" for c in report.additive_changes)
        # new_nullable → safe on parquet
        assert any(c.column == "new_nullable" for c in report.safe_changes)
