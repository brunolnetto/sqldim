import pytest
from sqldim import DimensionModel, FactModel, Field, SCD2Mixin
from sqldim.exceptions import DestructiveMigrationError
from sqldim.migrations.guards import (
    destructive_operation,
    drop_scd2_history,
    drop_dimension_table,
)
from sqldim.migrations.ops import add_backfill_hint, initialize_scd2_rows, BackfillHint
from sqldim.migrations.context import (
    DimensionalMigrationContext,
    SchemaChange,
    CHANGE_ADD_COLUMN,
    CHANGE_DROP_COLUMN,
    CHANGE_SCD_UPGRADE,
    CHANGE_SCD_DOWNGRADE,
    CHANGE_NK_CHANGE,
)
from sqldim.migrations.generator import generate_migration, MigrationScript

# ── Models ────────────────────────────────────────────────────────────────────


class CustomerDim(DimensionModel, SCD2Mixin, table=True):
    __natural_key__ = ["customer_code"]
    __scd_type__ = 2
    id: int = Field(primary_key=True)
    customer_code: str
    name: str
    city: str


class OrderFact(FactModel, table=True):
    __grain__ = "one row per order"
    id: int = Field(primary_key=True)
    customer_id: int = Field(foreign_key="customerdim.id")
    amount: float


# ── Guards ────────────────────────────────────────────────────────────────────


def test_destructive_guard_raises_without_env():
    with pytest.raises(DestructiveMigrationError):
        drop_scd2_history("customerdim")


def test_destructive_guard_allows_with_env(monkeypatch):
    monkeypatch.setenv("SQLDIM_ALLOW_DESTRUCTIVE", "true")
    sql = drop_scd2_history("customerdim")
    assert "customerdim" in sql


def test_drop_dimension_table_raises(monkeypatch):
    with pytest.raises(DestructiveMigrationError):
        drop_dimension_table("customerdim")


def test_drop_dimension_table_allowed(monkeypatch):
    monkeypatch.setenv("SQLDIM_ALLOW_DESTRUCTIVE", "true")
    sql = drop_dimension_table("customerdim")
    assert "customerdim" in sql


def test_custom_destructive_decorator():
    @destructive_operation
    def risky_op():
        return "done"

    with pytest.raises(DestructiveMigrationError):
        risky_op()


# ── Ops ───────────────────────────────────────────────────────────────────────


def test_backfill_hint():
    hint = add_backfill_hint("customerdim", "segment", "Backfill from CRM")
    assert isinstance(hint, BackfillHint)
    assert hint.table == "customerdim"
    assert hint.column == "segment"
    assert "CRM" in hint.note
    assert "customerdim" in repr(hint)


def test_initialize_scd2_rows_default():
    sql = initialize_scd2_rows("customerdim")
    assert "valid_from" in sql
    assert "1970-01-01" in sql
    assert "NULL" in sql


def test_initialize_scd2_rows_custom():
    sql = initialize_scd2_rows(
        "customerdim", valid_from_default="2020-01-01", valid_to_default="9999-12-31"
    )
    assert "2020-01-01" in sql
    assert "9999-12-31" in sql


# ── Context / Diff ────────────────────────────────────────────────────────────


def test_diff_add_column():
    ctx = DimensionalMigrationContext([CustomerDim])
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name"],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    changes = ctx.diff(current)
    types = [c.change_type for c in changes]
    assert CHANGE_ADD_COLUMN in types
    added = [c for c in changes if c.change_type == CHANGE_ADD_COLUMN]
    assert any(c.details["column"] == "city" for c in added)


def test_diff_drop_column():
    ctx = DimensionalMigrationContext([CustomerDim])
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city", "old_col"],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    changes = ctx.diff(current)
    dropped = [c for c in changes if c.change_type == CHANGE_DROP_COLUMN]
    assert any(c.details["column"] == "old_col" for c in dropped)


def test_diff_scd_upgrade():
    class SimpleDim(DimensionModel, table=True):
        __natural_key__ = ["code"]
        __scd_type__ = 2
        id: int = Field(primary_key=True)
        code: str

    ctx = DimensionalMigrationContext([SimpleDim])
    current = {
        "simpledim": {"columns": ["id", "code"], "scd_type": 1, "natural_key": ["code"]}
    }
    changes = ctx.diff(current)
    assert any(c.change_type == CHANGE_SCD_UPGRADE for c in changes)


def test_diff_scd_downgrade():
    class SimpleDim2(DimensionModel, table=True):
        __natural_key__ = ["code"]
        __scd_type__ = 1
        id: int = Field(primary_key=True)
        code: str

    ctx = DimensionalMigrationContext([SimpleDim2])
    current = {
        "simpledim2": {
            "columns": ["id", "code"],
            "scd_type": 2,
            "natural_key": ["code"],
        }
    }
    changes = ctx.diff(current)
    assert any(c.change_type == CHANGE_SCD_DOWNGRADE for c in changes)
    downgrade = [c for c in changes if c.change_type == CHANGE_SCD_DOWNGRADE][0]
    assert downgrade.is_destructive is True


def test_diff_nk_change():
    ctx = DimensionalMigrationContext([CustomerDim])
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city"],
            "scd_type": 2,
            "natural_key": ["old_key"],
        }
    }
    changes = ctx.diff(current)
    assert any(c.change_type == CHANGE_NK_CHANGE for c in changes)


def test_diff_new_table_skipped():
    ctx = DimensionalMigrationContext([CustomerDim])
    changes = ctx.diff(
        {}
    )  # empty current state → no diffs (new table = CREATE, handled by Alembic)
    assert changes == []


def test_schema_change_repr():
    c = SchemaChange(
        CHANGE_ADD_COLUMN, CustomerDim, {"column": "segment", "table": "customerdim"}
    )
    assert "add_column" in repr(c)
    assert "CustomerDim" in repr(c)


def test_destructive_changes_sorted_last():
    ctx = DimensionalMigrationContext([CustomerDim])
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city", "stale_col"],
            "scd_type": 1,
            "natural_key": ["customer_code"],
        }
    }
    changes = ctx.diff(current)
    destructive = [i for i, c in enumerate(changes) if c.is_destructive]
    non_destructive = [i for i, c in enumerate(changes) if not c.is_destructive]
    if destructive and non_destructive:
        assert max(non_destructive) < min(destructive)


# ── Generator ─────────────────────────────────────────────────────────────────


def test_generate_migration_add_column():
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name"],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    script = generate_migration([CustomerDim], current, message="add city")
    assert isinstance(script, MigrationScript)
    assert len(script.upgrade_ops) > 0
    rendered = script.render()
    assert "add city" in rendered
    assert "def upgrade():" in rendered
    assert "def downgrade():" in rendered


def test_generate_migration_scd_upgrade():
    class UpgradeDim(DimensionModel, table=True):
        __natural_key__ = ["code"]
        __scd_type__ = 2
        id: int = Field(primary_key=True)
        code: str

    current = {
        "upgradedim": {
            "columns": ["id", "code"],
            "scd_type": 1,
            "natural_key": ["code"],
        }
    }
    script = generate_migration([UpgradeDim], current, "scd1 to scd2")
    assert any("valid_from" in op for op in script.upgrade_ops)
    rendered = script.render()
    assert "scd1 to scd2" in rendered


def test_generate_migration_drop_column_warns():
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city", "ghost_col"],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    script = generate_migration([CustomerDim], current, "drop ghost")
    assert any("ghost_col" in w for w in script.warnings)


def test_generate_migration_nk_change_warns():
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city"],
            "scd_type": 2,
            "natural_key": ["old_nk"],
        }
    }
    script = generate_migration([CustomerDim], current, "change nk")
    assert any(
        "natural_key" in w.lower() or "Natural key" in w for w in script.warnings
    )


def test_render_includes_warnings():
    current = {
        "customerdim": {
            "columns": ["id", "customer_code", "name", "city", "ghost_col"],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    script = generate_migration([CustomerDim], current, "drop ghost")
    rendered = script.render()
    assert "# WARNINGS:" in rendered
    assert "ghost_col" in rendered


def test_generate_migration_no_changes():
    # Include all columns from CustomerDim + SCD2Mixin to produce zero diff
    current = {
        "customerdim": {
            "columns": [
                "id",
                "customer_code",
                "name",
                "city",
                "valid_from",
                "valid_to",
                "is_current",
                "checksum",
            ],
            "scd_type": 2,
            "natural_key": ["customer_code"],
        }
    }
    script = generate_migration([CustomerDim], current, "no change")
    assert script.upgrade_ops == []
    rendered = script.render()
    assert "pass" in rendered


def test_detect_scd_upgrade_no_change():
    # Line 55 in _detect_scd_upgrade: returns None when SCD type is unchanged
    ctx = DimensionalMigrationContext([CustomerDim, OrderFact])
    result = ctx._detect_scd_upgrade(1, 1, CustomerDim, "customerdim")
    assert result is None
    result2 = ctx._detect_scd_upgrade(2, 2, CustomerDim, "customerdim")
    assert result2 is None


def test_detect_scd_change_for_fact_model():
    # Line 55 in _detect_scd_change: returns None for FactModel (not DimensionModel)
    ctx = DimensionalMigrationContext([CustomerDim, OrderFact])
    result = ctx._detect_scd_change(OrderFact, "orderfact", {})
    assert result is None


def test_detect_nk_change_for_fact_model():
    # Line 67 in _detect_nk_change: returns None for FactModel (not DimensionModel)
    ctx = DimensionalMigrationContext([CustomerDim, OrderFact])
    result = ctx._detect_nk_change(OrderFact, "orderfact", {})
    assert result is None


# ── Introspect / diff_from_connection ────────────────────────────────────────


def test_introspect_returns_current_state():
    """context.py lines 194-241: introspect() reads column/scd/nk info from a live DB."""
    import duckdb

    ctx = DimensionalMigrationContext([CustomerDim])
    con = duckdb.connect()
    # Create the customerdim table that roughly matches CustomerDim + SCD2Mixin columns
    con.execute("""
        CREATE TABLE customerdim (
            id INTEGER,
            customer_code VARCHAR,
            name VARCHAR,
            city VARCHAR,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN,
            checksum VARCHAR
        )
    """)
    state = ctx.introspect(con)
    assert "customerdim" in state
    entry = state["customerdim"]
    assert "customer_code" in entry["columns"]
    assert "valid_from" in entry["columns"]
    # SCD2 indicator columns present → inferred as scd_type 2
    assert entry["scd_type"] == 2


def test_introspect_parses_nk_from_index():
    """context.py lines 229-231: NK is parsed from duckdb_indexes() when ix_<table>_nk exists."""
    import duckdb

    ctx = DimensionalMigrationContext([CustomerDim])
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE customerdim (
            id INTEGER,
            customer_code VARCHAR,
            name VARCHAR,
            city VARCHAR,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN,
            checksum VARCHAR
        )
    """)
    # Create the natural-key index that introspect() looks for
    con.execute("CREATE INDEX ix_customerdim_nk ON customerdim (customer_code)")
    state = ctx.introspect(con)
    assert "customerdim" in state
    # NK should be inferred from the index
    assert "customer_code" in state["customerdim"]["natural_key"]


def test_introspect_exception_on_columns_query_skips_table():
    """context.py lines 210-211: when columns query raises, the table is skipped."""

    ctx = DimensionalMigrationContext([CustomerDim])

    class MockCon:
        def execute(self, sql, params=None):
            raise Exception("query failed")

    state = ctx.introspect(MockCon())
    # All tables skipped due to the exception
    assert state == {}


def test_introspect_exception_on_indexes_query_uses_model_nk():
    """context.py lines 232-233: when duckdb_indexes() raises, falls back to model NK."""
    import duckdb

    ctx = DimensionalMigrationContext([CustomerDim])

    call_count = [0]
    real_con = duckdb.connect()
    real_con.execute("""
        CREATE TABLE customerdim (
            id INTEGER, customer_code VARCHAR, name VARCHAR, city VARCHAR,
            valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, checksum VARCHAR
        )
    """)

    class PartialFailCon:
        """Succeeds for information_schema but raises on duckdb_indexes()."""

        def execute(self, sql, params=None):
            call_count[0] += 1
            if "duckdb_indexes" in sql:
                raise Exception("not a duckdb backend")
            return real_con.execute(sql, params)

    state = ctx.introspect(PartialFailCon())
    assert "customerdim" in state
    # NK falls back to model's __natural_key__
    assert state["customerdim"]["natural_key"] == ["customer_code"]


def test_introspect_nonexistent_table_skipped():
    """context.py lines 194-241: tables not yet created are silently skipped."""
    import duckdb

    ctx = DimensionalMigrationContext([CustomerDim])
    con = duckdb.connect()
    # Don't create any tables — introspect on empty DB should return {}
    state = ctx.introspect(con)
    assert state == {}


def test_diff_from_connection():
    """context.py line 260: diff_from_connection() = introspect + diff."""
    import duckdb

    ctx = DimensionalMigrationContext([CustomerDim])
    con = duckdb.connect()
    # Create the table with missing 'city' column so diff detects ADD_COLUMN
    con.execute("""
        CREATE TABLE customerdim (
            id INTEGER,
            customer_code VARCHAR,
            name VARCHAR,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN,
            checksum VARCHAR
        )
    """)
    changes = ctx.diff_from_connection(con)
    added_cols = [c for c in changes if c.change_type == CHANGE_ADD_COLUMN]
    assert any(c.details["column"] == "city" for c in added_cols)


def test_parse_nk_no_parens_returns_empty():
    """context.py line 167: _parse_nk_from_index_sql when no parens → []."""
    result = DimensionalMigrationContext._parse_nk_from_index_sql("NO PARENS HERE")
    assert result == []
