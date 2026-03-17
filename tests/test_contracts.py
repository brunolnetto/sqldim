"""Tests for sqldim.contracts — DataContract, versions, schema, SLA, registry."""
import pytest
from sqldim.medallion import Layer
from sqldim.contracts import (
    ColumnSpec,
    SLASpec,
    ContractVersion,
    ChangeKind,
    DataContract,
    ContractRegistry,
)


# ── ContractVersion ───────────────────────────────────────────────────────────

class TestContractVersion:
    def test_parse_three_parts(self):
        v = ContractVersion.parse("2.1.0")
        assert v.major == 2
        assert v.minor == 1
        assert v.patch == 0

    def test_str_roundtrip(self):
        assert str(ContractVersion.parse("1.3.7")) == "1.3.7"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            ContractVersion.parse("1.2")
        with pytest.raises(ValueError):
            ContractVersion.parse("1.2.3.4")
        with pytest.raises(ValueError):
            ContractVersion.parse("a.b.c")

    def test_ordering(self):
        v100 = ContractVersion.parse("1.0.0")
        v110 = ContractVersion.parse("1.1.0")
        v200 = ContractVersion.parse("2.0.0")
        assert v100 < v110 < v200

    def test_equality(self):
        assert ContractVersion.parse("1.2.3") == ContractVersion.parse("1.2.3")
        assert ContractVersion.parse("1.0.0") != ContractVersion.parse("2.0.0")

    def test_major_change(self):
        old = ContractVersion.parse("1.2.3")
        new = ContractVersion.parse("2.0.0")
        assert old.classify_change(new) is ChangeKind.MAJOR

    def test_minor_change(self):
        old = ContractVersion.parse("1.2.3")
        new = ContractVersion.parse("1.3.0")
        assert old.classify_change(new) is ChangeKind.MINOR

    def test_patch_change(self):
        old = ContractVersion.parse("1.2.3")
        new = ContractVersion.parse("1.2.4")
        assert old.classify_change(new) is ChangeKind.PATCH

    def test_compatible_with_same_major(self):
        v = ContractVersion.parse("1.5.0")
        assert v.is_compatible_with(ContractVersion.parse("1.0.0")) is True

    def test_incompatible_different_major(self):
        v = ContractVersion.parse("2.0.0")
        assert v.is_compatible_with(ContractVersion.parse("1.0.0")) is False


# ── ColumnSpec ────────────────────────────────────────────────────────────────

class TestColumnSpec:
    def test_required_fields(self):
        c = ColumnSpec(name="order_id", dtype="uuid")
        assert c.name == "order_id"
        assert c.dtype == "uuid"

    def test_defaults(self):
        c = ColumnSpec(name="x", dtype="int")
        assert c.nullable is True
        assert c.primary_key is False
        assert c.foreign_key is None

    def test_pk_not_nullable_by_default(self):
        c = ColumnSpec(name="id", dtype="uuid", primary_key=True)
        assert c.primary_key is True

    def test_foreign_key(self):
        c = ColumnSpec(name="cust_id", dtype="uuid", foreign_key="customers.id")
        assert c.foreign_key == "customers.id"

    def test_is_breaking_change_type_change(self):
        old = ColumnSpec(name="amount", dtype="int")
        new = ColumnSpec(name="amount", dtype="decimal")
        assert old.is_breaking_change(new) is True

    def test_not_breaking_same_type_add_nullable(self):
        old = ColumnSpec(name="amount", dtype="decimal", nullable=False)
        new = ColumnSpec(name="amount", dtype="decimal", nullable=True)
        assert old.is_breaking_change(new) is False

    def test_is_breaking_nullable_to_required(self):
        old = ColumnSpec(name="x", dtype="int", nullable=True)
        new = ColumnSpec(name="x", dtype="int", nullable=False)
        assert old.is_breaking_change(new) is True


# ── SLASpec ───────────────────────────────────────────────────────────────────

class TestSLASpec:
    def test_all_none_by_default(self):
        sla = SLASpec()
        assert sla.freshness_minutes  is None
        assert sla.completeness_pct   is None
        assert sla.latency_p99_minutes is None

    def test_set_values(self):
        sla = SLASpec(freshness_minutes=30, completeness_pct=99.5, latency_p99_minutes=5)
        assert sla.freshness_minutes  == 30
        assert sla.completeness_pct   == 99.5
        assert sla.latency_p99_minutes == 5

    def test_is_met_freshness_ok(self):
        sla = SLASpec(freshness_minutes=30)
        assert sla.is_freshness_met(actual_minutes=10) is True

    def test_is_met_freshness_fail(self):
        sla = SLASpec(freshness_minutes=30)
        assert sla.is_freshness_met(actual_minutes=31) is False

    def test_is_met_completeness_ok(self):
        sla = SLASpec(completeness_pct=99.5)
        assert sla.is_completeness_met(actual_pct=99.9) is True

    def test_is_met_completeness_fail(self):
        sla = SLASpec(completeness_pct=99.5)
        assert sla.is_completeness_met(actual_pct=99.0) is False

    def test_no_sla_always_met(self):
        sla = SLASpec()
        assert sla.is_freshness_met(999)    is True
        assert sla.is_completeness_met(0.0) is True


# ── DataContract ──────────────────────────────────────────────────────────────

class TestDataContract:
    def _make(self, name="orders_v2", version="2.1.0", layer=Layer.SILVER):
        return DataContract(
            name=name,
            version=ContractVersion.parse(version),
            owner="data-platform",
            layer=layer,
            columns=[
                ColumnSpec("order_id",    "uuid",       nullable=False, primary_key=True),
                ColumnSpec("customer_id", "uuid",       nullable=False,
                           foreign_key="customers.id"),
                ColumnSpec("amount",      "decimal"),
                ColumnSpec("created_at",  "timestamp"),
            ],
            sla=SLASpec(freshness_minutes=30, completeness_pct=99.5),
        )

    def test_basic_attributes(self):
        c = self._make()
        assert c.name    == "orders_v2"
        assert c.owner   == "data-platform"
        assert c.layer   is Layer.SILVER

    def test_column_lookup_found(self):
        c = self._make()
        col = c.column("order_id")
        assert col is not None
        assert col.dtype == "uuid"

    def test_column_lookup_missing_returns_none(self):
        c = self._make()
        assert c.column("nonexistent") is None

    def test_no_breaking_change_same_contract(self):
        c = self._make()
        assert c.is_breaking_change_from(c) is False

    def test_breaking_change_column_removed(self):
        old = self._make()
        new = DataContract(
            name="orders_v2", version=ContractVersion.parse("2.1.0"),
            owner="data-platform", layer=Layer.SILVER,
            columns=[ColumnSpec("order_id", "uuid")],  # customer_id removed
        )
        assert new.is_breaking_change_from(old) is True

    def test_breaking_change_type_changed(self):
        old = self._make()
        new = DataContract(
            name="orders_v2", version=ContractVersion.parse("2.1.0"),
            owner="data-platform", layer=Layer.SILVER,
            columns=[
                ColumnSpec("order_id",    "uuid",    nullable=False, primary_key=True),
                ColumnSpec("customer_id", "uuid",    nullable=False),
                ColumnSpec("amount",      "bigint"),  # was decimal → breaking
                ColumnSpec("created_at",  "timestamp"),
            ],
        )
        assert new.is_breaking_change_from(old) is True

    def test_non_breaking_new_nullable_column(self):
        old = self._make()
        new = DataContract(
            name="orders_v2", version=ContractVersion.parse("2.1.0"),
            owner="data-platform", layer=Layer.SILVER,
            columns=[
                ColumnSpec("order_id",    "uuid",      nullable=False, primary_key=True),
                ColumnSpec("customer_id", "uuid",      nullable=False),
                ColumnSpec("amount",      "decimal"),
                ColumnSpec("created_at",  "timestamp"),
                ColumnSpec("notes",       "text"),     # new nullable column
            ],
        )
        assert new.is_breaking_change_from(old) is False


# ── ContractRegistry ──────────────────────────────────────────────────────────

class TestContractRegistry:
    def _contract(self, version="1.0.0"):
        return DataContract(
            name="orders",
            version=ContractVersion.parse(version),
            owner="team",
            layer=Layer.SILVER,
            columns=[ColumnSpec("id", "uuid")],
        )

    def setup_method(self):
        self.reg = ContractRegistry()

    def test_empty_registry_raises_on_get(self):
        with pytest.raises(KeyError):
            self.reg.get("orders")

    def test_register_and_get_latest(self):
        self.reg.register(self._contract("1.0.0"))
        self.reg.register(self._contract("1.1.0"))
        c = self.reg.get("orders")
        assert str(c.version) == "1.1.0"

    def test_get_specific_version(self):
        self.reg.register(self._contract("1.0.0"))
        self.reg.register(self._contract("1.1.0"))
        c = self.reg.get_version("orders", "1.0.0")
        assert str(c.version) == "1.0.0"

    def test_get_version_not_found_raises(self):
        self.reg.register(self._contract("1.0.0"))
        with pytest.raises(KeyError):
            self.reg.get_version("orders", "9.9.9")

    def test_history_sorted_ascending(self):
        self.reg.register(self._contract("1.1.0"))
        self.reg.register(self._contract("1.0.0"))
        self.reg.register(self._contract("2.0.0"))
        versions = [str(c.version) for c in self.reg.history("orders")]
        assert versions == ["1.0.0", "1.1.0", "2.0.0"]

    def test_is_registered_true(self):
        self.reg.register(self._contract())
        assert self.reg.is_registered("orders") is True

    def test_is_registered_false(self):
        assert self.reg.is_registered("orders") is False

    def test_all_names(self):
        self.reg.register(self._contract())
        c2 = DataContract(
            name="customers", version=ContractVersion.parse("1.0.0"),
            owner="t", layer=Layer.GOLD, columns=[],
        )
        self.reg.register(c2)
        assert sorted(self.reg.all_names()) == ["customers", "orders"]
