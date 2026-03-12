import pytest
from sqldim.scd.detection import HashDetection, ColumnarDetection, ChangeRecord
from sqldim.scd.audit import AuditLog, AuditEntry

# ── HashDetection ─────────────────────────────────────────────────────────────

def test_hash_compute_deterministic():
    hd = HashDetection(["name", "city"])
    r = {"name": "Alice", "city": "NYC"}
    assert hd.compute(r) == hd.compute(r)

def test_hash_changed():
    hd = HashDetection(["name", "city"])
    r = {"name": "Alice", "city": "NYC"}
    stored = hd.compute(r)
    changed = {"name": "Alice", "city": "LA"}
    assert hd.has_changed(changed, stored) is True

def test_hash_unchanged():
    hd = HashDetection(["name", "city"])
    r = {"name": "Alice", "city": "NYC"}
    stored = hd.compute(r)
    assert hd.has_changed(r, stored) is False

def test_hash_missing_column_defaults_to_empty():
    hd = HashDetection(["name", "city"])
    r = {"name": "Alice"}  # city missing
    checksum = hd.compute(r)
    assert isinstance(checksum, str)

def test_hash_sha256():
    hd = HashDetection(["email"], algorithm="sha256")
    r = {"email": "a@b.com"}
    h = hd.compute(r)
    assert len(h) == 64  # SHA256 hex digest

# ── ColumnarDetection ────────────────────────────────────────────────────────

class FakeRow:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def test_columnar_diff_detects_change():
    cd = ColumnarDetection(["name", "city"])
    incoming = {"name": "Alice", "city": "LA"}
    existing = FakeRow(name="Alice", city="NYC")
    record = cd.diff(incoming, existing, natural_key="U1")
    assert bool(record) is True
    assert "city" in record.changed_columns
    assert record.changed_columns["city"]["old"] == "NYC"
    assert record.changed_columns["city"]["new"] == "LA"

def test_columnar_diff_no_change():
    cd = ColumnarDetection(["name", "city"])
    incoming = {"name": "Alice", "city": "NYC"}
    existing = FakeRow(name="Alice", city="NYC")
    record = cd.diff(incoming, existing, natural_key="U1")
    assert bool(record) is False
    assert record.changed_columns == {}

def test_change_record_bool():
    cr = ChangeRecord(natural_key="X", changed_columns={})
    assert not cr
    cr2 = ChangeRecord(natural_key="X", changed_columns={"col": {"old": 1, "new": 2}})
    assert cr2

# ── AuditLog ─────────────────────────────────────────────────────────────────

def test_audit_log_record():
    log = AuditLog()
    cr = ChangeRecord(natural_key="U1", changed_columns={"city": {"old": "NYC", "new": "LA"}})
    entry = log.record("userdim", cr)
    assert isinstance(entry, AuditEntry)
    assert len(log) == 1

def test_audit_log_filter_by_table():
    log = AuditLog()
    cr = ChangeRecord(natural_key="X", changed_columns={"col": {"old": 1, "new": 2}})
    log.record("table_a", cr)
    log.record("table_b", cr)
    assert len(log.entries("table_a")) == 1
    assert len(log.entries()) == 2

def test_audit_log_clear():
    log = AuditLog()
    cr = ChangeRecord(natural_key="X", changed_columns={"col": {"old": 1, "new": 2}})
    log.record("t", cr)
    log.clear()
    assert len(log) == 0

def test_audit_entry_summary():
    entry = AuditEntry(
        table="customerdim",
        natural_key="C1",
        changed_columns={"city": {"old": "A", "new": "B"}},
        operation="VERSION",
    )
    s = entry.summary()
    assert "VERSION" in s
    assert "customerdim" in s
    assert "city" in s
