"""sqldim.contracts — schema contracts, data quality rules, and validation engine.

Two complementary sub-systems live here:

Schema contracts (metadata layer)
    ContractVersion, ColumnSpec, SLASpec, DataContract, ContractRegistry

SQL validation engine (runtime layer)
    Rule, built-in column/table rules, SCD2 rules, Freshness,
    ContractEngine, ContractReport, ContractViolationError,
    SourceContract, StateContract, OutputContract
"""
# --- schema / metadata contracts ---
from sqldim.contracts.version   import ContractVersion, ChangeKind
from sqldim.contracts.schema    import ColumnSpec
from sqldim.contracts.sla       import SLASpec
from sqldim.contracts.contract  import DataContract
from sqldim.contracts.registry  import ContractRegistry

# --- SQL validation engine ---
from sqldim.contracts.report     import Severity, ContractViolation, ContractReport
from sqldim.contracts.exceptions import ContractViolationError
from sqldim.contracts.rules      import (
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
from sqldim.contracts.scd_rules  import (
    SCD2Invariants,
    NoOrphanVersions,
    MonotonicValidFrom,
    NoGapPeriods,
    HashConsistency,
)
from sqldim.contracts.freshness  import Freshness, RowCountDelta
from sqldim.contracts.engine     import ContractEngine
from sqldim.contracts.composite  import SourceContract, StateContract, OutputContract
from sqldim.contracts.gates      import QualityGate, CheckResult, GateResult

__all__ = [
    # schema / metadata
    "ContractVersion", "ChangeKind",
    "ColumnSpec", "SLASpec",
    "DataContract", "ContractRegistry",
    # validation engine — core
    "Severity", "ContractViolation", "ContractReport",
    "ContractViolationError",
    "ContractEngine",
    # composite contracts
    "SourceContract", "StateContract", "OutputContract",
    # column / table rules
    "Rule",
    "NotNull", "NoDuplicates", "NullRate", "TypeMatch",
    "ColumnExists", "RowCount", "ValueRange", "RegexMatch",
    # SCD2 rules
    "SCD2Invariants", "NoOrphanVersions", "MonotonicValidFrom",
    "NoGapPeriods", "HashConsistency",
    # time-series rules
    "Freshness", "RowCountDelta",
    # quality gates (pipeline boundary guards)
    "QualityGate", "CheckResult", "GateResult",
]
