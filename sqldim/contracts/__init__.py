"""sqldim.contracts — Data contract model and registry."""
from sqldim.contracts.version  import ContractVersion, ChangeKind
from sqldim.contracts.schema   import ColumnSpec
from sqldim.contracts.sla      import SLASpec
from sqldim.contracts.contract import DataContract
from sqldim.contracts.registry import ContractRegistry

__all__ = [
    "ContractVersion",
    "ChangeKind",
    "ColumnSpec",
    "SLASpec",
    "DataContract",
    "ContractRegistry",
]
