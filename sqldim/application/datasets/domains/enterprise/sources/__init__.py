"""sources sub-package for the enterprise domain — exposes all source classes."""

from sqldim.application.datasets.domains.enterprise.sources.employees import (
    EmployeesSource,
    _EMPLOYEES_SPEC,
    get_dept_choices,
    get_title_ladder,
)
from sqldim.application.datasets.domains.enterprise.sources.accounts import (
    AccountsSource,
    _ACCOUNTS_SPEC,
)

__all__ = [
    "EmployeesSource",
    "_EMPLOYEES_SPEC",
    "AccountsSource",
    "_ACCOUNTS_SPEC",
    "get_dept_choices",
    "get_title_ladder",
]
