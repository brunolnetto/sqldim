"""EmployeesSource — enterprise domain source."""

from __future__ import annotations


import random


from sqldim.application.datasets.base import (
    DatasetFactory,
    SchematicSource,
    SourceProvider,
)
from sqldim.application.datasets.schema import (
    ChangeRule,
    DatasetSpec,
    EntitySchema,
    EventSpec,
    FieldSpec,
)


# ── EmployeesSource ───────────────────────────────────────────────────────────


def get_dept_choices():
    return ["Engineering", "Finance", "Marketing", "Product", "HR", "Legal", "Sales"]


def get_title_ladder():
    return {
        "Engineer": "Senior Engineer",
        "Analyst": "Senior Analyst",
        "Manager": "Senior Manager",
        "Developer": "Lead Developer",
        "Associate": "Senior Associate",
    }


_EMPLOYEES_SPEC = DatasetSpec(
    name="employee",
    schemas={
        "source": EntitySchema(
            name="employee",
            fields=[
                FieldSpec("employee_id", "INTEGER", kind="seq", start=100),
                FieldSpec("full_name", "VARCHAR", kind="faker", method="name"),
                FieldSpec(
                    "title",
                    "VARCHAR",
                    kind="choices",
                    choices=list(get_title_ladder().keys()),
                ),
                FieldSpec(
                    "department", "VARCHAR", kind="choices", choices=get_dept_choices()
                ),
                FieldSpec(
                    "hire_date",
                    "DATE",
                    kind="computed",
                    sql_export=False,
                    fn=lambda fake, i: (
                        f"202{random.randint(0, 3)}-{random.randint(1, 12):02d}-01"
                    ),
                ),
                FieldSpec(
                    "updated_at",
                    "TIMESTAMP",
                    kind="const",
                    value="2024-01-01 00:00:00",
                    sql_export=False,
                ),
            ],
        ),
    },
    events=EventSpec(
        changes=[
            ChangeRule(
                "title",
                condition=lambda i, r: i % 3 == 0,
                mutate=lambda v, r, fake: get_title_ladder().get(v, v),
            ),
            ChangeRule(
                "department",
                condition=lambda i, r: i % 2 == 0,
                mutate=lambda v, r, fake: random.choice(
                    [d for d in get_dept_choices() if d != v]
                ),
            ),
        ],
        timestamp_field="updated_at",
        event_ts="2024-06-01 09:00:00",
    ),
)


@DatasetFactory.register("employees")
class EmployeesSource(SchematicSource):
    """
    OLTP HR employee directory — feeds a ``LazyType6SCDProcessor``.

    Events (driven by ``_EMPLOYEES_SPEC.events``):
      * Every third employee is promoted (title ascends the ladder).
      * Every second employee transfers to a different department.

    Changes modelled:
      * Promotions   — title upgrades tracked as SCD Type 1 (in-place overwrite)
      * Transfers    — department changes tracked as SCD Type 2 (new version)

    OLTP schema::

        employee_id  INTEGER  PRIMARY KEY
        full_name    VARCHAR
        title        VARCHAR
        department   VARCHAR
        hire_date    DATE
        updated_at   TIMESTAMP

    sqldim output adds::

        valid_from  VARCHAR
        valid_to    VARCHAR
        is_current  BOOLEAN
        checksum    VARCHAR
    """

    _spec = _EMPLOYEES_SPEC

    provider = SourceProvider(
        name="HR Information System (Workday / BambooHR)",
        description="Employee directory with titles, departments, and hire dates.",
        url="https://developer.workday.com/",
        auth_required=True,
        requires=["requests", "oauthlib"],
    )
