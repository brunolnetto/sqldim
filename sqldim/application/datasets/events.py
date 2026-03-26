"""
application/datasets/events.py
================================
Domain-event infrastructure for multi-table aggregate events.

Overview
--------
The existing ``EventSpec`` / ``ChangeRule`` handles **single-table** source-level
CDC simulation — mutations applied to one table's rows during data generation.

This module introduces two complementary abstractions for **aggregate-level**
events that respect business rules across table boundaries:

``AggregateState``
    In-memory snapshot of row lists keyed by table name.  Acts as the
    mutable working set that ``DomainEvent.apply()`` reads from and writes to.

``DomainEvent``
    Abstract base for a business fact that may touch one or more source tables.
    Business rules live here, not scattered across individual source classes.

``EventRepository``
    Aggregate root that materialises each ``BaseSource.snapshot()`` into an
    ``AggregateState`` and dispatches named ``DomainEvent`` objects.

Usage example
-------------
::

    from sqldim.application.datasets.domains.ecommerce.sources import CustomersSource, OrdersSource
    from sqldim.application.datasets.domains.ecommerce.events import CustomerBulkCancelEvent
    from sqldim.application.datasets.events import EventRepository

    repo = (
        EventRepository(
            {"customers": CustomersSource(n=50), "orders": OrdersSource(n=200)}
        )
        .register(CustomerBulkCancelEvent())
    )

    changes = repo.emit("customer_bulk_cancel", customer_id=3)
    # changes["orders"]    — cancelled order rows for customer 3
    # changes["customers"] — updated customer row with refreshed timestamp

    # Downstream: feed changed rows into sqldim as a SQLSource
    from sqldim.sources import SQLSource
    customers_src = SQLSource(rows_to_sql(changes["customers"]))
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


# ── AggregateState ────────────────────────────────────────────────────────────


class AggregateState:
    """
    In-memory row state for a set of named tables.

    ``EventRepository`` maintains a single ``AggregateState`` that is mutated
    in place as events are emitted, so later ``emit()`` calls always see the
    result of earlier ones.

    Parameters
    ----------
    tables : Mapping of table name → initial list of row dicts.
    """

    def __init__(self, tables: dict[str, list[dict]]) -> None:
        self._tables: dict[str, list[dict]] = {
            name: list(rows) for name, rows in tables.items()
        }

    def get(self, table: str) -> list[dict]:
        """Return a shallow copy of the current rows for *table*."""
        if table not in self._tables:
            available = sorted(self._tables)
            raise KeyError(
                f"Table '{table}' not in aggregate state. Available: {available}"
            )
        return list(self._tables[table])

    def update(self, table: str, rows: list[dict]) -> None:
        """Replace the current rows for *table* in the aggregate state."""
        if table not in self._tables:
            available = sorted(self._tables)
            raise KeyError(
                f"Table '{table}' not in aggregate state. Available: {available}"
            )
        self._tables[table] = list(rows)

    @property
    def table_names(self) -> list[str]:
        """Sorted list of all table names in this aggregate."""
        return sorted(self._tables)

    def snapshot(self) -> dict[str, list[dict]]:
        """Deep copy of the full current state as a plain dict."""
        return {name: list(rows) for name, rows in self._tables.items()}


# ── DomainEvent ───────────────────────────────────────────────────────────────


class DomainEvent(ABC):
    """
    Abstract base for a business event that may span multiple source tables.

    Each concrete subclass encodes one named business fact and the rules that
    govern how it changes the aggregate state.

    Subclass checklist
    ------------------
    1. Set ``name`` (class attribute) — unique identifier used for registration.
    2. Implement ``apply(state, **kwargs)`` — read state via ``state.get()``,
       apply business rules, persist changes via ``state.update()``, and return
       only the rows that changed keyed by table name.

    Example
    -------
    ::

        class CustomerBulkCancelEvent(DomainEvent):
            name = "customer_bulk_cancel"

            def apply(
                self, state: AggregateState, *, customer_id: int,
                event_ts: str = "2024-06-01 09:00:00",
            ) -> dict[str, list[dict]]:
                orders = state.get("orders")
                updated_orders = [
                    {**o, "status": "cancelled"}
                    if o["customer_id"] == customer_id and o["status"] == "placed"
                    else o
                    for o in orders
                ]
                state.update("orders", updated_orders)
                changed_orders = [
                    o for o in updated_orders
                    if o["customer_id"] == customer_id and o["status"] == "cancelled"
                ]
                return {"orders": changed_orders}
    """

    name: str  # set per subclass

    @abstractmethod
    def apply(self, state: AggregateState, **kwargs: Any) -> dict[str, list[dict]]:
        """
        Apply business rules to the aggregate state.

        Mutate *state* in place via ``state.update()`` and return only the rows
        that changed, keyed by table name.  Tables with no changes may be
        omitted from the returned dict.
        """


# ── EventRepository ───────────────────────────────────────────────────────────


class EventRepository:
    """
    Aggregate root that manages multi-table state and dispatches ``DomainEvent``
    objects.

    At construction time every source's ``snapshot()`` is materialised into an
    in-memory ``AggregateState``.  Subsequent ``emit()`` calls dispatch named
    events whose business rules may update rows across multiple tables.

    Parameters
    ----------
    sources : Mapping of table name → ``BaseSource`` instance.  Each source's
              ``snapshot()`` is executed once to seed the aggregate state.

    Usage
    -----
    ::

        repo = (
            EventRepository({
                "customers": CustomersSource(n=50),
                "orders":    OrdersSource(n=200),
            })
            .register(CustomerBulkCancelEvent())
        )

        # Emit an event — returns only changed rows
        changes = repo.emit("customer_bulk_cancel", customer_id=5)

        # Full current state of all tables
        state_copy = repo.snapshot()

        # Feed current state of one table into sqldim as a SQLSource
        from sqldim.sources import SQLSource
        src = repo.as_source("orders")
    """

    def __init__(self, sources: dict[str, Any]) -> None:
        rows: dict[str, list[dict]] = {}
        for name, src in sources.items():
            rows[name] = _materialise(src.snapshot())
        self._state = AggregateState(rows)
        self._events: dict[str, DomainEvent] = {}

    def register(self, event: DomainEvent) -> "EventRepository":
        """Register a ``DomainEvent`` by its ``.name``.  Returns *self* for chaining."""
        self._events[event.name] = event
        return self

    def emit(self, event_name: str, **kwargs: Any) -> dict[str, list[dict]]:
        """
        Dispatch a named event and return the rows that changed.

        The aggregate state is mutated in place, so later ``emit()`` calls
        see the results of earlier ones.

        Parameters
        ----------
        event_name : Name as declared in ``DomainEvent.name``.
        **kwargs   : Event-specific payload forwarded to ``DomainEvent.apply()``.

        Returns
        -------
        dict mapping table_name → list of changed rows.  Only tables that were
        actually modified are included.

        Raises
        ------
        KeyError : If no event is registered under *event_name*.
        """
        if event_name not in self._events:
            available = sorted(self._events)
            raise KeyError(
                f"No event '{event_name}' registered. Available: {available}"
            )
        return self._events[event_name].apply(self._state, **kwargs)

    def snapshot(self) -> dict[str, list[dict]]:
        """Deep copy of the full current aggregate state."""
        return self._state.snapshot()

    def as_source(self, table: str):
        """Return a ``SQLSource`` of the current in-memory state of *table*."""
        from sqldim.sources import SQLSource

        return SQLSource(rows_to_sql(self._state.get(table)))


# ── Utility functions ─────────────────────────────────────────────────────────


def rows_to_sql(rows: list[dict]) -> str:
    """
    Convert a list of row dicts to a ``SELECT … UNION ALL …`` SQL string.

    Each column value is rendered as a typed SQL literal so DuckDB infers
    the correct column types.  The result is suitable for wrapping in a
    ``SQLSource`` and passing directly to any sqldim processor.

    Parameters
    ----------
    rows : Non-empty list of row dicts with identical keys.

    Returns
    -------
    ``"SELECT 1 WHERE 1 = 0"`` for an empty list, otherwise a
    ``SELECT … UNION ALL SELECT …`` string.
    """
    if not rows:
        return "SELECT 1 WHERE 1 = 0"
    return " UNION ALL ".join(_row_to_select(row) for row in rows)


def _row_to_select(row: dict) -> str:
    parts = [f"{_sql_literal(v)} AS {k}" for k, v in row.items()]
    return "SELECT " + ", ".join(parts)


def _sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _materialise(sql_source: Any) -> list[dict]:
    """Execute a ``SQLSource`` (or compatible object) and return rows as list[dict]."""
    import duckdb

    con = duckdb.connect()
    query = sql_source.as_sql(con)
    result = con.execute(query)
    cols = [d[0] for d in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]
