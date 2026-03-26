"""
sqldim/examples/datasets/base.py
==================================
Base class, factory, and source-provider descriptor for all OLTP dataset
simulators bundled with sqldim.

Design goals
------------
* ``BaseSource`` defines the interface every dataset must satisfy:
  ``snapshot()`` returns the full initial extract; ``event_batch(n)``
  returns change events.  ``setup`` / ``teardown`` create and drop the
  sqldim-managed target table using the class-level ``DIM_DDL`` string by
  default, so simple sources need no boilerplate.

* ``SourceProvider`` is a lightweight descriptor that documents where the
  live data would come from in a real deployment.  It is used in showcase
  output to help users understand what each synthetic dataset represents.

* ``DatasetFactory`` is a simple registry that lets user code request a
  dataset by name rather than importing the class directly::

      from sqldim.application.datasets.base import DatasetFactory
      src = DatasetFactory.create("products", n=10, seed=7)

Usage pattern
-------------
::

    from sqldim.application.datasets.base import BaseSource, SourceProvider

    class MySource(BaseSource):
        provider = SourceProvider(
            name="My ERP system",
            url="https://erp.example.com/api/items",
            description="Product master from ERP REST API",
        )
        OLTP_DDL = \"\"\"CREATE TABLE IF NOT EXISTS {table} (item_id INTEGER, name VARCHAR)\"\"\"
        DIM_DDL  = \"\"\"CREATE TABLE IF NOT EXISTS {table} (item_id INTEGER, name VARCHAR,
                            valid_from VARCHAR, valid_to VARCHAR, is_current BOOLEAN, checksum VARCHAR)\"\"\"

        def snapshot(self):
            from sqldim.sources import SQLSource
            return SQLSource("SELECT 1 AS item_id, 'demo' AS name")
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import duckdb


# ── SourceProvider ─────────────────────────────────────────────────────────────


@dataclass
class SourceProvider:
    """
    Documents the live data source a dataset simulates.

    Used in showcase output so users understand what real-world API or
    database the synthetic dataset represents.

    Attributes
    ----------
    name        : Human-readable name of the source system.
    description : One-sentence summary of the data it contains.
    url         : Link to the API docs / dataset page (optional).
    auth_required : Whether the real source needs credentials.
    requires    : Extra Python packages needed to connect for real
                  (e.g. ``["dlt", "shopify-python-api"]``).
    """

    name: str
    description: str = ""
    url: str | None = None
    auth_required: bool = False
    requires: list[str] = field(default_factory=list)

    def _describe_lines(self) -> list:
        return [
            f"  Source: {self.name}",
            f"    {self.description}" if self.description else "",
            f"    Docs:   {self.url}" if self.url else "",
            "    Note:   authentication required" if self.auth_required else "",
            f"    Needs:  pip install {' '.join(self.requires)}"
            if self.requires
            else "",
        ]

    def describe(self) -> str:
        """One-block summary suitable for showcase output."""
        return "\n".join(p for p in self._describe_lines() if p)


# ── BaseSource ─────────────────────────────────────────────────────────────────


class BaseSource(ABC):
    """
    Abstract base for all OLTP dataset simulators.

    Subclass checklist
    ------------------
    1. Set ``OLTP_DDL`` — the source-system schema (informational).
    2. Set ``DIM_DDL`` (or ``FACT_DDL`` / ``EDGE_DDL``) — the sqldim-managed
       analytical target.  The default ``setup()`` formats this with
       ``table=<name>`` and executes it.
    3. Implement ``snapshot()`` — returns a ``SQLSource`` of the full initial
       OLTP extract.
    4. Optionally override ``event_batch(n)`` — returns change events.
    5. Optionally set ``provider`` — a ``SourceProvider`` instance that
       describes the live data source this simulates.
    6. Optionally override ``setup()`` / ``teardown()`` if the defaults
       (which format ``DIM_DDL``) are insufficient.
    """

    # ── Class-level schema declarations ─────────────────────────────────────
    OLTP_DDL: str = ""
    """DDL for the transactional source table (informational only)."""

    DIM_DDL: str = ""
    """DDL for the sqldim-managed analytical target.
    Used by the default ``setup()`` implementation."""

    provider: SourceProvider | None = None
    """Optional descriptor documenting the live data source this simulates."""

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def setup(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        """
        Create the sqldim-managed target table (empty).

        Default implementation formats ``DIM_DDL`` with ``table=table`` and
        executes it.  Override when setting up multiple tables or using a
        different DDL attribute (e.g. ``EDGE_DDL``).
        """
        if not self.DIM_DDL:
            raise NotImplementedError(
                f"{type(self).__name__} must define DIM_DDL or override setup()"
            )
        con.execute(self.DIM_DDL.format(table=table))

    def teardown(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        """Drop the sqldim-managed target table."""
        con.execute(f"DROP TABLE IF EXISTS {table}")

    # ── Data interface ───────────────────────────────────────────────────────

    @abstractmethod
    def snapshot(self):
        """
        Full initial OLTP extract (T0).

        Returns a ``SQLSource`` (or any object accepted by
        ``sqldim.sources.coerce_source``).
        """

    def event_batch(self, n: int = 1):
        """
        Incremental change events at event T_n (default: first batch).

        Returns a ``SQLSource``.  Raise ``NotImplementedError`` for sources
        that model only a static snapshot.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not model incremental events. "
            "Implement event_batch() or use snapshot() only."
        )

    # ── Introspection ────────────────────────────────────────────────────────

    def describe_provider(self) -> str:
        """Return a formatted string describing the live data source."""
        if self.provider is None:
            return f"  Source: {type(self).__name__} (synthetic / no live provider)"
        return self.provider.describe()


# ── DatasetFactory ─────────────────────────────────────────────────────────────


class DatasetFactory:
    """
    Registry and factory for named dataset sources.

    Decorate a ``BaseSource`` subclass with ``@DatasetFactory.register(name)``
    to make it discoverable::

        @DatasetFactory.register("products")
        class ProductsSource(BaseSource):
            ...

    Then create instances by name::

        src = DatasetFactory.create("products", n=10, seed=7)

    Convenience bulk helpers
    ------------------------
    ``setup_all(con, [(src, table), ...])``
        Call ``src.setup(con, table)`` for every pair.

    ``teardown_all(con, [(src, table), ...])``
        Call ``src.teardown(con, table)`` for every pair.
    """

    _registry: dict[str, type[BaseSource]] = {}

    @classmethod
    def register(cls, name: str):
        """Class decorator — register *klass* under *name*."""

        def decorator(klass: type[BaseSource]) -> type[BaseSource]:
            cls._registry[name] = klass
            return klass

        return decorator

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> BaseSource:
        """Create a registered source by name, forwarding **kwargs to __init__."""
        if name not in cls._registry:
            raise KeyError(
                f"No dataset registered under '{name}'. "
                f"Available: {sorted(cls._registry)}"
            )
        return cls._registry[name](**kwargs)

    @classmethod
    def available(cls) -> list[str]:
        """Return sorted list of all registered dataset names."""
        return sorted(cls._registry)

    @classmethod
    def setup_all(
        cls,
        con: duckdb.DuckDBPyConnection,
        specs: list[tuple[BaseSource, str]],
    ) -> None:
        """Call ``src.setup(con, table)`` for every ``(src, table)`` pair."""
        for src, table in specs:
            src.setup(con, table)

    @classmethod
    def teardown_all(
        cls,
        con: duckdb.DuckDBPyConnection,
        specs: list[tuple[BaseSource, str]],
    ) -> None:
        """Call ``src.teardown(con, table)`` for every ``(src, table)`` pair."""
        for src, table in specs:
            src.teardown(con, table)


# ── SchematicSource ────────────────────────────────────────────────────────────


class SchematicSource(BaseSource):
    """
    ``BaseSource`` subclass driven by a single-schema ``DatasetSpec`` — no
    boilerplate DDL strings or manual row-building loops.

    Subclass checklist
    ------------------
    1. Declare ``_spec: ClassVar[DatasetSpec]`` — the spec's ``"source"`` role
       provides the ``EntitySchema`` that drives DDL generation, synthetic row
       creation, and SQL rendering.
    2. Supply ``events=EventSpec(...)`` on the ``DatasetSpec`` if the source
       models incremental CDC events; omit it for snapshot-only sources.
    3. Optionally override ``provider`` (``SourceProvider`` instance).
    4. Optionally override ``event_batch()`` for non-standard event logic.

    The ``DIM_DDL`` and ``OLTP_DDL`` attributes are computed from
    ``_spec.source`` rather than being raw SQL strings.
    """

    _spec: "Any"  # DatasetSpec  (filled by subclass; must have "source" role)

    def __init__(
        self,
        n: int = 5,
        seed: int = 42,
        *,
        spec: "Any | None" = None,
        schema_name: str = "source",
        n_entities: "int | None" = None,
    ) -> None:
        from faker import Faker

        fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        # New-style API: spec= overrides the class-level _spec; n_entities= overrides n.
        if spec is not None:
            self._spec = spec
        self._schema_name = schema_name
        schema = getattr(self._spec, schema_name)
        count = n_entities if n_entities is not None else n
        self._initial: list[dict] = schema.generate(count, fake)
        event_spec = self._spec.events
        self._events1: list[dict] = (
            event_spec.apply(self._initial, fake) if event_spec is not None else []
        )

    # Properties replace the class-level str attributes on BaseSource
    @property
    def DIM_DDL(self) -> str:  # type: ignore[override]  # noqa: N802
        return getattr(self._spec, self._schema_name).dim_ddl()

    @property
    def OLTP_DDL(self) -> str:  # type: ignore[override]  # noqa: N802
        return getattr(self._spec, self._schema_name).oltp_ddl()

    def snapshot(self):
        from sqldim.sources import SQLSource

        return SQLSource(self._spec.source.to_sql(self._initial))

    def event_batch(self, n: int = 1):
        from sqldim.sources import SQLSource

        if n == 1:
            return SQLSource(self._spec.source.to_sql(self._events1))
        raise ValueError(f"{type(self).__name__} has 1 event batch (requested n={n})")

    @property
    def initial(self) -> list[dict]:
        """Copy of the initial synthetic rows."""
        return list(self._initial)

    @property
    def events(self) -> list[dict]:
        """Copy of the event-batch rows."""
        return list(self._events1)
