"""PipelineSource ABC and concrete implementations (§11.10).

This module holds the three integration strategies that connect the NL graph
to a data source.  ``ask.py`` re-exports all names from here to keep the public
API unchanged.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import duckdb

from sqldim.application.datasets.dataset import Dataset

__all__ = [
    "PipelineSource",
    "DatasetPipelineSource",
    "MedallionPipelineSource",
    "ObservatoryPipelineSource",
]


# ---------------------------------------------------------------------------
# PipelineSource — protocol for connecting the NL graph to any data source
# ---------------------------------------------------------------------------


class PipelineSource(ABC):
    """Common interface for all NL graph data-source integrations.

    Lifecycle
    ---------
    1. ``setup()`` — prepare the connection / load data
    2. ``get_connection()`` — return the live DuckDB connection
    3. ``get_table_names()`` — return the tables to expose as NL vocabulary
    4. ``teardown()`` — cleanup (close connections, drop temp tables, etc.)
    """

    @abstractmethod
    def setup(self) -> None:
        """Prepare the underlying connection."""

    @abstractmethod
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return the live DuckDB connection to use for schema introspection and query execution."""

    @abstractmethod
    def get_table_names(self) -> list[str]:
        """Return the table names that should form the NL vocabulary."""

    @abstractmethod
    def teardown(self) -> None:
        """Release resources.  Only called after :meth:`setup` has succeeded."""

    @property
    def label(self) -> str:
        """Human-readable label for log/verbose output."""
        return self.__class__.__name__


class DatasetPipelineSource(PipelineSource):
    """Wraps a :class:`~sqldim.application.datasets.dataset.Dataset` into a
    :class:`PipelineSource`.

    This is the original direct-to-dataset path: the dataset is loaded into an
    ephemeral in-memory DuckDB connection and its OLTP schema is used as the NL
    vocabulary.

    Parameters
    ----------
    dataset:
        A pre-loaded :class:`Dataset` instance.
    """

    def __init__(self, dataset: Dataset) -> None:
        self._dataset = dataset
        self._con: duckdb.DuckDBPyConnection | None = None

    def setup(self) -> None:
        self._con = duckdb.connect(":memory:")
        self._dataset.setup(self._con)
        # Populate each table with its synthetic snapshot rows.  We use
        # INSERT BY NAME so SCD audit columns (valid_from, valid_to, …) are
        # left as NULL — they are all nullable and not needed for NL queries.
        for src, table in self._dataset.sources:
            try:
                # Skip sources that already populated their table in setup()
                # (e.g. static-fixture sources that self-insert on creation).
                already_loaded = self._con.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0] > 0
                if already_loaded:
                    continue
                snap_sql = src.snapshot().as_sql(self._con)
                self._con.execute(
                    f"INSERT INTO {table} BY NAME SELECT * FROM ({snap_sql})"
                )
            except NotImplementedError:
                pass  # Static-fixture sources that don't expose snapshot()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("setup() must be called before get_connection()")
        return self._con

    def get_table_names(self) -> list[str]:
        return self._dataset.table_names()

    def teardown(self) -> None:
        if self._con is not None:
            self._dataset.teardown(self._con)
            self._con.close()
            self._con = None

    @property
    def label(self) -> str:
        return f"dataset:{self._dataset.__class__.__name__}"


class MedallionPipelineSource(PipelineSource):
    """Connects the NL graph to a pre-existing medallion pipeline DuckDB.

    The caller is responsible for providing an open, pre-populated DuckDB
    connection (file-backed or in-memory).  This source does *not* close the
    connection on :meth:`teardown` — ownership remains with the caller.

    Parameters
    ----------
    con:
        An open DuckDB connection containing the medallion tables.
    layer:
        Which medallion layer to target (``"bronze"``, ``"silver"``, or
        ``"gold"``).  Only used when *registry* is provided.
    registry:
        Optional :class:`~sqldim.medallion.MedallionRegistry`.  When present,
        table discovery is restricted to datasets registered at *layer*.
        When absent, all tables found via ``SHOW TABLES`` are exposed.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        layer: str = "gold",
        registry: Any = None,
    ) -> None:
        self._con = con
        self._layer = layer
        self._registry = registry

    def setup(self) -> None:
        pass  # connection is already set up by the caller

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def get_table_names(self) -> list[str]:
        if self._registry is not None:
            try:
                from sqldim.medallion.layer import Layer

                layer_enum = Layer(self._layer)
                return sorted(self._registry.datasets_in(layer_enum))
            except Exception:  # noqa: BLE001
                pass  # fall through to SHOW TABLES discovery
        rows: list[Any] = self._con.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]

    def teardown(self) -> None:
        pass  # caller owns the connection

    @property
    def label(self) -> str:
        return f"medallion:{self._layer}"


class ObservatoryPipelineSource(PipelineSource):
    """Exposes a :class:`~sqldim.observability.DriftObservatory`'s six-table
    Kimball star schema to the NL graph.

    This allows natural-language queries like *"which datasets have the highest
    breaking-change rate?"* to run directly against pipeline observability data.

    The observatory's DuckDB connection is borrowed — it is not closed on
    :meth:`teardown`.

    Parameters
    ----------
    observatory:
        A fully-initialised :class:`~sqldim.observability.DriftObservatory`
        instance.
    """

    _OBS_TABLE_NAMES: list[str] = [
        "obs_dataset_dim",
        "obs_evolution_type_dim",
        "obs_rule_dim",
        "obs_pipeline_run_dim",
        "obs_schema_evolution_fact",
        "obs_quality_drift_fact",
    ]

    def __init__(self, observatory: Any) -> None:
        self._observatory = observatory  # DriftObservatory

    def setup(self) -> None:
        pass  # schema is ensured inside DriftObservatory.__init__

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self._observatory._con  # type: ignore[return-value]

    def get_table_names(self) -> list[str]:
        return list(self._OBS_TABLE_NAMES)

    def teardown(self) -> None:
        pass  # caller owns the observatory

    @property
    def label(self) -> str:
        return "observatory"
