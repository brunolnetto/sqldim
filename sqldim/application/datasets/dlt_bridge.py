"""
application/datasets/dlt_bridge.py
====================================
Thin bridge between a `dlt <https://dlthub.com>`_ pipeline and the sqldim
``SQLSource`` abstraction.

``DLTBridge`` runs a dlt pipeline against any dlt source and returns the
staged table as a ``SQLSource`` that sqldim processors can consume directly.
This removes the need for intermediate files and keeps the two libraries
loosely coupled — sqldim only ever sees a ``SQLSource``; dlt only ever sees
a callable source factory.

Typical usage
-------------
::

    from sqldim.application.datasets.dlt_bridge import DLTBridge
    from sqldim.application.examples.features.integrations.ecommerce_api import (
        ecommerce_api_source,
    )

    bridge = DLTBridge(
        source_factory=ecommerce_api_source,
        table_name="customers",
        col_order=["customer_id", "full_name", "email", "address", "city", "tier"],
        pipeline_name="ecommerce_pipeline",
        dataset_name="ecommerce_staging",
    )
    source = bridge.run(pipelines_dir="/tmp/dlt_staging")
    # source is a SQLSource — pass it directly to any sqldim processor

Notes
-----
* The bridge uses dlt's ``duckdb`` destination by default (in-process,
  zero configuration).
* ``dev_mode=True`` is set so repeated calls with the same pipeline name
  don't accumulate state across invocations.
* dlt-internal columns (``_dlt_load_id``, ``_dlt_id``, …) are stripped
  before returning the ``SQLSource``.
"""

from __future__ import annotations  # pragma: no cover

import tempfile  # pragma: no cover
from collections.abc import Callable  # pragma: no cover
from typing import Any  # pragma: no cover

import dlt  # pragma: no cover  # type: ignore[import-not-found]

from sqldim.sources import SQLSource  # pragma: no cover

from sqldim.application.datasets.events import rows_to_sql  # pragma: no cover

_DLT_INTERNAL_COLS = {"_dlt_load_id", "_dlt_id"}  # pragma: no cover


def _drop_internal_cols(df):  # pragma: no cover
    return df[[c for c in df.columns if c not in _DLT_INTERNAL_COLS]]


def _apply_col_order(df, col_order):  # pragma: no cover
    if col_order:
        ordered = [c for c in col_order if c in df.columns]
        return df[ordered]
    return df


class DLTBridge:  # pragma: no cover
    """
    Run a dlt pipeline and expose the staged table as a ``SQLSource``.

    Parameters
    ----------
    source_factory:
        A zero-argument callable that returns a dlt source or resource
        (e.g. a ``@dlt.source``-decorated function).
    table_name:
        The name of the dlt-staged table to read back after the pipeline run.
    col_order:
        Optional list of business-column names.  When supplied the returned
        ``SQLSource`` contains only these columns, in this order.  dlt-internal
        columns are always excluded regardless of this setting.
    pipeline_name:
        Passed verbatim to ``dlt.pipeline()``.  Defaults to
        ``"sqldim_dlt_bridge"``.
    dataset_name:
        The dlt dataset / schema name used for staging.  Defaults to
        ``"staging"``.
    destination:
        dlt destination string.  Defaults to ``"duckdb"`` (in-process, no
        external credentials required).
    """

    def __init__(
        self,
        source_factory: Callable[[], Any],
        table_name: str,
        col_order: list[str] | None = None,
        *,
        pipeline_name: str = "sqldim_dlt_bridge",
        dataset_name: str = "staging",
        destination: str = "duckdb",
    ) -> None:
        self._source_factory = source_factory
        self._table_name = table_name
        self._col_order = col_order
        self._pipeline_name = pipeline_name
        self._dataset_name = dataset_name
        self._destination = destination

    def run(self, pipelines_dir: str | None = None) -> SQLSource:
        """
        Execute the dlt pipeline and return the staged data as a ``SQLSource``.

        Parameters
        ----------
        pipelines_dir:
            Directory where dlt stores pipeline state.  A temporary directory
            is created (and immediately used) when *None* is passed.

        Returns
        -------
        SQLSource
            Ready to be passed to any sqldim processor.
        """
        own_tmp = pipelines_dir is None
        if own_tmp:
            pipelines_dir = tempfile.mkdtemp(prefix="sqldim_dlt_bridge_")

        pipeline = dlt.pipeline(
            pipeline_name=self._pipeline_name,
            destination=self._destination,
            dataset_name=self._dataset_name,
            pipelines_dir=pipelines_dir,
            dev_mode=True,
        )
        pipeline.run(self._source_factory())

        with pipeline.dataset() as ds:
            df = ds[self._table_name].df()

        # Drop dlt-internal bookkeeping columns
        df = _drop_internal_cols(df)
        df = _apply_col_order(df, self._col_order)

        rows = df.to_dict(orient="records")
        return SQLSource(rows_to_sql(rows))
