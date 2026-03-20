"""Lineage emitters — pluggable backends for lineage event export.

All emitters implement the :class:`LineageEmitter` protocol.  The
:class:`ConsoleLineageEmitter` ships as a zero-dependency default;
:class:`OpenLineageEmitter` requires the ``[lineage]`` optional dependency.
"""

from __future__ import annotations

import json
import sys
from typing import Protocol, runtime_checkable

from sqldim.lineage.events import LineageEvent


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class LineageEmitter(Protocol):
    """Protocol for emitting :class:`LineageEvent` instances."""

    def emit(self, event: LineageEvent) -> None: ...


# ---------------------------------------------------------------------------
# Console emitter (zero dependencies)
# ---------------------------------------------------------------------------


class ConsoleLineageEmitter:
    """Writes lineage events as JSON lines to *stream* (default ``stderr``).

    This is the zero-dependency fallback — no OpenLineage SDK required.
    """

    def __init__(self, *, stream=None) -> None:
        self._stream = stream or sys.stderr

    def emit(self, event: LineageEvent) -> None:
        try:
            self._stream.write(json.dumps(event.to_dict(), default=str) + "\n")
            self._stream.flush()
        except Exception:  # noqa: BLE001 — never crash the pipeline
            pass


# ---------------------------------------------------------------------------
# OpenLineage emitter (optional dependency)
# ---------------------------------------------------------------------------


class OpenLineageEmitter:
    """Converts :class:`LineageEvent` to OpenLineage ``RunEvent`` and emits it.

    Requires the ``[lineage]`` optional dependency group.  Raises
    :exc:`ImportError` with a helpful message if the SDK is not installed.

    Parameters
    ----------
    namespace:
        Default namespace for jobs (overrides per-event namespace if set).
    url:
        OpenLineage API endpoint (e.g. ``"http://localhost:5000/api/v1"``).
    """

    def __init__(
        self,
        *,
        namespace: str = "sqldim",
        url: str = "http://localhost:5000/api/v1",
    ) -> None:
        try:
            from openlineage.client import OpenLineageClient
        except ImportError:
            raise ImportError(
                "The [lineage] optional dependency group is required for "
                "OpenLineage emission.  Install it with:  "
                "pip install sqldim[lineage]"
            ) from None

        self._namespace = namespace
        self._client = OpenLineageClient(url=url)
        self._ol = None  # lazy-import the module namespace

    # Map our RunState string values to OpenLineage RunState constants.
    # Populated lazily on first emit; kept as a class attribute to avoid re-importing.
    _STATE_MAP: "dict | None" = None

    @staticmethod
    def _resolve_facets(d) -> dict:
        """Resolve facets for one DatasetRef, serialising any ColumnLineageFacet."""
        facets = dict(d.facets) if d.facets else {}
        col_facet = facets.pop("columnLineage", None)
        if col_facet is not None:
            try:
                from sqldim.lineage.column import ColumnLineageFacet

                if isinstance(col_facet, ColumnLineageFacet):
                    facets["columnLineage"] = col_facet.to_dict()
            except ImportError:  # pragma: no cover
                pass
        return facets

    @staticmethod
    def _to_ol_datasets(refs, Dataset):
        """Convert a list of DatasetRef to OpenLineage Dataset objects."""
        return [
            Dataset(
                namespace=d.namespace,
                name=d.name,
                facets=OpenLineageEmitter._resolve_facets(d) or None,
            )
            for d in refs
        ]

    def _ol_state(self, state_value: str, OLRunState):
        """Return the OLRunState constant for *state_value*, building the map once."""
        if OpenLineageEmitter._STATE_MAP is None:
            OpenLineageEmitter._STATE_MAP = {
                "START": OLRunState.START,
                "RUNNING": OLRunState.RUNNING,
                "COMPLETE": OLRunState.COMPLETE,
                "FAIL": OLRunState.FAIL,
                "ABORT": OLRunState.ABORT,
            }
        return OpenLineageEmitter._STATE_MAP[state_value]

    def emit(self, event: LineageEvent) -> None:
        from openlineage.client.run import (
            Run,
            RunEvent,
            RunState as OLRunState,
            Job,
            Dataset,
        )

        ol_event = RunEvent(
            eventType=self._ol_state(event.state.value, OLRunState),
            eventTime=event.event_time.isoformat(),
            run=Run(runId=event.run_id, facets=event.facets or None),
            job=Job(
                namespace=event.namespace or self._namespace,
                name=event.job_name,
            ),
            inputs=self._to_ol_datasets(event.inputs, Dataset),
            outputs=self._to_ol_datasets(event.outputs, Dataset),
        )
        self._client.emit(ol_event)
