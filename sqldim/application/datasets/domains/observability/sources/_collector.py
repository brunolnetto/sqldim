"""Shared telemetry collector — runs domain pipelines with instrumentation.

This module is the heart of the observability domain: it imports each of the
other 12 domain pipeline builders, executes them inside an
:class:`~sqldim.observability.OTelCollector`, and returns the captured
:class:`PipelineSpan` and :class:`MetricSample` objects.

The result is cached per-process so that both :class:`PipelineSpanSource`
and :class:`MetricSampleSource` share the same execution run without
re-building every pipeline twice.
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING

import duckdb

from sqldim.observability import (
    Counter,
    Histogram,
    MetricKind,
    MetricSample,
    OTelCollector,
    PipelineSpan,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# The 12 non-observability domain names, in alphabetical order.
_DOMAIN_NAMES: list[str] = [
    "devops",
    "dgm",
    "ecommerce",
    "enterprise",
    "fintech",
    "hierarchy",
    "media",
    "nba_analytics",
    "retail",
    "saas_growth",
    "supply_chain",
    "user_activity",
]

_BASE_MODULE = "sqldim.application.datasets.domains"

# ── Process-level cache ──────────────────────────────────────────────────────

_cached_spans: list[PipelineSpan] | None = None
_cached_metrics: list[MetricSample] | None = None


def _reset_cache() -> None:
    """Clear the cached telemetry (useful in tests)."""
    global _cached_spans, _cached_metrics  # noqa: PLW0603
    _cached_spans = None
    _cached_metrics = None


def collect_telemetry(
    *,
    seed: int = 42,
) -> tuple[list[PipelineSpan], list[MetricSample]]:
    """Run all domain pipelines with instrumentation and return telemetry.

    Returns
    -------
    (spans, metrics)
        Lists of captured :class:`PipelineSpan` and :class:`MetricSample`.
    """
    global _cached_spans, _cached_metrics  # noqa: PLW0603
    if _cached_spans is not None and _cached_metrics is not None:
        return _cached_spans, _cached_metrics

    collector = OTelCollector()
    row_counter = Counter(collector, "rows_created", labels={})
    build_hist = Histogram(collector, "build_duration_s", labels={})

    for domain in _DOMAIN_NAMES:
        module_path = f"{_BASE_MODULE}.{domain}.pipeline.builder"
        try:
            mod = importlib.import_module(module_path)
        except ImportError:
            logger.warning("Could not import %s — skipping", module_path)
            continue

        con = duckdb.connect(":memory:")
        try:
            with collector.start_span(
                f"{domain}.build_pipeline",
                domain=domain,
                pipeline=f"{domain}.build_pipeline",
            ) as span:
                mod.build_pipeline(con, seed=seed)
                span.attributes["tables"] = ",".join(
                    getattr(mod, "TABLE_NAMES", [])
                )

            # Record row counts per table.
            for table_name in getattr(mod, "TABLE_NAMES", []):
                try:
                    (count,) = con.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()  # type: ignore[misc]
                    row_counter.increment(float(count))
                    collector.record_metric(
                        MetricSample(
                            name="table_row_count",
                            value=float(count),
                            kind=MetricKind.GAUGE,
                            labels={"domain": domain, "table": table_name},
                        )
                    )
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Could not count rows for %s.%s", domain, table_name
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Pipeline %s failed: %s", domain, exc)
        finally:
            con.close()

    # Record per-domain build durations as histogram samples.
    for span in collector.spans():
        if span.name.endswith(".build_pipeline"):
            build_hist.record(span.duration_s)

    _cached_spans = collector.spans()
    _cached_metrics = collector.metrics()
    return _cached_spans, _cached_metrics
