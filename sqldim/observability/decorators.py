"""Opt-in auto-instrumentation decorators for pipeline functions.

```
@traced(collector)
def my_loader(...): ...

@metered(collector, name="rows_loaded", kind=MetricKind.COUNTER)
def my_processor(...): ...
```

Both decorators are **opt-in** — apply them explicitly where you want
telemetry.  They do not modify global state.
"""
from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, TypeVar

from sqldim.observability.collector import OTelCollector
from sqldim.observability.metrics import MetricKind, MetricSample

F = TypeVar("F", bound=Callable[..., Any])


def traced(
    collector: OTelCollector,
    *,
    name: str | None = None,
) -> Callable[[F], F]:
    """Decorator that wraps *func* in a :meth:`OTelCollector.start_span` context.

    Parameters
    ----------
    collector:
        The :class:`OTelCollector` instance to record the span in.
    name:
        Override the span name (defaults to ``func.__qualname__``).
    """

    def decorator(func: F) -> F:
        span_name = name or func.__qualname__

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            with collector.start_span(span_name):
                return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with collector.start_span(span_name):
                return await func(*args, **kwargs)

        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


def metered(
    collector: OTelCollector,
    *,
    name: str | None = None,
    kind: MetricKind = MetricKind.COUNTER,
    labels: dict[str, str] | None = None,
    value: float = 1.0,
) -> Callable[[F], F]:
    """Decorator that records a :class:`MetricSample` after *func* returns.

    Parameters
    ----------
    collector:
        The :class:`OTelCollector` instance to record the metric in.
    name:
        Override the metric name (defaults to ``func.__qualname__``).
    kind:
        The :class:`MetricKind` for the recorded sample.
    labels:
        Static labels attached to every sample.
    value:
        The metric value to record.  For counters this is typically ``1.0``
        (a single invocation).  Override with a callable ``value=func`` to
        record the function's return value instead.
    """

    def decorator(func: F) -> F:
        metric_name = name or func.__qualname__
        static_labels = labels or {}

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            _record(collector, metric_name, kind, static_labels, value, result)
            return result

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            result = await func(*args, **kwargs)
            _record(collector, metric_name, kind, static_labels, value, result)
            return result

        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


def _record(
    collector: OTelCollector,
    name: str,
    kind: MetricKind,
    labels: dict[str, str],
    value: float | Callable,
    result: Any,
) -> None:
    actual_value = value(result) if callable(value) else value
    collector.record_metric(
        MetricSample(name=name, value=float(actual_value), kind=kind, labels=labels)
    )
