"""Quality gates — checks that must pass before a dataset crosses a layer boundary."""
from __future__ import annotations

import inspect
from typing import Callable, NamedTuple

from sqldim.medallion import Layer


class CheckResult(NamedTuple):
    """Result of a single gate check."""

    name: str
    passed: bool
    detail: str = ""


class GateResult:
    """Aggregate result from running all checks in a :class:`QualityGate`."""

    def __init__(self, results: list[CheckResult]) -> None:
        self.results = results

    @property
    def ok(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failing(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]


class QualityGate:
    """A named gate that runs callable checks before a layer promotion.

    Parameters
    ----------
    name:
        Human-readable identifier for this gate.
    layer_from:
        The source layer (e.g. ``Layer.BRONZE``).
    layer_to:
        The target layer.  Must be exactly one step above *layer_from*;
        raises :class:`ValueError` for non-adjacent promotions (or
        demotions).
    """

    def __init__(self, name: str, layer_from: Layer, layer_to: Layer) -> None:
        if not layer_from.can_promote_to(layer_to):
            raise ValueError(
                f"Invalid gate: cannot promote directly from {layer_from!r} to {layer_to!r}."
            )
        self.name = name
        self.layer_from = layer_from
        self.layer_to = layer_to
        self._checks: list[Callable[..., CheckResult]] = []

    def add_check(self, check: Callable[..., CheckResult]) -> "QualityGate":
        """Register a check callable and return *self* for chaining."""
        self._checks.append(check)
        return self

    def run(self, **kwargs) -> GateResult:
        """Execute every registered check, forwarding *kwargs* where accepted.

        If a check raises an exception it is caught and recorded as a
        failing :class:`CheckResult` with the exception message as detail.
        """
        results: list[CheckResult] = []
        for check in self._checks:
            try:
                sig = inspect.signature(check)
                accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
                result = check(**accepted)
            except Exception as exc:  # noqa: BLE001
                result = CheckResult(
                    name=getattr(check, "__name__", repr(check)),
                    passed=False,
                    detail=str(exc),
                )
            results.append(result)
        return GateResult(results=results)
