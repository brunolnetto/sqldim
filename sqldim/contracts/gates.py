"""Quality gates — checks that must pass before a dataset crosses a layer boundary."""
from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Callable, NamedTuple

from sqldim.medallion import Layer

if TYPE_CHECKING:
    from sqldim.lineage.emitter import LineageEmitter


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
    lineage_emitter:
        Optional :class:`~sqldim.lineage.LineageEmitter`.  When provided,
        ``START`` / ``COMPLETE`` / ``FAIL`` events are emitted around each
        :meth:`run` call.  Pass ``None`` (default) to skip lineage tracking.
    """

    def __init__(
        self,
        name: str,
        layer_from: Layer,
        layer_to: Layer,
        *,
        lineage_emitter: LineageEmitter | None = None,
    ) -> None:
        if not layer_from.can_promote_to(layer_to):
            raise ValueError(
                f"Invalid gate: cannot promote directly from {layer_from!r} to {layer_to!r}."
            )
        self.name = name
        self.layer_from = layer_from
        self.layer_to = layer_to
        self._checks: list[Callable[..., CheckResult]] = []
        self._lineage_emitter = lineage_emitter

    def add_check(self, check: Callable[..., CheckResult]) -> "QualityGate":
        """Register a check callable and return *self* for chaining."""
        self._checks.append(check)
        return self

    def _run_check_safe(self, check: Callable[..., CheckResult], kwargs: dict) -> CheckResult:
        """Run a single *check* forwarding accepted kwargs; capture any exception as a failure."""
        try:
            sig = inspect.signature(check)
            accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
            return check(**accepted)
        except Exception as exc:  # noqa: BLE001
            return CheckResult(
                name=getattr(check, "__name__", repr(check)),
                passed=False,
                detail=str(exc),
            )

    def _make_gate_facets(self, results: list, gate_result: "GateResult") -> dict:
        """Build lineage facets dict summarising gate check results."""
        facets: dict = {
            "check_count": len(results),
            "pass_count": sum(1 for r in results if r.passed),
            "fail_count": len(gate_result.failing),
        }
        if gate_result.failing:
            facets["failing_checks"] = [
                {"name": r.name, "detail": r.detail}
                for r in gate_result.failing
            ]
        return facets

    def run(self, **kwargs) -> GateResult:
        """Execute every registered check, forwarding *kwargs* where accepted.

        If a check raises an exception it is caught and recorded as a
        failing :class:`CheckResult` with the exception message as detail.

        If a *lineage_emitter* was provided at construction, a ``START``
        lineage event is emitted before checks run and a ``COMPLETE`` or
        ``FAIL`` event is emitted afterwards.
        """
        from sqldim.lineage.events import DatasetRef, LineageEvent, RunState

        run_id: str | None = None
        if self._lineage_emitter is not None:
            run_id = LineageEvent(job_name=f"gate.{self.name}").run_id
            self._lineage_emitter.emit(
                LineageEvent(
                    run_id=run_id,
                    job_name=f"gate.{self.name}",
                    state=RunState.START,
                    inputs=[DatasetRef(
                        namespace=f"sqldim.{self.layer_from.value}",
                        name=self.name,
                    )],
                    outputs=[DatasetRef(
                        namespace=f"sqldim.{self.layer_to.value}",
                        name=self.name,
                    )],
                )
            )

        results = [self._run_check_safe(check, kwargs) for check in self._checks]
        gate_result = GateResult(results=results)

        if run_id is not None:
            state = RunState.COMPLETE if gate_result.ok else RunState.FAIL
            self._lineage_emitter.emit(  # type: ignore[union-attr]
                LineageEvent(
                    run_id=run_id,
                    job_name=f"gate.{self.name}",
                    state=state,
                    inputs=[DatasetRef(
                        namespace=f"sqldim.{self.layer_from.value}",
                        name=self.name,
                    )],
                    outputs=[DatasetRef(
                        namespace=f"sqldim.{self.layer_to.value}",
                        name=self.name,
                    )],
                    facets=self._make_gate_facets(results, gate_result),
                )
            )

        return gate_result
