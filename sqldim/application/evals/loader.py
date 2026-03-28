"""Auto-discovery loader — reads ``artifacts/evals.json`` and
``artifacts/drift.json`` from every domain folder and hydrates
``EvalCase`` / ``DriftEvalCase`` objects.

Usage
-----
    from sqldim.application.evals.loader import load_eval_suite, load_drift_cases

    suite = load_eval_suite()                     # all domains
    retail_drifts = load_drift_cases("retail")    # one domain
    all_drifts = load_drift_suite()               # all 8 drift domains
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from sqldim.application.evals.cases import EvalCase
from sqldim.application.evals.drift import DriftEvalCase

_DOMAINS_ROOT = Path(__file__).resolve().parent.parent / "datasets" / "domains"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _dict_to_eval_case(d: dict[str, Any]) -> EvalCase:
    """Deserialise one JSON object into an :class:`EvalCase`."""
    pipeline_source_factory: Any = None

    ps_tag = d.get("pipeline_source")
    if ps_tag == "retail_medallion":
        from sqldim.application.datasets.domains.retail.sources.medallion import (
            RetailMedallionPipelineSource,
        )
        pipeline_source_factory = RetailMedallionPipelineSource

    return EvalCase(
        id=d["id"],
        dataset=d["dataset"],
        utterance=d["utterance"],
        expect_result=d.get("expect_result", True),
        expect_columns=d.get("expect_columns", []),
        expect_table=d.get("expect_table"),
        max_hops=d.get("max_hops", 11),
        tags=d.get("tags", []),
        expected_sql=d.get("expected_sql"),
        pipeline_source_factory=pipeline_source_factory,
    )


def _resolve_event_fn(domain: str, fn_name: str):
    """Import *fn_name* from ``domains.{domain}.events.scenarios``."""
    mod_path = (
        f"sqldim.application.datasets.domains.{domain}.events.scenarios"
    )
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


def _make_pipeline_source_factory(domain: str):
    """Return a zero-arg factory that builds a DatasetPipelineSource for *domain*."""
    if domain == "retail":
        from sqldim.application.datasets.domains.retail.sources.medallion import (
            RetailMedallionPipelineSource,
        )
        return RetailMedallionPipelineSource

    def _factory(d=domain):
        from sqldim.application.ask import DatasetPipelineSource, load_dataset
        return DatasetPipelineSource(load_dataset(d))

    return _factory


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def load_eval_cases(domain: str) -> list[EvalCase]:
    """Load eval cases for a single *domain* from its ``artifacts/evals.json``."""
    path = _DOMAINS_ROOT / domain / "artifacts" / "evals.json"
    if not path.exists():
        return []
    with path.open() as f:
        data = json.load(f)
    return [_dict_to_eval_case(c) for c in data.get("cases", [])]


def load_eval_suite() -> list[EvalCase]:
    """Load and concatenate eval cases from **all** domain ``artifacts/evals.json`` files."""
    result: list[EvalCase] = []
    for artifact in sorted(_DOMAINS_ROOT.glob("*/artifacts/evals.json")):
        domain = artifact.parent.parent.name
        with artifact.open() as f:
            data = json.load(f)
        result.extend(_dict_to_eval_case(c) for c in data.get("cases", []))
    return result


def load_drift_cases(domain: str) -> list[DriftEvalCase]:
    """Load drift cases for a single *domain* from its ``artifacts/drift.json``."""
    path = _DOMAINS_ROOT / domain / "artifacts" / "drift.json"
    if not path.exists():
        return []
    with path.open() as f:
        data = json.load(f)

    pipeline_source_factory = _make_pipeline_source_factory(domain)
    result: list[DriftEvalCase] = []
    for c in data.get("cases", []):
        probes = [_dict_to_eval_case(p) for p in c.get("probes", [])]
        event_fn = _resolve_event_fn(domain, c["event_fn"])
        result.append(
            DriftEvalCase(
                id=c["id"],
                pipeline_source_factory=pipeline_source_factory,
                event_fn=event_fn,
                event_description=c["event_description"],
                cases=probes,
            )
        )
    return result


def load_drift_suite() -> list[DriftEvalCase]:
    """Load and concatenate drift cases from **all** domain ``artifacts/drift.json`` files."""
    result: list[DriftEvalCase] = []
    for artifact in sorted(_DOMAINS_ROOT.glob("*/artifacts/drift.json")):
        domain = artifact.parent.parent.name
        result.extend(load_drift_cases(domain))
    return result
