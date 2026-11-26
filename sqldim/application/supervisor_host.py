"""Local host for the DGM supervisor (§12.10).

The supervisor never builds an :class:`AuthContext` itself: the host does,
from its own notion of who is calling.  This module is the host used by the
command-line tools.  Its policy is deliberately narrow:

* the principal is the local operating-system user, in the tenant ``local``;
* it is granted ``READ`` — and nothing else — on exactly the bundled example
  datasets the user selected for the run.

Every dataset is opened through :class:`DataAccess`, so a tool can reach only
the datasets of its own request.
"""

from __future__ import annotations

import getpass
from collections.abc import Iterable
from datetime import timedelta
from typing import Any

from sqldim.core.query.dgm.nl.supervisor._auth import (
    Action,
    AuthContext,
    grants_for,
)
from sqldim.core.query.dgm.nl.supervisor._graph import (
    SupervisorRuntime,
    make_runtime,
)

__all__ = ["LOCAL_TENANT", "local_auth_context", "local_supervisor_runtime"]

LOCAL_TENANT = "local"


def local_auth_context(datasets: Iterable[str]) -> AuthContext:
    """READ on the selected bundled datasets for the local OS user."""
    from sqldim.application.ask import KNOWN_DATASETS

    selected = sorted(set(datasets))
    unknown = [d for d in selected if d not in KNOWN_DATASETS]
    if unknown:
        raise ValueError(f"not bundled datasets: {', '.join(unknown)}")
    return AuthContext(
        principal_id=getpass.getuser() or "local-user",
        tenant_id=LOCAL_TENANT,
        grants=grants_for(selected, {Action.READ}),
    )


def _open_bundled(dataset_id: str) -> Any:
    from sqldim.application._pipeline_sources import DatasetPipelineSource
    from sqldim.application.ask import load_dataset

    return DatasetPipelineSource(load_dataset(dataset_id))


def local_supervisor_runtime(
    datasets: Iterable[str],
    *,
    model: Any = None,
    demo: bool = False,
    timeout: timedelta = timedelta(minutes=5),
) -> SupervisorRuntime:
    """Runtime for a CLI run.  ``demo`` is the user's explicit ``--demo``."""
    from sqldim.application.ask import make_default_budget

    return make_runtime(
        local_auth_context(datasets),
        _open_bundled,
        make_default_budget(),
        model=model,
        demo=demo,
        timeout=timeout,
    )
