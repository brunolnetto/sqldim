"""
Growth Accounting — moved to real-world showcases
==================================================

The growth accounting state machine (New / Retained / Resurrected /
Churned / Stale) has been integrated into the two real-world examples
that own the relevant domain datasets:

  user_activity  — page-view events → bitmask retention + growth states
  saas_growth    — session events   → plan-tier SCD-2 + growth states

See:
    sqldim.application.examples.real_world.user_activity.showcase
    sqldim.application.examples.real_world.saas_growth.showcase

This shim exists for backward compatibility with existing test and tool
references; it delegates to both real-world showcases.
"""
from __future__ import annotations

import asyncio

from sqldim.application.examples.real_world.user_activity.showcase import (
    run_showcase as _run_user_activity,
)
from sqldim.application.examples.real_world.saas_growth.showcase import (
    run_showcase as _run_saas_growth,
)

EXAMPLE_METADATA = {
    "name": "growth-accounting",
    "title": "Growth Accounting",
    "description": (
        "Redirect — growth accounting is now part of user_activity and "
        "saas_growth real-world showcases."
    ),
    "entry_point": "run_showcase",
}


def run_showcase() -> None:
    """Delegate to both real-world showcases that contain growth accounting."""
    asyncio.run(_run_user_activity())
    asyncio.run(_run_saas_growth())


if __name__ == "__main__":  # pragma: no cover
    run_showcase()
