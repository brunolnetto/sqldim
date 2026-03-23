"""
application/examples/features/integrations/flask_dlt.py
===========================================================
Example 16 — Flask API + dlt HTTP source → sqldim SCD2 warehouse
=================================================================

This is a **self-contained, runnable script** that wires together:

1. **Flask API** — defined in :mod:`ecommerce_api`, backed by a DuckDB
   in-memory store populated from the ecommerce ``LoyaltyCustomersSource``.
   Exposes two endpoints:

   * ``GET /customers``                  — full customer list (JSON)
   * ``POST /customers/<id>/upgrade``    — tier-upgrade event (updates tier
     in the API store, returns the changed row)

2. **dlt HTTP source** — ``@dlt.resource`` functions that call the Flask
   endpoints and yield row dicts, exactly as a real dlt pipeline would
   call an external REST API.

3. **sqldim pipeline** — ``LazySCDProcessor`` that consumes the dlt
   resource output via :class:`~sqldim.application.datasets.dlt_bridge.DLTBridge`
   and builds an SCD Type-2 ``dim_customer`` dimension.
   Two pipeline runs demonstrate incremental loading:

   * **T0** — initial load via ``GET /customers``
   * **T1** — upgrade three customers via ``POST ...upgrade``, then re-run
     the dlt extract; only the changed rows trigger SCD2 versioning.

Design notes
------------
* The Flask app (defined in ``ecommerce_api.py``) runs in a background
  thread with use_reloader=False and threaded=True so the dlt pipeline can
  call it from the main thread.

* ``DLTBridge`` handles running the dlt pipeline and converting the staged
  DuckDB table into a ``SQLSource`` that sqldim processes natively.

* Everything is ephemeral — temp dirs are cleaned up at the end.

Run
---
::

    PYTHONPATH=. .venv/bin/python application/examples/features/integrations/flask_dlt.py

    # or, if the venv python is active:
    python -m sqldim.application.examples.features.integrations.flask_dlt
"""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
import threading
import time
from typing import Any

import duckdb
import dlt
import requests

from sqldim.core.kimball.dimensions.scd.processors.scd_engine import LazySCDProcessor
from sqldim.sinks import DuckDBSink

from sqldim.application.datasets.domains.ecommerce import LoyaltyCustomersSource
from sqldim.application.datasets.dlt_bridge import DLTBridge
from sqldim.application.examples.features.integrations.ecommerce_api import (
    build_flask_app,
    get_store,
    _app,
)
from sqldim.application.examples.features.utils import make_tmp_db

# ── dlt resources ─────────────────────────────────────────────────────────────

_BASE_URL = ""  # set at runtime once we know which port Flask is on

# Column order matching the dim_customer DDL (business columns only)
_CUSTOMER_COL_ORDER = ["customer_id", "email", "full_name", "loyalty_tier", "country_code", "acquisition_channel"]


@dlt.resource(name="customers", write_disposition="replace")
def dlt_customers():
    """dlt resource — fetches all customers from the live Flask API."""
    resp = requests.get(f"{_BASE_URL}/customers", timeout=5)
    resp.raise_for_status()
    yield from resp.json()


@dlt.source(name="ecommerce_api")
def ecommerce_api_source():
    """dlt source grouping all ecommerce API resources."""
    yield dlt_customers()


# ── Pipeline helpers ──────────────────────────────────────────────────────────


def _load_rows_into_warehouse(
    bridge: DLTBridge,
    staging_dir: str,
    warehouse_db: str,
    tracked_fields: list[str],
) -> Any:
    """Run DLTBridge, then push the resulting SQLSource through a LazySCDProcessor."""
    source = bridge.run(pipelines_dir=staging_dir)
    with DuckDBSink(warehouse_db) as sink:
        proc = LazySCDProcessor(
            "customer_id",
            tracked_fields,
            sink,
            con=sink._con,
        )
        return proc.process(source, "dim_customer")


# ── Main example ──────────────────────────────────────────────────────────────


def run_flask_dlt_example() -> None:
    global _BASE_URL

    print("\n── Example 16: Flask API + dlt HTTP source → sqldim SCD2 ─────")

    # Pick a free port dynamically to avoid cross-run collisions
    with socket.socket() as _s:
        _s.bind(("127.0.0.1", 0))
        _port = _s.getsockname()[1]
    _BASE_URL = f"http://127.0.0.1:{_port}"

    # Seed the Flask store from LoyaltyCustomersSource — loyalty_tier already present.
    raw = LoyaltyCustomersSource(n_entities=20, seed=42)
    _export_keys = ["customer_id", "email", "full_name", "loyalty_tier", "country_code", "acquisition_channel"]
    customers: list[dict[str, Any]] = [
        {k: row[k] for k in _export_keys}
        for row in raw.initial
    ]
    build_flask_app(customers)

    # Start Flask in a daemon thread
    threading.Thread(
        target=lambda: _app.run(
            host="127.0.0.1",
            port=_port,
            use_reloader=False,
            threaded=True,
            debug=False,
        ),
        daemon=True,
    ).start()

    # Wait for Flask to be ready (up to 2 s)
    for _ in range(20):
        try:
            requests.get(f"{_BASE_URL}/customers", timeout=0.5)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    else:
        raise RuntimeError(f"Flask did not start on {_BASE_URL}")

    _store = get_store()
    print(f"  Flask API running on {_BASE_URL}")
    print(f"  API store: {len(_store)} customers")

    staging_dir = tempfile.mkdtemp(prefix="sqldim_dlt_ex16_")
    warehouse_db = make_tmp_db()

    bridge = DLTBridge(
        source_factory=ecommerce_api_source,
        table_name="customers",
        col_order=_CUSTOMER_COL_ORDER,
    )

    # Create dim_customer using LoyaltyCustomersSource schema
    _DIM_DDL = """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id          INTEGER,
        email                VARCHAR,
        full_name            VARCHAR,
        loyalty_tier         VARCHAR,
        country_code         VARCHAR,
        acquisition_channel  VARCHAR,
        valid_from    VARCHAR,
        valid_to      VARCHAR,
        is_current    BOOLEAN,
        checksum      VARCHAR
    )
    """
    setup_con = duckdb.connect(warehouse_db)
    setup_con.execute(_DIM_DDL)
    setup_con.close()

    tracked = ["email", "full_name", "loyalty_tier", "country_code", "acquisition_channel"]

    try:
        # ── T0: initial extract -> stage -> warehouse ─────────────────────
        print("\n  T0 — initial dlt extract + sqldim load …")
        result_t0 = _load_rows_into_warehouse(bridge, staging_dir + "/t0", warehouse_db, tracked)
        print(f"    sqldim T0  →  inserted={result_t0.inserted}")

        # ── Apply three tier-upgrade events via the Flask API ─────────────
        print("\n  Applying upgrade events via POST /customers/<id>/upgrade …")
        for cid in [1, 2, 3]:
            resp = requests.post(
                f"{_BASE_URL}/customers/{cid}/upgrade",
                json={"loyalty_tier": "gold"},
                timeout=5,
            )
            resp.raise_for_status()
            d = resp.json()
            print(
                f"    customer_id={cid:<3}  "
                f"{d['full_name']:<28}  "
                f"loyalty_tier={d['loyalty_tier']}"
            )

        # ── T1: incremental extract -> stage -> warehouse ─────────────────
        print("\n  T1 — incremental dlt extract + sqldim load …")
        result_t1 = _load_rows_into_warehouse(bridge, staging_dir + "/t1", warehouse_db, tracked)
        print(
            f"    sqldim T1  →  inserted={result_t1.inserted}, "
            f"versioned={result_t1.versioned}, unchanged={result_t1.unchanged}"
        )

        # ── Report ────────────────────────────────────────────────────────
        print("\n  Warehouse dim_customer:")
        wh = duckdb.connect(warehouse_db)
        total, current, hist = wh.execute(
            "SELECT COUNT(*), SUM(is_current::INT), SUM((NOT is_current)::INT) "
            "FROM dim_customer"
        ).fetchone()
        print(f"    total={total}  current={current}  historical={hist}")

        print("    SCD2 history for upgraded customers (IDs 1-3):")
        history_rows = wh.execute(
            "SELECT customer_id, full_name, loyalty_tier, valid_from, is_current "
            "FROM dim_customer WHERE customer_id <= 3 "
            "ORDER BY customer_id, valid_from"
        ).fetchall()
        for r in history_rows:
            flag = "✓" if r[4] else "H"
            print(
                f"      [{flag}] id={r[0]}  {r[1]:<28}  "
                f"tier={r[2]:<10}  valid_from={r[3]}"
            )

        wh.execute("DROP TABLE IF EXISTS dim_customer")
        wh.close()

    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
        if os.path.exists(warehouse_db):
            os.unlink(warehouse_db)

    print("\n  Done — Flask + dlt + sqldim pipeline complete.")


if __name__ == "__main__":  # pragma: no cover
    run_flask_dlt_example()
