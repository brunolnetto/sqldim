"""Shared fixtures for tests/sinks.

Handles cleanup of stale pytest-postgresql processes so that interrupted or
crashed test runs don't leave a zombie postgres on PG_PORT that causes
``mirakuru.exceptions.AlreadyRunning`` on the next ``pytest`` invocation.
"""

from __future__ import annotations

import signal
import socket

import pytest


# Must match the port declared in test_postgresql_sink.py
_PG_PORT = 15437


def _pid_on_port(port: int) -> int | None:
    """Return the PID of the process listening on *port*, or ``None``."""
    try:
        import psutil
    except ImportError:
        return None

    for conn in psutil.net_connections(kind="inet"):
        if conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
            return conn.pid
    return None


def _port_in_use(port: int) -> bool:
    """Fast check: can we bind to *port*?"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return False
        except OSError:
            return True


@pytest.fixture(scope="session", autouse=True)
def _kill_stale_pg_on_port() -> None:  # type: ignore[return]
    """Kill any leftover postgres process holding PG_PORT before tests start.

    pytest-postgresql raises ``AlreadyRunning`` when it finds the port already
    occupied (e.g. from an interrupted prior run).  This fixture clears it
    pre-emptively so subsequent runs are self-healing.
    """
    if not _port_in_use(_PG_PORT):
        return

    pid = _pid_on_port(_PG_PORT)
    if pid is None:
        # psutil not available — fall back to a best-effort socket approach;
        # nothing to kill, the port check alone was enough to warn us.
        return

    try:
        import psutil

        proc = psutil.Process(pid)
        name = proc.name()
        # Only touch postgres-related processes to avoid accidental kills.
        if "postgres" not in name.lower():
            return
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:  # noqa: BLE001
        pass
