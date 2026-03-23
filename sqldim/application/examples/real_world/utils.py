"""
sqldim/examples/real_world/utils.py
=====================================
Shared helpers for all real-world showcase scripts.

Re-exports the common utilities from :mod:`sqldim.application.examples.utils` so that
real-world showcases import from their own subfolder rather than jumping
two package levels up.

Usage::

    from sqldim.application.examples.real_world.utils import tmp_db, banner
"""

from sqldim.application.examples.utils import (  # noqa: F401  # pragma: no cover
    tmp_db,
    make_tmp_db,
    setup_dim,
    teardown_dim,
    section,
    banner,
    print_rows,
    show_provider,
    model_ddl,
)

__all__ = [  # pragma: no cover
    "tmp_db",
    "make_tmp_db",
    "setup_dim",
    "teardown_dim",
    "section",
    "banner",
    "print_rows",
    "show_provider",
    "model_ddl",
]
