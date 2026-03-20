"""
sqldim/examples/features/utils.py
===================================
Shared helpers for all feature showcase scripts.

Re-exports the common utilities from :mod:`sqldim.examples.utils` so that
feature showcases import from their own subfolder rather than jumping two
package levels up.

Usage::

    from sqldim.examples.features.utils import make_tmp_db, section, banner
"""

from sqldim.examples.utils import (  # noqa: F401
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

__all__ = [
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
