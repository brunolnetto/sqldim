import os
from functools import wraps
from typing import Callable, Any
from sqldim.exceptions import DestructiveMigrationError


def destructive_operation(fn: Callable) -> Callable:
    """
    Decorator that guards destructive migration operations.
    Raises DestructiveMigrationError unless SQLDIM_ALLOW_DESTRUCTIVE=true.
    """

    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        allow = os.environ.get("SQLDIM_ALLOW_DESTRUCTIVE", "false").lower() == "true"
        if not allow:
            raise DestructiveMigrationError(
                f"Operation '{fn.__name__}' is destructive and may cause data loss. "
                "Set SQLDIM_ALLOW_DESTRUCTIVE=true to proceed."
            )
        return fn(*args, **kwargs)

    return wrapper


@destructive_operation
def drop_scd2_history(table: str) -> str:
    """
    Collapses all SCD2 versions into a single row per natural key.
    WARNING: Permanently destroys historical data.
    """
    return f"DELETE FROM {table} WHERE is_current = FALSE"


@destructive_operation
def drop_dimension_table(table: str) -> str:
    """Drop an entire dimension table. WARNING: Destroys all historical records."""
    return f"DROP TABLE {table}"
