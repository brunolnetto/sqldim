"""sqldim exception hierarchy.

All sqldim errors inherit from :class:`SqldimError`, organised into
four groups: schema-definition errors, SCD runtime errors, data-load
errors, and migration errors.
"""

from typing import Any


class SqldimError(Exception):
    """Base exception for all sqldim errors."""


# Schema errors (raised at definition / introspection time)
class SchemaError(SqldimError):
    """Raised when a model definition violates dimensional modeling rules."""


class GrainViolationError(SchemaError):
    """Raised when a FactModel is missing a __grain__ declaration."""


class NaturalKeyError(SchemaError):
    """Raised when a DimensionModel has a missing or ambiguous natural key."""


# SCD errors (raised at load time)
class SCDError(SqldimError):
    """Base for SCD-related errors."""


class DestructiveOperationError(SCDError):
    """Raised when a destructive SCD operation is attempted without explicit opt-in."""


# Load errors (raised during ETL)
class LoadError(SqldimError):
    """Base for loading errors."""


class SKResolutionError(LoadError):
    """Raised when a natural key cannot be resolved to a surrogate key in the dimension."""


class IdempotencyError(LoadError):
    """Raised when a duplicate record is inserted without an idempotency strategy."""


# Migration errors
class MigrationError(SqldimError):
    """Base for migration errors."""


class DestructiveMigrationError(MigrationError):
    """Raised when a migration would cause data loss without SQLDIM_ALLOW_DESTRUCTIVE=true."""


# Semantic query errors
class SemanticError(SqldimError):
    """Raised when a query violates dimensional semantics (e.g. summing a non-additive measure)."""


class InvalidJoinError(SemanticError):
    """Raised when no FK path exists between a fact and a requested dimension."""


class GrainCompatibilityError(SemanticError):
    """Raised when fact/edge tables with incompatible grain declarations are joined."""


# Narwhals transform errors
class TransformError(LoadError):
    """Base for transform pipeline errors."""


class TransformTypeError(TransformError):
    """Raised when a transform produces a dtype incompatible with the target model column."""

    def __init__(self, column: str, expected: Any, got: Any, hint: str = ""):
        self.column = column
        self.expected = expected
        self.got = got
        self.hint = hint
        msg = f"Column '{column}': expected {expected}, got {got}"
        if hint:
            msg += f". Hint: {hint}"
        super().__init__(msg)
