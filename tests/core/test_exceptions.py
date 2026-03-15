import pytest
from sqldim.exceptions import (
    SqldimError, SchemaError, GrainViolationError, NaturalKeyError,
    SCDError, DestructiveOperationError,
    LoadError, SKResolutionError, IdempotencyError,
    MigrationError, DestructiveMigrationError,
    SemanticError, InvalidJoinError,
)

def test_hierarchy():
    assert issubclass(SchemaError, SqldimError)
    assert issubclass(GrainViolationError, SchemaError)
    assert issubclass(NaturalKeyError, SchemaError)
    assert issubclass(SCDError, SqldimError)
    assert issubclass(DestructiveOperationError, SCDError)
    assert issubclass(LoadError, SqldimError)
    assert issubclass(SKResolutionError, LoadError)
    assert issubclass(IdempotencyError, LoadError)
    assert issubclass(MigrationError, SqldimError)
    assert issubclass(DestructiveMigrationError, MigrationError)
    assert issubclass(SemanticError, SqldimError)
    assert issubclass(InvalidJoinError, SemanticError)

def test_raise_and_catch():
    with pytest.raises(SqldimError):
        raise SKResolutionError("Natural key 'X' not found in CustomerDimension")

    with pytest.raises(LoadError):
        raise IdempotencyError("Duplicate record detected")

    with pytest.raises(SemanticError):
        raise InvalidJoinError("No FK path from SalesFact to RegionDim")
