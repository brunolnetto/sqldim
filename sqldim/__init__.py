"""sqldim — SQL-first dimensional modelling toolkit for Python.

Provides Kimball-style dimension and fact models, SCD handlers (type 1/2/3/6),
narwhals-backed vectorised loaders, a fluent query builder, and storage sinks
for DuckDB, PostgreSQL, Parquet, Delta Lake, MotherDuck, and Iceberg.

Quick start:

    from sqldim import DimensionModel, SCD2Mixin, DimensionalLoader, DuckDBSink
All public names are re-exported from this module so callers never need to
import from sub-packages directly.  See the sub-package docstrings for full
API documentation of each component group."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Static analysis only — these are resolved lazily at runtime.
    from sqldim.core.kimball.fields import Field
    from sqldim.core.kimball.models import DimensionModel, FactModel, BridgeModel
    from sqldim.core.kimball.mixins import (
        SCD2Mixin,
        SCD3Mixin,
        DatelistMixin,
        CumulativeMixin,
    )
    from sqldim.core.kimball.facts import (
        TransactionFact,
        PeriodicSnapshotFact,
        AccumulatingFact,
        CumulativeFact,
        ActivityFact,
    )
    from sqldim.core.kimball.dimensions.scd.backfill import backfill_scd2
    from sqldim.core.kimball.schema_graph import SchemaGraph
    from sqldim.core.graph.models import VertexModel, EdgeModel, Vertex
    from sqldim.core.graph import GraphModel, TraversalEngine
    from sqldim.core.graph.schema_graph import (
        SchemaGraph as GraphSchemaGraph,
        GraphSchema,
    )
    from sqldim.core.kimball.dimensions.scd.processors.adapter import NarwhalsAdapter
    from sqldim.core.kimball.dimensions.scd.processors.transforms import (
        col,
        TransformPipeline,
    )
    from sqldim.core.kimball.dimensions.scd.processors.backfill import (
        backfill_scd2_narwhals,
    )
    from sqldim.core.kimball.dimensions.scd.processors.scd_engine import (
        NarwhalsHashStrategy,
        NarwhalsSCDProcessor,
    )
    from sqldim.core.loaders.fact.snapshot import (
        LazyTransactionLoader,
        LazySnapshotLoader,
    )
    from sqldim.core.loaders.fact.accumulating import LazyAccumulatingLoader
    from sqldim.core.loaders.fact.cumulative import LazyCumulativeLoader
    from sqldim.core.loaders.dimension.bitmask import LazyBitmaskLoader
    from sqldim.core.loaders.dimension.array_metric import LazyArrayMetricLoader
    from sqldim.core.loaders.dimension.edge_projection import LazyEdgeProjectionLoader
    from sqldim.exceptions import TransformTypeError
    from sqldim.core.kimball.dimensions.scd.handler import SCDHandler, SCDResult
    from sqldim.core.loaders.dimension.dimensional import DimensionalLoader, SKResolver
    from sqldim.core.kimball.schema import DWSchema
    from sqldim.sinks import (
        SinkAdapter,
        DuckDBSink,
        PostgreSQLSink,
        ParquetSink,
        DeltaLakeSink,
        MotherDuckSink,
        IcebergSink,
    )
    from sqldim.sources import (
        SourceAdapter,
        coerce_source,
        ParquetSource,
        CSVSource,
        DuckDBSource,
        PostgreSQLSource,
        DeltaSource,
        SQLSource,
        StreamSourceAdapter,
        StreamResult,
        CSVStreamSource,
        ParquetStreamSource,
    )
    from sqldim.session import AsyncDimensionalSession
    from sqldim.config import SqldimConfig
    from sqldim.exceptions import (
        SqldimError,
        SchemaError,
        GrainViolationError,
        NaturalKeyError,
        SCDError,
        DestructiveOperationError,
        LoadError,
        SKResolutionError,
        IdempotencyError,
        MigrationError,
        DestructiveMigrationError,
        SemanticError,
        InvalidJoinError,
        GrainCompatibilityError,
    )
    from sqldim.medallion import Layer, MedallionRegistry, ModelKind, SilverBuildOrder
    from sqldim.core.query.dgm import (
        DGMQuery,
        PropRef,
        AggRef,
        WinRef,
        ScalarPred,
        PathPred,
        AND,
        OR,
        NOT,
        VerbHop,
        BridgeHop,
        Compose,
    )

# Maps each public name to (module_path, attr_in_module).
# When attr_in_module == name the second element can be omitted (use name).
_LAZY: dict[str, tuple[str, str]] = {
    # Core kimball
    "Field": ("sqldim.core.kimball.fields", "Field"),
    "DimensionModel": ("sqldim.core.kimball.models", "DimensionModel"),
    "FactModel": ("sqldim.core.kimball.models", "FactModel"),
    "BridgeModel": ("sqldim.core.kimball.models", "BridgeModel"),
    "SCD2Mixin": ("sqldim.core.kimball.mixins", "SCD2Mixin"),
    "SCD3Mixin": ("sqldim.core.kimball.mixins", "SCD3Mixin"),
    "DatelistMixin": ("sqldim.core.kimball.mixins", "DatelistMixin"),
    "CumulativeMixin": ("sqldim.core.kimball.mixins", "CumulativeMixin"),
    "TransactionFact": ("sqldim.core.kimball.facts", "TransactionFact"),
    "PeriodicSnapshotFact": ("sqldim.core.kimball.facts", "PeriodicSnapshotFact"),
    "AccumulatingFact": ("sqldim.core.kimball.facts", "AccumulatingFact"),
    "CumulativeFact": ("sqldim.core.kimball.facts", "CumulativeFact"),
    "ActivityFact": ("sqldim.core.kimball.facts", "ActivityFact"),
    "backfill_scd2": ("sqldim.core.kimball.dimensions.scd.backfill", "backfill_scd2"),
    "SchemaGraph": ("sqldim.core.kimball.schema_graph", "SchemaGraph"),
    # Graph extension
    "VertexModel": ("sqldim.core.graph.models", "VertexModel"),
    "EdgeModel": ("sqldim.core.graph.models", "EdgeModel"),
    "Vertex": ("sqldim.core.graph.models", "Vertex"),
    "GraphModel": ("sqldim.core.graph", "GraphModel"),
    "TraversalEngine": ("sqldim.core.graph", "TraversalEngine"),
    "GraphSchemaGraph": ("sqldim.core.graph.schema_graph", "SchemaGraph"),  # alias
    "GraphSchema": ("sqldim.core.graph.schema_graph", "GraphSchema"),
    # Narwhals integration
    "NarwhalsAdapter": (
        "sqldim.core.kimball.dimensions.scd.processors.adapter",
        "NarwhalsAdapter",
    ),
    "col": ("sqldim.core.kimball.dimensions.scd.processors.transforms", "col"),
    "TransformPipeline": (
        "sqldim.core.kimball.dimensions.scd.processors.transforms",
        "TransformPipeline",
    ),
    "backfill_scd2_narwhals": (
        "sqldim.core.kimball.dimensions.scd.processors.backfill",
        "backfill_scd2_narwhals",
    ),
    "NarwhalsHashStrategy": (
        "sqldim.core.kimball.dimensions.scd.processors.scd_engine",
        "NarwhalsHashStrategy",
    ),
    "NarwhalsSCDProcessor": (
        "sqldim.core.kimball.dimensions.scd.processors.scd_engine",
        "NarwhalsSCDProcessor",
    ),
    # Lazy loaders
    "LazyTransactionLoader": (
        "sqldim.core.loaders.fact.snapshot",
        "LazyTransactionLoader",
    ),
    "LazySnapshotLoader": ("sqldim.core.loaders.fact.snapshot", "LazySnapshotLoader"),
    "LazyAccumulatingLoader": (
        "sqldim.core.loaders.fact.accumulating",
        "LazyAccumulatingLoader",
    ),
    "LazyCumulativeLoader": (
        "sqldim.core.loaders.fact.cumulative",
        "LazyCumulativeLoader",
    ),
    "LazyBitmaskLoader": ("sqldim.core.loaders.dimension.bitmask", "LazyBitmaskLoader"),
    "LazyArrayMetricLoader": (
        "sqldim.core.loaders.dimension.array_metric",
        "LazyArrayMetricLoader",
    ),
    "LazyEdgeProjectionLoader": (
        "sqldim.core.loaders.dimension.edge_projection",
        "LazyEdgeProjectionLoader",
    ),
    # Exceptions — sqldim.exceptions is lightweight (no sqlmodel/sqlalchemy)
    "TransformTypeError": ("sqldim.exceptions", "TransformTypeError"),
    "SqldimError": ("sqldim.exceptions", "SqldimError"),
    "SchemaError": ("sqldim.exceptions", "SchemaError"),
    "GrainViolationError": ("sqldim.exceptions", "GrainViolationError"),
    "NaturalKeyError": ("sqldim.exceptions", "NaturalKeyError"),
    "SCDError": ("sqldim.exceptions", "SCDError"),
    "DestructiveOperationError": ("sqldim.exceptions", "DestructiveOperationError"),
    "LoadError": ("sqldim.exceptions", "LoadError"),
    "SKResolutionError": ("sqldim.exceptions", "SKResolutionError"),
    "IdempotencyError": ("sqldim.exceptions", "IdempotencyError"),
    "MigrationError": ("sqldim.exceptions", "MigrationError"),
    "DestructiveMigrationError": ("sqldim.exceptions", "DestructiveMigrationError"),
    "SemanticError": ("sqldim.exceptions", "SemanticError"),
    "InvalidJoinError": ("sqldim.exceptions", "InvalidJoinError"),
    "GrainCompatibilityError": ("sqldim.exceptions", "GrainCompatibilityError"),
    # SCD handler
    "SCDHandler": ("sqldim.core.kimball.dimensions.scd.handler", "SCDHandler"),
    "SCDResult": ("sqldim.core.kimball.dimensions.scd.handler", "SCDResult"),
    # Loaders
    "DimensionalLoader": (
        "sqldim.core.loaders.dimension.dimensional",
        "DimensionalLoader",
    ),
    "SKResolver": ("sqldim.core.loaders.dimension.dimensional", "SKResolver"),
    # DWSchema orchestrator
    "DWSchema": ("sqldim.core.kimball.schema", "DWSchema"),
    # Sinks
    "SinkAdapter": ("sqldim.sinks", "SinkAdapter"),
    "DuckDBSink": ("sqldim.sinks", "DuckDBSink"),
    "PostgreSQLSink": ("sqldim.sinks", "PostgreSQLSink"),
    "ParquetSink": ("sqldim.sinks", "ParquetSink"),
    "DeltaLakeSink": ("sqldim.sinks", "DeltaLakeSink"),
    "MotherDuckSink": ("sqldim.sinks", "MotherDuckSink"),
    "IcebergSink": ("sqldim.sinks", "IcebergSink"),
    # Sources
    "SourceAdapter": ("sqldim.sources", "SourceAdapter"),
    "coerce_source": ("sqldim.sources", "coerce_source"),
    "ParquetSource": ("sqldim.sources", "ParquetSource"),
    "CSVSource": ("sqldim.sources", "CSVSource"),
    "DuckDBSource": ("sqldim.sources", "DuckDBSource"),
    "PostgreSQLSource": ("sqldim.sources", "PostgreSQLSource"),
    "DeltaSource": ("sqldim.sources", "DeltaSource"),
    "SQLSource": ("sqldim.sources", "SQLSource"),
    "StreamSourceAdapter": ("sqldim.sources", "StreamSourceAdapter"),
    "StreamResult": ("sqldim.sources", "StreamResult"),
    "CSVStreamSource": ("sqldim.sources", "CSVStreamSource"),
    "ParquetStreamSource": ("sqldim.sources", "ParquetStreamSource"),
    # Session
    "AsyncDimensionalSession": ("sqldim.session", "AsyncDimensionalSession"),
    # Config
    "SqldimConfig": ("sqldim.config", "SqldimConfig"),
    # Medallion
    "Layer": ("sqldim.medallion", "Layer"),
    "MedallionRegistry": ("sqldim.medallion", "MedallionRegistry"),
    "ModelKind": ("sqldim.medallion", "ModelKind"),
    "SilverBuildOrder": ("sqldim.medallion", "SilverBuildOrder"),
    # DGM query algebra
    "DGMQuery": ("sqldim.core.query.dgm", "DGMQuery"),
    "PropRef": ("sqldim.core.query.dgm", "PropRef"),
    "AggRef": ("sqldim.core.query.dgm", "AggRef"),
    "WinRef": ("sqldim.core.query.dgm", "WinRef"),
    "ScalarPred": ("sqldim.core.query.dgm", "ScalarPred"),
    "PathPred": ("sqldim.core.query.dgm", "PathPred"),
    "AND": ("sqldim.core.query.dgm", "AND"),
    "OR": ("sqldim.core.query.dgm", "OR"),
    "NOT": ("sqldim.core.query.dgm", "NOT"),
    "VerbHop": ("sqldim.core.query.dgm", "VerbHop"),
    "BridgeHop": ("sqldim.core.query.dgm", "BridgeHop"),
    "Compose": ("sqldim.core.query.dgm", "Compose"),
}


def __getattr__(name: str) -> object:
    """Lazy-load public names on first access (PEP 562)."""
    if name in _LAZY:
        module_path, attr = _LAZY[name]
        mod = importlib.import_module(module_path)
        value = getattr(mod, attr)
        globals()[name] = value  # cache so subsequent accesses skip __getattr__
        return value
    raise AttributeError(f"module 'sqldim' has no attribute {name!r}")


__all__ = [
    # Core schema
    "Field",
    "DimensionModel",
    "FactModel",
    "BridgeModel",
    "TransactionFact",
    "PeriodicSnapshotFact",
    "AccumulatingFact",
    "CumulativeFact",
    "ActivityFact",
    "DatelistMixin",
    "CumulativeMixin",
    "backfill_scd2",
    "SCD2Mixin",
    "SCD3Mixin",
    "SchemaGraph",
    # Graph extension
    "VertexModel",
    "EdgeModel",
    "Vertex",
    "GraphModel",
    "TraversalEngine",
    "GraphSchemaGraph",
    "GraphSchema",
    # Narwhals integration
    "NarwhalsAdapter",
    "col",
    "TransformPipeline",
    "backfill_scd2_narwhals",
    "NarwhalsHashStrategy",
    "NarwhalsSCDProcessor",
    "TransformTypeError",
    # Lazy loaders
    "LazyTransactionLoader",
    "LazySnapshotLoader",
    "LazyAccumulatingLoader",
    "LazyCumulativeLoader",
    "LazyBitmaskLoader",
    "LazyArrayMetricLoader",
    "LazyEdgeProjectionLoader",
    # SCD
    "SCDHandler",
    "SCDResult",
    # Loaders
    "DimensionalLoader",
    "SKResolver",
    # DWSchema orchestrator
    "DWSchema",
    # Sinks
    "SinkAdapter",
    "DuckDBSink",
    "PostgreSQLSink",
    "ParquetSink",
    "DeltaLakeSink",
    "MotherDuckSink",
    "IcebergSink",
    # Sources
    "SourceAdapter",
    "coerce_source",
    "ParquetSource",
    "CSVSource",
    "DuckDBSource",
    "PostgreSQLSource",
    "DeltaSource",
    "SQLSource",
    "StreamSourceAdapter",
    "StreamResult",
    "CSVStreamSource",
    "ParquetStreamSource",
    # Session
    "AsyncDimensionalSession",
    # Config
    "SqldimConfig",
    # Exceptions
    "SqldimError",
    "SchemaError",
    "GrainViolationError",
    "NaturalKeyError",
    "SCDError",
    "DestructiveOperationError",
    "LoadError",
    "SKResolutionError",
    "IdempotencyError",
    "MigrationError",
    "DestructiveMigrationError",
    "SemanticError",
    "InvalidJoinError",
    # Exceptions (public API)
    "GrainCompatibilityError",
    # Medallion
    "Layer",
    "MedallionRegistry",
    "ModelKind",
    "SilverBuildOrder",
    # DGM query algebra
    "DGMQuery",
    "PropRef",
    "AggRef",
    "WinRef",
    "ScalarPred",
    "PathPred",
    "AND",
    "OR",
    "NOT",
    "VerbHop",
    "BridgeHop",
    "Compose",
]
