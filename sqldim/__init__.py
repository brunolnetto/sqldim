"""sqldim — SQL-first dimensional modelling toolkit for Python.

Provides Kimball-style dimension and fact models, SCD handlers (type 1/2/3/6),
narwhals-backed vectorised loaders, a fluent query builder, and storage sinks
for DuckDB, PostgreSQL, Parquet, Delta Lake, MotherDuck, and Iceberg.

Quick start:

    from sqldim import DimensionModel, SCD2Mixin, DimensionalLoader, DuckDBSink
All public names are re-exported from this module so callers never need to
import from sub-packages directly.  See the sub-package docstrings for full
API documentation of each component group."""

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
from sqldim.core.graph.schema_graph import SchemaGraph as GraphSchemaGraph, GraphSchema
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
from sqldim.core.loaders.snapshot import LazyTransactionLoader, LazySnapshotLoader
from sqldim.core.loaders.accumulating import LazyAccumulatingLoader
from sqldim.core.loaders.cumulative import LazyCumulativeLoader
from sqldim.core.loaders.bitmask import LazyBitmaskLoader
from sqldim.core.loaders.array_metric import LazyArrayMetricLoader
from sqldim.core.loaders.edge_projection import (
    LazyEdgeProjectionLoader,
)
from sqldim.exceptions import TransformTypeError
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler, SCDResult
from sqldim.core.loaders.dimensional import DimensionalLoader, SKResolver
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
