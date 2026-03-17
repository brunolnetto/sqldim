"""sqldim — SQL-first dimensional modelling toolkit for Python.

Provides Kimball-style dimension and fact models, SCD handlers (type 1/2/3/6),
narwhals-backed vectorised loaders, a fluent query builder, and storage sinks
for DuckDB, PostgreSQL, Parquet, Delta Lake, MotherDuck, and Iceberg.

Quick start:

    from sqldim import DimensionModel, SCD2Mixin, DimensionalLoader, DuckDBSink
All public names are re-exported from this module so callers never need to
import from sub-packages directly.  See the sub-package docstrings for full
API documentation of each component group."""
from sqldim.core.fields import Field
from sqldim.core.models import DimensionModel, FactModel, BridgeModel
from sqldim.core.mixins import SCD2Mixin, SCD3Mixin, DatelistMixin, CumulativeMixin
from sqldim.core.facts import TransactionFact, PeriodicSnapshotFact, AccumulatingFact, CumulativeFact, ActivityFact
from sqldim.scd.backfill import backfill_scd2
from sqldim.core.graph import SchemaGraph
from sqldim.graph.models import VertexModel, EdgeModel, Vertex
from sqldim.graph import GraphModel, TraversalEngine
from sqldim.graph.schema_graph import SchemaGraph as GraphSchemaGraph, GraphSchema
from sqldim.scd.processors.adapter import NarwhalsAdapter
from sqldim.scd.processors.transforms import col, TransformPipeline
from sqldim.scd.processors.backfill import backfill_scd2_narwhals
from sqldim.scd.processors.scd_engine import NarwhalsHashStrategy, NarwhalsSCDProcessor
from sqldim.loaders.cumulative import CumulativeLoader
from sqldim.loaders.bitmask import BitmaskerLoader
from sqldim.loaders.array_metric import ArrayMetricLoader
from sqldim.loaders.edge_projection import EdgeProjectionLoader
from sqldim.exceptions import TransformTypeError
from sqldim.scd.handler import SCDHandler, SCDResult
from sqldim.loaders.dimensional import DimensionalLoader, SKResolver
from sqldim.session import AsyncDimensionalSession
from sqldim.config import SqldimConfig
from sqldim.exceptions import (
    SqldimError,
    SchemaError, GrainViolationError, NaturalKeyError,
    SCDError, DestructiveOperationError,
    LoadError, SKResolutionError, IdempotencyError,
    MigrationError, DestructiveMigrationError,
    SemanticError, InvalidJoinError,
)
from sqldim.medallion import Layer, MedallionRegistry, ModelKind, SilverBuildOrder

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
    "CumulativeLoader",
    "BitmaskerLoader",
    "ArrayMetricLoader",
    "EdgeProjectionLoader",
    "TransformTypeError",
    # SCD
    "SCDHandler",
    "SCDResult",
    # Loaders
    "DimensionalLoader",
    "SKResolver",
    # Session
    "AsyncDimensionalSession",
    # Config
    "SqldimConfig",
    # Exceptions
    "SqldimError",
    "SchemaError", "GrainViolationError", "NaturalKeyError",
    "SCDError", "DestructiveOperationError",
    "LoadError", "SKResolutionError", "IdempotencyError",
    "MigrationError", "DestructiveMigrationError",
    "SemanticError", "InvalidJoinError",
    # Medallion
    "Layer",
    "MedallionRegistry",
    "ModelKind",
    "SilverBuildOrder",
]
