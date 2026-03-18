# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-03-17

### Added
- Core Kimball models: `DimensionModel`, `FactModel`, `BridgeModel`
- SCD Type 1, 2, 3, and 6 processors with vectorized change detection
- `SCD2Mixin`, `SCD3Mixin`, `DatelistMixin`, `CumulativeMixin` mixins
- Fact types: `TransactionFact`, `PeriodicSnapshotFact`, `AccumulatingFact`, `CumulativeFact`, `ActivityFact`
- `DimensionalLoader` with topological dependency resolution and `SKResolver`
- Specialized loaders: `CumulativeLoader`, `BitmaskerLoader`, `ArrayMetricLoader`, `EdgeProjectionLoader`
- Narwhals-backed vectorized processing: `NarwhalsAdapter`, `NarwhalsSCDProcessor`, `TransformPipeline`
- Dual-paradigm graph extension: `VertexModel`, `EdgeModel`, `GraphModel`, `TraversalEngine`
- Sinks: DuckDB, PostgreSQL, MotherDuck, Parquet, Apache Iceberg, Delta Lake
- Sources: CSV, Parquet, Kafka
- Medallion architecture: `Layer`, `MedallionRegistry`, `SilverBuildOrder`
- Data contracts: schema enforcement, SLA checks, freshness rules
- `AsyncDimensionalSession` for async workflow support
- `SqldimConfig` with environment variable support (`SQLDIM_` prefix)
- Alembic-based schema migrations via CLI
- CLI: `schema graph`, `migrations generate`, `example run`
