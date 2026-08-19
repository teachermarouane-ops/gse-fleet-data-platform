# gse-fleet-data-platform# GSE Fleet Data Platform

An end-to-end data platform demonstrating Data Product and Data Quality Engineering principles, built around airport Ground Support Equipment (GSE) maintenance data.

## Problem

GSE fleet data (tow tractors, belt loaders, GPUs) comes from multiple disconnected sources:
- Real-time sensor/telemetry feeds (engine hours, faults, temperature)
- Daily technician maintenance logs
- Batch scheduling exports

Without a governed data platform, this leads to concrete failures:
1. **Silent staleness** — a sensor feed stops updating and dashboards keep showing old data as if it's current, leading to missed maintenance decisions.
2. **Silent data loss** — inconsistent equipment ID formats across systems cause records to drop during joins, quietly corrupting reliability metrics like mean-time-between-failures.
3. **Breaking changes with no warning** — an upstream schema change (e.g. a renamed column) breaks downstream dashboards without anyone knowing until a manager notices a broken chart.

## What this platform does

- **Data Contracts**: every data source declares its schema, freshness SLA, and ownership up front. Breaking the contract fails the pipeline loudly instead of silently.
- **Data Quality Gates**: automated validation (Soda) at each processing layer — catches format mismatches, missing values, and freshness violations immediately.
- **Observability**: *(planned)* a dashboard to track pipeline health, freshness, and quality-check results over time — not yet built.
- **CI/CD**: schema/quality checks run automatically on every pipeline change, catching breaking changes before they reach production.

## Architecture

- **Ingestion**: Kafka (streaming telemetry) + Airflow (batch logs/scheduling)
- **Storage**: Medallion architecture (bronze/silver/gold) on MinIO (S3-compatible)
- **Processing**: PySpark transformations
- **Quality**: Soda Core checks per layer
- **Governance**: data contracts (YAML), basic lineage documentation
- **Orchestration**: Apache Airflow
- **CI/CD**: GitHub Actions
- **Containerization**: Docker Compose

## Status

This platform is functionally complete for its core data engineering scope:

- ✅ **Streaming ingestion** — Kafka → MinIO (bronze), running end-to-end
- ✅ **Batch ingestion** — synthetic technician logs and scheduling data, on an independent daily pipeline
- ✅ **PySpark processing** — bronze → silver validation (ID patterns, required fields, ranges)
- ✅ **Data quality gates (Soda Core)** — schema checks, missing-field validation, pattern matching, and freshness checks, enforced on both the streaming and batch datasets
- ✅ **Orchestration (Apache Airflow)** — two scheduled DAGs: `gse_bronze_pipeline` (hourly, streaming) and `gse_batch_pipeline` (daily, batch), each validating data quality before completing
- ✅ **CI/CD (GitHub Actions)** — the data contract check runs automatically on every push, against a live MinIO instance spun up in CI

**Not yet built:** a gold layer and an observability dashboard for pipeline health/quality history over time. This is the one piece of the original architecture still planned but not implemented.

## Author

Marouane — Maintenance & Operations Manager  ( Data Scientist & Data Engineer ).
