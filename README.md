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

## How to Run This Project

This project was developed and run inside a **GitHub Codespace** (a cloud dev environment tied to this repo). To reproduce it:

### 1. Open the Codespace
- On this repo's GitHub page, click **Code → Codespaces → Create codespace on main** (or reopen an existing one)

### 2. Start the streaming infrastructure
```bash
./start.sh
```
This starts Kafka, Zookeeper, and MinIO via Docker Compose.

### 3. Set up Airflow (first time only)
```bash
mkdir -p ~/airflow
cd ~/airflow
python3 -m venv airflow_venv
source airflow_venv/bin/activate
pip install apache-airflow==3.1.0
airflow db migrate
```

### 4. Start Airflow (every session)
```bash
cd ~/airflow
source airflow_venv/bin/activate
export AIRFLOW__API__BASE_URL="<your Codespace's forwarded URL for port 8080>"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:8080/execution/"
airflow standalone
```
Note: the two environment variables above are required — without them, Airflow's login will fail (URL mismatch) and background tasks can get stuck (internal traffic routed incorrectly). The standalone command prints an auto-generated admin password on first run; save it.

### 5. Open the Airflow UI
- In the Codespace's **Ports** tab, open port **8080** in your browser
- Log in with the printed admin credentials
- Both `gse_bronze_pipeline` (hourly) and `gse_batch_pipeline` (daily) should appear and can be triggered manually or left to run on schedule

### Notes
- Docker containers and the Airflow process stop when the Codespace goes idle — repeat steps 2 and 4 each new session (step 3 is one-time only)
- If Kafka fails to restart with a ZooKeeper error, run `docker-compose restart zookeeper kafka` together
