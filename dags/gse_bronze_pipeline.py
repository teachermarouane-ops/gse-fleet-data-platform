from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/workspaces/gse-fleet-data-platform"
PYTHON_BIN = "/home/codespace/.python/current/bin/python3"

with DAG(
    dag_id="gse_bronze_pipeline",
    description="Bronze to silver processing, then Soda data contract check",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["gse", "portfolio"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} spark_jobs/bronze_to_silver.py",
    )

    soda_check = BashOperator(
        task_id="soda_bronze_check",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} spark_jobs/soda_bronze_check.py",
    )

    bronze_to_silver >> soda_check
