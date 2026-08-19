from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/workspaces/gse-fleet-data-platform"
PYTHON_BIN = "/home/codespace/.python/current/bin/python3"

with DAG(
    dag_id="gse_batch_pipeline",
    description="Generates and ingests synthetic technician logs and scheduling data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gse", "portfolio", "batch"],
) as dag:

    generate_batch_data = BashOperator(
        task_id="generate_batch_data",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} producer/batch_data_generator.py",
    )

    soda_batch_check = BashOperator(
        task_id="soda_batch_check",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} spark_jobs/soda_batch_check.py",
    )

    generate_batch_data >> soda_batch_check
