from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


# ============================================================
# Local ETL project configuration
# ============================================================

PROJECT_DIR = (
    "/mnt/c/Users/juang/OneDrive/Documentos/Portfolio/"
    "AdventureWorks2025/retail-cloud-etl"
)

RUN_PIPELINE_SCRIPT = f"{PROJECT_DIR}/run_pipeline.py"

# IMPORTANT:
# Use the ETL project's Python environment, not Airflow's environment.
PYTHON_EXECUTABLE = f"{PROJECT_DIR}/venv/bin/python"


# ============================================================
# Databricks configuration
# ============================================================

DATABRICKS_CONNECTION_ID = "databricks_default"

BRONZE_JOB_ID = 742301753067439
SILVER_JOB_ID = 460598946212978
GOLD_JOB_ID = 817310634167046
SQL_ANALYTICS_JOB_ID = 197294479432480


# ============================================================
# Default task configuration
# ============================================================

default_args = {
    "owner": "juan",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="adventureworks_full_pipeline",
    description=(
        "AdventureWorks end-to-end ETL pipeline: "
        "AWS S3 ingestion, Databricks Volume upload, "
        "Bronze, Silver, Gold, and SQL Analytics"
    ),
    start_date=pendulum.datetime(
        2026,
        7,
        21,
        tz="America/Bogota",
    ),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=[
        "adventureworks",
        "aws",
        "s3",
        "databricks",
        "etl",
    ],
) as dag:

    # ========================================================
    # Task 1:
    # 1. Connect to AWS S3
    # 2. Validate required CSV files
    # 3. Download files locally
    # 4. Read metadata
    # 5. Upload files to the Databricks Volume
    # ========================================================

    run_ingestion_pipeline = BashOperator(
        task_id="run_ingestion_pipeline",
        cwd=PROJECT_DIR,
        bash_command=(
            "set -euo pipefail\n"
            f'echo "Using ETL Python: {PYTHON_EXECUTABLE}"\n'
            f'"{PYTHON_EXECUTABLE}" -u "{RUN_PIPELINE_SCRIPT}"'
        ),
        env={
            "PYTHONUNBUFFERED": "1",
            "DATABRICKS_HOST": (
                "{{ conn.databricks_default.host }}"
            ),
            "DATABRICKS_TOKEN": (
                "{{ conn.databricks_default.password }}"
            ),
        },
        append_env=True,
    )

    # ========================================================
    # Task 2: Databricks Bronze layer
    # ========================================================

    run_bronze_job = DatabricksRunNowOperator(
        task_id="run_bronze_job",
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        job_id=BRONZE_JOB_ID,
        polling_period_seconds=30,
        wait_for_termination=True,
        deferrable=False,
    )

    # ========================================================
    # Task 3: Databricks Silver layer
    # ========================================================

    run_silver_job = DatabricksRunNowOperator(
        task_id="run_silver_job",
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        job_id=SILVER_JOB_ID,
        polling_period_seconds=30,
        wait_for_termination=True,
        deferrable=False,
    )

    # ========================================================
    # Task 4: Databricks Gold layer
    # ========================================================

    run_gold_job = DatabricksRunNowOperator(
        task_id="run_gold_job",
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        job_id=GOLD_JOB_ID,
        polling_period_seconds=30,
        wait_for_termination=True,
        deferrable=False,
    )

    # ========================================================
    # Task 5: Databricks SQL Analytics layer
    # ========================================================

    run_sql_analytics_job = DatabricksRunNowOperator(
        task_id="run_sql_analytics_job",
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        job_id=SQL_ANALYTICS_JOB_ID,
        polling_period_seconds=30,
        wait_for_termination=True,
        deferrable=False,
    )

    # ========================================================
    # Pipeline execution order
    # ========================================================

    (
        run_ingestion_pipeline
        >> run_bronze_job
        >> run_silver_job
        >> run_gold_job
        >> run_sql_analytics_job
    )
