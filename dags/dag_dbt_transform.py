"""
dag_dbt_transform.py
─────────────────────
Runs dbt models after staging is complete.
Schedule: daily at 02:00 AM (after ingestion DAG).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

DEFAULT_ARGS = {
    "owner":            "de_team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR     = "/opt/airflow/dbt"
DBT_PROFILE = "--profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

with DAG(
    dag_id="dag_dbt_transform",
    description="Run dbt models: staging → marts → reports",
    schedule="0 2 * * *",          # 02:00 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "transform"],
) as dag:

    # Wait for the ingestion DAG to finish first
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="dag_ingest_daily",
        external_task_id="load_to_staging",
        timeout=3600,
        poke_interval=60,
        mode="poke",
    )

    # dbt debug — confirms connection is healthy
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && dbt debug {DBT_PROFILE} || true",
    )

    # Run staging models
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging {DBT_PROFILE}",
    )

    # Run mart models (dimensions + fact)
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts {DBT_PROFILE}",
    )

    # Run report models
    dbt_reports = BashOperator(
        task_id="dbt_run_reports",
        bash_command=f"cd {DBT_DIR} && dbt run --select reports {DBT_PROFILE}",
    )

    # Test all models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test {DBT_PROFILE}",
    )

    # Generate docs (optional but impressive for CV)
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate {DBT_PROFILE}",
    )

    wait_for_ingestion >> dbt_debug >> dbt_staging >> dbt_marts >> dbt_reports >> dbt_test >> dbt_docs
