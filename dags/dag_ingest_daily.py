"""
dag_ingest_daily.py
────────────────────
Daily ingestion DAG: OLTP → validate → staging (OLAP).
Schedule: every day at 01:00 AM (data from previous day).
"""

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "de_team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Connection helpers ─────────────────────────────────────────────────────────

def _get_oltp_params():
    return dict(
        host=os.environ["OLTP_HOST"],
        port=int(os.environ.get("OLTP_PORT", 5432)),
        dbname=os.environ["OLTP_DB"],
        user=os.environ["OLTP_USER"],
        password=os.environ["OLTP_PASSWORD"],
    )

def _get_olap_params():
    return dict(
        host=os.environ["OLAP_HOST"],
        port=int(os.environ.get("OLAP_PORT", 8123)),
        dbname=os.environ.get("OLAP_STAGING_DB", "staging"),
        user=os.environ["OLAP_USER"],
        password=os.environ["OLAP_PASSWORD"],
    )


# ── Task functions ─────────────────────────────────────────────────────────────

def extract(**context):
    """Extract data from OLTP. Save to XCom as parquet paths."""
    import sys, os, tempfile
    import pandas as pd
    from datetime import date, datetime

    sys.path.insert(0, "/opt/airflow")
    from ingestion.extractor import PostgresExtractor, ExtractionOrchestrator

    execution_date = context["logical_date"].date()
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    if conf.get("full_load"):
        execution_date = date(1970, 1, 1)
    elif conf.get("execution_date"):
        try:
            execution_date = datetime.fromisoformat(conf["execution_date"]).date()
        except ValueError:
            execution_date = date.fromisoformat(conf["execution_date"])

    with PostgresExtractor(**_get_oltp_params()) as extractor:
        orchestrator = ExtractionOrchestrator(extractor)
        data = orchestrator.run_daily(execution_date)

    # Save each DataFrame to a temp parquet file (avoid XCom size limits)
    tmpdir = tempfile.mkdtemp(prefix="de_ingest_")
    paths = {}
    for table, df in data.items():
        if df is not None and len(df) > 0:
            path = os.path.join(tmpdir, f"{table}.parquet")
            df.to_parquet(path, index=False)
            paths[table] = path

    context["ti"].xcom_push(key="parquet_paths", value=paths)
    print(f"✅ Extracted {sum(len(d) for d in data.values()):,} total rows")


def validate(**context):
    """Validate extracted data and push cleaned paths back."""
    import sys, os, tempfile
    import pandas as pd

    sys.path.insert(0, "/opt/airflow")
    from ingestion.validator import PipelineValidator

    paths: dict = context["ti"].xcom_pull(key="parquet_paths", task_ids="extract")
    data = {table: pd.read_parquet(path) for table, path in paths.items()}

    validator = PipelineValidator()
    cleaned = validator.validate_all(data)

    tmpdir = tempfile.mkdtemp(prefix="de_validated_")
    clean_paths = {}
    for table, df in cleaned.items():
        path = os.path.join(tmpdir, f"{table}.parquet")
        df.to_parquet(path, index=False)
        clean_paths[table] = path

    context["ti"].xcom_push(key="clean_paths", value=clean_paths)
    print(f"✅ Validation complete — {len(clean_paths)} tables passed")


def load_to_staging(**context):
    """Load validated DataFrames to staging schema in OLAP DB."""
    import sys
    import pandas as pd

    sys.path.insert(0, "/opt/airflow")
    from ingestion.loader import StagingLoader

    clean_paths: dict = context["ti"].xcom_pull(key="clean_paths", task_ids="validate")
    data = {table: pd.read_parquet(path) for table, path in clean_paths.items()}

    olap = _get_olap_params()
    loader = StagingLoader(
        host=olap["host"], port=olap["port"],
        dbname=olap["dbname"], user=olap["user"], password=olap["password"]
    )
    execution_date = str(context["logical_date"].date())
    loader.load_all(data, execution_date=execution_date)
    print("✅ Staging load complete")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_ingest_daily",
    description="Daily extraction from OLTP → validate → staging",
    schedule="0 1 * * *",         # 01:00 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "etl"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t_load = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    t_extract >> t_validate >> t_load
