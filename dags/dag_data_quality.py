"""
dag_data_quality.py
────────────────────
Daily data quality checks on the analytics schema.
Runs after dbt transform. Logs results to Airflow logs.
Schedule: daily at 03:00 AM.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

DEFAULT_ARGS = {
    "owner":            "de_team",
    "depends_on_past":  False,
    "retries":          0,
    "email_on_failure": False,
}


def _get_olap_client():
    import clickhouse_connect
    h  = os.environ["OLAP_HOST"]
    po = int(os.environ.get("OLAP_PORT", 8123))
    u  = os.environ["OLAP_USER"]
    pw = os.environ["OLAP_PASSWORD"]
    db = os.environ.get("OLAP_ANALYTICS_DB", "analytics")
    return clickhouse_connect.get_client(
        host=h,
        port=po,
        username=u,
        password=pw,
        database=db,
    )


def check_row_counts(**context):
    """
    Verify that key analytics tables are not empty.
    Raises ValueError if any table has 0 rows.
    """
    client = _get_olap_client()

    tables = [
        "dim_users",
        "dim_products",
        "dim_sellers",
        "dim_date",
        "fact_orders",
    ]

    issues = []
    for t in tables:
        count = client.command(f"SELECT count() FROM {t}")
        print(f"  {t}: {count:,} rows")
        if count == 0:
            issues.append(f"{t} is EMPTY")

    if issues:
        raise ValueError(f"Data quality FAIL: {'; '.join(issues)}")
    print("✅ Row counts OK")


def check_null_keys(**context):
    """Check for NULL foreign keys in fact_orders."""
    client = _get_olap_client()

    sql = """
        SELECT
            SUM(CASE WHEN user_key    IS NULL THEN 1 ELSE 0 END) AS null_user_key,
            SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
            SUM(CASE WHEN date_key    IS NULL THEN 1 ELSE 0 END) AS null_date_key
        FROM fact_orders
    """
    result = client.query_df(sql).iloc[0]
    issues = [f"{col}={val}" for col, val in result.items() if val > 0]

    if issues:
        raise ValueError(f"NULL foreign keys in fact_orders: {', '.join(issues)}")
    print("✅ No NULL foreign keys")


def check_revenue_anomaly(**context):
    """
    Alert if today's total revenue deviates > 80% from 7-day average.
    Uses window function on analytics.fact_orders.
    """
    client = _get_olap_client()

    sql = """
        WITH daily_rev AS (
            SELECT
                d.full_date,
                SUM(f.total_amount) AS revenue
            FROM fact_orders f
            JOIN dim_date d ON f.date_key = d.date_key
                        WHERE f.status = 'delivered'
                            AND d.full_date >= today() - 8
            GROUP BY d.full_date
        ),
        stats AS (
            SELECT
                AVG(revenue) OVER ()   AS avg_rev,
                revenue,
                full_date
            FROM daily_rev
        )
        SELECT * FROM stats
        ORDER BY full_date DESC
        LIMIT 1
    """
    df = client.query_df(sql)
    if df.empty:
        print("⚠️  No revenue data — skipping anomaly check")
        return

    rev     = float(df.iloc[0]["revenue"])
    avg_rev = float(df.iloc[0]["avg_rev"])

    if avg_rev > 0:
        pct_diff = abs(rev - avg_rev) / avg_rev
        print(f"  Today revenue: {rev:,.0f} | 7d avg: {avg_rev:,.0f} | Diff: {pct_diff:.1%}")
        if pct_diff > 0.8:
            print(f"⚠️  ANOMALY DETECTED: revenue deviation {pct_diff:.1%} > 80%")
        else:
            print("✅ Revenue within normal range")


with DAG(
    dag_id="dag_data_quality",
    description="Daily data quality checks on analytics schema",
    schedule="0 3 * * *",          # 03:00 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["quality", "monitoring"],
) as dag:

    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform",
        external_dag_id="dag_dbt_transform",
        external_task_id="dbt_test",
        timeout=3600,
        poke_interval=60,
        mode="poke",
    )

    t_row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    t_null_keys = PythonOperator(
        task_id="check_null_keys",
        python_callable=check_null_keys,
    )

    t_revenue_anomaly = PythonOperator(
        task_id="check_revenue_anomaly",
        python_callable=check_revenue_anomaly,
    )

    wait_for_transform >> [t_row_counts, t_null_keys] >> t_revenue_anomaly
