#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${POSTGRES_USER:?Missing POSTGRES_USER in .env}"
: "${POSTGRES_PASSWORD:?Missing POSTGRES_PASSWORD in .env}"
: "${POSTGRES_OLTP_DB:?Missing POSTGRES_OLTP_DB in .env}"
: "${CLICKHOUSE_USER:?Missing CLICKHOUSE_USER in .env}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${CLICKHOUSE_STAGING_DB:?Missing CLICKHOUSE_STAGING_DB in .env}"
: "${CLICKHOUSE_ANALYTICS_DB:?Missing CLICKHOUSE_ANALYTICS_DB in .env}"

if [[ -n "${OLAP_ANALYTICS_DB:-}" && "$CLICKHOUSE_ANALYTICS_DB" != "$OLAP_ANALYTICS_DB" ]]; then
  echo "WARN: CLICKHOUSE_ANALYTICS_DB ($CLICKHOUSE_ANALYTICS_DB) differs from OLAP_ANALYTICS_DB ($OLAP_ANALYTICS_DB). Using OLAP_ANALYTICS_DB for this run."
  export CLICKHOUSE_ANALYTICS_DB="$OLAP_ANALYTICS_DB"
fi

if [[ -z "$CLICKHOUSE_PASSWORD" ]]; then
  echo "ERROR: CLICKHOUSE_PASSWORD is empty. Set a non-empty password in .env."
  exit 1
fi

log() { echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"; }

wait_for_clickhouse() {
  local retries=30
  local i=0
  local pass_arg=()
  if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    pass_arg=("--password" "$CLICKHOUSE_PASSWORD")
  fi

  while (( i < retries )); do
    if docker exec clickhouse_olap clickhouse-client --user "$CLICKHOUSE_USER" ${pass_arg[@]+"${pass_arg[@]}"} --query "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 5
  done
  return 1
}

wait_for_dag() {
  local dag_id="$1"
  local exec_date="$2"
  local retries=90
  local i=0
  local raw_state=""
  local state=""

  while (( i < retries )); do
    raw_state=$(docker exec airflow_scheduler airflow dags state "$dag_id" "$exec_date" 2>/dev/null || true)
    state=$(printf "%s\n" "$raw_state" | tr -d '\r' | grep -Eo 'success|failed|upstream_failed|running|queued' | tail -n1 || true)

    if [[ "$state" == "success" ]]; then
      log "DAG $dag_id success"
      return 0
    fi

    if [[ "$state" == "failed" || "$state" == "upstream_failed" ]]; then
      log "DAG $dag_id failed with state=$state"
      return 1
    fi

    if (( i % 3 == 0 )); then
      log "DAG $dag_id state=${state:-unknown} (waiting...)"
    fi

    i=$((i + 1))
    sleep 20
  done

  log "DAG $dag_id timeout waiting for completion"
  return 1
}

log "Starting services"
docker-compose up -d

log "Ensuring Airflow deps"
docker exec airflow_scheduler python -m pip install --user clickhouse-connect==0.13.0
docker exec airflow_scheduler python -m pip install --user dbt-clickhouse==1.10.0

log "Waiting for ClickHouse"
wait_for_clickhouse
log "ClickHouse is ready"

log "Seeding OLTP data"
pip3 install faker==24.1.0 psycopg2-binary==2.9.9
python3 ingestion/data_generator.py \
  --host localhost --port 5432 \
  --db "$POSTGRES_OLTP_DB" --user "$POSTGRES_USER" --password "$POSTGRES_PASSWORD" \
  --users 200 --products 100 --orders 500

EXEC_DATE="$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S+00:00'))")"

# Pause all DAGs to prevent scheduler from creating new interfering runs
log "Pausing all DAGs (prevent scheduled runs from interfering)"
docker exec airflow_scheduler airflow dags pause dag_ingest_daily 2>/dev/null || true
docker exec airflow_scheduler airflow dags pause dag_dbt_transform 2>/dev/null || true
docker exec airflow_scheduler airflow dags pause dag_data_quality 2>/dev/null || true

log "Triggering dag_ingest_daily"
docker exec airflow_scheduler airflow dags unpause dag_ingest_daily
docker exec airflow_scheduler airflow dags trigger -e "$EXEC_DATE" --conf '{"full_load": true}' dag_ingest_daily
wait_for_dag "dag_ingest_daily" "$EXEC_DATE"
docker exec airflow_scheduler airflow dags pause dag_ingest_daily 2>/dev/null || true

log "Triggering dag_dbt_transform"
docker exec airflow_scheduler airflow dags unpause dag_dbt_transform
docker exec airflow_scheduler airflow dags trigger -e "$EXEC_DATE" dag_dbt_transform
wait_for_dag "dag_dbt_transform" "$EXEC_DATE"
docker exec airflow_scheduler airflow dags pause dag_dbt_transform 2>/dev/null || true

log "Triggering dag_data_quality"
docker exec airflow_scheduler airflow dags unpause dag_data_quality
docker exec airflow_scheduler airflow dags trigger -e "$EXEC_DATE" dag_data_quality
wait_for_dag "dag_data_quality" "$EXEC_DATE"
docker exec airflow_scheduler airflow dags pause dag_data_quality 2>/dev/null || true

log "Running basic ClickHouse checks"
PASS_ARG=()
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
  PASS_ARG=("--password" "$CLICKHOUSE_PASSWORD")
fi

docker exec clickhouse_olap clickhouse-client --user "$CLICKHOUSE_USER" ${PASS_ARG[@]+"${PASS_ARG[@]}"} \
  --query "SELECT count() AS stg_users FROM ${CLICKHOUSE_STAGING_DB}.stg_users"

docker exec clickhouse_olap clickhouse-client --user "$CLICKHOUSE_USER" ${PASS_ARG[@]+"${PASS_ARG[@]}"} \
  --query "SELECT count() AS fact_orders FROM ${CLICKHOUSE_ANALYTICS_DB}.fact_orders"

docker exec clickhouse_olap clickhouse-client --user "$CLICKHOUSE_USER" ${PASS_ARG[@]+"${PASS_ARG[@]}"} \
  --query "SELECT count() AS rpt_daily_revenue FROM ${CLICKHOUSE_ANALYTICS_DB}.rpt_daily_revenue"

log "Full test completed"
