# E-Commerce Data Pipeline

End-to-end batch data pipeline simulating a real-world e-commerce analytics platform.

## Architecture

```
[OLTP PostgreSQL]
  Raw transactional data (orders, users, products)
       │
       ▼  Python (incremental extract)
[Staging – ClickHouse]
  Raw copies in staging schema
       │
       ▼  dbt (SQL transformations)
[Analytics – ClickHouse]
  Star schema: dim_* + fact_orders + report tables
       │
  Orchestrated by Apache Airflow (3 DAGs)
  Containerized via Docker Compose
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt-core + dbt-postgres |
| Storage | PostgreSQL 15 (OLTP) + ClickHouse (OLAP) |
| Bulk load | PySpark 3.5 |
| Ingestion | Python + Pandas |
| Containerization | Docker Compose |

## Quick Start

### 1. Prerequisites
- Docker Desktop running
- Python 3.11+

### 2. Setup

```bash
# Clone & enter project
cd de_project

# Copy env file
cp .env.example .env
# Edit .env — generate AIRFLOW_FERNET_KEY and set ClickHouse creds (password required):
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Start all services
docker-compose up -d

# Wait ~60s for Airflow to initialize, then open:
# Airflow UI → http://localhost:8080  (admin / admin)
# Spark UI   → http://localhost:8090
# ClickHouse HTTP → http://localhost:8123
```

### 3. Seed the OLTP database with fake data

```bash
# Install local deps
pip install -r requirements.txt

# Seed 500 users, 200 products, 2000 orders
python ingestion/data_generator.py \
  --host localhost --port 5432 \
  --db ecommerce_oltp --user deuser --password depassword \
  --users 500 --products 200 --orders 2000
```

### 4. Run the pipeline manually (or trigger via Airflow UI)

```bash
# Trigger ingestion DAG
docker exec airflow_scheduler \
  airflow dags trigger dag_ingest_daily \
  --conf '{"execution_date": "2025-01-01"}'
```

### 5. PySpark Historical Load (one-time)

```bash
docker exec spark_master spark-submit \
  --packages com.clickhouse:clickhouse-jdbc:0.6.0 \
  /spark_jobs/spark_initial_load.py \
  --input-dir /data \
  --jdbc-url jdbc:clickhouse://clickhouse_olap:8123/staging \
  --user default --password ""
```

## Project Structure

```
de_project/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── sql/
│   ├── init_oltp.sql        # OLTP schema
│   └── init_olap.sql        # ClickHouse staging tables
├── ingestion/
│   ├── data_generator.py    # Faker-based OOP data generator
│   ├── extractor.py         # Incremental extract from OLTP
│   ├── loader.py            # Bulk load to staging
│   └── validator.py         # Data quality checks
├── spark_jobs/
│   └── spark_initial_load.py  # PySpark historical backfill
├── dags/
│   ├── dag_ingest_daily.py  # Extract → Validate → Stage
│   ├── dag_dbt_transform.py # dbt run (staging → marts → reports)
│   └── dag_data_quality.py  # Row counts, null checks, anomaly detection
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/         # stg_orders, stg_users, stg_products, ...
│       ├── marts/           # dim_date, dim_users, dim_products, fact_orders
│       └── reports/         # rpt_daily_revenue, rpt_seller_ranking, ...
└── data/                    # CSV files for initial Spark load
```

## DAG Schedule

| DAG | Time | Purpose |
|-----|------|---------|
| `dag_ingest_daily` | 01:00 AM | OLTP → staging |
| `dag_dbt_transform` | 02:00 AM | staging → star schema |
| `dag_data_quality` | 03:00 AM | Quality checks & alerts |

## dbt Models

| Layer | Models | Pattern |
|-------|--------|---------|
| Staging | `stg_orders`, `stg_users`, `stg_products`, `stg_order_items` | Views |
| Marts | `dim_date`, `dim_users`, `dim_products`, `dim_sellers`, `fact_orders` | Tables |
| Reports | `rpt_daily_revenue`, `rpt_seller_ranking`, `rpt_product_performance` | Tables |


## Star Schema

```
dim_date ──┐
dim_users ─┤
           ├── fact_orders ──── dim_products ── dim_sellers
dim_* ─────┘
```
