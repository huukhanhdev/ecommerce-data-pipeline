FROM apache/airflow:2.8.1-python3.11

# Install dbt as the airflow user (default user in this image)
RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-postgres==1.7.9 \
    pandas==2.2.1 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==2.0.28 \
    pyarrow==15.0.0 \
    faker==24.1.0 \
    python-dotenv==1.0.1
