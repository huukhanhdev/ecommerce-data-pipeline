"""
extractor.py
────────────
Extracts data incrementally from the OLTP database.
Supports full-load and incremental-load patterns.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class PostgresExtractor:
    """
    Extracts tables from a PostgreSQL source database.

    Supports:
    - full_table(): dump entire table
    - incremental(): extract rows modified since a given watermark date
    """

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self._conn_params = dict(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self._conn: Optional[psycopg2.extensions.connection] = None

    # ── Connection management ─────────────────────────────────────────────

    def connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self._conn_params, cursor_factory=RealDictCursor)
            logger.info(f"Connected to {self._conn_params['dbname']}@{self._conn_params['host']}")
        return self

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Connection closed")

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.close()

    # ── Core extraction ───────────────────────────────────────────────────

    def _query_to_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        df = pd.DataFrame([dict(r) for r in rows])
        logger.info(f"  Fetched {len(df):,} rows")
        return df

    def full_table(self, table: str) -> pd.DataFrame:
        """Full extract of a table."""
        logger.info(f"Full extract: {table}")
        return self._query_to_df(f"SELECT * FROM {table}")

    def incremental_orders(self, since: date) -> pd.DataFrame:
        """
        Incremental extract: orders created or updated since `since` date.
        This is the primary incremental pattern — other tables are relatively static.
        """
        logger.info(f"Incremental extract: orders since {since}")
        sql = """
            SELECT
                o.order_id,
                o.user_id,
                o.status,
                o.shipping_fee,
                o.payment_method,
                o.created_at,
                o.updated_at
            FROM orders o
            WHERE o.created_at >= %s
               OR o.updated_at >= %s
            ORDER BY o.created_at
        """
        ts = datetime.combine(since, datetime.min.time())
        return self._query_to_df(sql, (ts, ts))

    def incremental_order_items(self, since: date) -> pd.DataFrame:
        """Extract order items linked to recent orders."""
        logger.info(f"Incremental extract: order_items since {since}")
        sql = """
            SELECT oi.*
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.created_at >= %s
               OR o.updated_at >= %s
        """
        ts = datetime.combine(since, datetime.min.time())
        return self._query_to_df(sql, (ts, ts))

    def extract_all_static_tables(self) -> dict[str, pd.DataFrame]:
        """
        Extracts dimension-like tables that rarely change.
        These are re-extracted fully on each run (small tables).
        """
        tables = ["users", "sellers", "products", "categories"]
        result = {}
        for t in tables:
            logger.info(f"Extracting static table: {t}")
            result[t] = self.full_table(t)
        return result


class ExtractionOrchestrator:
    """
    High-level orchestrator for daily incremental extraction.
    Called by the Airflow DAG.
    """

    def __init__(self, extractor: PostgresExtractor):
        self.extractor = extractor

    def run_daily(self, execution_date: Optional[date] = None) -> dict[str, pd.DataFrame]:
        """
        Run daily extraction. Defaults to yesterday if no date given.
        Returns a dict of {table_name: DataFrame}.
        """
        if execution_date is None:
            execution_date = date.today() - timedelta(days=1)

        logger.info(f"=== Daily extraction for {execution_date} ===")
        result = self.extractor.extract_all_static_tables()
        result["orders"]      = self.extractor.incremental_orders(execution_date)
        result["order_items"] = self.extractor.incremental_order_items(execution_date)

        for name, df in result.items():
            logger.info(f"  {name}: {len(df):,} rows extracted")

        return result
