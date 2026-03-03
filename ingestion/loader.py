"""
loader.py
─────────
Loads Pandas DataFrames into the staging schema of the OLAP database.
Uses UPSERT-style truncate+insert per batch (idempotent runs).
"""

import logging
from datetime import datetime
from typing import Dict

import pandas as pd
import clickhouse_connect

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class StagingLoader:
    """
    Loads DataFrames into the staging database in ClickHouse.

    Strategy: truncate staging table then bulk insert → idempotent (safe to re-run).
    For order-related tables, appends instead of truncating to preserve history.
    """

    # Tables that are full-refreshed every run (small dimension-like tables)
    FULL_REFRESH_TABLES = {"stg_users", "stg_sellers", "stg_products", "stg_categories"}

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.staging_db = dbname
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=dbname,
        )
        logger.info(f"StagingLoader connected to {dbname}@{host}")

    # ── Helper ────────────────────────────────────────────────────────────

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["_loaded_at"] = pd.to_datetime(datetime.utcnow())
        return df

    def _normalize_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert pandas NA/NaT to None for ClickHouse inserts."""
        return df.where(pd.notna(df), None)

    def _truncate(self, table: str):
        full_name = f"{self.staging_db}.{table}"
        self.client.command(f"TRUNCATE TABLE {full_name}")
        logger.info(f"  Truncated {full_name}")

    # ── Public API ────────────────────────────────────────────────────────

    def load(self, table_name: str, df: pd.DataFrame, execution_date: str = ""):
        """
        Load a DataFrame into a staging table.

        - FULL_REFRESH_TABLES → truncate then insert
        - Transactional tables (orders, order_items) → append only
        """
        if df is None or len(df) == 0:
            logger.warning(f"  Skipping {table_name}: empty DataFrame")
            return

        stg_table = f"stg_{table_name}"
        df = self._add_metadata(df)
        df = self._normalize_nulls(df)

        if stg_table in self.FULL_REFRESH_TABLES:
            self._truncate(stg_table)
            mode = "append"
        else:
            mode = "append"   # orders accumulate over time

        if mode == "append":
            self.client.insert_df(stg_table, df, database=self.staging_db)
            logger.info(f"  ✅ Loaded {len(df):,} rows → {self.staging_db}.{stg_table}")

    def load_all(self, data: Dict[str, pd.DataFrame], execution_date: str = ""):
        """Load all extracted tables into staging. Called by Airflow DAG."""
        logger.info(f"=== Loading to staging (exec_date={execution_date}) ===")
        for table_name, df in data.items():
            self.load(table_name, df, execution_date)
        logger.info("=== Staging load complete ===")
