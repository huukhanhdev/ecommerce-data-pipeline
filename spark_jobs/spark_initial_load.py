"""
spark_initial_load.py
─────────────────────
PySpark job for historical bulk load (initial one-time load).
Reads CSV files → validates → writes to staging tables in ClickHouse.

Run inside the Spark container:
    docker exec -it spark_master spark-submit \
        --packages com.clickhouse:clickhouse-jdbc:0.6.0 \
        /spark_jobs/spark_initial_load.py \
        --input-dir /data \
        --jdbc-url jdbc:clickhouse://clickhouse_olap:8123/staging \
        --user default --password ""
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DecimalType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("spark_initial_load")


# ─── Schemas ──────────────────────────────────────────────────────────────────

SCHEMAS = {
    "orders": StructType([
        StructField("order_id",       IntegerType(), False),
        StructField("user_id",        IntegerType(), False),
        StructField("status",         StringType(),  False),
        StructField("shipping_fee",   DecimalType(10,2), True),
        StructField("payment_method", StringType(),  True),
        StructField("created_at",     TimestampType(), True),
        StructField("updated_at",     TimestampType(), True),
    ]),
    "order_items": StructType([
        StructField("item_id",    IntegerType(),    False),
        StructField("order_id",   IntegerType(),    False),
        StructField("product_id", IntegerType(),    False),
        StructField("quantity",   IntegerType(),    False),
        StructField("unit_price", DecimalType(12,2), False),
    ]),
    "users": StructType([
        StructField("user_id",    IntegerType(), False),
        StructField("name",       StringType(),  True),
        StructField("email",      StringType(),  True),
        StructField("phone",      StringType(),  True),
        StructField("city",       StringType(),  True),
        StructField("gender",     StringType(),  True),
        StructField("created_at", TimestampType(), True),
    ]),
}

VALID_STATUSES  = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
VALID_GENDERS   = ["male", "female", "other"]


# ─── Spark Job ────────────────────────────────────────────────────────────────

class SparkInitialLoader:

    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_user: str, jdbc_password: str):
        self.spark    = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = {
            "user":     jdbc_user,
            "password": jdbc_password,
            "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
        }

    def _write_to_staging(self, df, staging_table: str):
        """Write a Spark DataFrame to PostgreSQL staging schema."""
        df_with_meta = df.withColumn("_loaded_at", F.current_timestamp())
        row_count = df_with_meta.count()

        df_with_meta.write.jdbc(
            url=self.jdbc_url,
            table=f"staging.{staging_table}",
            mode="append",
            properties=self.jdbc_props,
        )
        logger.info(f"  ✅ Written {row_count:,} rows → staging.{staging_table}")

    def load_orders(self, input_dir: str):
        """Load orders CSV: filter invalid statuses, deduplicate."""
        logger.info("Loading orders...")
        df = (
            self.spark.read
            .option("header", True)
            .schema(SCHEMAS["orders"])
            .csv(f"{input_dir}/orders.csv")
        )
        # Transformations with PySpark
        df_clean = (
            df
            .filter(F.col("order_id").isNotNull())
            .filter(F.col("status").isin(VALID_STATUSES))
            .dropDuplicates(["order_id"])
            .withColumn(
                "shipping_fee",
                F.when(F.col("shipping_fee").isNull(), F.lit(0)).otherwise(F.col("shipping_fee"))
            )
        )
        self._write_to_staging(df_clean, "stg_orders")

    def load_order_items(self, input_dir: str):
        """Load order_items CSV: filter nulls, positive quantity & price."""
        logger.info("Loading order_items...")
        df = (
            self.spark.read
            .option("header", True)
            .schema(SCHEMAS["order_items"])
            .csv(f"{input_dir}/order_items.csv")
        )
        df_clean = (
            df
            .filter(F.col("item_id").isNotNull())
            .filter(F.col("quantity") > 0)
            .filter(F.col("unit_price") > 0)
            .dropDuplicates(["item_id"])
        )

        # Example window function: add row_number over orders
        from pyspark.sql.window import Window
        w = Window.partitionBy("order_id").orderBy("item_id")
        df_clean = df_clean.withColumn("item_seq", F.row_number().over(w))

        self._write_to_staging(df_clean.drop("item_seq"), "stg_order_items")

    def load_users(self, input_dir: str):
        """Load users CSV: clean gender column, dedupe by email."""
        logger.info("Loading users...")
        df = (
            self.spark.read
            .option("header", True)
            .schema(SCHEMAS["users"])
            .csv(f"{input_dir}/users.csv")
        )
        df_clean = (
            df
            .filter(F.col("user_id").isNotNull())
            .filter(F.col("email").isNotNull())
            .filter(F.col("gender").isin(VALID_GENDERS))
            .dropDuplicates(["email"])
        )
        self._write_to_staging(df_clean, "stg_users")

    def run(self, input_dir: str):
        logger.info("=== Spark Initial Load Started ===")
        self.load_users(input_dir)
        self.load_orders(input_dir)
        self.load_order_items(input_dir)
        logger.info("=== Spark Initial Load Complete ===")


# ─── Entry Point ──────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir",  required=True)
    p.add_argument("--jdbc-url",   required=True)
    p.add_argument("--user",       required=True)
    p.add_argument("--password",   required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("EcommerceInitialLoad")
        .config("spark.sql.shuffle.partitions", "4")  # small dataset
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    loader = SparkInitialLoader(spark, args.jdbc_url, args.user, args.password)
    loader.run(args.input_dir)

    spark.stop()
    sys.exit(0)
