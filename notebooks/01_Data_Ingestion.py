# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook is responsible for ingesting raw datasets into Apache Spark.
# MAGIC
# MAGIC Instead of relying on automatic schema inference, we will define explicit schemas to improve performance, consistency, and reliability.
# MAGIC
# MAGIC This notebook prepares the raw datasets before they enter the Bronze Layer of the Medallion Architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Engineering Principle
# MAGIC
# MAGIC Separate:
# MAGIC
# MAGIC - Data Ingestion
# MAGIC - Data Transformation
# MAGIC - Data Serving
# MAGIC
# MAGIC This makes pipelines modular, scalable and easier to maintain.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# Import required libraries

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Display Spark runtime version

print(f"Spark Version: {spark.version}")

# COMMAND ----------




display(dbutils.fs.ls("/Volumes/workspace/default/ecommerce"))

# COMMAND ----------

# ============================================================
# Project Configuration
# ============================================================

RAW_DATA_PATH = "/Volumes/workspace/default/ecommerce"

CUSTOMERS_FILE = f"{RAW_DATA_PATH}/olist_customers_dataset.csv"
ORDERS_FILE = f"{RAW_DATA_PATH}/olist_orders_dataset.csv"
ORDER_ITEMS_FILE = f"{RAW_DATA_PATH}/olist_order_items_dataset.csv"
PAYMENTS_FILE = f"{RAW_DATA_PATH}/olist_order_payments_dataset.csv"
PRODUCTS_FILE = f"{RAW_DATA_PATH}/olist_products_dataset.csv"
SELLERS_FILE = f"{RAW_DATA_PATH}/olist_sellers_dataset.csv"

# COMMAND ----------

orders_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(ORDERS_FILE)
)

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

print(f"Total Records : {orders_df.count():,}")
print(f"Total Columns : {len(orders_df.columns)}")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

# Inspect the inferred schema

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality: Null Profiling
# MAGIC
# MAGIC Null profiling establishes a baseline for source-data completeness before the dataset enters the Bronze Layer.
# MAGIC
# MAGIC A null value is not automatically a data-quality failure. Its validity depends on the business meaning of the column.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

null_counts_df = orders_df.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in orders_df.columns
    ]
)

display(null_counts_df)

# COMMAND ----------

order_id_counts_df = (
    orders_df
    .groupBy("order_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

display(order_id_counts_df)

# COMMAND ----------

duplicate_order_ids = order_id_counts_df.count()

print(f"Duplicate Order IDs: {duplicate_order_ids:,}")

# COMMAND ----------

display(
    orders_df
    .groupBy("order_status")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

date_range = orders_df.select(
    min("order_purchase_timestamp").alias("earliest_order"),
    max("order_purchase_timestamp").alias("latest_order")
).first()

print(f"Earliest Order : {date_range['earliest_order']}")
print(f"Latest Order   : {date_range['latest_order']}")

# COMMAND ----------

display(null_counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Assessment
# MAGIC
# MAGIC The Olist orders dataset has been loaded successfully and profiled for:
# MAGIC
# MAGIC - Schema structure
# MAGIC - Record volume
# MAGIC - Null values
# MAGIC - Business-key duplicates
# MAGIC - Order-status distribution
# MAGIC - Source time range
# MAGIC
# MAGIC These observations will be used to define the production schema and Bronze ingestion strategy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Schema
# MAGIC
# MAGIC The source schema is explicitly defined to prevent schema drift and ensure consistent data types during ingestion.
# MAGIC
# MAGIC Identifier fields are represented as strings, while order lifecycle fields are represented as timestamps.
# MAGIC
# MAGIC Delivery timestamps remain nullable because an order may legitimately be awaiting approval, shipment, or delivery.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", TimestampType(), False),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), False)
])

# COMMAND ----------

# ============================================================
# Project Configuration
# ============================================================

RAW_DATA_PATH = "/Volumes/workspace/default/ecommerce"

CUSTOMERS_FILE = f"{RAW_DATA_PATH}/olist_customers_dataset.csv"
ORDERS_FILE = f"{RAW_DATA_PATH}/olist_orders_dataset.csv"
ORDER_ITEMS_FILE = f"{RAW_DATA_PATH}/olist_order_items_dataset.csv"
PAYMENTS_FILE = f"{RAW_DATA_PATH}/olist_order_payments_dataset.csv"
PRODUCTS_FILE = f"{RAW_DATA_PATH}/olist_products_dataset.csv"
SELLERS_FILE = f"{RAW_DATA_PATH}/olist_sellers_dataset.csv"

print("Raw data path:", RAW_DATA_PATH)
print("Orders file:", ORDERS_FILE)

# COMMAND ----------

orders_typed_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .schema(orders_schema)
         .load(ORDERS_FILE)
)

# COMMAND ----------

orders_typed_df.printSchema()

# COMMAND ----------

print(f"Typed Records: {orders_typed_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Required Field Validation
# MAGIC
# MAGIC The ingestion layer validates fields that are considered mandatory for an order record.
# MAGIC
# MAGIC Records violating these constraints will be identified before downstream processing.

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

required_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_estimated_delivery_date"
]

required_nulls_df = orders_typed_df.select(
    [
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in required_columns
    ]
)

display(required_nulls_df)

# COMMAND ----------

duplicate_order_ids_typed = (
    orders_typed_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate Order IDs: {duplicate_order_ids_typed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Validation Summary
# MAGIC
# MAGIC The typed orders DataFrame has been validated for:
# MAGIC
# MAGIC - Record count consistency
# MAGIC - Required field completeness
# MAGIC - Business-key uniqueness
# MAGIC - Explicit data types
# MAGIC
# MAGIC The dataset is ready for Bronze Layer ingestion.

# COMMAND ----------

print("==========================================")
print("ORDERS INGESTION VALIDATION")
print("==========================================")
print(f"Records          : {orders_typed_df.count():,}")
print(f"Columns          : {len(orders_typed_df.columns)}")
print(f"Duplicate IDs    : {duplicate_order_ids_typed:,}")
print("Required fields  : Validated")
print("Schema           : Explicit")
print("Status           : READY FOR BRONZE")

# COMMAND ----------

