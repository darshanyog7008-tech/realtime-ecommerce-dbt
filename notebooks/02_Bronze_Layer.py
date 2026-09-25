# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC The Bronze Layer provides persistent Delta Lake storage for source data after ingestion.
# MAGIC
# MAGIC This layer preserves the source structure while adding ingestion metadata required for traceability, auditing, and downstream processing.
# MAGIC
# MAGIC ### Responsibilities
# MAGIC
# MAGIC - Persist source data in Delta format
# MAGIC - Preserve the ingested records
# MAGIC - Add ingestion metadata
# MAGIC - Maintain data lineage
# MAGIC - Provide a reliable source for Silver transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sdp_catalog.realtime_ecommerce;

# COMMAND ----------

# ============================================================
# Bronze Layer Configuration
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

BRONZE_TABLE = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_orders"

print(f"Bronze Table: {BRONZE_TABLE}")

# COMMAND ----------

# ============================================================
# Source Configuration
# ============================================================

RAW_DATA_PATH = "/Volumes/workspace/default/ecommerce"

ORDERS_FILE = f"{RAW_DATA_PATH}/olist_orders_dataset.csv"

print(f"Source File: {ORDERS_FILE}")

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

# Read orders using the production schema
# and capture the Unity Catalog-supported source file metadata.

from pyspark.sql.functions import col

orders_typed_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .schema(orders_schema)
         .load(ORDERS_FILE)
         .select(
             "*",
             
             col("_metadata.file_path").alias("source_file")
         )
)

print(f"Source Records: {orders_typed_df.count():,}")

# COMMAND ----------

print(f"Source Records: {orders_typed_df.count():,}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

bronze_orders_df = (
    orders_typed_df
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Bronze DataFrame created successfully.")
print(f"Columns: {len(bronze_orders_df.columns)}")

# COMMAND ----------

bronze_orders_df.show(10, truncate=False)

# COMMAND ----------

# Validate Bronze DataFrame

bronze_record_count = bronze_orders_df.count()
bronze_column_count = len(bronze_orders_df.columns)

print(f"Bronze Records : {bronze_record_count:,}")
print(f"Bronze Columns : {bronze_column_count}")

# COMMAND ----------

# Validate ingestion metadata

display(
    bronze_orders_df.select(
        "order_id",
        "source_file",
        "ingestion_timestamp"
    ).limit(10)
)

# COMMAND ----------

# Persist Orders data as a Delta table

(
    bronze_orders_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

# COMMAND ----------

# Verify Bronze Delta table

bronze_table_df = spark.table(BRONZE_TABLE)

print(f"Bronze Table Records: {bronze_table_df.count():,}")

# COMMAND ----------

bronze_table_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders
# MAGIC FROM sdp_catalog.realtime_ecommerce.bronze_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC               Olist CSV
# MAGIC                   │
# MAGIC                   ▼
# MAGIC           Explicit Schema
# MAGIC                   │
# MAGIC                   ▼
# MAGIC           Data Validation
# MAGIC                   │
# MAGIC                   ▼
# MAGIC         ┌──────────────────┐
# MAGIC         │  BRONZE DELTA    │
# MAGIC         │                  │
# MAGIC         │ orders           │
# MAGIC         │ + source_file    │
# MAGIC         │ + ingestion_time │
# MAGIC         └────────┬─────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC               SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Bronze Ingestion
# MAGIC
# MAGIC The Bronze ingestion process is standardized through a reusable function.
# MAGIC
# MAGIC This reduces code duplication and ensures that all source datasets follow the same ingestion pattern, metadata conventions, and Delta storage strategy.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col


def ingest_to_bronze(
    source_path,
    schema,
    target_table
):
    """
    Ingest a CSV source into a Delta Bronze table.

    The function applies an explicit schema, captures
    source-file metadata, adds ingestion timestamp,
    and persists the result as a Delta table.
    """

    source_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
        .select(
            "*",
            col("_metadata.file_path").alias("source_file")
        )
    )

    bronze_df = (
        source_df
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )

    (
        bronze_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    return bronze_df

# COMMAND ----------

# MAGIC %md
# MAGIC                 ingest_to_bronze()
# MAGIC                        │
# MAGIC         ┌──────────────┼──────────────┐
# MAGIC         ▼              ▼              ▼
# MAGIC     Customers       Products       Sellers
# MAGIC         │              │              │
# MAGIC         ▼              ▼              ▼
# MAGIC      Delta           Delta           Delta

# COMMAND ----------

# ============================================================
# Customer Schema
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

print("Customer schema created successfully.")

# COMMAND ----------

customers_path = f"{RAW_DATA_PATH}/olist_customers_dataset.csv"

customers_bronze_df = ingest_to_bronze(
    customers_path,
    customer_schema,
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_customers"
)

print(f"Bronze Customers: {customers_bronze_df.count():,}")

# COMMAND ----------

# ============================================================
# Order Items Schema
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

print("Order Items schema created successfully.")

# COMMAND ----------

# ============================================================
# Bronze: Order Items
# ============================================================

order_items_path = (
    f"{RAW_DATA_PATH}/olist_order_items_dataset.csv"
)

order_items_bronze_df = ingest_to_bronze(
    order_items_path,
    order_items_schema,
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_order_items"
)

print(
    f"Bronze Order Items: "
    f"{order_items_bronze_df.count():,}"
)

# COMMAND ----------

print(f"Columns: {len(order_items_bronze_df.columns)}")

order_items_bronze_df.printSchema()

# COMMAND ----------

# ============================================================
# Payments Schema
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

print("Payments schema created successfully.")

# COMMAND ----------

# ============================================================
# Bronze: Payments
# ============================================================

payments_path = (
    f"{RAW_DATA_PATH}/olist_order_payments_dataset.csv"
)

payments_bronze_df = ingest_to_bronze(
    payments_path,
    payments_schema,
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_payments"
)

print(
    f"Bronze Payments: "
    f"{payments_bronze_df.count():,}"
)

# COMMAND ----------

print(f"Columns: {len(payments_bronze_df.columns)}")

payments_bronze_df.printSchema()

# COMMAND ----------

# ============================================================
# Products Schema
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True)
])

print("Products schema created successfully.")

# COMMAND ----------

# ============================================================
# Bronze: Products
# ============================================================

products_path = (
    f"{RAW_DATA_PATH}/olist_products_dataset.csv"
)

products_bronze_df = ingest_to_bronze(
    products_path,
    products_schema,
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_products"
)

print(
    f"Bronze Products: "
    f"{products_bronze_df.count():,}"
)

# COMMAND ----------

print(f"Columns: {len(products_bronze_df.columns)}")

products_bronze_df.printSchema()

# COMMAND ----------

# ============================================================
# Sellers Schema
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

print("Sellers schema created successfully.")

# COMMAND ----------

# ============================================================
# Bronze: Sellers
# ============================================================

sellers_path = (
    f"{RAW_DATA_PATH}/olist_sellers_dataset.csv"
)

sellers_bronze_df = ingest_to_bronze(
    sellers_path,
    sellers_schema,
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_sellers"
)

print(
    f"Bronze Sellers: "
    f"{sellers_bronze_df.count():,}"
)

# COMMAND ----------

print(f"Columns: {len(sellers_bronze_df.columns)}")

sellers_bronze_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN sdp_catalog.realtime_ecommerce;

# COMMAND ----------

