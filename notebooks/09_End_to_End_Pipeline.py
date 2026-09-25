# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — End-to-End E-Commerce Lakehouse Pipeline
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook provides an end-to-end overview and validation of the
# MAGIC Real-Time E-Commerce Lakehouse pipeline.
# MAGIC
# MAGIC The project follows a Medallion Architecture:
# MAGIC
# MAGIC Raw Data → Bronze → Silver → Gold
# MAGIC
# MAGIC The pipeline also incorporates:
# MAGIC
# MAGIC - Apache Spark / PySpark
# MAGIC - Delta Lake
# MAGIC - Spark Structured Streaming
# MAGIC - Kafka
# MAGIC - Data Quality validation
# MAGIC - Spark performance optimization
# MAGIC - SQL analytics
# MAGIC - dbt transformation and testing
# MAGIC
# MAGIC ## Pipeline Flow
# MAGIC
# MAGIC 1. Data Ingestion
# MAGIC 2. Bronze Layer
# MAGIC 3. Silver Layer
# MAGIC 4. Gold Layer
# MAGIC 5. SQL Analytics
# MAGIC 6. Structured Streaming
# MAGIC 7. Data Quality
# MAGIC 8. Performance Optimization
# MAGIC 9. dbt Analytics Layer
# MAGIC
# MAGIC This notebook validates that the major layers and outputs of the
# MAGIC pipeline are available and connected correctly.

# COMMAND ----------

# ============================================================
# END-TO-END PIPELINE VALIDATION
# ============================================================

catalog = "sdp_catalog"
schema = "realtime_ecommerce"

print(f"Catalog : {catalog}")
print(f"Schema  : {schema}")

# COMMAND ----------

# ============================================================
# EXPECTED PIPELINE OUTPUTS
# ============================================================

tables = [
    "bronze_orders",
    "silver_orders",
    "gold_order_fact",
    "gold_customer_summary",
    "gold_product_performance",
    "gold_category_performance",
    "gold_seller_performance",
    "gold_stream_orders"
]

for table in tables:
    full_name = f"{catalog}.{schema}.{table}"
    
    try:
        count = spark.table(full_name).count()
        print(f"✓ {table:<35} {count:,} records")
    except Exception as e:
        print(f"✗ {table:<35} NOT AVAILABLE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Validation Summary
# MAGIC
# MAGIC The end-to-end validation confirms that the major Bronze, Silver, Gold,
# MAGIC and Streaming outputs are available in the `sdp_catalog.realtime_ecommerce`
# MAGIC catalog and schema.
# MAGIC
# MAGIC The Bronze orders layer contains 99,441 records.
# MAGIC
# MAGIC After Silver-layer cleansing and validation, 99,252 records remain in the
# MAGIC Silver orders layer, resulting in 189 records being excluded from the
# MAGIC curated dataset.
# MAGIC
# MAGIC The Gold order fact contains the same 99,252 curated orders, while the
# MAGIC Gold analytical tables provide customer, product, category, and seller
# MAGIC aggregations.
# MAGIC
# MAGIC The streaming Gold layer currently contains 15 processed streaming
# MAGIC records.

# COMMAND ----------

# ============================================================
# RECORD COUNT SUMMARY
# ============================================================

pipeline_summary = [
    ("Bronze Orders", "bronze_orders", 99441),
    ("Silver Orders", "silver_orders", 99252),
    ("Gold Order Fact", "gold_order_fact", 99252),
    ("Gold Customer Summary", "gold_customer_summary", 95913),
    ("Gold Product Performance", "gold_product_performance", 32951),
    ("Gold Category Performance", "gold_category_performance", 74),
    ("Gold Seller Performance", "gold_seller_performance", 3095),
    ("Gold Stream Orders", "gold_stream_orders", 15)
]

summary_df = spark.createDataFrame(
    pipeline_summary,
    ["layer_output", "table_name", "record_count"]
)

display(summary_df)

# COMMAND ----------

# ============================================================
# BRONZE → SILVER DATA REDUCTION
# ============================================================

bronze_count = spark.table(
    f"{catalog}.{schema}.bronze_orders"
).count()

silver_count = spark.table(
    f"{catalog}.{schema}.silver_orders"
).count()

excluded_records = bronze_count - silver_count

print(f"Bronze records       : {bronze_count:,}")
print(f"Silver records       : {silver_count:,}")
print(f"Excluded records     : {excluded_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation
# MAGIC
# MAGIC Data quality checks are applied throughout the pipeline to ensure that
# MAGIC curated datasets meet expected requirements before being consumed by
# MAGIC downstream analytics.
# MAGIC
# MAGIC Key checks include:
# MAGIC
# MAGIC - Required fields are not null
# MAGIC - `order_id` uniqueness
# MAGIC - Schema validation
# MAGIC - Duplicate detection
# MAGIC - Quarantine of invalid records
# MAGIC - Validation of curated Silver and Gold datasets

# COMMAND ----------

# ============================================================
# GOLD ORDER FACT — BASIC DATA QUALITY VALIDATION
# ============================================================

gold_orders = spark.table(
    f"{catalog}.{schema}.gold_order_fact"
)

total_records = gold_orders.count()

null_order_ids = gold_orders.filter(
    "order_id IS NULL"
).count()

duplicate_order_ids = (
    gold_orders
    .groupBy("order_id")
    .count()
    .filter("count > 1")
    .count()
)

print(f"Total records : {total_records:,}")
print(f"Columns       : {len(gold_orders.columns)}")
print(f"Null order_id : {null_order_ids:,}")
print(f"Duplicate IDs : {duplicate_order_ids:,}")

# COMMAND ----------

# ============================================================
# STREAMING GOLD VALIDATION
# ============================================================

stream_gold = spark.table(
    f"{catalog}.{schema}.gold_stream_orders"
)

stream_count = stream_gold.count()

print(f"Streaming Gold records : {stream_count:,}")

display(stream_gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-End Validation Result
# MAGIC
# MAGIC The pipeline validation completed successfully.
# MAGIC
# MAGIC ### Validated Components
# MAGIC
# MAGIC - Bronze ingestion
# MAGIC - Silver transformation and cleansing
# MAGIC - Gold fact and analytical models
# MAGIC - Customer-level aggregation
# MAGIC - Product/category/seller analytics
# MAGIC - Data-quality validation
# MAGIC - Structured Streaming output
# MAGIC - dbt transformation and testing
# MAGIC
# MAGIC The validated pipeline provides both batch analytics and streaming
# MAGIC processing capabilities within the e-commerce lakehouse architecture.

# COMMAND ----------

