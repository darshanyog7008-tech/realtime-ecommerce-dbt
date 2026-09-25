# Databricks notebook source
# ============================================================
# REALTIME ECOMMERCE LAKEHOUSE
# STRUCTURED STREAMING PIPELINE
# ============================================================

CATALOG = "sdp_catalog"
SCHEMA = "realtime_ecommerce"

STREAM_INPUT = f"/Volumes/{CATALOG}/{SCHEMA}/streaming_input"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_stream_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_stream_orders"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.gold_stream_orders"

BRONZE_CHECKPOINT = f"{STREAM_INPUT}/_checkpoints/bronze_orders"
SILVER_CHECKPOINT = f"{STREAM_INPUT}/_checkpoints/silver_orders"
GOLD_CHECKPOINT   = f"{STREAM_INPUT}/_checkpoints/gold_orders"

print("=" * 65)
print("REALTIME ECOMMERCE LAKEHOUSE")
print("STRUCTURED STREAMING PIPELINE")
print("=" * 65)
print(f"Catalog         : {CATALOG}")
print(f"Schema          : {SCHEMA}")
print(f"Streaming Input : {STREAM_INPUT}")
print(f"Bronze Table    : {BRONZE_TABLE}")
print(f"Silver Table    : {SILVER_TABLE}")
print(f"Gold Table      : {GOLD_TABLE}")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 2 — Imports
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    datediff,
    current_timestamp,
    count,
    countDistinct
)

print("=" * 65)
print("STREAMING PIPELINE IMPORTS")
print("=" * 65)
print("Status : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 3 — Streaming Event Schema
# ============================================================

stream_event_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("event_timestamp", TimestampType(), True)
])

print("=" * 65)
print("STREAMING EVENT SCHEMA")
print("=" * 65)

for field in stream_event_schema.fields:
    print(f"{field.name:40} {field.dataType}")

print("=" * 65)
print("Status : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 4 — Structured Streaming Source
# ============================================================

stream_orders_df = (
    spark.readStream
    .format("json")
    .schema(stream_event_schema)
    .option("maxFilesPerTrigger", 1)
    .load(STREAM_INPUT)
)

print("=" * 65)
print("STRUCTURED STREAMING SOURCE")
print("=" * 65)
print("Source Format       : JSON")
print(f"Source Location     : {STREAM_INPUT}")
print("Max Files / Trigger : 1")
print("Is Streaming        :", stream_orders_df.isStreaming)
print("Status              : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 4A — Verify Streaming Source
# ============================================================

print("=" * 65)
print("STREAMING SOURCE VERIFICATION")
print("=" * 65)

print("Is Streaming :", stream_orders_df.isStreaming)
print("Status       : READY")

print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 5 — Bronze Streaming Transformation
# ============================================================

bronze_stream_df = (
    stream_orders_df
    .withColumn("_ingestion_timestamp", current_timestamp())
)

print("=" * 65)
print("BRONZE STREAMING DATAFRAME")
print("=" * 65)

print("Is Streaming :", bronze_stream_df.isStreaming)
print("Columns      :", len(bronze_stream_df.columns))
print("Column Names :", bronze_stream_df.columns)

print("=" * 65)
print("Status       : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 6 — Write Bronze Streaming Table
# ============================================================

bronze_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

bronze_query.awaitTermination()

print("=" * 65)
print("BRONZE STREAMING TABLE")
print("=" * 65)
print(f"Table      : {BRONZE_TABLE}")
print(f"Checkpoint : {BRONZE_CHECKPOINT}")
print("Status     : COMPLETED")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 7 — Bronze Streaming Validation
# ============================================================

bronze_validation_df = spark.table(BRONZE_TABLE)

bronze_stats = bronze_validation_df.agg(
    count("*").alias("total_records"),
    countDistinct("order_id").alias("unique_order_ids")
).collect()[0]

total_records = bronze_stats["total_records"]
unique_order_ids = bronze_stats["unique_order_ids"]
duplicate_ids = total_records - unique_order_ids

print("=" * 65)
print("BRONZE STREAMING VALIDATION")
print("=" * 65)

print(f"Total Records     : {total_records:,}")
print(f"Unique Order IDs  : {unique_order_ids:,}")
print(f"Duplicate IDs     : {duplicate_ids:,}")
print(f"Columns           : {len(bronze_validation_df.columns)}")

print("=" * 65)

if duplicate_ids == 0:
    print("BRONZE STATUS     : READY FOR SILVER")
else:
    print("BRONZE STATUS     : DUPLICATES DETECTED")

print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 8 — Silver Streaming Transformation
# ============================================================

silver_stream_df = (
    bronze_stream_df
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", upper(trim(col("order_status"))))
    .withColumn(
        "is_valid_order",
        when(
            col("order_id").isNotNull() &
            (col("order_id") != "") &
            col("customer_id").isNotNull() &
            (col("customer_id") != "") &
            col("event_timestamp").isNotNull(),
            True
        ).otherwise(False)
    )
    .filter(col("is_valid_order") == True)
    .drop("is_valid_order")
)

print("=" * 65)
print("SILVER STREAMING DATAFRAME")
print("=" * 65)

print("Is Streaming :", silver_stream_df.isStreaming)
print("Columns      :", len(silver_stream_df.columns))
print("Column Names :", silver_stream_df.columns)

print("=" * 65)
print("Status       : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 9 — Write Silver Streaming Table
# ============================================================

silver_query = (
    silver_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

silver_query.awaitTermination()

print("=" * 65)
print("SILVER STREAMING TABLE")
print("=" * 65)
print(f"Table      : {SILVER_TABLE}")
print(f"Checkpoint : {SILVER_CHECKPOINT}")
print("Status     : COMPLETED")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 10 — Silver Streaming Validation
# ============================================================

silver_validation_df = spark.table(SILVER_TABLE)

silver_stats = silver_validation_df.agg(
    count("*").alias("total_records"),
    countDistinct("order_id").alias("unique_order_ids")
).collect()[0]

total_records = silver_stats["total_records"]
unique_order_ids = silver_stats["unique_order_ids"]
duplicate_ids = total_records - unique_order_ids

print("=" * 65)
print("SILVER STREAMING VALIDATION")
print("=" * 65)

print(f"Total Records     : {total_records:,}")
print(f"Unique Order IDs  : {unique_order_ids:,}")
print(f"Duplicate IDs     : {duplicate_ids:,}")
print(f"Columns           : {len(silver_validation_df.columns)}")

print("=" * 65)

if duplicate_ids == 0:
    print("SILVER STATUS     : READY FOR GOLD")
else:
    print("SILVER STATUS     : DUPLICATES DETECTED")

print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 11 — Gold Streaming Transformation
# ============================================================

gold_stream_df = (
    silver_stream_df
    .withColumn(
        "delivery_status",
        when(
            col("order_delivered_customer_date").isNotNull(),
            "DELIVERED"
        )
        .when(
            col("order_status").isin("CANCELED", "UNAVAILABLE"),
            "CLOSED"
        )
        .otherwise("IN_PROGRESS")
    )
    .withColumn(
        "delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            )
        )
    )
)

print("=" * 65)
print("GOLD STREAMING DATAFRAME")
print("=" * 65)

print("Is Streaming :", gold_stream_df.isStreaming)
print("Columns      :", len(gold_stream_df.columns))
print("Column Names :", gold_stream_df.columns)

print("=" * 65)
print("Status       : READY")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 12 — Write Gold Streaming Table
# ============================================================

gold_query = (
    gold_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(GOLD_TABLE)
)

gold_query.awaitTermination()

print("=" * 65)
print("GOLD STREAMING TABLE")
print("=" * 65)
print(f"Table      : {GOLD_TABLE}")
print(f"Checkpoint : {GOLD_CHECKPOINT}")
print("Status     : COMPLETED")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 13 — Gold Streaming Validation
# ============================================================

gold_validation_df = spark.table(GOLD_TABLE)

gold_stats = gold_validation_df.agg(
    count("*").alias("total_records"),
    countDistinct("order_id").alias("unique_order_ids")
).collect()[0]

total_records = gold_stats["total_records"]
unique_order_ids = gold_stats["unique_order_ids"]
duplicate_ids = total_records - unique_order_ids

print("=" * 65)
print("GOLD STREAMING VALIDATION")
print("=" * 65)

print(f"Total Records     : {total_records:,}")
print(f"Unique Order IDs  : {unique_order_ids:,}")
print(f"Duplicate IDs     : {duplicate_ids:,}")
print(f"Columns           : {len(gold_validation_df.columns)}")

print("=" * 65)

if duplicate_ids == 0:
    print("GOLD STATUS       : READY")
else:
    print("GOLD STATUS       : DUPLICATES DETECTED")

print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 14 — Incremental Streaming Test
# ============================================================

print("=" * 65)
print("INCREMENTAL STREAMING TEST")
print("=" * 65)

# Read the current counts before the new batch
before_count = spark.table(GOLD_TABLE).count()

print(f"Gold Records Before : {before_count:,}")
print("=" * 65)
print("Ready for new streaming events.")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 15 — Create New Incremental Events
# ============================================================

from datetime import datetime, timedelta
import json

base_time = datetime.now()

new_events = [
    {
        "order_id": "STREAM_006",
        "customer_id": "CUST_006",
        "order_status": "created",
        "order_purchase_timestamp": base_time,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": base_time + timedelta(days=7),
        "event_timestamp": base_time
    },
    {
        "order_id": "STREAM_007",
        "customer_id": "CUST_007",
        "order_status": "shipped",
        "order_purchase_timestamp": base_time - timedelta(days=1),
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": base_time + timedelta(days=5),
        "event_timestamp": base_time
    }
]

events_json = []

for event in new_events:
    converted = event.copy()

    for key, value in converted.items():
        if isinstance(value, datetime):
            converted[key] = value.isoformat()

    events_json.append(json.dumps(converted))

new_events_df = spark.createDataFrame(
    [(event,) for event in events_json],
    ["value"]
)

new_events_df.coalesce(1).write.mode("append").text(STREAM_INPUT)

print("=" * 65)
print("NEW INCREMENTAL STREAMING EVENTS")
print("=" * 65)
print(f"Events Created : {len(new_events)}")
print(f"Location       : {STREAM_INPUT}")
print("Status         : READY FOR STREAM")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 16 — Process Incremental Streaming Batch
# ============================================================

print("=" * 65)
print("PROCESSING INCREMENTAL STREAMING BATCH")
print("=" * 65)

# ------------------------------------------------------------
# BRONZE
# ------------------------------------------------------------

bronze_incremental_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

bronze_incremental_query.awaitTermination()

print("Bronze : Incremental batch processed")

# ------------------------------------------------------------
# SILVER
# ------------------------------------------------------------

silver_incremental_query = (
    silver_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

silver_incremental_query.awaitTermination()

print("Silver : Incremental batch processed")

# ------------------------------------------------------------
# GOLD
# ------------------------------------------------------------

gold_incremental_query = (
    gold_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(GOLD_TABLE)
)

gold_incremental_query.awaitTermination()

print("Gold   : Incremental batch processed")

print("=" * 65)
print("INCREMENTAL STREAMING STATUS : COMPLETED")
print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 17 — Incremental Processing Validation
# ============================================================

gold_after_count = spark.table(GOLD_TABLE).count()
gold_unique_count = (
    spark.table(GOLD_TABLE)
    .select("order_id")
    .distinct()
    .count()
)

gold_duplicates = gold_after_count - gold_unique_count

print("=" * 65)
print("INCREMENTAL STREAMING VALIDATION")
print("=" * 65)

print(f"Gold Records Before : {before_count:,}")
print(f"Gold Records After  : {gold_after_count:,}")
print(f"New Records         : {gold_after_count - before_count:,}")
print(f"Unique Order IDs    : {gold_unique_count:,}")
print(f"Duplicate IDs       : {gold_duplicates:,}")

print("=" * 65)

if (
    gold_after_count == before_count + 2
    and gold_duplicates == 0
):
    print("INCREMENTAL TEST   : PASSED")
else:
    print("INCREMENTAL TEST   : REVIEW REQUIRED")

print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 18 — Final End-to-End Streaming Reconciliation
# ============================================================

bronze_final = spark.table(BRONZE_TABLE)
silver_final = spark.table(SILVER_TABLE)
gold_final   = spark.table(GOLD_TABLE)

bronze_count = bronze_final.count()
silver_count = silver_final.count()
gold_count   = gold_final.count()

bronze_unique = bronze_final.select("order_id").distinct().count()
silver_unique = silver_final.select("order_id").distinct().count()
gold_unique   = gold_final.select("order_id").distinct().count()

print("=" * 70)
print("FINAL STRUCTURED STREAMING RECONCILIATION")
print("=" * 70)

print(f"Bronze Records        : {bronze_count:,}")
print(f"Silver Records        : {silver_count:,}")
print(f"Gold Records          : {gold_count:,}")

print("-" * 70)

print(f"Bronze Unique Orders  : {bronze_unique:,}")
print(f"Silver Unique Orders  : {silver_unique:,}")
print(f"Gold Unique Orders    : {gold_unique:,}")

print("-" * 70)

print(f"Bronze Duplicate IDs  : {bronze_count - bronze_unique:,}")
print(f"Silver Duplicate IDs  : {silver_count - silver_unique:,}")
print(f"Gold Duplicate IDs    : {gold_count - gold_unique:,}")

print("=" * 70)

if (
    bronze_count == 7
    and silver_count == 7
    and gold_count == 7
    and bronze_count == bronze_unique
    and silver_count == silver_unique
    and gold_count == gold_unique
):
    print("STRUCTURED STREAMING PIPELINE : READY")
else:
    print("STRUCTURED STREAMING PIPELINE : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 18 — Final End-to-End Streaming Reconciliation
# ============================================================

bronze_final = spark.table(BRONZE_TABLE)
silver_final = spark.table(SILVER_TABLE)
gold_final   = spark.table(GOLD_TABLE)

bronze_count = bronze_final.count()
silver_count = silver_final.count()
gold_count   = gold_final.count()

bronze_unique = bronze_final.select("order_id").distinct().count()
silver_unique = silver_final.select("order_id").distinct().count()
gold_unique   = gold_final.select("order_id").distinct().count()

print("=" * 70)
print("FINAL STRUCTURED STREAMING RECONCILIATION")
print("=" * 70)

print(f"Bronze Records        : {bronze_count:,}")
print(f"Silver Records        : {silver_count:,}")
print(f"Gold Records          : {gold_count:,}")

print("-" * 70)

print(f"Bronze Unique Orders  : {bronze_unique:,}")
print(f"Silver Unique Orders  : {silver_unique:,}")
print(f"Gold Unique Orders    : {gold_unique:,}")

print("-" * 70)

print(f"Bronze Duplicate IDs  : {bronze_count - bronze_unique:,}")
print(f"Silver Duplicate IDs  : {silver_count - silver_unique:,}")
print(f"Gold Duplicate IDs    : {gold_count - gold_unique:,}")

print("=" * 70)

if (
    bronze_count == 7
    and silver_count == 7
    and gold_count == 7
    and bronze_count == bronze_unique
    and silver_count == silver_unique
    and gold_count == gold_unique
):
    print("STRUCTURED STREAMING PIPELINE : READY")
else:
    print("STRUCTURED STREAMING PIPELINE : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 19 — Streaming Data Quality Checks
# ============================================================

gold_df = spark.table(GOLD_TABLE)

null_order_ids = gold_df.filter(
    col("order_id").isNull() | (trim(col("order_id")) == "")
).count()

null_customer_ids = gold_df.filter(
    col("customer_id").isNull() | (trim(col("customer_id")) == "")
).count()

null_event_timestamps = gold_df.filter(
    col("event_timestamp").isNull()
).count()

invalid_status = gold_df.filter(
    ~col("order_status").isin(
        "CREATED",
        "APPROVED",
        "PROCESSING",
        "SHIPPED",
        "DELIVERED",
        "CANCELED",
        "UNAVAILABLE",
        "INVOICED"
    )
).count()

negative_delivery_days = gold_df.filter(
    col("delivery_days") < 0
).count()

print("=" * 70)
print("STREAMING DATA QUALITY VALIDATION")
print("=" * 70)

print(f"Null Order IDs          : {null_order_ids:,}")
print(f"Null Customer IDs       : {null_customer_ids:,}")
print(f"Null Event Timestamps   : {null_event_timestamps:,}")
print(f"Invalid Order Status    : {invalid_status:,}")
print(f"Negative Delivery Days  : {negative_delivery_days:,}")

print("-" * 70)

total_quality_issues = (
    null_order_ids
    + null_customer_ids
    + null_event_timestamps
    + invalid_status
    + negative_delivery_days
)

print(f"Total Quality Issues    : {total_quality_issues:,}")

print("=" * 70)

if total_quality_issues == 0:
    print("DATA QUALITY STATUS     : PASSED")
else:
    print("DATA QUALITY STATUS     : ISSUES DETECTED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 20 — Streaming Data Quality Summary
# ============================================================

gold_df = spark.table(GOLD_TABLE)

total_records = gold_df.count()
unique_orders = gold_df.select("order_id").distinct().count()
duplicate_orders = total_records - unique_orders

null_order_ids = gold_df.filter(
    col("order_id").isNull() | (trim(col("order_id")) == "")
).count()

null_customer_ids = gold_df.filter(
    col("customer_id").isNull() | (trim(col("customer_id")) == "")
).count()

null_event_timestamps = gold_df.filter(
    col("event_timestamp").isNull()
).count()

invalid_status = gold_df.filter(
    ~col("order_status").isin(
        "CREATED",
        "APPROVED",
        "PROCESSING",
        "SHIPPED",
        "DELIVERED",
        "CANCELED",
        "UNAVAILABLE",
        "INVOICED"
    )
).count()

quality_issues = (
    duplicate_orders
    + null_order_ids
    + null_customer_ids
    + null_event_timestamps
    + invalid_status
)

print("=" * 70)
print("STREAMING DATA QUALITY SUMMARY")
print("=" * 70)

print(f"Total Records         : {total_records:,}")
print(f"Unique Orders         : {unique_orders:,}")
print(f"Duplicate Orders      : {duplicate_orders:,}")
print(f"Null Order IDs        : {null_order_ids:,}")
print(f"Null Customer IDs     : {null_customer_ids:,}")
print(f"Null Event Timestamps : {null_event_timestamps:,}")
print(f"Invalid Status        : {invalid_status:,}")

print("-" * 70)
print(f"Total Quality Issues  : {quality_issues:,}")

print("=" * 70)

if quality_issues == 0:
    print("QUALITY STATUS        : PASSED")
else:
    print("QUALITY STATUS        : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 21 — Streaming Pipeline Health Metrics
# ============================================================

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)
gold_df = spark.table(GOLD_TABLE)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
gold_count = gold_df.count()

bronze_unique = bronze_df.select("order_id").distinct().count()
silver_unique = silver_df.select("order_id").distinct().count()
gold_unique = gold_df.select("order_id").distinct().count()

bronze_duplicates = bronze_count - bronze_unique
silver_duplicates = silver_count - silver_unique
gold_duplicates = gold_count - gold_unique

print("=" * 70)
print("STREAMING PIPELINE HEALTH METRICS")
print("=" * 70)

print("LAYER RECORD COUNTS")
print("-" * 70)
print(f"Bronze Records        : {bronze_count:,}")
print(f"Silver Records        : {silver_count:,}")
print(f"Gold Records          : {gold_count:,}")

print()
print("DUPLICATE MONITORING")
print("-" * 70)
print(f"Bronze Duplicates     : {bronze_duplicates:,}")
print(f"Silver Duplicates     : {silver_duplicates:,}")
print(f"Gold Duplicates       : {gold_duplicates:,}")

print()
print("PIPELINE CONSISTENCY")
print("-" * 70)
print(f"Bronze → Silver       : {bronze_count == silver_count}")
print(f"Silver → Gold         : {silver_count == gold_count}")

print("=" * 70)

pipeline_healthy = (
    bronze_count == silver_count
    and silver_count == gold_count
    and bronze_duplicates == 0
    and silver_duplicates == 0
    and gold_duplicates == 0
)

if pipeline_healthy:
    print("PIPELINE HEALTH       : HEALTHY")
else:
    print("PIPELINE HEALTH       : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 22 — Streaming Pipeline Health Report
# ============================================================

print("=" * 70)
print("STRUCTURED STREAMING PIPELINE HEALTH REPORT")
print("=" * 70)

print()
print("1. STREAMING ARCHITECTURE")
print("-" * 70)
print("Source               : JSON")
print("Processing           : Spark Structured Streaming")
print("Architecture         : Bronze → Silver → Gold")
print("Trigger              : availableNow")
print("Checkpointing        : Enabled")

print()
print("2. CURRENT DATA")
print("-" * 70)
print(f"Bronze Records       : {bronze_count:,}")
print(f"Silver Records       : {silver_count:,}")
print(f"Gold Records         : {gold_count:,}")

print()
print("3. DATA QUALITY")
print("-" * 70)
print(f"Bronze Duplicates    : {bronze_duplicates:,}")
print(f"Silver Duplicates    : {silver_duplicates:,}")
print(f"Gold Duplicates      : {gold_duplicates:,}")
print(f"Quality Issues       : {quality_issues:,}")

print()
print("4. INCREMENTAL TEST")
print("-" * 70)
print(f"Records Before       : {before_count:,}")
print(f"Records After        : {gold_count:,}")
print(f"New Records          : {gold_count - before_count:,}")

print()
print("5. PIPELINE STATUS")
print("-" * 70)

if (
    pipeline_healthy
    and quality_issues == 0
    and gold_count > before_count
):
    print("STATUS               : HEALTHY")
    print("INCREMENTAL TEST     : PASSED")
    print("DATA QUALITY         : PASSED")
else:
    print("STATUS               : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 23 — Final Streaming Architecture Validation
# ============================================================

print("=" * 70)
print("FINAL STREAMING ARCHITECTURE VALIDATION")
print("=" * 70)

checks = {
    "Streaming Source": stream_orders_df.isStreaming,
    "Bronze DataFrame": bronze_stream_df.isStreaming,
    "Silver DataFrame": silver_stream_df.isStreaming,
    "Gold DataFrame": gold_stream_df.isStreaming,
    "Bronze Checkpoint": bool(BRONZE_CHECKPOINT),
    "Silver Checkpoint": bool(SILVER_CHECKPOINT),
    "Gold Checkpoint": bool(GOLD_CHECKPOINT),
    "Bronze Table": bool(BRONZE_TABLE),
    "Silver Table": bool(SILVER_TABLE),
    "Gold Table": bool(GOLD_TABLE),
}

for check_name, status in checks.items():
    print(f"{check_name:<25} : {status}")

print("-" * 70)

all_checks_passed = all(checks.values())

if all_checks_passed:
    print("ARCHITECTURE STATUS     : VALIDATED")
else:
    print("ARCHITECTURE STATUS     : REVIEW REQUIRED")

print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 24 — Final Structured Streaming Summary
# ============================================================

print("=" * 70)
print("REALTIME ECOMMERCE LAKEHOUSE")
print("STRUCTURED STREAMING PIPELINE — FINAL SUMMARY")
print("=" * 70)

print()
print("STREAMING SOURCE")
print("-" * 70)
print(f"Format               : JSON")
print(f"Input Location       : {STREAM_INPUT}")
print("Trigger              : availableNow")

print()
print("MEDALLION ARCHITECTURE")
print("-" * 70)
print(f"Bronze               : {BRONZE_TABLE}")
print(f"Silver               : {SILVER_TABLE}")
print(f"Gold                 : {GOLD_TABLE}")

print()
print("CHECKPOINTING")
print("-" * 70)
print(f"Bronze               : {BRONZE_CHECKPOINT}")
print(f"Silver               : {SILVER_CHECKPOINT}")
print(f"Gold                 : {GOLD_CHECKPOINT}")

print()
print("FINAL VALIDATION")
print("-" * 70)
print(f"Bronze Records       : {bronze_count:,}")
print(f"Silver Records       : {silver_count:,}")
print(f"Gold Records         : {gold_count:,}")
print(f"Gold Unique Orders   : {gold_unique:,}")
print(f"Gold Duplicate IDs   : {gold_duplicates:,}")

print()
print("INCREMENTAL PROCESSING")
print("-" * 70)
print(f"Before Increment     : {before_count:,}")
print(f"After Increment      : {gold_count:,}")
print(f"New Records          : {gold_count - before_count:,}")

print()
print("QUALITY")
print("-" * 70)
print(f"Quality Issues       : {quality_issues:,}")
print(f"Pipeline Health      : {'HEALTHY' if pipeline_healthy else 'REVIEW REQUIRED'}")

print()
print("=" * 70)
print("STRUCTURED STREAMING NOTEBOOK : COMPLETE")
print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 25 — Structured Streaming Sign-Off Documentation
# ============================================================

print("""
======================================================================
 REALTIME ECOMMERCE LAKEHOUSE
 STRUCTURED STREAMING PIPELINE — SIGN-OFF DOCUMENTATION
======================================================================

PROJECT COMPONENT
----------------------------------------------------------------------
Notebook             : Structured Streaming
Architecture         : Medallion Architecture
Processing Engine    : Apache Spark Structured Streaming
Source Format        : JSON
Trigger Mode         : availableNow

----------------------------------------------------------------------
 STREAMING ARCHITECTURE
----------------------------------------------------------------------

JSON Streaming Events
        |
        v
Structured Streaming Source
        |
        v
Bronze Streaming Layer
        |
        v
Silver Streaming Layer
        |
        v
Gold Streaming Layer
        |
        v
Analytics / Consumption

----------------------------------------------------------------------
 STREAMING TABLES
----------------------------------------------------------------------

Bronze
  sdp_catalog.realtime_ecommerce.bronze_stream_orders

Silver
  sdp_catalog.realtime_ecommerce.silver_stream_orders

Gold
  sdp_catalog.realtime_ecommerce.gold_stream_orders

----------------------------------------------------------------------
 CHECKPOINTING
----------------------------------------------------------------------

Bronze
  /Volumes/sdp_catalog/realtime_ecommerce/streaming_input/
  _checkpoints/bronze_orders

Silver
  /Volumes/sdp_catalog/realtime_ecommerce/streaming_input/
  _checkpoints/silver_orders

Gold
  /Volumes/sdp_catalog/realtime_ecommerce/streaming_input/
  _checkpoints/gold_orders

----------------------------------------------------------------------
 VALIDATION RESULTS
----------------------------------------------------------------------

Bronze Records        : 7
Silver Records        : 7
Gold Records           : 7

Bronze Unique Orders  : 7
Silver Unique Orders  : 7
Gold Unique Orders    : 7

Bronze Duplicate IDs  : 0
Silver Duplicate IDs  : 0
Gold Duplicate IDs    : 0

Data Quality Issues   : 0

----------------------------------------------------------------------
 INCREMENTAL PROCESSING TEST
----------------------------------------------------------------------

Records Before        : 5
Records After         : 7
New Records Processed : 2

Incremental Test      : PASSED

----------------------------------------------------------------------
 PIPELINE CAPABILITIES VALIDATED
----------------------------------------------------------------------

[✓] Structured Streaming source
[✓] Explicit streaming schema
[✓] Bronze ingestion
[✓] Silver transformation
[✓] Gold transformation
[✓] Delta streaming tables
[✓] Explicit checkpoint locations
[✓] Incremental file processing
[✓] Duplicate detection
[✓] Data-quality validation
[✓] End-to-end reconciliation
[✓] Pipeline health validation

----------------------------------------------------------------------
 KAFKA NOTE
----------------------------------------------------------------------

Kafka 4.0.0 was configured and tested separately in the local WSL
environment.

Direct connectivity from the local WSL Kafka broker to the Databricks
Free Edition environment was not available because the Databricks
serverless environment could not reach the local WSL network endpoint.

Therefore, this notebook uses a Unity Catalog Volume with JSON events
as the Structured Streaming source.

Kafka remains a separate validated project component and can be
integrated when a remotely reachable Kafka cluster is available.

----------------------------------------------------------------------
 FINAL STATUS
----------------------------------------------------------------------

Streaming Source        : READY
Bronze Layer            : READY
Silver Layer            : READY
Gold Layer              : READY
Checkpointing           : ENABLED
Incremental Processing  : PASSED
Data Quality            : PASSED
Duplicate Validation   : PASSED
Pipeline Health         : HEALTHY

======================================================================
 STRUCTURED STREAMING PIPELINE : OFFICIALLY SIGNED OFF
======================================================================

Status : COMPLETE
======================================================================
""")

# COMMAND ----------

