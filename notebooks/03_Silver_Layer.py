# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC The Silver Layer transforms Bronze data into clean, standardized, validated, and analytics-ready datasets.
# MAGIC
# MAGIC Unlike the Bronze Layer, where source data is preserved with minimal changes, the Silver Layer applies data-quality rules and business transformations.
# MAGIC
# MAGIC ### Responsibilities
# MAGIC
# MAGIC - Read data from Bronze Delta tables
# MAGIC - Standardize column values
# MAGIC - Handle business-valid NULL values
# MAGIC - Remove invalid records
# MAGIC - Deduplicate where appropriate
# MAGIC - Derive useful business attributes
# MAGIC - Apply data-quality rules
# MAGIC - Create relationships between datasets
# MAGIC - Persist transformed data as Delta tables
# MAGIC
# MAGIC ### Design Principle
# MAGIC
# MAGIC Bronze preserves the source.
# MAGIC
# MAGIC Silver improves the data.
# MAGIC
# MAGIC Gold serves business analytics.

# COMMAND ----------

# ============================================================
# Silver Layer Configuration
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

BRONZE_ORDERS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_orders"
BRONZE_CUSTOMERS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_customers"
BRONZE_ORDER_ITEMS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_order_items"
BRONZE_PAYMENTS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_payments"
BRONZE_PRODUCTS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_products"
BRONZE_SELLERS = f"{CATALOG}.{PROJECT_SCHEMA}.bronze_sellers"

SILVER_ORDERS = f"{CATALOG}.{PROJECT_SCHEMA}.silver_orders"

print("Silver target:")
print(SILVER_ORDERS)

# COMMAND ----------

# ============================================================
# Read Bronze Orders
# ============================================================

orders_bronze_df = spark.table(BRONZE_ORDERS)

print(f"Bronze Orders Records: {orders_bronze_df.count():,}")
print(f"Bronze Orders Columns: {len(orders_bronze_df.columns)}")

# COMMAND ----------

orders_bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, when

orders_null_profile = orders_bronze_df.select(
    [
        count(
            when(col(c).isNull(), 1)
        ).alias(c)
        for c in orders_bronze_df.columns
    ]
)

display(orders_null_profile)

# COMMAND ----------

from pyspark.sql.functions import trim, lower

orders_silver_df = (
    orders_bronze_df
    .withColumn(
        "order_status",
        lower(trim(col("order_status")))
    )
)

# COMMAND ----------

display(
    orders_silver_df
    .groupBy("order_status")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

silver_order_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

orders_silver_df = orders_silver_df.select(
    *silver_order_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Lifecycle Validation
# MAGIC
# MAGIC An order follows a logical lifecycle:
# MAGIC
# MAGIC Purchase → Approval → Carrier Handover → Customer Delivery
# MAGIC
# MAGIC The Silver layer validates these timestamps and identifies records with logically inconsistent dates.
# MAGIC
# MAGIC Invalid records are flagged rather than immediately deleted, allowing the pipeline to preserve traceability.

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    when
)

orders_silver_df = (
    orders_silver_df
    .withColumn(
        "is_timestamp_valid",
        when(
            (col("order_approved_at").isNotNull()) &
            (col("order_approved_at") < col("order_purchase_timestamp")),
            False
        )
        .when(
            (col("order_delivered_carrier_date").isNotNull()) &
            (col("order_delivered_carrier_date") < col("order_purchase_timestamp")),
            False
        )
        .when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_customer_date") < col("order_purchase_timestamp")),
            False
        )
        .when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_carrier_date").isNotNull()) &
            (col("order_delivered_customer_date") < col("order_delivered_carrier_date")),
            False
        )
        .otherwise(True)
    )
)

# COMMAND ----------

display(
    orders_silver_df
    .groupBy("is_timestamp_valid")
    .count()
    .orderBy("is_timestamp_valid")
)

# COMMAND ----------

from pyspark.sql.functions import (
    unix_timestamp
)

orders_silver_df = (
    orders_silver_df
    .withColumn(
        "approval_duration_minutes",
        when(
            col("order_approved_at").isNotNull(),
            (
                unix_timestamp("order_approved_at") -
                unix_timestamp("order_purchase_timestamp")
            ) / 60
        )
    )
)

# COMMAND ----------

orders_silver_df = (
    orders_silver_df
    .withColumn(
        "delivery_duration_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            (
                unix_timestamp("order_delivered_customer_date") -
                unix_timestamp("order_purchase_timestamp")
            ) / 86400
        )
    )
)

# COMMAND ----------

orders_silver_df = (
    orders_silver_df
    .withColumn(
        "is_late_delivery",
        when(
            col("order_delivered_customer_date").isNull(),
            None
        )
        .when(
            col("order_delivered_customer_date") >
            col("order_estimated_delivery_date"),
            True
        )
        .otherwise(False)
    )
)

# COMMAND ----------

display(
    orders_silver_df.select(
        "order_id",
        "order_status",
        "approval_duration_minutes",
        "delivery_duration_days",
        "is_late_delivery",
        "is_timestamp_valid"
    ).limit(20)
)

# COMMAND ----------

display(
    orders_silver_df.groupBy(
        "is_timestamp_valid",
        "is_late_delivery"
    ).count()
)

# COMMAND ----------

invalid_orders_df = (
    orders_silver_df
    .filter(col("is_timestamp_valid") == False)
)

print(f"Invalid timestamp records: {invalid_orders_df.count():,}")

display(
    invalid_orders_df.select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ).limit(20)
)

# COMMAND ----------

orders_silver_df = (
    orders_silver_df
    .withColumn(
        "timestamp_quality_issue",
        when(
            (col("order_approved_at").isNotNull()) &
            (col("order_approved_at") < col("order_purchase_timestamp")),
            "approval_before_purchase"
        )
        .when(
            (col("order_delivered_carrier_date").isNotNull()) &
            (col("order_delivered_carrier_date") < col("order_purchase_timestamp")),
            "carrier_before_purchase"
        )
        .when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_customer_date") < col("order_purchase_timestamp")),
            "delivery_before_purchase"
        )
        .when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_carrier_date").isNotNull()) &
            (col("order_delivered_customer_date") < col("order_delivered_carrier_date")),
            "delivery_before_carrier"
        )
        .otherwise(None)
    )
)

# COMMAND ----------

display(
    orders_silver_df
    .groupBy("timestamp_quality_issue")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# ============================================================
# Keep only records that pass timestamp validation
# ============================================================

silver_valid_orders_df = (
    orders_silver_df
    .filter(col("is_timestamp_valid") == True)
)

print(
    f"Valid Silver Orders: "
    f"{silver_valid_orders_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Quarantine invalid records
# ============================================================

quarantine_orders_df = (
    orders_silver_df
    .filter(col("is_timestamp_valid") == False)
)

print(
    f"Quarantined Orders: "
    f"{quarantine_orders_df.count():,}"
)

# COMMAND ----------

total_records = orders_silver_df.count()
valid_records = silver_valid_orders_df.count()
quarantined_records = quarantine_orders_df.count()

print(f"Total Records       : {total_records:,}")
print(f"Valid Records       : {valid_records:,}")
print(f"Quarantined Records : {quarantined_records:,}")
print(f"Reconciled Records  : {valid_records + quarantined_records:,}")

# COMMAND ----------

silver_valid_orders_df = (
    silver_valid_orders_df
    .drop(
        "is_timestamp_valid",
        "timestamp_quality_issue"
    )
)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

silver_valid_orders_df = (
    silver_valid_orders_df
    .withColumn(
        "approval_duration_minutes",
        when(
            col("order_approved_at").isNotNull(),
            (
                unix_timestamp("order_approved_at")
                - unix_timestamp("order_purchase_timestamp")
            ) / 60
        )
    )
    .withColumn(
        "delivery_duration_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            (
                unix_timestamp("order_delivered_customer_date")
                - unix_timestamp("order_purchase_timestamp")
            ) / 86400
        )
    )
    .withColumn(
        "is_late_delivery",
        when(
            col("order_delivered_customer_date").isNull(),
            None
        )
        .when(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date"),
            True
        )
        .otherwise(False)
    )
)

# COMMAND ----------

display(
    silver_valid_orders_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "approval_duration_minutes",
        "delivery_duration_days",
        "is_late_delivery"
    ).limit(20)
)

# COMMAND ----------

display(
    silver_valid_orders_df.select(
        "approval_duration_minutes",
        "delivery_duration_days",
        "is_late_delivery"
    ).describe()
)

# COMMAND ----------

# MAGIC %md
# MAGIC bronze_orders
# MAGIC       │
# MAGIC       ▼
# MAGIC Silver transformations
# MAGIC       │
# MAGIC       ├── standardized status
# MAGIC       ├── timestamp validation
# MAGIC       ├── quality classification
# MAGIC       ├── quarantine
# MAGIC       ├── approval duration
# MAGIC       ├── delivery duration
# MAGIC       └── late delivery flag

# COMMAND ----------

# ============================================================
# Silver Table Configuration
# ============================================================

SILVER_ORDERS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_orders"
)

SILVER_QUARANTINE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_orders_quarantine"
)

print("Silver Orders      :", SILVER_ORDERS)
print("Silver Quarantine  :", SILVER_QUARANTINE)

# COMMAND ----------

# ============================================================
# Persist Clean Silver Orders
# ============================================================

(
    silver_valid_orders_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_ORDERS)
)

print("Silver Orders table created successfully.")

# COMMAND ----------

# ============================================================
# Persist Invalid Records for Investigation
# ============================================================

(
    quarantine_orders_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_QUARANTINE)
)

print("Silver quarantine table created successfully.")

# COMMAND ----------

silver_check_df = spark.table(SILVER_ORDERS)

print(f"Silver Orders Records : {silver_check_df.count():,}")
print(f"Silver Orders Columns : {len(silver_check_df.columns)}")

# COMMAND ----------

quarantine_check_df = spark.table(SILVER_QUARANTINE)

print(
    f"Quarantine Records : "
    f"{quarantine_check_df.count():,}"
)

# COMMAND ----------

silver_check_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids
# MAGIC FROM sdp_catalog.realtime_ecommerce.silver_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Orders Summary
# MAGIC
# MAGIC The Orders dataset has been transformed from Bronze into a validated Silver dataset.
# MAGIC
# MAGIC ### Processing performed
# MAGIC
# MAGIC - Standardized order status
# MAGIC - Validated order lifecycle timestamps
# MAGIC - Classified timestamp quality issues
# MAGIC - Quarantined invalid records
# MAGIC - Derived approval duration
# MAGIC - Derived delivery duration
# MAGIC - Derived late-delivery indicator
# MAGIC - Persisted clean data as Delta
# MAGIC - Preserved invalid records for investigation
# MAGIC
# MAGIC ### Reconciliation
# MAGIC
# MAGIC Total Bronze Orders: 99,441
# MAGIC
# MAGIC Valid Silver Orders: 99,252
# MAGIC
# MAGIC Quarantined Orders: 189
# MAGIC
# MAGIC Total Reconciled: 99,441
# MAGIC
# MAGIC The Silver Orders dataset is now ready for enrichment with customer, product, seller, order-item, and payment data.

# COMMAND ----------

# ============================================================
# Cell 34 — Read Bronze Customers
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

BRONZE_CUSTOMERS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_customers"
)

customers_bronze_df = spark.table(BRONZE_CUSTOMERS)

print(f"Bronze Customers Records: {customers_bronze_df.count():,}")
print(f"Bronze Customers Columns: {len(customers_bronze_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 35 — Customer Data Quality Check
# ============================================================

from pyspark.sql.functions import col, count, when

customer_null_profile = customers_bronze_df.select(
    [
        count(
            when(col(c).isNull(), 1)
        ).alias(c)
        for c in customers_bronze_df.columns
    ]
)

display(customer_null_profile)

# COMMAND ----------

# ============================================================
# Cell 36 — Standardize Customer Attributes
# ============================================================

from pyspark.sql.functions import col, lower, upper, trim

silver_customers_df = (
    customers_bronze_df
    .withColumn(
        "customer_city",
        lower(trim(col("customer_city")))
    )
    .withColumn(
        "customer_state",
        upper(trim(col("customer_state")))
    )
)

print("Customer attributes standardized successfully.")

# COMMAND ----------

# ============================================================
# Cell 37 — Select Silver Customer Columns
# ============================================================

silver_customers_df = silver_customers_df.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
)

print("Final Silver customer columns selected.")
print(f"Columns: {len(silver_customers_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 38 — Validate Silver Customers
# ============================================================

total_customers = silver_customers_df.count()

unique_customer_ids = (
    silver_customers_df
    .select("customer_id")
    .distinct()
    .count()
)

print(f"Silver Customers Records : {total_customers:,}")
print(f"Unique Customer IDs      : {unique_customer_ids:,}")
print(f"Duplicate Customer IDs   : {total_customers - unique_customer_ids:,}")

# COMMAND ----------

# ============================================================
# Cell 39 — Persist Silver Customers
# ============================================================

SILVER_CUSTOMERS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_customers"
)

(
    silver_customers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_CUSTOMERS)
)

print(f"Successfully created: {SILVER_CUSTOMERS}")

# COMMAND ----------

# ============================================================
# Cell 40 — Verify Silver Customers
# ============================================================

silver_customers_check = spark.table(SILVER_CUSTOMERS)

print(
    f"Silver Customers Records: "
    f"{silver_customers_check.count():,}"
)

print(
    f"Silver Customers Columns: "
    f"{len(silver_customers_check.columns)}"
)

# COMMAND ----------

# ============================================================
# Cell 41 — Read Bronze Order Items
# ============================================================

BRONZE_ORDER_ITEMS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_order_items"
)

order_items_bronze_df = spark.table(BRONZE_ORDER_ITEMS)

print(f"Bronze Order Items Records: {order_items_bronze_df.count():,}")
print(f"Bronze Order Items Columns: {len(order_items_bronze_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 42 — Order Items Data Quality Check
# ============================================================

from pyspark.sql.functions import col, count, when

order_items_quality_df = order_items_bronze_df.select(
    count(when(col("order_id").isNull(), 1)).alias("null_order_id"),
    count(when(col("order_item_id").isNull(), 1)).alias("null_order_item_id"),
    count(when(col("product_id").isNull(), 1)).alias("null_product_id"),
    count(when(col("seller_id").isNull(), 1)).alias("null_seller_id"),
    count(when(col("price").isNull(), 1)).alias("null_price"),
    count(when(col("freight_value").isNull(), 1)).alias("null_freight_value"),
    count(when(col("price") < 0, 1)).alias("negative_price"),
    count(when(col("freight_value") < 0, 1)).alias("negative_freight")
)

display(order_items_quality_df)

# COMMAND ----------

quality_row = order_items_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value}")

# COMMAND ----------

# ============================================================
# Cell 43 — Validate Order Item Uniqueness
# ============================================================

total_order_items = order_items_bronze_df.count()

unique_order_items = (
    order_items_bronze_df
    .select("order_id", "order_item_id")
    .distinct()
    .count()
)

print(f"Total Order Items       : {total_order_items:,}")
print(f"Unique Order-Item Keys  : {unique_order_items:,}")
print(f"Duplicate Keys          : {total_order_items - unique_order_items:,}")

# COMMAND ----------

# ============================================================
# Cell 44 — Create Silver Order Items
# ============================================================

silver_order_items_df = order_items_bronze_df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
)

print("Silver Order Items DataFrame created successfully.")
print(f"Columns: {len(silver_order_items_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 45 — Derive Item Total Value
# ============================================================

from pyspark.sql.functions import round

silver_order_items_df = (
    silver_order_items_df
    .withColumn(
        "item_total_value",
        round(
            col("price") + col("freight_value"),
            2
        )
    )
)

print("Item total value derived successfully.")

# COMMAND ----------

# ============================================================
# Cell 46 — Validate Item Total Value
# ============================================================

item_value_quality_df = silver_order_items_df.select(
    count("*").alias("total_records"),
    count(
        when(col("item_total_value").isNull(), 1)
    ).alias("null_item_total_value"),
    count(
        when(col("item_total_value") < 0, 1)
    ).alias("negative_item_total_value")
)

quality_row = item_value_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value:,}")

# COMMAND ----------

# ============================================================
# Cell 47 — Persist Silver Order Items
# ============================================================

SILVER_ORDER_ITEMS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_order_items"
)

(
    silver_order_items_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_ORDER_ITEMS)
)

print(f"Successfully created: {SILVER_ORDER_ITEMS}")

# COMMAND ----------

# ============================================================
# Cell 48 — Verify Silver Order Items
# ============================================================

silver_order_items_check = spark.table(SILVER_ORDER_ITEMS)

print(f"Silver Order Items Records: {silver_order_items_check.count():,}")
print(f"Silver Order Items Columns: {len(silver_order_items_check.columns)}")

# COMMAND ----------

# ============================================================
# Cell 49 — Read Bronze Payments
# ============================================================

BRONZE_PAYMENTS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_payments"
)

payments_bronze_df = spark.table(BRONZE_PAYMENTS)

print(f"Bronze Payments Records: {payments_bronze_df.count():,}")
print(f"Bronze Payments Columns: {len(payments_bronze_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 50 — Payments Data Quality Check
# ============================================================

from pyspark.sql.functions import col, count, when

payments_quality_df = payments_bronze_df.select(
    count(when(col("order_id").isNull(), 1)).alias("null_order_id"),
    count(when(col("payment_sequential").isNull(), 1)).alias("null_payment_sequential"),
    count(when(col("payment_type").isNull(), 1)).alias("null_payment_type"),
    count(when(col("payment_installments").isNull(), 1)).alias("null_payment_installments"),
    count(when(col("payment_value").isNull(), 1)).alias("null_payment_value"),
    count(when(col("payment_value") < 0, 1)).alias("negative_payment_value")
)

quality_row = payments_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value}")

# COMMAND ----------

# ============================================================
# Cell 51 — Validate Payment Record Uniqueness
# ============================================================

total_payments = payments_bronze_df.count()

unique_payment_records = (
    payments_bronze_df
    .select("order_id", "payment_sequential")
    .distinct()
    .count()
)

print(f"Total Payment Records     : {total_payments:,}")
print(f"Unique Payment Keys       : {unique_payment_records:,}")
print(f"Duplicate Payment Keys    : {total_payments - unique_payment_records:,}")

# COMMAND ----------

# ============================================================
# Cell 52 — Create Silver Payments DataFrame
# ============================================================

from pyspark.sql.functions import lower, trim

silver_payments_df = (
    payments_bronze_df
    .withColumn(
        "payment_type",
        lower(trim(col("payment_type")))
    )
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
)

print("Silver Payments DataFrame created successfully.")
print(f"Columns: {len(silver_payments_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 53 — Validate Payment Types
# ============================================================

display(
    silver_payments_df
    .groupBy("payment_type")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# ============================================================
# Cell 54 — Validate Payment Values
# ============================================================

payment_value_check = silver_payments_df.select(
    count("*").alias("total_records"),
    count(
        when(col("payment_value") <= 0, 1)
    ).alias("zero_or_negative_payments"),
    count(
        when(col("payment_installments") <= 0, 1)
    ).alias("invalid_installments")
)

quality_row = payment_value_check.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value:,}")

# COMMAND ----------

# ============================================================
# Cell 55 — Inspect Payment Anomalies
# ============================================================

payment_anomalies_df = (
    silver_payments_df
    .filter(
        (col("payment_value") <= 0) |
        (col("payment_installments") <= 0)
    )
)

print(f"Payment anomalies found: {payment_anomalies_df.count()}")

display(
    payment_anomalies_df.orderBy(
        "order_id",
        "payment_sequential"
    )
)

# COMMAND ----------

# ============================================================
# Cell 56 — Inspect Payment Anomaly Details
# ============================================================

display(
    payment_anomalies_df.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
)

# COMMAND ----------

# ============================================================
# Cell 57 — Classify Payment Quality Issues
# ============================================================

from pyspark.sql.functions import when

silver_payments_df = (
    silver_payments_df
    .withColumn(
        "payment_quality_issue",
        when(
            col("payment_value") == 0,
            "zero_payment_value"
        )
        .when(
            col("payment_installments") == 0,
            "zero_installments"
        )
        .otherwise(None)
    )
)

# COMMAND ----------

display(
    silver_payments_df
    .groupBy("payment_quality_issue")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# ============================================================
# Cell 59 — Payment Quality Reconciliation
# ============================================================

total_payments = silver_payments_df.count()

flagged_payments = (
    silver_payments_df
    .filter(col("payment_quality_issue").isNotNull())
    .count()
)

print(f"Total Payment Records : {total_payments:,}")
print(f"Flagged Records       : {flagged_payments:,}")
print(f"Clean Records         : {total_payments - flagged_payments:,}")

# COMMAND ----------

# ============================================================
# Cell 60 — Persist Silver Payments
# ============================================================

SILVER_PAYMENTS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_payments"
)

(
    silver_payments_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_PAYMENTS)
)

print(f"Successfully created: {SILVER_PAYMENTS}")

# COMMAND ----------

# ============================================================
# Cell 61 — Verify Silver Payments
# ============================================================

silver_payments_check = spark.table(SILVER_PAYMENTS)

print(f"Silver Payment Records : {silver_payments_check.count():,}")
print(f"Silver Payment Columns : {len(silver_payments_check.columns)}")

# COMMAND ----------

# ============================================================
# Cell 62 — Read Bronze Products
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

BRONZE_PRODUCTS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_products"
)

products_bronze_df = spark.table(BRONZE_PRODUCTS)

print(f"Bronze Products Records: {products_bronze_df.count():,}")
print(f"Bronze Products Columns: {len(products_bronze_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 63 — Product Data Quality Profile
# ============================================================

from pyspark.sql.functions import col, count, when

product_quality_df = products_bronze_df.select(
    count(when(col("product_id").isNull(), 1)).alias("null_product_id"),
    count(
        when(col("product_category_name").isNull(), 1)
    ).alias("null_category"),
    count(
        when(col("product_name_lenght").isNull(), 1)
    ).alias("null_name_length"),
    count(
        when(col("product_description_lenght").isNull(), 1)
    ).alias("null_description_length"),
    count(
        when(col("product_photos_qty").isNull(), 1)
    ).alias("null_photos_qty"),
    count(
        when(col("product_weight_g").isNull(), 1)
    ).alias("null_weight"),
    count(
        when(col("product_length_cm").isNull(), 1)
    ).alias("null_length"),
    count(
        when(col("product_height_cm").isNull(), 1)
    ).alias("null_height"),
    count(
        when(col("product_width_cm").isNull(), 1)
    ).alias("null_width")
)

quality_row = product_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value:,}")

# COMMAND ----------

# ============================================================
# Cell 64 — Inspect Products with Missing Attributes
# ============================================================

missing_product_attributes_df = (
    products_bronze_df
    .filter(
        col("product_category_name").isNull() |
        col("product_weight_g").isNull()
    )
)

print(
    f"Products with missing attributes: "
    f"{missing_product_attributes_df.count():,}"
)

display(
    missing_product_attributes_df.select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ).limit(20)
)

# COMMAND ----------

# ============================================================
# Cell 65 — Product Data Quality Flags
# ============================================================

from pyspark.sql.functions import col, when, lower, trim

silver_products_df = (
    products_bronze_df
    .withColumn(
        "product_category_name",
        when(
            col("product_category_name").isNull(),
            "unknown"
        ).otherwise(
            lower(trim(col("product_category_name")))
        )
    )
    .withColumn(
        "has_missing_descriptive_attributes",
        when(
            col("product_name_lenght").isNull() |
            col("product_description_lenght").isNull() |
            col("product_photos_qty").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "has_missing_physical_attributes",
        when(
            col("product_weight_g").isNull() |
            col("product_length_cm").isNull() |
            col("product_height_cm").isNull() |
            col("product_width_cm").isNull(),
            True
        ).otherwise(False)
    )
)

print("Product quality flags created successfully.")

# COMMAND ----------

# ============================================================
# Cell 66 — Validate Product Quality Flags
# ============================================================

print(
    "Products with missing descriptive attributes:",
    silver_products_df
        .filter(col("has_missing_descriptive_attributes") == True)
        .count()
)

print(
    "Products with missing physical attributes:",
    silver_products_df
        .filter(col("has_missing_physical_attributes") == True)
        .count()
)

print(
    "Unknown product categories:",
    silver_products_df
        .filter(col("product_category_name") == "unknown")
        .count()
)

# COMMAND ----------

# ============================================================
# Cell 67 — Select Final Silver Product Columns
# ============================================================

silver_products_df = silver_products_df.select(
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "has_missing_descriptive_attributes",
    "has_missing_physical_attributes"
)

print("Final Silver product columns selected.")
print(f"Columns: {len(silver_products_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 68 — Derive Product Volume
# ============================================================

from pyspark.sql.functions import round

silver_products_df = (
    silver_products_df
    .withColumn(
        "product_volume_cm3",
        when(
            col("product_length_cm").isNotNull() &
            col("product_height_cm").isNotNull() &
            col("product_width_cm").isNotNull(),
            round(
                col("product_length_cm") *
                col("product_height_cm") *
                col("product_width_cm"),
                2
            )
        )
    )
)

print("Product volume derived successfully.")

# COMMAND ----------

# ============================================================
# Cell 69 — Validate Product Volume
# ============================================================

product_volume_quality_df = silver_products_df.select(
    count("*").alias("total_products"),
    count(
        when(col("product_volume_cm3").isNull(), 1)
    ).alias("null_product_volume"),
    count(
        when(col("product_volume_cm3") <= 0, 1)
    ).alias("zero_or_negative_volume")
)

quality_row = product_volume_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value:,}")

# COMMAND ----------

# ============================================================
# Cell 70 — Persist Silver Products
# ============================================================

SILVER_PRODUCTS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_products"
)

(
    silver_products_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_PRODUCTS)
)

print(f"Successfully created: {SILVER_PRODUCTS}")

# COMMAND ----------

# ============================================================
# Cell 71 — Verify Silver Products
# ============================================================

silver_products_check = spark.table(SILVER_PRODUCTS)

print(f"Silver Products Records: {silver_products_check.count():,}")
print(f"Silver Products Columns: {len(silver_products_check.columns)}")

# COMMAND ----------

# ============================================================
# Cell 72 — Read Bronze Sellers
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

BRONZE_SELLERS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.bronze_sellers"
)

sellers_bronze_df = spark.table(BRONZE_SELLERS)

print(f"Bronze Sellers Records: {sellers_bronze_df.count():,}")
print(f"Bronze Sellers Columns: {len(sellers_bronze_df.columns)}")

# COMMAND ----------

sellers_bronze_df.printSchema()

print(sellers_bronze_df.columns)

# COMMAND ----------

# ============================================================
# Cell 73 — Sellers Data Quality Check
# ============================================================

from pyspark.sql.functions import col, count, when

sellers_quality_df = sellers_bronze_df.select(
    count(when(col("seller_id").isNull(), 1)).alias("null_seller_id"),
    count(when(col("seller_zip_code_prefix").isNull(), 1)).alias("null_zip_code"),
    count(when(col("seller_city").isNull(), 1)).alias("null_city"),
    count(when(col("seller_state").isNull(), 1)).alias("null_state")
)

quality_row = sellers_quality_df.collect()[0]

for column_name, value in quality_row.asDict().items():
    print(f"{column_name}: {value:,}")

# COMMAND ----------

# ============================================================
# Cell 74 — Validate Seller ID Uniqueness
# ============================================================

total_sellers = sellers_bronze_df.count()

unique_sellers = (
    sellers_bronze_df
    .select("seller_id")
    .distinct()
    .count()
)

print(f"Total Sellers         : {total_sellers:,}")
print(f"Unique Seller IDs     : {unique_sellers:,}")
print(f"Duplicate Seller IDs  : {total_sellers - unique_sellers:,}")

# COMMAND ----------

# ============================================================
# Cell 75 — Standardize Seller Attributes
# ============================================================

from pyspark.sql.functions import col, lower, upper, trim

silver_sellers_df = (
    sellers_bronze_df
    .withColumn(
        "seller_city",
        lower(trim(col("seller_city")))
    )
    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )
)

print("Seller location attributes standardized successfully.")

# COMMAND ----------

# ============================================================
# Cell 76 — Select Silver Seller Columns
# ============================================================

silver_sellers_df = silver_sellers_df.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
)

print("Final Silver seller columns selected.")
print(f"Columns: {len(silver_sellers_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 77 — Validate Silver Sellers
# ============================================================

total_silver_sellers = silver_sellers_df.count()

unique_silver_sellers = (
    silver_sellers_df
    .select("seller_id")
    .distinct()
    .count()
)

print(f"Silver Sellers Records : {total_silver_sellers:,}")
print(f"Unique Seller IDs      : {unique_silver_sellers:,}")
print(f"Duplicate Seller IDs   : {total_silver_sellers - unique_silver_sellers:,}")

# COMMAND ----------

# ============================================================
# Cell 78 — Persist Silver Sellers
# ============================================================

SILVER_SELLERS = (
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_sellers"
)

(
    silver_sellers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_SELLERS)
)

print(f"Successfully created: {SILVER_SELLERS}")

# COMMAND ----------

# ============================================================
# Cell 79 — Verify Silver Sellers
# ============================================================

silver_sellers_check = spark.table(SILVER_SELLERS)

print(f"Silver Sellers Records : {silver_sellers_check.count():,}")
print(f"Silver Sellers Columns : {len(silver_sellers_check.columns)}")

# COMMAND ----------

# ============================================================
# Cell 80 — Load Silver Tables for Referential Integrity Checks
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

silver_orders = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_orders"
)

silver_customers = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_customers"
)

silver_order_items = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_order_items"
)

silver_payments = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_payments"
)

silver_products = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_products"
)

silver_sellers = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_sellers"
)

print("All Silver tables loaded successfully.")

# COMMAND ----------

# ============================================================
# Cell 81 — Referential Integrity: Orders → Customers
# ============================================================

orders_without_customer = (
    silver_orders
    .join(
        silver_customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .count()
)

print(
    f"Orders without matching customer: "
    f"{orders_without_customer:,}"
)

# COMMAND ----------

# ============================================================
# Cell 82 — Referential Integrity: Order Items → Orders
# ============================================================

order_items_without_order = (
    silver_order_items
    .select("order_id")
    .distinct()
    .join(
        silver_orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    .count()
)

print(
    f"Order IDs in items without matching order: "
    f"{order_items_without_order:,}"
)

# COMMAND ----------

# ============================================================
# Cell 82A — Inspect Order Items Without Matching Orders
# ============================================================

unmatched_order_items_df = (
    silver_order_items
    .select("order_id")
    .distinct()
    .join(
        silver_orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
)

print(
    f"Unmatched Order IDs: "
    f"{unmatched_order_items_df.count():,}"
)

display(unmatched_order_items_df.limit(20))

# COMMAND ----------

# ============================================================
# Cell 82B — Check Unmatched Orders Against Quarantine
# ============================================================

silver_orders_quarantine = spark.table(
    f"{CATALOG}.{PROJECT_SCHEMA}.silver_orders_quarantine"
)

matched_in_quarantine = (
    unmatched_order_items_df
    .join(
        silver_orders_quarantine.select("order_id"),
        on="order_id",
        how="inner"
    )
    .count()
)

print(
    f"Unmatched IDs found in Orders Quarantine: "
    f"{matched_in_quarantine:,}"
)

print(
    f"Unexplained unmatched IDs: "
    f"{189 - matched_in_quarantine:,}"
)

# COMMAND ----------

# ============================================================
# Cell 83 — Referential Integrity: Payments → Orders
# ============================================================

payments_without_order = (
    silver_payments
    .select("order_id")
    .distinct()
    .join(
        silver_orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
)

total_unmatched_payments = payments_without_order.count()

print(
    f"Payment Order IDs without matching valid order: "
    f"{total_unmatched_payments:,}"
)

# COMMAND ----------

# ============================================================
# Cell 83A — Reconcile Payments Against Orders Quarantine
# ============================================================

payments_matched_in_quarantine = (
    payments_without_order
    .join(
        silver_orders_quarantine.select("order_id"),
        on="order_id",
        how="inner"
    )
    .count()
)

unexplained_payment_orders = (
    total_unmatched_payments - payments_matched_in_quarantine
)

print(
    f"Unmatched payment order IDs       : {total_unmatched_payments:,}"
)

print(
    f"Found in Orders Quarantine        : "
    f"{payments_matched_in_quarantine:,}"
)

print(
    f"Unexplained payment order IDs     : "
    f"{unexplained_payment_orders:,}"
)

# COMMAND ----------

# ============================================================
# Cell 84 — Referential Integrity: Order Items → Products
# ============================================================

order_items_without_product = (
    silver_order_items
    .select("product_id")
    .distinct()
    .join(
        silver_products.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

unmatched_products_count = order_items_without_product.count()

print(
    f"Product IDs in Order Items without matching Product: "
    f"{unmatched_products_count:,}"
)

# COMMAND ----------

# ============================================================
# Cell 85 — Referential Integrity: Order Items → Sellers
# ============================================================

order_items_without_seller = (
    silver_order_items
    .select("seller_id")
    .distinct()
    .join(
        silver_sellers.select("seller_id"),
        on="seller_id",
        how="left_anti"
    )
)

unmatched_sellers_count = order_items_without_seller.count()

print(
    f"Seller IDs in Order Items without matching Seller: "
    f"{unmatched_sellers_count:,}"
)

# COMMAND ----------

# ============================================================
# Cell 86 — Final Silver Layer Reconciliation Summary
# ============================================================

print("=" * 65)
print("FINAL SILVER LAYER RECONCILIATION")
print("=" * 65)

print(f"Valid Silver Orders              : {silver_orders.count():,}")
print(f"Quarantined Orders                : {silver_orders_quarantine.count():,}")
print(f"Silver Customers                  : {silver_customers.count():,}")
print(f"Silver Order Items                : {silver_order_items.count():,}")
print(f"Silver Payments                   : {silver_payments.count():,}")
print(f"Silver Products                   : {silver_products.count():,}")
print(f"Silver Sellers                    : {silver_sellers.count():,}")

print("-" * 65)

print(f"Orders → Customers unmatched      : {orders_without_customer:,}")
print(f"Items → Valid Orders unmatched    : {order_items_without_order:,}")
print(f"Items explained by quarantine     : {matched_in_quarantine:,}")
print(f"Payments → Valid Orders unmatched : {total_unmatched_payments:,}")
print(f"Payments explained by quarantine  : {payments_matched_in_quarantine:,}")
print(f"Items → Products unmatched        : {unmatched_products_count:,}")
print(f"Items → Sellers unmatched         : {unmatched_sellers_count:,}")

print("=" * 65)

# COMMAND ----------

