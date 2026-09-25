# Databricks notebook source
# ============================================================
# Cell 1 — Gold Layer Setup
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

print("=" * 60)
print("GOLD LAYER INITIALIZATION")
print("=" * 60)

print(f"Catalog : {CATALOG}")
print(f"Schema  : {PROJECT_SCHEMA}")
print("Status  : READY")

# COMMAND ----------

# ============================================================
# Cell 2 — Load Silver Tables
# ============================================================

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
# Cell 3 — Verify Silver Input Counts
# ============================================================

print(f"Orders        : {silver_orders.count():,}")
print(f"Customers     : {silver_customers.count():,}")
print(f"Order Items   : {silver_order_items.count():,}")
print(f"Payments      : {silver_payments.count():,}")
print(f"Products      : {silver_products.count():,}")
print(f"Sellers       : {silver_sellers.count():,}")

# COMMAND ----------

# ============================================================
# Cell 4 — Aggregate Order Items to Order Level
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    countDistinct,
    round
)

order_item_summary_df = (
    silver_order_items
    .groupBy("order_id")
    .agg(
        count("*").alias("total_items"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("items_revenue"),
        round(sum("freight_value"), 2).alias("total_freight"),
        round(avg("price"), 2).alias("average_item_price"),
        round(min("price"), 2).alias("minimum_item_price"),
        round(max("price"), 2).alias("maximum_item_price")
    )
)

print(
    f"Order-level item summary records: "
    f"{order_item_summary_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 5 — Validate Order Item Summary Grain
# ============================================================

total_summary_rows = order_item_summary_df.count()

unique_order_ids = (
    order_item_summary_df
    .select("order_id")
    .distinct()
    .count()
)

duplicate_order_ids = total_summary_rows - unique_order_ids

print(f"Summary Records      : {total_summary_rows:,}")
print(f"Unique Order IDs     : {unique_order_ids:,}")
print(f"Duplicate Order IDs  : {duplicate_order_ids:,}")

# COMMAND ----------

# ============================================================
# ============================================================
# Cell 6 — Aggregate Payments to Order Level
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    sum,
    max,
    countDistinct,
    round
)

# Load persisted Silver Payments table
silver_payments = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_payments"
)

# Aggregate payments to one row per order
payment_summary_df = (
    silver_payments
    .groupBy("order_id")
    .agg(
        count("*").alias("payment_count"),
        countDistinct("payment_type").alias("payment_type_count"),
        round(sum("payment_value"), 2).alias("total_payment_value"),
        max("payment_installments").alias("max_payment_installments")
    )
)

print(
    f"Order-level payment summary records: "
    f"{payment_summary_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 7 — Validate Payment Summary Grain
# ============================================================

total_payment_summary = payment_summary_df.count()

unique_payment_orders = (
    payment_summary_df
    .select("order_id")
    .distinct()
    .count()
)

duplicate_payment_orders = (
    total_payment_summary - unique_payment_orders
)

print(f"Payment Summary Records : {total_payment_summary:,}")
print(f"Unique Order IDs        : {unique_payment_orders:,}")
print(f"Duplicate Order IDs     : {duplicate_payment_orders:,}")

# COMMAND ----------

# ============================================================
# Cell 8 — Prepare Customer Enrichment
# ============================================================

# Reload persisted table in case the session was restarted
silver_customers = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_customers"
)

customer_gold_df = (
    silver_customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
)

print(
    f"Customer enrichment records: "
    f"{customer_gold_df.count():,}"
)

print(
    f"Customer columns: "
    f"{len(customer_gold_df.columns)}"
)

# COMMAND ----------

# ============================================================
# Cell 9 — Validate Customer Grain
# ============================================================

total_customers = customer_gold_df.count()

unique_customer_ids = (
    customer_gold_df
    .select("customer_id")
    .distinct()
    .count()
)

duplicate_customer_ids = (
    total_customers - unique_customer_ids
)

print(f"Customer Records      : {total_customers:,}")
print(f"Unique Customer IDs   : {unique_customer_ids:,}")
print(f"Duplicate Customer IDs: {duplicate_customer_ids:,}")

# COMMAND ----------

# ============================================================
# Cell 10 — Prepare Gold Orders
# ============================================================

silver_orders = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_orders"
)

orders_gold_df = (
    silver_orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
)

print(f"Gold Order Records: {orders_gold_df.count():,}")
print(f"Gold Order Columns: {len(orders_gold_df.columns)}")

# COMMAND ----------

# ============================================================
# Cell 11 — Build Order + Customer + Item Gold Base
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    min,
    max,
    round
)

# ------------------------------------------------------------
# Load Silver Orders
# ------------------------------------------------------------

orders_gold_df = (
    spark.table(
        "sdp_catalog.realtime_ecommerce.silver_orders"
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
)

# ------------------------------------------------------------
# Load Silver Customers
# ------------------------------------------------------------

customer_gold_df = (
    spark.table(
        "sdp_catalog.realtime_ecommerce.silver_customers"
    )
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
)

# ------------------------------------------------------------
# Load Silver Order Items
# ------------------------------------------------------------

silver_order_items = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_order_items"
)

# ------------------------------------------------------------
# Aggregate Items → Order Grain
# ------------------------------------------------------------

order_item_summary_df = (
    silver_order_items
    .groupBy("order_id")
    .agg(
        count("*").alias("total_items"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("items_revenue"),
        round(sum("freight_value"), 2).alias("total_freight"),
        round(avg("price"), 2).alias("average_item_price"),
        round(min("price"), 2).alias("minimum_item_price"),
        round(max("price"), 2).alias("maximum_item_price")
    )
)

# ------------------------------------------------------------
# Join at Order Grain
# ------------------------------------------------------------

gold_order_base_df = (
    orders_gold_df
    .join(
        customer_gold_df,
        on="customer_id",
        how="left"
    )
    .join(
        order_item_summary_df,
        on="order_id",
        how="left"
    )
)

print(
    f"Gold Order Base Records: "
    f"{gold_order_base_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 12 — Validate Gold Order Base Grain
# ============================================================

total_gold_base = gold_order_base_df.count()

unique_gold_orders = (
    gold_order_base_df
    .select("order_id")
    .distinct()
    .count()
)

duplicate_gold_orders = (
    total_gold_base - unique_gold_orders
)

print(f"Gold Base Records    : {total_gold_base:,}")
print(f"Unique Order IDs     : {unique_gold_orders:,}")
print(f"Duplicate Order IDs  : {duplicate_gold_orders:,}")

# COMMAND ----------

# ============================================================
# Cell 13 — Add Payment Summary to Gold Order Base
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    max,
    round
)

# Load Silver Payments
silver_payments = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_payments"
)

# Aggregate payments to one row per order
payment_summary_df = (
    silver_payments
    .groupBy("order_id")
    .agg(
        count("*").alias("payment_count"),
        countDistinct("payment_type").alias("payment_type_count"),
        round(sum("payment_value"), 2).alias("total_payment_value"),
        max("payment_installments").alias("max_payment_installments")
    )
)

# Add payment summary to Gold order base
gold_order_base_df = (
    gold_order_base_df
    .join(
        payment_summary_df,
        on="order_id",
        how="left"
    )
)

print(
    f"Gold Order Base Records: "
    f"{gold_order_base_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 14 — Fill Gold Aggregate Defaults
# ============================================================

from pyspark.sql.functions import col, coalesce, lit

gold_order_base_df = (
    gold_order_base_df
    .withColumn(
        "total_items",
        coalesce(col("total_items"), lit(0))
    )
    .withColumn(
        "unique_products",
        coalesce(col("unique_products"), lit(0))
    )
    .withColumn(
        "unique_sellers",
        coalesce(col("unique_sellers"), lit(0))
    )
    .withColumn(
        "items_revenue",
        coalesce(col("items_revenue"), lit(0.0))
    )
    .withColumn(
        "total_freight",
        coalesce(col("total_freight"), lit(0.0))
    )
    .withColumn(
        "payment_count",
        coalesce(col("payment_count"), lit(0))
    )
    .withColumn(
        "payment_type_count",
        coalesce(col("payment_type_count"), lit(0))
    )
    .withColumn(
        "total_payment_value",
        coalesce(col("total_payment_value"), lit(0.0))
    )
    .withColumn(
        "max_payment_installments",
        coalesce(col("max_payment_installments"), lit(0))
    )
)

print("Gold aggregate defaults applied successfully.")

# COMMAND ----------

# ============================================================
# Cell 15 — Derive Order-Level Business KPIs
# ============================================================

from pyspark.sql.functions import (
    col,
    round,
    unix_timestamp,
    when
)

gold_order_base_df = (
    gold_order_base_df

    # --------------------------------------------------------
    # Order financial metrics
    # --------------------------------------------------------
    .withColumn(
        "order_item_value",
        round(col("items_revenue"), 2)
    )

    .withColumn(
        "order_total_value",
        round(
            col("items_revenue") + col("total_freight"),
            2
        )
    )

    # --------------------------------------------------------
    # Approval time — hours
    # --------------------------------------------------------
    .withColumn(
        "approval_time_hours",
        when(
            col("order_approved_at").isNotNull() &
            col("order_purchase_timestamp").isNotNull(),
            round(
                (
                    unix_timestamp("order_approved_at")
                    - unix_timestamp("order_purchase_timestamp")
                ) / 3600,
                2
            )
        )
    )

    # --------------------------------------------------------
    # Delivery time — days
    # --------------------------------------------------------
    .withColumn(
        "delivery_time_days",
        when(
            col("order_delivered_customer_date").isNotNull() &
            col("order_purchase_timestamp").isNotNull(),
            round(
                (
                    unix_timestamp("order_delivered_customer_date")
                    - unix_timestamp("order_purchase_timestamp")
                ) / 86400,
                2
            )
        )
    )

    # --------------------------------------------------------
    # Delivery delay — days
    # Positive = late
    # Negative = early
    # --------------------------------------------------------
    .withColumn(
        "delivery_delay_days",
        when(
            col("order_delivered_customer_date").isNotNull() &
            col("order_estimated_delivery_date").isNotNull(),
            round(
                (
                    unix_timestamp("order_delivered_customer_date")
                    - unix_timestamp("order_estimated_delivery_date")
                ) / 86400,
                2
            )
        )
    )

    # --------------------------------------------------------
    # Delivery status
    # --------------------------------------------------------
    .withColumn(
        "delivery_performance",
        when(
            col("order_delivered_customer_date").isNull(),
            "not_delivered"
        )
        .when(
            col("delivery_delay_days") > 0,
            "late"
        )
        .otherwise(
            "on_time_or_early"
        )
    )
)

print("Order-level business KPIs created successfully.")

# COMMAND ----------

# ============================================================
# Cell 16 — Validate Gold Business KPIs
# ============================================================

from pyspark.sql.functions import (
    col,
    sum,
    when,
    count
)

gold_kpi_validation = gold_order_base_df.agg(
    count("*").alias("total_orders"),

    sum(
        when(col("order_total_value") < 0, 1).otherwise(0)
    ).alias("negative_order_values"),

    sum(
        when(col("approval_time_hours") < 0, 1).otherwise(0)
    ).alias("negative_approval_times"),

    sum(
        when(col("delivery_time_days") < 0, 1).otherwise(0)
    ).alias("negative_delivery_times"),

    sum(
        when(col("order_item_value") < 0, 1).otherwise(0)
    ).alias("negative_item_values"),

    sum(
        when(col("total_freight") < 0, 1).otherwise(0)
    ).alias("negative_freight_values")
)

display(gold_kpi_validation)

# COMMAND ----------

# ============================================================
# Cell 17 — Add Order & Revenue Classification
# ============================================================

from pyspark.sql.functions import col, when

gold_order_base_df = (
    gold_order_base_df

    # --------------------------------------------------------
    # Order value bucket
    # --------------------------------------------------------
    .withColumn(
        "order_value_bucket",
        when(col("order_total_value") < 100, "low")
        .when(col("order_total_value") < 500, "medium")
        .when(col("order_total_value") < 1000, "high")
        .otherwise("premium")
    )

    # --------------------------------------------------------
    # Order size bucket
    # --------------------------------------------------------
    .withColumn(
        "order_size_bucket",
        when(col("total_items") == 0, "no_items")
        .when(col("total_items") == 1, "single_item")
        .when(col("total_items") <= 3, "small")
        .when(col("total_items") <= 5, "medium")
        .otherwise("large")
    )

    # --------------------------------------------------------
    # Payment coverage
    # --------------------------------------------------------
    .withColumn(
        "payment_status",
        when(col("payment_count") == 0, "no_payment")
        .when(
            col("total_payment_value") >= col("order_total_value"),
            "fully_paid"
        )
        .otherwise("partial_or_mismatched")
    )
)

print("Order classifications created successfully.")

# COMMAND ----------

# ============================================================
# Cell 18 — Preview Gold Order Fact
# ============================================================

display(
    gold_order_base_df
    .select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_status",
        "order_purchase_timestamp",
        "total_items",
        "unique_products",
        "unique_sellers",
        "order_item_value",
        "total_freight",
        "order_total_value",
        "payment_count",
        "total_payment_value",
        "approval_time_hours",
        "delivery_time_days",
        "delivery_delay_days",
        "delivery_performance",
        "order_value_bucket",
        "order_size_bucket",
        "payment_status"
    )
    .orderBy("order_purchase_timestamp")
    .limit(20)
)

# COMMAND ----------

# ============================================================
# Cell 19 — Final Gold Order Fact Validation
# ============================================================

total_gold_orders = gold_order_base_df.count()

unique_gold_order_ids = (
    gold_order_base_df
    .select("order_id")
    .distinct()
    .count()
)

duplicate_gold_order_ids = (
    total_gold_orders - unique_gold_order_ids
)

print("=" * 60)
print("GOLD ORDER FACT VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_gold_orders:,}")
print(f"Unique Order IDs    : {unique_gold_order_ids:,}")
print(f"Duplicate Order IDs : {duplicate_gold_order_ids:,}")
print(f"Columns             : {len(gold_order_base_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 20 — Persist Gold Order Fact
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_ORDER_FACT = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_order_fact"
)

(
    gold_order_base_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_ORDER_FACT)
)

print(
    f"Successfully created: {GOLD_ORDER_FACT}"
)

# COMMAND ----------

# ============================================================
# Cell 21 — Verify Persisted Gold Order Fact
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_ORDER_FACT = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_order_fact"
)

gold_order_fact_check = spark.table(GOLD_ORDER_FACT)

total_records = gold_order_fact_check.count()
unique_orders = (
    gold_order_fact_check
    .select("order_id")
    .distinct()
    .count()
)

print("=" * 60)
print("PERSISTED GOLD ORDER FACT")
print("=" * 60)
print(f"Records          : {total_records:,}")
print(f"Unique Order IDs : {unique_orders:,}")
print(f"Columns          : {len(gold_order_fact_check.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 22 — Product Performance Aggregation
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    round
)

# Load persisted Silver Order Items
silver_order_items = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_order_items"
)

# Aggregate item-level data to product grain
gold_product_performance_df = (
    silver_order_items
    .groupBy("product_id")
    .agg(
        count("*").alias("units_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("product_revenue"),
        round(sum("freight_value"), 2).alias("product_freight"),
        round(avg("price"), 2).alias("average_selling_price")
    )
)

print(
    f"Product Performance Records: "
    f"{gold_product_performance_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 23 — CLEAN REBUILD: Product Performance
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    round
)

# ------------------------------------------------------------
# 1. Load Silver Order Items
# ------------------------------------------------------------

silver_order_items = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_order_items"
)

# ------------------------------------------------------------
# 2. Build Product Performance from scratch
# ------------------------------------------------------------

product_performance_metrics = (
    silver_order_items
    .groupBy("product_id")
    .agg(
        count("*").alias("units_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("product_revenue"),
        round(sum("freight_value"), 2).alias("product_freight"),
        round(avg("price"), 2).alias("average_selling_price")
    )
)

# ------------------------------------------------------------
# 3. Load Silver Products
# ------------------------------------------------------------

silver_products = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_products"
)

# ------------------------------------------------------------
# 4. Select EXACT existing column names
# ------------------------------------------------------------

product_attributes = (
    silver_products
    .select(
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
        "has_missing_physical_attributes",
        "product_volume_cm3"
    )
)

# ------------------------------------------------------------
# 5. Final Product Performance DataFrame
# ------------------------------------------------------------

gold_product_performance_df = (
    product_performance_metrics
    .join(
        product_attributes,
        on="product_id",
        how="left"
    )
)

print(
    f"Product Performance Records: "
    f"{gold_product_performance_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 24 — Validate Product Performance Grain
# ============================================================

total_product_rows = gold_product_performance_df.count()

unique_product_ids = (
    gold_product_performance_df
    .select("product_id")
    .distinct()
    .count()
)

duplicate_product_ids = (
    total_product_rows - unique_product_ids
)

print("=" * 60)
print("PRODUCT PERFORMANCE GRAIN VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_product_rows:,}")
print(f"Unique Product IDs  : {unique_product_ids:,}")
print(f"Duplicate Products  : {duplicate_product_ids:,}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 25 — Product Performance Classification
# ============================================================

from pyspark.sql.functions import col, when

gold_product_performance_df = (
    gold_product_performance_df

    # --------------------------------------------------------
    # Sales volume classification
    # --------------------------------------------------------
    .withColumn(
        "sales_volume_bucket",
        when(col("units_sold") <= 5, "low")
        .when(col("units_sold") <= 20, "medium")
        .when(col("units_sold") <= 50, "high")
        .otherwise("very_high")
    )

    # --------------------------------------------------------
    # Revenue classification
    # --------------------------------------------------------
    .withColumn(
        "revenue_bucket",
        when(col("product_revenue") < 500, "low")
        .when(col("product_revenue") < 2000, "medium")
        .when(col("product_revenue") < 5000, "high")
        .otherwise("premium")
    )

    # --------------------------------------------------------
    # Product data quality status
    # --------------------------------------------------------
    .withColumn(
        "product_data_quality",
        when(
            col("has_missing_descriptive_attributes") |
            col("has_missing_physical_attributes"),
            "incomplete"
        )
        .otherwise("complete")
    )
)

print("Product performance classifications created successfully.")

# COMMAND ----------

# ============================================================
# Cell 26 — Final Product Performance Validation
# ============================================================

total_products = gold_product_performance_df.count()

unique_products = (
    gold_product_performance_df
    .select("product_id")
    .distinct()
    .count()
)

duplicate_products = total_products - unique_products

print("=" * 60)
print("GOLD PRODUCT PERFORMANCE VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_products:,}")
print(f"Unique Product IDs  : {unique_products:,}")
print(f"Duplicate Products  : {duplicate_products:,}")
print(f"Columns             : {len(gold_product_performance_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 27 — Persist Gold Product Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_PRODUCT_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_product_performance"
)

(
    gold_product_performance_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_PRODUCT_PERFORMANCE)
)

print(
    f"Successfully created: {GOLD_PRODUCT_PERFORMANCE}"
)

# COMMAND ----------

# ============================================================
# Cell 28 — Verify Gold Product Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_PRODUCT_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_product_performance"
)

gold_product_check = spark.table(
    GOLD_PRODUCT_PERFORMANCE
)

total_records = gold_product_check.count()

unique_products = (
    gold_product_check
    .select("product_id")
    .distinct()
    .count()
)

print("=" * 60)
print("PERSISTED GOLD PRODUCT PERFORMANCE")
print("=" * 60)

print(f"Records            : {total_records:,}")
print(f"Unique Product IDs : {unique_products:,}")
print(f"Columns            : {len(gold_product_check.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 29 — Seller Performance Aggregation
# ============================================================

from pyspark.sql.functions import (
    count,
    countDistinct,
    sum,
    avg,
    round
)

# Load Silver Order Items
silver_order_items = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_order_items"
)

# Aggregate order items to seller grain
seller_performance_metrics = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        count("*").alias("units_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products"),
        round(sum("price"), 2).alias("seller_revenue"),
        round(sum("freight_value"), 2).alias("seller_freight"),
        round(avg("price"), 2).alias("average_selling_price")
    )
)

print(
    f"Seller Performance Records: "
    f"{seller_performance_metrics.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 30 — Enrich Seller Performance
# ============================================================

from pyspark.sql.functions import col

silver_sellers = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_sellers"
)

seller_attributes = (
    silver_sellers
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )
)

gold_seller_performance_df = (
    seller_performance_metrics
    .join(
        seller_attributes,
        on="seller_id",
        how="left"
    )
)

print(
    f"Seller Performance Records: "
    f"{gold_seller_performance_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 31 — Validate Seller Performance Grain
# ============================================================

total_sellers = gold_seller_performance_df.count()

unique_seller_ids = (
    gold_seller_performance_df
    .select("seller_id")
    .distinct()
    .count()
)

duplicate_seller_ids = (
    total_sellers - unique_seller_ids
)

print("=" * 60)
print("SELLER PERFORMANCE GRAIN VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_sellers:,}")
print(f"Unique Seller IDs   : {unique_seller_ids:,}")
print(f"Duplicate Sellers   : {duplicate_seller_ids:,}")
print(f"Columns             : {len(gold_seller_performance_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 32 — Seller Performance Classification
# ============================================================

from pyspark.sql.functions import col, when

gold_seller_performance_df = (
    gold_seller_performance_df

    # --------------------------------------------------------
    # Sales volume bucket
    # --------------------------------------------------------
    .withColumn(
        "sales_volume_bucket",
        when(col("units_sold") <= 5, "low")
        .when(col("units_sold") <= 20, "medium")
        .when(col("units_sold") <= 50, "high")
        .otherwise("very_high")
    )

    # --------------------------------------------------------
    # Revenue bucket
    # --------------------------------------------------------
    .withColumn(
        "revenue_bucket",
        when(col("seller_revenue") < 500, "low")
        .when(col("seller_revenue") < 2000, "medium")
        .when(col("seller_revenue") < 5000, "high")
        .otherwise("premium")
    )
)

print("Seller performance classifications created successfully.")

# COMMAND ----------

# ============================================================
# Cell 33 — Final Seller Performance Validation
# ============================================================

total_sellers = gold_seller_performance_df.count()

unique_sellers = (
    gold_seller_performance_df
    .select("seller_id")
    .distinct()
    .count()
)

duplicate_sellers = total_sellers - unique_sellers

print("=" * 60)
print("GOLD SELLER PERFORMANCE VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_sellers:,}")
print(f"Unique Seller IDs   : {unique_sellers:,}")
print(f"Duplicate Sellers   : {duplicate_sellers:,}")
print(f"Columns             : {len(gold_seller_performance_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 34 — Persist Gold Seller Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_SELLER_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_seller_performance"
)

(
    gold_seller_performance_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_SELLER_PERFORMANCE)
)

print(
    f"Successfully created: {GOLD_SELLER_PERFORMANCE}"
)

# COMMAND ----------

# ============================================================
# Cell 35 — Verify Gold Seller Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_SELLER_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_seller_performance"
)

gold_seller_check = spark.table(
    GOLD_SELLER_PERFORMANCE
)

total_records = gold_seller_check.count()

unique_sellers = (
    gold_seller_check
    .select("seller_id")
    .distinct()
    .count()
)

duplicate_sellers = total_records - unique_sellers

print("=" * 60)
print("PERSISTED GOLD SELLER PERFORMANCE")
print("=" * 60)

print(f"Records            : {total_records:,}")
print(f"Unique Seller IDs  : {unique_sellers:,}")
print(f"Duplicate Sellers  : {duplicate_sellers:,}")
print(f"Columns            : {len(gold_seller_check.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 36 — Customer Summary
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    countDistinct,
    round
)

# Load persisted Gold Order Fact
gold_order_fact = spark.table(
    "sdp_catalog.realtime_ecommerce.gold_order_fact"
)

# Aggregate orders to customer grain
gold_customer_summary_df = (
    gold_order_fact
    .groupBy("customer_id", "customer_unique_id")
    .agg(
        count("*").alias("total_orders"),
        countDistinct("order_status").alias("order_status_types"),
        round(sum("order_total_value"), 2).alias("customer_total_spend"),
        round(avg("order_total_value"), 2).alias("average_order_value"),
        round(sum("total_items"), 2).alias("total_items_purchased"),
        round(avg("delivery_time_days"), 2).alias("average_delivery_days"),
        min("order_purchase_timestamp").alias("first_order_timestamp"),
        max("order_purchase_timestamp").alias("last_order_timestamp")
    )
)

print(
    f"Customer Summary Records: "
    f"{gold_customer_summary_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 37 — Check Customer Order Distribution
# ============================================================

from pyspark.sql.functions import (
    count,
    max,
    sum,
    when
)

customer_order_check = (
    gold_order_fact
    .groupBy("customer_unique_id")
    .agg(
        count("*").alias("order_count")
    )
)

total_customers = customer_order_check.count()

repeat_customers = (
    customer_order_check
    .filter("order_count > 1")
    .count()
)

max_orders_per_customer = (
    customer_order_check
    .agg(max("order_count").alias("max_orders"))
    .collect()[0]["max_orders"]
)

print("=" * 60)
print("CUSTOMER ORDER DISTRIBUTION")
print("=" * 60)
print(f"Unique Customers       : {total_customers:,}")
print(f"Repeat Customers       : {repeat_customers:,}")
print(f"Max Orders / Customer  : {max_orders_per_customer}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 38 — Customer Summary at Customer Grain
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    countDistinct,
    round,
    datediff,
    current_date
)

# gold_order_fact is already loaded from Cell 37

gold_customer_summary_df = (
    gold_order_fact
    .groupBy("customer_unique_id")
    .agg(
        count("*").alias("total_orders"),

        countDistinct("customer_id")
        .alias("customer_account_count"),

        countDistinct("order_status")
        .alias("order_status_types"),

        round(
            sum("order_total_value"), 2
        ).alias("customer_total_spend"),

        round(
            avg("order_total_value"), 2
        ).alias("average_order_value"),

        sum("total_items")
        .alias("total_items_purchased"),

        round(
            avg("delivery_time_days"), 2
        ).alias("average_delivery_days"),

        min("order_purchase_timestamp")
        .alias("first_order_timestamp"),

        max("order_purchase_timestamp")
        .alias("last_order_timestamp")
    )
)

print(
    f"Customer Summary Records: "
    f"{gold_customer_summary_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 39 — Customer Behavior Classification
# ============================================================

from pyspark.sql.functions import col, when

gold_customer_summary_df = (
    gold_customer_summary_df

    # --------------------------------------------------------
    # Customer type
    # --------------------------------------------------------
    .withColumn(
        "customer_type",
        when(
            col("total_orders") == 1,
            "one_time"
        )
        .otherwise("repeat")
    )

    # --------------------------------------------------------
    # Spending segment
    # --------------------------------------------------------
    .withColumn(
        "spending_segment",
        when(
            col("customer_total_spend") < 100,
            "low"
        )
        .when(
            col("customer_total_spend") < 500,
            "medium"
        )
        .when(
            col("customer_total_spend") < 1000,
            "high"
        )
        .otherwise("premium")
    )

    # --------------------------------------------------------
    # Order frequency
    # --------------------------------------------------------
    .withColumn(
        "order_frequency",
        when(
            col("total_orders") == 1,
            "single_order"
        )
        .when(
            col("total_orders") <= 3,
            "occasional"
        )
        .when(
            col("total_orders") <= 5,
            "frequent"
        )
        .otherwise("high_frequency")
    )
)

print(
    "Customer behavior classifications created successfully."
)

# COMMAND ----------

# ============================================================
# Cell 40 — Final Customer Summary Validation
# ============================================================

total_customer_rows = gold_customer_summary_df.count()

unique_customer_ids = (
    gold_customer_summary_df
    .select("customer_unique_id")
    .distinct()
    .count()
)

duplicate_customer_ids = (
    total_customer_rows - unique_customer_ids
)

print("=" * 60)
print("GOLD CUSTOMER SUMMARY VALIDATION")
print("=" * 60)

print(f"Total Records        : {total_customer_rows:,}")
print(f"Unique Customers     : {unique_customer_ids:,}")
print(f"Duplicate Customers  : {duplicate_customer_ids:,}")
print(f"Columns              : {len(gold_customer_summary_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 41 — Persist Gold Customer Summary
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_CUSTOMER_SUMMARY = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_customer_summary"
)

(
    gold_customer_summary_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_CUSTOMER_SUMMARY)
)

print(
    f"Successfully created: {GOLD_CUSTOMER_SUMMARY}"
)

# COMMAND ----------

# ============================================================
# Cell 42 — Verify Gold Customer Summary
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_CUSTOMER_SUMMARY = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_customer_summary"
)

gold_customer_check = spark.table(
    GOLD_CUSTOMER_SUMMARY
)

total_records = gold_customer_check.count()

unique_customers = (
    gold_customer_check
    .select("customer_unique_id")
    .distinct()
    .count()
)

duplicate_customers = (
    total_records - unique_customers
)

print("=" * 60)
print("PERSISTED GOLD CUSTOMER SUMMARY")
print("=" * 60)

print(f"Records              : {total_records:,}")
print(f"Unique Customers     : {unique_customers:,}")
print(f"Duplicate Customers  : {duplicate_customers:,}")
print(f"Columns              : {len(gold_customer_check.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 43 — Category Performance
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    round
)

# Load Silver tables
silver_order_items = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_order_items"
)

silver_products = spark.table(
    "sdp_catalog.realtime_ecommerce.silver_products"
)

# Product → category mapping
product_category_df = (
    silver_products
    .select(
        "product_id",
        "product_category_name"
    )
)

# Join items with product category
category_items_df = (
    silver_order_items
    .join(
        product_category_df,
        on="product_id",
        how="left"
    )
)

# Aggregate to category grain
gold_category_performance_df = (
    category_items_df
    .groupBy("product_category_name")
    .agg(
        count("*").alias("units_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("category_revenue"),
        round(sum("freight_value"), 2).alias("category_freight"),
        round(avg("price"), 2).alias("average_selling_price")
    )
)

print(
    f"Category Performance Records: "
    f"{gold_category_performance_df.count():,}"
)

# COMMAND ----------

# ============================================================
# Cell 44 — Category Performance Validation
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    when
)

total_categories = gold_category_performance_df.count()

unique_categories = (
    gold_category_performance_df
    .select("product_category_name")
    .distinct()
    .count()
)

duplicate_categories = (
    total_categories - unique_categories
)

null_category_rows = (
    gold_category_performance_df
    .filter(
        col("product_category_name").isNull()
    )
    .count()
)

print("=" * 60)
print("GOLD CATEGORY PERFORMANCE VALIDATION")
print("=" * 60)

print(f"Total Categories     : {total_categories:,}")
print(f"Unique Categories    : {unique_categories:,}")
print(f"Duplicate Categories : {duplicate_categories:,}")
print(f"NULL Category Rows   : {null_category_rows:,}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 45 — Category Performance Classification
# ============================================================

from pyspark.sql.functions import col, when

gold_category_performance_df = (
    gold_category_performance_df

    # --------------------------------------------------------
    # Sales volume bucket
    # --------------------------------------------------------
    .withColumn(
        "sales_volume_bucket",
        when(col("units_sold") <= 100, "low")
        .when(col("units_sold") <= 1000, "medium")
        .when(col("units_sold") <= 5000, "high")
        .otherwise("very_high")
    )

    # --------------------------------------------------------
    # Revenue bucket
    # --------------------------------------------------------
    .withColumn(
        "revenue_bucket",
        when(col("category_revenue") < 10000, "low")
        .when(col("category_revenue") < 50000, "medium")
        .when(col("category_revenue") < 100000, "high")
        .otherwise("premium")
    )
)

print("Category performance classifications created successfully.")

# COMMAND ----------

# ============================================================
# Cell 46 — Final Category Performance Validation
# ============================================================

total_categories = gold_category_performance_df.count()

unique_categories = (
    gold_category_performance_df
    .select("product_category_name")
    .distinct()
    .count()
)

duplicate_categories = (
    total_categories - unique_categories
)

print("=" * 60)
print("FINAL GOLD CATEGORY PERFORMANCE VALIDATION")
print("=" * 60)

print(f"Total Records       : {total_categories:,}")
print(f"Unique Categories   : {unique_categories:,}")
print(f"Duplicate Categories: {duplicate_categories:,}")
print(f"Columns             : {len(gold_category_performance_df.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 47 — Persist Gold Category Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_CATEGORY_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_category_performance"
)

(
    gold_category_performance_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_CATEGORY_PERFORMANCE)
)

print(
    f"Successfully created: {GOLD_CATEGORY_PERFORMANCE}"
)

# COMMAND ----------

# ============================================================
# Cell 48 — Verify Gold Category Performance
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_CATEGORY_PERFORMANCE = (
    f"{CATALOG}.{PROJECT_SCHEMA}.gold_category_performance"
)

gold_category_check = spark.table(
    GOLD_CATEGORY_PERFORMANCE
)

total_records = gold_category_check.count()

unique_categories = (
    gold_category_check
    .select("product_category_name")
    .distinct()
    .count()
)

duplicate_categories = (
    total_records - unique_categories
)

print("=" * 60)
print("PERSISTED GOLD CATEGORY PERFORMANCE")
print("=" * 60)

print(f"Records              : {total_records:,}")
print(f"Unique Categories    : {unique_categories:,}")
print(f"Duplicate Categories : {duplicate_categories:,}")
print(f"Columns              : {len(gold_category_check.columns)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 49 — FINAL GOLD LAYER RECONCILIATION
# ============================================================

CATALOG = "sdp_catalog"
PROJECT_SCHEMA = "realtime_ecommerce"

GOLD_ORDER = f"{CATALOG}.{PROJECT_SCHEMA}.gold_order_fact"
GOLD_PRODUCT = f"{CATALOG}.{PROJECT_SCHEMA}.gold_product_performance"
GOLD_SELLER = f"{CATALOG}.{PROJECT_SCHEMA}.gold_seller_performance"
GOLD_CUSTOMER = f"{CATALOG}.{PROJECT_SCHEMA}.gold_customer_summary"
GOLD_CATEGORY = f"{CATALOG}.{PROJECT_SCHEMA}.gold_category_performance"

# ------------------------------------------------------------
# Load persisted Gold tables
# ------------------------------------------------------------

gold_order = spark.table(GOLD_ORDER)
gold_product = spark.table(GOLD_PRODUCT)
gold_seller = spark.table(GOLD_SELLER)
gold_customer = spark.table(GOLD_CUSTOMER)
gold_category = spark.table(GOLD_CATEGORY)

# ------------------------------------------------------------
# Record counts
# ------------------------------------------------------------

order_count = gold_order.count()
product_count = gold_product.count()
seller_count = gold_seller.count()
customer_count = gold_customer.count()
category_count = gold_category.count()

# ------------------------------------------------------------
# Grain validation
# ------------------------------------------------------------

duplicate_orders = (
    order_count
    - gold_order.select("order_id").distinct().count()
)

duplicate_products = (
    product_count
    - gold_product.select("product_id").distinct().count()
)

duplicate_sellers = (
    seller_count
    - gold_seller.select("seller_id").distinct().count()
)

duplicate_customers = (
    customer_count
    - gold_customer.select("customer_unique_id").distinct().count()
)

duplicate_categories = (
    category_count
    - gold_category.select("product_category_name").distinct().count()
)

# ------------------------------------------------------------
# Final reconciliation output
# ------------------------------------------------------------

print("=" * 75)
print(" " * 20 + "FINAL GOLD LAYER RECONCILIATION")
print("=" * 75)

print(f"Gold Orders                 : {order_count:,}")
print(f"Gold Products               : {product_count:,}")
print(f"Gold Sellers                : {seller_count:,}")
print(f"Gold Customers              : {customer_count:,}")
print(f"Gold Categories             : {category_count:,}")

print("-" * 75)

print(f"Order duplicate IDs         : {duplicate_orders:,}")
print(f"Product duplicate IDs       : {duplicate_products:,}")
print(f"Seller duplicate IDs        : {duplicate_sellers:,}")
print(f"Customer duplicate IDs      : {duplicate_customers:,}")
print(f"Category duplicate IDs      : {duplicate_categories:,}")

print("-" * 75)

if (
    duplicate_orders == 0
    and duplicate_products == 0
    and duplicate_sellers == 0
    and duplicate_customers == 0
    and duplicate_categories == 0
):
    print("GOLD LAYER STATUS            : READY FOR STREAMING")
else:
    print("GOLD LAYER STATUS            : REQUIRES REVIEW")

print("=" * 75)

# COMMAND ----------

# %skip
# # ============================================================
# # Cell 50 — Streaming Environment Check
# # ============================================================

# print("=" * 60)
# print("STREAMING ENVIRONMENT CHECK")
# print("=" * 60)

# print(f"Spark Version : {spark.version}")

# print("\nAvailable Kafka-related Spark configuration:")
# for key, value in spark.conf.getAll.items():
#     if "kafka" in key.lower():
#         print(f"{key} = {value}")

# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 51 — Check Spark Kafka Connector
# ============================================================


# COMMAND ----------

# ============================================================
# Cell 52 — Kafka Connectivity Test
# ============================================================

# try:

#     kafka_probe = (
#         spark.read
#         .format("kafka")
#         .option(
#             "kafka.bootstrap.servers",
#             "localhost:9092"
#         )
#         .option(
#             "subscribe",
#             "order_events"
#         )
#         .option(
#             "startingOffsets",
#             "earliest"
#         )
#         .load()
#     )

#     print("=" * 60)
#     print("KAFKA CONNECTIVITY TEST")
#     print("=" * 60)

#     print("Kafka source initialized successfully.")

#     print(
#         f"Kafka records visible: "
#         f"{kafka_probe.count():,}"
#     )

#     print("=" * 60)

# except Exception as e:

#     print("=" * 60)
#     print("KAFKA CONNECTIVITY TEST FAILED")
#     print("=" * 60)

#     print(str(e)[:4000])

#     print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 52 — Test Kafka Network Reachability
# ============================================================

# import socket

# host = "172.22.52.93"
# port = 9092

# try:
#     sock = socket.create_connection((host, port), timeout=5)
#     print("=" * 60)
#     print("KAFKA NETWORK TEST")
#     print("=" * 60)
#     print(f"Host   : {host}")
#     print(f"Port   : {port}")
#     print("Status : CONNECTED")
#     print("=" * 60)
#     sock.close()

# except Exception as e:
#     print("=" * 60)
#     print("KAFKA NETWORK TEST")
#     print("=" * 60)
#     print(f"Host   : {host}")
#     print(f"Port   : {port}")
#     print("Status : FAILED")
#     print(f"Error  : {e}")
#     print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 50 — Streaming Input Location
# ============================================================

# CATALOG = "sdp_catalog"
# PROJECT_SCHEMA = "realtime_ecommerce"

# STREAM_VOLUME = f"/Volumes/{CATALOG}/{PROJECT_SCHEMA}/streaming_input"

# print("=" * 60)
# print("STREAMING INPUT CONFIGURATION")
# print("=" * 60)
# print(f"Streaming Input : {STREAM_VOLUME}")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 51 — Create Streaming Input Volume
# ============================================================

# CATALOG = "sdp_catalog"
# PROJECT_SCHEMA = "realtime_ecommerce"

# spark.sql(f"""
# CREATE VOLUME IF NOT EXISTS
# {CATALOG}.{PROJECT_SCHEMA}.streaming_input
# """)

# print("=" * 60)
# print("STREAMING INPUT VOLUME")
# print("=" * 60)
# print("Status : READY")
# print(f"Path   : /Volumes/{CATALOG}/{PROJECT_SCHEMA}/streaming_input")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 52 — Streaming Event Schema
# ============================================================

# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     StringType,
#     DoubleType,
#     TimestampType
# )

# stream_event_schema = StructType([
#     StructField("order_id", StringType(), False),
#     StructField("customer_id", StringType(), True),
#     StructField("order_status", StringType(), True),
#     StructField("order_purchase_timestamp", TimestampType(), True),
#     StructField("order_delivered_customer_date", TimestampType(), True),
#     StructField("order_estimated_delivery_date", TimestampType(), True),
#     StructField("event_timestamp", TimestampType(), True)
# ])

# print("=" * 60)
# print("STREAMING EVENT SCHEMA")
# print("=" * 60)

# for field in stream_event_schema.fields:
#     print(f"{field.name:40} {field.dataType}")

# print("=" * 60)


# COMMAND ----------

# ============================================================
# Cell 53 — Read Streaming Events
# ============================================================

# STREAM_VOLUME = "/Volumes/sdp_catalog/realtime_ecommerce/streaming_input"

# stream_orders_df = (
#     spark.readStream
#     .format("json")
#     .schema(stream_event_schema)
#     .option("maxFilesPerTrigger", 1)
#     .load(STREAM_VOLUME)
# )

# print("=" * 60)
# print("STRUCTURED STREAMING SOURCE")
# print("=" * 60)
# print("Source Format       : JSON")
# print(f"Source Location     : {STREAM_VOLUME}")
# print("Max Files / Trigger : 1")
# print("Streaming Source    : READY")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 54 — Generate Streaming Test Events
# ============================================================

# from datetime import datetime, timedelta
# import json

# STREAM_VOLUME = "/Volumes/sdp_catalog/realtime_ecommerce/streaming_input"

# base_time = datetime.now()

# test_events = [
#     {
#         "order_id": "STREAM_001",
#         "customer_id": "CUST_001",
#         "order_status": "created",
#         "order_purchase_timestamp": base_time,
#         "order_delivered_customer_date": None,
#         "order_estimated_delivery_date": base_time + timedelta(days=7),
#         "event_timestamp": base_time
#     },
#     {
#         "order_id": "STREAM_002",
#         "customer_id": "CUST_002",
#         "order_status": "created",
#         "order_purchase_timestamp": base_time,
#         "order_delivered_customer_date": None,
#         "order_estimated_delivery_date": base_time + timedelta(days=6),
#         "event_timestamp": base_time
#     },
#     {
#         "order_id": "STREAM_003",
#         "customer_id": "CUST_003",
#         "order_status": "created",
#         "order_purchase_timestamp": base_time,
#         "order_delivered_customer_date": None,
#         "order_estimated_delivery_date": base_time + timedelta(days=5),
#         "event_timestamp": base_time
#     }
# ]

# # Convert timestamps to ISO strings for JSON
# json_events = []

# for event in test_events:
#     converted = event.copy()

#     for key, value in converted.items():
#         if isinstance(value, datetime):
#             converted[key] = value.isoformat()

#     json_events.append(converted)

# # Create a DataFrame
# events_df = spark.createDataFrame(
#     [(json.dumps(event),) for event in json_events],
#     ["value"]
# )

# # Write one JSON file into the streaming volume
# events_df.coalesce(1).write.mode("append").text(STREAM_VOLUME)

# print("=" * 60)
# print("STREAMING TEST EVENTS")
# print("=" * 60)
# print(f"Events Created : {len(test_events)}")
# print(f"Location       : {STREAM_VOLUME}")
# print("Status         : READY FOR STREAM")
# print("=" * 60)


# COMMAND ----------

# ============================================================
# Cell 55 — Start Structured Streaming Query
# # ============================================================

# CHECKPOINT_PATH = "/Volumes/sdp_catalog/realtime_ecommerce/streaming_input/_checkpoints/orders_test"

# stream_query = (
#     stream_orders_df
#     .writeStream
#     .format("memory")
#     .queryName("stream_orders_test")
#     .outputMode("append")
#     .option("checkpointLocation", CHECKPOINT_PATH)
#     .trigger(availableNow=True)
#     .start()
# )

# stream_query.awaitTermination()

# print("=" * 60)
# print("STRUCTURED STREAMING QUERY")
# print("=" * 60)
# print("Query Name : stream_orders_test")
# print("Checkpoint : " + CHECKPOINT_PATH)
# print("Status     : COMPLETED")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 56 — Validate Streamed Records
# ============================================================

# stream_result_df = spark.sql("""
#     SELECT *
#     FROM stream_orders_test
#     ORDER BY event_timestamp
# """)

# print("=" * 60)
# print("STREAMING RECORD VALIDATION")
# print("=" * 60)

# print(f"Records Received : {stream_result_df.count()}")
# print(f"Columns          : {len(stream_result_df.columns)}")

# print("=" * 60)

# display(stream_result_df)

# COMMAND ----------

# ============================================================
# Cell 57 — Bronze Streaming DataFrame Validation
# ============================================================

# from pyspark.sql.functions import current_timestamp

# bronze_stream_df = (
#     stream_orders_df
#     .withColumn("_ingestion_timestamp", current_timestamp())
# )

# print("=" * 60)
# print("BRONZE STREAMING DATAFRAME")
# print("=" * 60)
# print("Is Streaming :", bronze_stream_df.isStreaming)
# print("Columns      :", len(bronze_stream_df.columns))
# print("Column Names :", bronze_stream_df.columns)
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 58 — Write Bronze Streaming Table
# ============================================================

# BRONZE_TABLE = "sdp_catalog.realtime_ecommerce.bronze_stream_orders"

# BRONZE_CHECKPOINT = (
#     "/Volumes/sdp_catalog/realtime_ecommerce/"
#     "streaming_input/_checkpoints/bronze_orders"
# )

# bronze_query = (
#     bronze_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", BRONZE_CHECKPOINT)
#     .trigger(availableNow=True)
#     .toTable(BRONZE_TABLE)
# )

# bronze_query.awaitTermination()

# print("=" * 60)
# print("BRONZE STREAMING TABLE")
# print("=" * 60)
# print(f"Table      : {BRONZE_TABLE}")
# print(f"Checkpoint : {BRONZE_CHECKPOINT}")
# print("Status     : COMPLETED")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 59 — Bronze Streaming Table Validation
# ============================================================

# BRONZE_TABLE = "sdp_catalog.realtime_ecommerce.bronze_stream_orders"

# bronze_validation_df = spark.sql(f"""
#     SELECT *
#     FROM {BRONZE_TABLE}
#     ORDER BY event_timestamp
# """)

# print("=" * 60)
# print("BRONZE STREAMING TABLE VALIDATION")
# print("=" * 60)

# print(f"Records       : {bronze_validation_df.count()}")
# print(f"Columns       : {len(bronze_validation_df.columns)}")
# print(f"Duplicate IDs : {bronze_validation_df.select('order_id').distinct().count()}")
# print("=" * 60)

# display(bronze_validation_df)

# COMMAND ----------

# ============================================================
# Cell 60 — Correct Bronze Validation
# ============================================================

# from pyspark.sql.functions import count, countDistinct

# bronze_stats = bronze_validation_df.agg(
#     count("*").alias("total_records"),
#     countDistinct("order_id").alias("unique_order_ids")
# ).collect()[0]

# total_records = bronze_stats["total_records"]
# unique_order_ids = bronze_stats["unique_order_ids"]
# duplicate_ids = total_records - unique_order_ids

# print("=" * 60)
# print("FINAL BRONZE STREAMING VALIDATION")
# print("=" * 60)
# print(f"Total Records     : {total_records:,}")
# print(f"Unique Order IDs  : {unique_order_ids:,}")
# print(f"Duplicate IDs     : {duplicate_ids:,}")
# print(f"Columns           : {len(bronze_validation_df.columns)}")
# print("=" * 60)

# if duplicate_ids == 0:
#     print("BRONZE STATUS     : READY FOR SILVER")
# else:
#     print("BRONZE STATUS     : DUPLICATES DETECTED")

# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 61 — Silver Streaming Transformation
# ============================================================

# from pyspark.sql.functions import (
#     col,
#     trim,
#     upper,
#     when
# )

# silver_stream_df = (
#     bronze_stream_df
#     .withColumn("order_id", trim(col("order_id")))
#     .withColumn("customer_id", trim(col("customer_id")))
#     .withColumn("order_status", upper(trim(col("order_status"))))
#     .withColumn(
#         "is_valid_order",
#         when(
#             col("order_id").isNotNull() &
#             (col("order_id") != "") &
#             col("customer_id").isNotNull() &
#             (col("customer_id") != "") &
#             col("event_timestamp").isNotNull(),
#             True
#         ).otherwise(False)
#     )
#     .filter(col("is_valid_order") == True)
#     .drop("is_valid_order")
# )

# print("=" * 60)
# print("SILVER STREAMING DATAFRAME")
# print("=" * 60)
# print("Is Streaming :", silver_stream_df.isStreaming)
# print("Columns      :", len(silver_stream_df.columns))
# print("Column Names :", silver_stream_df.columns)
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 62 — Write Silver Streaming Table
# ============================================================

# SILVER_TABLE = "sdp_catalog.realtime_ecommerce.silver_stream_orders"

# SILVER_CHECKPOINT = (
#     "/Volumes/sdp_catalog/realtime_ecommerce/"
#     "streaming_input/_checkpoints/silver_orders"
# )

# silver_query = (
#     silver_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", SILVER_CHECKPOINT)
#     .trigger(availableNow=True)
#     .toTable(SILVER_TABLE)
# )

# silver_query.awaitTermination()

# print("=" * 60)
# print("SILVER STREAMING TABLE")
# print("=" * 60)
# print(f"Table      : {SILVER_TABLE}")
# print(f"Checkpoint : {SILVER_CHECKPOINT}")
# print("Status     : COMPLETED")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 63 — Silver Streaming Validation
# ============================================================

# SILVER_TABLE = "sdp_catalog.realtime_ecommerce.silver_stream_orders"

# silver_validation_df = spark.sql(f"""
#     SELECT *
#     FROM {SILVER_TABLE}
#     ORDER BY event_timestamp
# """)

# silver_stats = silver_validation_df.agg(
#     count("*").alias("total_records"),
#     countDistinct("order_id").alias("unique_order_ids")
# ).collect()[0]

# total_records = silver_stats["total_records"]
# unique_order_ids = silver_stats["unique_order_ids"]
# duplicate_ids = total_records - unique_order_ids

# print("=" * 60)
# print("SILVER STREAMING TABLE VALIDATION")
# print("=" * 60)
# print(f"Total Records     : {total_records:,}")
# print(f"Unique Order IDs  : {unique_order_ids:,}")
# print(f"Duplicate IDs     : {duplicate_ids:,}")
# print(f"Columns           : {len(silver_validation_df.columns)}")
# print("=" * 60)

# if duplicate_ids == 0:
#     print("SILVER STATUS     : READY FOR GOLD")
# else:
#     print("SILVER STATUS     : DUPLICATES DETECTED")

# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 64 — Gold Streaming Transformation
# ============================================================

# from pyspark.sql.functions import (
#     col,
#     when,
#     datediff
# )

# gold_stream_df = (
#     silver_stream_df
#     .withColumn(
#         "delivery_status",
#         when(
#             col("order_delivered_customer_date").isNotNull(),
#             "DELIVERED"
#         )
#         .when(
#             col("order_status").isin("CANCELED", "UNAVAILABLE"),
#             "CLOSED"
#         )
#         .otherwise("IN_PROGRESS")
#     )
#     .withColumn(
#         "delivery_days",
#         when(
#             col("order_delivered_customer_date").isNotNull(),
#             datediff(
#                 col("order_delivered_customer_date"),
#                 col("order_purchase_timestamp")
#             )
#         )
#     )
# )

# print("=" * 60)
# print("GOLD STREAMING DATAFRAME")
# print("=" * 60)
# print("Is Streaming :", gold_stream_df.isStreaming)
# print("Columns      :", len(gold_stream_df.columns))
# print("Column Names :", gold_stream_df.columns)
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 65 — Write Gold Streaming Table
# ============================================================

# GOLD_TABLE = "sdp_catalog.realtime_ecommerce.gold_stream_orders"

# GOLD_CHECKPOINT = (
#     "/Volumes/sdp_catalog/realtime_ecommerce/"
#     "streaming_input/_checkpoints/gold_orders"
# )

# gold_query = (
#     gold_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", GOLD_CHECKPOINT)
#     .trigger(availableNow=True)
#     .toTable(GOLD_TABLE)
# )

# gold_query.awaitTermination()

# print("=" * 60)
# print("GOLD STREAMING TABLE")
# print("=" * 60)
# print(f"Table      : {GOLD_TABLE}")
# print(f"Checkpoint : {GOLD_CHECKPOINT}")
# print("Status     : COMPLETED")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 66 — Final Gold Streaming Validation
# ============================================================

# GOLD_TABLE = "sdp_catalog.realtime_ecommerce.gold_stream_orders"

# gold_validation_df = spark.sql(f"""
#     SELECT *
#     FROM {GOLD_TABLE}
#     ORDER BY event_timestamp
# """)

# gold_stats = gold_validation_df.agg(
#     count("*").alias("total_records"),
#     countDistinct("order_id").alias("unique_order_ids")
# ).collect()[0]

# total_records = gold_stats["total_records"]
# unique_order_ids = gold_stats["unique_order_ids"]
# duplicate_ids = total_records - unique_order_ids

# print("=" * 65)
# print("FINAL GOLD STREAMING VALIDATION")
# print("=" * 65)
# print(f"Gold Records       : {total_records:,}")
# print(f"Unique Order IDs   : {unique_order_ids:,}")
# print(f"Duplicate IDs      : {duplicate_ids:,}")
# print(f"Columns            : {len(gold_validation_df.columns)}")
# print("=" * 65)

# if duplicate_ids == 0:
#     print("GOLD STREAM STATUS : READY")
# else:
#     print("GOLD STREAM STATUS : DUPLICATES DETECTED")

# print("=" * 65)

# COMMAND ----------

# ============================================================
# Cell 67 — Generate Incremental Streaming Events
# ============================================================

# from datetime import datetime, timedelta
# import json

# STREAM_VOLUME = "/Volumes/sdp_catalog/realtime_ecommerce/streaming_input"

# base_time = datetime.now()

# incremental_events = [
#     {
#         "order_id": "STREAM_004",
#         "customer_id": "CUST_004",
#         "order_status": "created",
#         "order_purchase_timestamp": base_time,
#         "order_delivered_customer_date": None,
#         "order_estimated_delivery_date": base_time + timedelta(days=7),
#         "event_timestamp": base_time
#     },
#     {
#         "order_id": "STREAM_005",
#         "customer_id": "CUST_005",
#         "order_status": "shipped",
#         "order_purchase_timestamp": base_time - timedelta(days=1),
#         "order_delivered_customer_date": None,
#         "order_estimated_delivery_date": base_time + timedelta(days=5),
#         "event_timestamp": base_time
#     }
# ]

# json_events = []

# for event in incremental_events:
#     converted = event.copy()

#     for key, value in converted.items():
#         if isinstance(value, datetime):
#             converted[key] = value.isoformat()

#     json_events.append(converted)

# events_df = spark.createDataFrame(
#     [(json.dumps(event),) for event in json_events],
#     ["value"]
# )

# events_df.coalesce(1).write.mode("append").text(STREAM_VOLUME)

# print("=" * 60)
# print("INCREMENTAL STREAMING EVENTS")
# print("=" * 60)
# print(f"New Events Created : {len(incremental_events)}")
# print(f"Location           : {STREAM_VOLUME}")
# print("Status             : READY")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 68 — Process Incremental Streaming Batch
# ============================================================

# print("=" * 60)
# print("PROCESSING INCREMENTAL STREAMING BATCH")
# print("=" * 60)

# # ---------- BRONZE ----------
# bronze_incremental_query = (
#     bronze_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "/Volumes/sdp_catalog/realtime_ecommerce/"
#         "streaming_input/_checkpoints/bronze_orders"
#     )
#     .trigger(availableNow=True)
#     .toTable("sdp_catalog.realtime_ecommerce.bronze_stream_orders")
# )

# bronze_incremental_query.awaitTermination()

# print("Bronze : Incremental batch processed")

# # ---------- SILVER ----------
# silver_incremental_query = (
#     silver_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "/Volumes/sdp_catalog/realtime_ecommerce/"
#         "streaming_input/_checkpoints/silver_orders"
#     )
#     .trigger(availableNow=True)
#     .toTable("sdp_catalog.realtime_ecommerce.silver_stream_orders")
# )

# silver_incremental_query.awaitTermination()

# print("Silver : Incremental batch processed")

# # ---------- GOLD ----------
# gold_incremental_query = (
#     gold_stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "/Volumes/sdp_catalog/realtime_ecommerce/"
#         "streaming_input/_checkpoints/gold_orders"
#     )
#     .trigger(availableNow=True)
#     .toTable("sdp_catalog.realtime_ecommerce.gold_stream_orders")
# )

# gold_incremental_query.awaitTermination()

# print("Gold   : Incremental batch processed")

# print("=" * 60)
# print("INCREMENTAL STREAMING STATUS : COMPLETED")
# print("=" * 60)

# COMMAND ----------

# ============================================================
# Cell 69 — Final Streaming Reconciliation
# ============================================================

# BRONZE_TABLE = "sdp_catalog.realtime_ecommerce.bronze_stream_orders"
# SILVER_TABLE = "sdp_catalog.realtime_ecommerce.silver_stream_orders"
# GOLD_TABLE   = "sdp_catalog.realtime_ecommerce.gold_stream_orders"

# bronze_count = spark.table(BRONZE_TABLE).count()
# silver_count = spark.table(SILVER_TABLE).count()
# gold_count   = spark.table(GOLD_TABLE).count()

# bronze_unique = spark.table(BRONZE_TABLE).select("order_id").distinct().count()
# silver_unique = spark.table(SILVER_TABLE).select("order_id").distinct().count()
# gold_unique   = spark.table(GOLD_TABLE).select("order_id").distinct().count()

# print("=" * 70)
# print("FINAL STREAMING PIPELINE RECONCILIATION")
# print("=" * 70)

# print(f"Bronze Records        : {bronze_count:,}")
# print(f"Silver Records        : {silver_count:,}")
# print(f"Gold Records          : {gold_count:,}")

# print("-" * 70)

# print(f"Bronze Unique Orders  : {bronze_unique:,}")
# print(f"Silver Unique Orders  : {silver_unique:,}")
# print(f"Gold Unique Orders    : {gold_unique:,}")

# print("-" * 70)

# print(f"Bronze Duplicate IDs  : {bronze_count - bronze_unique:,}")
# print(f"Silver Duplicate IDs  : {silver_count - silver_unique:,}")
# print(f"Gold Duplicate IDs    : {gold_count - gold_unique:,}")

# print("=" * 70)

# if (
#     bronze_count == 5 and
#     silver_count == 5 and
#     gold_count == 5 and
#     bronze_count == bronze_unique and
#     silver_count == silver_unique and
#     gold_count == gold_unique
# ):
#     print("STREAMING PIPELINE STATUS : READY")
# else:
#     print("STREAMING PIPELINE STATUS : REVIEW REQUIRED")

# print("=" * 70)

# COMMAND ----------

