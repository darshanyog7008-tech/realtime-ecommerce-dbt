# Databricks notebook source
# ============================================================
# Cell 1 — SQL Analytics Environment
# ============================================================

CATALOG = "sdp_catalog"
SCHEMA = "realtime_ecommerce"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("=" * 70)
print("REALTIME ECOMMERCE LAKEHOUSE")
print("SQL ANALYTICS ENVIRONMENT")
print("=" * 70)

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")

print("=" * 70)
print("Status  : READY")
print("=" * 70)

# COMMAND ----------

# ============================================================
# Cell 2 — Gold Layer Inventory
# ============================================================

tables_df = spark.sql("SHOW TABLES")

print("=" * 70)
print("REALTIME ECOMMERCE — TABLE INVENTORY")
print("=" * 70)

tables_df.show(truncate=False)

print("=" * 70)
print("Status : TABLE INVENTORY READY")
print("=" * 70)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 3 — Gold Table Schema Inspection
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_order_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 4 — Customer Gold Table Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_customer_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 5 — Product Gold Table Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_product_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 6 — Seller Gold Table Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_seller_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 7 — Category Gold Table Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_category_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 8 — Overall Order KPIs
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(total_items) AS total_items
# MAGIC FROM gold_order_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 9 — Orders by Status
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS order_count
# MAGIC FROM gold_order_fact
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 10 — Conditional Aggregation
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'delivered' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS delivered_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'canceled' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS canceled_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status NOT IN ('delivered', 'canceled') THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS active_orders
# MAGIC
# MAGIC FROM gold_order_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 11 — Orders by Customer City
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     customer_city,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     SUM(total_items) AS total_items
# MAGIC FROM gold_order_fact
# MAGIC GROUP BY customer_city
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 12 — Customer Summary Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_customer_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 13 — Customer Summary Schema
# MAGIC -- ============================================================
# MAGIC
# MAGIC DESCRIBE TABLE gold_customer_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 13 — Customer Summary Profile
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     customer_type,
# MAGIC     COUNT(*) AS customer_count
# MAGIC FROM gold_customer_summary
# MAGIC GROUP BY customer_type
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 14 — Customer Type Percentage
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH customer_counts AS (
# MAGIC     SELECT
# MAGIC         customer_type,
# MAGIC         COUNT(*) AS customer_count
# MAGIC     FROM gold_customer_summary
# MAGIC     GROUP BY customer_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     customer_type,
# MAGIC     customer_count,
# MAGIC     ROUND(
# MAGIC         customer_count * 100.0
# MAGIC         / SUM(customer_count) OVER (),
# MAGIC         2
# MAGIC     ) AS customer_percentage
# MAGIC FROM customer_counts
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 15 — ROW_NUMBER() Ranking
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH customer_counts AS (
# MAGIC     SELECT
# MAGIC         customer_type,
# MAGIC         COUNT(*) AS customer_count
# MAGIC     FROM gold_customer_summary
# MAGIC     GROUP BY customer_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     customer_type,
# MAGIC     customer_count,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY customer_count DESC
# MAGIC     ) AS customer_rank
# MAGIC FROM customer_counts
# MAGIC ORDER BY customer_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 16 — ROW_NUMBER() Deduplication Pattern
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH ranked_orders AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY customer_id
# MAGIC             ORDER BY order_purchase_timestamp DESC
# MAGIC         ) AS rn
# MAGIC     FROM gold_order_fact
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ranked_orders
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 17 — RANK() vs DENSE_RANK()
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH customer_counts AS (
# MAGIC     SELECT
# MAGIC         customer_type,
# MAGIC         COUNT(*) AS customer_count
# MAGIC     FROM gold_customer_summary
# MAGIC     GROUP BY customer_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     customer_type,
# MAGIC     customer_count,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY customer_count DESC
# MAGIC     ) AS rank_value,
# MAGIC
# MAGIC     DENSE_RANK() OVER (
# MAGIC         ORDER BY customer_count DESC
# MAGIC     ) AS dense_rank_value
# MAGIC
# MAGIC FROM customer_counts
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 18 — Advanced Window Functions
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH customer_orders AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(*) AS order_count
# MAGIC     FROM gold_order_fact
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     order_count,
# MAGIC     ROUND(
# MAGIC         AVG(order_count) OVER (),
# MAGIC         2
# MAGIC     ) AS average_orders_per_customer,
# MAGIC     order_count
# MAGIC         - AVG(order_count) OVER () AS difference_from_average
# MAGIC FROM customer_orders
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 19 — Product Performance Analytics
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     product_category_name,
# MAGIC     COUNT(*) AS product_count,
# MAGIC     SUM(product_revenue) AS total_revenue,
# MAGIC     ROUND(AVG(product_revenue), 2) AS average_product_revenue
# MAGIC FROM gold_product_performance
# MAGIC GROUP BY product_category_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 20 — Advanced Revenue Classification
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH category_revenue AS (
# MAGIC     SELECT
# MAGIC         product_category_name,
# MAGIC         SUM(product_revenue) AS total_revenue
# MAGIC     FROM gold_product_performance
# MAGIC     GROUP BY product_category_name
# MAGIC ),
# MAGIC
# MAGIC ranked_categories AS (
# MAGIC     SELECT
# MAGIC         product_category_name,
# MAGIC         total_revenue,
# MAGIC
# MAGIC         RANK() OVER (
# MAGIC             ORDER BY total_revenue DESC
# MAGIC         ) AS revenue_rank,
# MAGIC
# MAGIC         SUM(total_revenue) OVER () AS overall_revenue
# MAGIC     FROM category_revenue
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     product_category_name,
# MAGIC     ROUND(total_revenue, 2) AS total_revenue,
# MAGIC     revenue_rank,
# MAGIC
# MAGIC     ROUND(
# MAGIC         total_revenue * 100.0 / overall_revenue,
# MAGIC         2
# MAGIC     ) AS revenue_percentage,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN total_revenue >= overall_revenue * 0.10
# MAGIC             THEN 'HIGH'
# MAGIC         WHEN total_revenue >= overall_revenue * 0.05
# MAGIC             THEN 'MEDIUM'
# MAGIC         ELSE 'LOW'
# MAGIC     END AS revenue_segment
# MAGIC
# MAGIC FROM ranked_categories
# MAGIC ORDER BY revenue_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Cell 21 — SQL Analytics Final Sign-Off
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH table_counts AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         'gold_order_fact' AS table_name,
# MAGIC         COUNT(*) AS record_count
# MAGIC     FROM gold_order_fact
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'gold_customer_summary',
# MAGIC         COUNT(*)
# MAGIC     FROM gold_customer_summary
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'gold_product_performance',
# MAGIC         COUNT(*)
# MAGIC     FROM gold_product_performance
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'gold_seller_performance',
# MAGIC         COUNT(*)
# MAGIC     FROM gold_seller_performance
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'gold_category_performance',
# MAGIC         COUNT(*)
# MAGIC     FROM gold_category_performance
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     record_count
# MAGIC FROM table_counts
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Analytics — Final Sign-Off
# MAGIC
# MAGIC ## Completed Areas
# MAGIC
# MAGIC - Gold layer table discovery
# MAGIC - Business KPI analysis
# MAGIC - GROUP BY and ORDER BY
# MAGIC - Conditional aggregation
# MAGIC - CTEs
# MAGIC - Subqueries / analytical patterns
# MAGIC - ROW_NUMBER()
# MAGIC - RANK()
# MAGIC - DENSE_RANK()
# MAGIC - Window functions
# MAGIC - Deduplication patterns
# MAGIC - Customer analytics
# MAGIC - Product performance analytics
# MAGIC - Revenue classification
# MAGIC - Percentage calculations
# MAGIC - CASE WHEN logic
# MAGIC
# MAGIC ## Gold Analytical Tables
# MAGIC
# MAGIC - gold_order_fact
# MAGIC - gold_customer_summary
# MAGIC - gold_product_performance
# MAGIC - gold_seller_performance
# MAGIC - gold_category_performance
# MAGIC
# MAGIC ## SQL Analytics Status
# MAGIC
# MAGIC COMPLETE
# MAGIC
# MAGIC The SQL Analytics layer has been validated against the
# MAGIC Realtime Ecommerce Lakehouse Gold layer and is ready
# MAGIC for interview demonstration and portfolio documentation.

# COMMAND ----------

