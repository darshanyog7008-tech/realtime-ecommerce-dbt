# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Real-Time E-Commerce Lakehouse Project Explanation
# MAGIC
# MAGIC ## Project Overview
# MAGIC
# MAGIC This project implements a Real-Time E-Commerce Lakehouse using
# MAGIC Databricks and Apache Spark.
# MAGIC
# MAGIC The objective is to build a scalable data platform capable of
# MAGIC processing e-commerce data through batch and streaming pipelines,
# MAGIC transforming raw data into curated analytical datasets, and exposing
# MAGIC business-ready information for analytics.
# MAGIC
# MAGIC ## Technology Stack
# MAGIC
# MAGIC - Databricks
# MAGIC - Apache Spark / PySpark
# MAGIC - Delta Lake
# MAGIC - Spark Structured Streaming
# MAGIC - Apache Kafka
# MAGIC - SQL
# MAGIC - dbt
# MAGIC - Unity Catalog
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC The project follows the Medallion Architecture:
# MAGIC
# MAGIC Raw Data
# MAGIC    ↓
# MAGIC Bronze Layer
# MAGIC    ↓
# MAGIC Silver Layer
# MAGIC    ↓
# MAGIC Gold Layer
# MAGIC    ↓
# MAGIC Analytics / Streaming Consumers
# MAGIC
# MAGIC The architecture separates ingestion, cleansing, transformation,
# MAGIC aggregation, and analytical consumption into distinct layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC The Bronze layer stores the ingested e-commerce data with minimal
# MAGIC transformation.
# MAGIC
# MAGIC Its primary purpose is to provide a reliable raw/landing layer that
# MAGIC can be used for downstream processing and recovery.
# MAGIC
# MAGIC Example dataset:
# MAGIC
# MAGIC - `bronze_orders`
# MAGIC
# MAGIC Validated record count:
# MAGIC
# MAGIC - 99,441 orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC The Silver layer applies data cleansing, validation, type conversion,
# MAGIC deduplication, and business-oriented transformations.
# MAGIC
# MAGIC Invalid or problematic records are separated from the curated dataset
# MAGIC where required.
# MAGIC
# MAGIC Example dataset:
# MAGIC
# MAGIC - `silver_orders`
# MAGIC
# MAGIC Validated record count:
# MAGIC
# MAGIC - 99,252 orders
# MAGIC
# MAGIC Bronze → Silver reduction:
# MAGIC
# MAGIC - Bronze: 99,441
# MAGIC - Silver: 99,252
# MAGIC - Excluded: 189

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC The Gold layer contains business-ready datasets designed for analytics.
# MAGIC
# MAGIC Key outputs include:
# MAGIC
# MAGIC - `gold_order_fact`
# MAGIC - `gold_customer_summary`
# MAGIC - `gold_product_performance`
# MAGIC - `gold_category_performance`
# MAGIC - `gold_seller_performance`
# MAGIC
# MAGIC Validated outputs:
# MAGIC
# MAGIC - Gold Order Fact: 99,252
# MAGIC - Customer Summary: 95,913
# MAGIC - Product Performance: 32,951
# MAGIC - Category Performance: 74
# MAGIC - Seller Performance: 3,095

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured Streaming
# MAGIC
# MAGIC The project includes a Spark Structured Streaming pipeline for processing
# MAGIC real-time order events.
# MAGIC
# MAGIC Kafka is used as the event streaming platform, while Spark Structured
# MAGIC Streaming processes the incoming events.
# MAGIC
# MAGIC The processed streaming data is written to the Gold streaming layer.
# MAGIC
# MAGIC Current validation:
# MAGIC
# MAGIC - `gold_stream_orders`: 15 records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality
# MAGIC
# MAGIC Data quality checks are applied throughout the pipeline.
# MAGIC
# MAGIC Examples include:
# MAGIC
# MAGIC - Required-field validation
# MAGIC - Null checks
# MAGIC - Duplicate detection
# MAGIC - Schema validation
# MAGIC - Quarantine of invalid records
# MAGIC - Gold-layer validation
# MAGIC
# MAGIC The Gold Order Fact validation confirmed:
# MAGIC
# MAGIC - Null `order_id`: 0
# MAGIC - Duplicate `order_id`: 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbt Layer
# MAGIC
# MAGIC dbt is used as the SQL transformation and analytics engineering layer
# MAGIC on top of the Databricks environment.
# MAGIC
# MAGIC The dbt project contains:
# MAGIC
# MAGIC - Source definitions
# MAGIC - Staging models
# MAGIC - Fact models
# MAGIC - Customer-level analytical marts
# MAGIC - Data-quality tests
# MAGIC - Documentation metadata
# MAGIC
# MAGIC The final dbt build completed successfully with:
# MAGIC
# MAGIC - 10 PASS
# MAGIC - 0 WARN
# MAGIC - 0 ERROR

# COMMAND ----------

