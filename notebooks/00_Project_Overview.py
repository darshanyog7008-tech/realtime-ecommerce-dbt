# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # Realtime E-Commerce Lakehouse
# MAGIC
# MAGIC ## Project Overview
# MAGIC
# MAGIC This project demonstrates how to build a modern end-to-end Data Engineering pipeline using:
# MAGIC
# MAGIC - Apache Kafka
# MAGIC - PySpark
# MAGIC - Spark Structured Streaming
# MAGIC - Delta Lake
# MAGIC - Spark SQL
# MAGIC - Medallion Architecture (Bronze, Silver, Gold)
# MAGIC
# MAGIC The project simulates a real-time e-commerce platform where customer orders are ingested, transformed, optimized, and analyzed to generate business insights.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Problem
# MAGIC
# MAGIC An e-commerce company receives thousands of events every minute.
# MAGIC
# MAGIC Examples include:
# MAGIC
# MAGIC - New customer orders
# MAGIC - Payment transactions
# MAGIC - Product purchases
# MAGIC - Delivery updates
# MAGIC - Customer reviews
# MAGIC
# MAGIC Business teams need near real-time insights such as:
# MAGIC
# MAGIC - Revenue
# MAGIC - Order trends
# MAGIC - Customer behavior
# MAGIC - Top-selling products
# MAGIC - Payment success rate
# MAGIC
# MAGIC The challenge is building a scalable data platform capable of processing both historical and streaming data efficiently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technology Stack
# MAGIC
# MAGIC | Component | Technology |
# MAGIC |-----------|------------|
# MAGIC | Programming | Python |
# MAGIC | Processing Engine | Apache Spark |
# MAGIC | Streaming | Apache Kafka |
# MAGIC | Data Lake | Delta Lake |
# MAGIC | Query Engine | Spark SQL |
# MAGIC | Architecture | Medallion Architecture |
# MAGIC | Development | Databricks |
# MAGIC | Version Control | GitHub |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset
# MAGIC
# MAGIC The project uses the Brazilian Olist E-Commerce dataset.
# MAGIC
# MAGIC Primary datasets:
# MAGIC
# MAGIC - olist_orders_dataset.csv
# MAGIC - olist_customers_dataset.csv
# MAGIC - olist_order_items_dataset.csv
# MAGIC - olist_order_payments_dataset.csv
# MAGIC - olist_products_dataset.csv
# MAGIC - olist_sellers_dataset.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-Level Architecture
# MAGIC
# MAGIC CSV Files
# MAGIC       │
# MAGIC       ▼
# MAGIC Kafka Producer
# MAGIC       │
# MAGIC       ▼
# MAGIC Kafka Topic
# MAGIC       │
# MAGIC       ▼
# MAGIC Spark Structured Streaming
# MAGIC       │
# MAGIC       ▼
# MAGIC Bronze Layer
# MAGIC       │
# MAGIC       ▼
# MAGIC Silver Layer
# MAGIC       │
# MAGIC       ▼
# MAGIC Gold Layer
# MAGIC       │
# MAGIC       ▼
# MAGIC Business Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this project we will understand:
# MAGIC
# MAGIC - Data Ingestion
# MAGIC - Spark Transformations
# MAGIC - Structured Streaming
# MAGIC - Delta Lake
# MAGIC - Medallion Architecture
# MAGIC - Spark SQL
# MAGIC - Performance Optimization
# MAGIC - Data Quality Validation
# MAGIC - Real-world Data Engineering Design Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏗️ Project Architecture
# MAGIC
# MAGIC The Realtime Ecommerce Lakehouse follows a Medallion Architecture implemented on Databricks.
# MAGIC
# MAGIC ```text
# MAGIC                     E-COMMERCE SOURCE DATA
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                    ┌─────────────────┐
# MAGIC                    │  DATA INGESTION │
# MAGIC                    └────────┬────────┘
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                    ┌─────────────────┐
# MAGIC                    │  BRONZE LAYER   │
# MAGIC                    │  Raw / Ingested │
# MAGIC                    └────────┬────────┘
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                    ┌─────────────────┐
# MAGIC                    │  SILVER LAYER   │
# MAGIC                    │ Cleaned /       │
# MAGIC                    │ Curated         │
# MAGIC                    └────────┬────────┘
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                    ┌─────────────────┐
# MAGIC                    │   GOLD LAYER    │
# MAGIC                    │ Business-ready  │
# MAGIC                    │ Analytics       │
# MAGIC                    └───────┬─────────┘
# MAGIC                            │
# MAGIC              ┌─────────────┼─────────────┐
# MAGIC              ▼             ▼             ▼
# MAGIC        SQL Analytics   Data Quality   Performance
# MAGIC              │
# MAGIC              ▼
# MAGIC        Business Insights
# MAGIC
# MAGIC
# MAGIC         ─────── SEPARATE STREAMING PATH ───────
# MAGIC
# MAGIC                  Streaming Input
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                  Bronze Stream
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                  Silver Stream
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                   Gold Stream
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                  Incremental Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cell 9 — Technology Stack
# MAGIC
# MAGIC ```markdown
# MAGIC # 🛠️ Technology Stack
# MAGIC
# MAGIC | Technology | Purpose |
# MAGIC |---|---|
# MAGIC | Python | Data engineering and PySpark development |
# MAGIC | PySpark | Distributed data processing |
# MAGIC | Apache Spark | Batch and streaming computation |
# MAGIC | Delta Lake | Reliable lakehouse storage |
# MAGIC | Databricks | Lakehouse development and execution |
# MAGIC | Photon | Accelerated SQL/DataFrame execution |
# MAGIC | Structured Streaming | Incremental event processing |
# MAGIC | SQL | Analytics and business transformations |
# MAGIC | Lakeflow Jobs | Workflow orchestration |
# MAGIC | Unity Catalog | Data organization and governance |
# MAGIC | Serverless Compute | Managed project execution |
# MAGIC
# MAGIC ### Core Engineering Concepts
# MAGIC
# MAGIC - Medallion Architecture
# MAGIC - ETL / ELT
# MAGIC - Incremental Processing
# MAGIC - Data Quality Validation
# MAGIC - Query Optimization
# MAGIC - Predicate Pushdown
# MAGIC - Column Pruning
# MAGIC - Shuffle Analysis
# MAGIC - Delta Lake Optimization
# MAGIC - Batch + Streaming Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔄 End-to-End Data Flow
# MAGIC
# MAGIC The project processes ecommerce data through both batch and streaming paths.
# MAGIC
# MAGIC ## Batch Processing Flow
# MAGIC
# MAGIC Source Data  
# MAGIC ↓  
# MAGIC Data Ingestion  
# MAGIC ↓  
# MAGIC Bronze Layer  
# MAGIC ↓  
# MAGIC Silver Layer  
# MAGIC ↓  
# MAGIC Gold Layer  
# MAGIC ↓  
# MAGIC Data Quality + SQL Analytics + Performance Analysis  
# MAGIC ↓  
# MAGIC Business-Ready Data
# MAGIC
# MAGIC ## Streaming Processing Flow
# MAGIC
# MAGIC Streaming Input  
# MAGIC ↓  
# MAGIC Structured Streaming  
# MAGIC ↓  
# MAGIC Bronze Stream  
# MAGIC ↓  
# MAGIC Silver Stream  
# MAGIC ↓  
# MAGIC Gold Stream  
# MAGIC ↓  
# MAGIC Incremental Analytics
# MAGIC
# MAGIC ## Orchestration
# MAGIC
# MAGIC The batch pipeline is orchestrated using Databricks Jobs with task dependencies:
# MAGIC
# MAGIC 01_Data_Ingestion  
# MAGIC → 02_Bronze_Layer  
# MAGIC → 03_Silver_Layer  
# MAGIC → 04_Gold_Layer  
# MAGIC
# MAGIC After Gold completes successfully, the following tasks can execute:
# MAGIC
# MAGIC - 07_Data_Quality
# MAGIC - 05_SQL_Analytics
# MAGIC - 08_Performance_Optimization
# MAGIC
# MAGIC The Structured Streaming pipeline is orchestrated separately through its own Databricks Job.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥉🥈🥇 Medallion Architecture
# MAGIC
# MAGIC The project follows a three-layer Medallion Architecture to progressively transform raw ecommerce data into reliable, business-ready datasets.
# MAGIC
# MAGIC ## 🥉 Bronze Layer — Raw Data
# MAGIC
# MAGIC The Bronze layer stores the ingested ecommerce data with minimal transformation.
# MAGIC
# MAGIC ### Responsibilities
# MAGIC - Preserve the source structure
# MAGIC - Maintain source-level records
# MAGIC - Add ingestion metadata
# MAGIC - Provide a reliable foundation for downstream processing
# MAGIC - Support traceability back to the source
# MAGIC
# MAGIC **Primary table:** `bronze_orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥈 Silver Layer — Cleaned & Curated Data
# MAGIC
# MAGIC The Silver layer applies data cleaning, validation and business transformations.
# MAGIC
# MAGIC ### Responsibilities
# MAGIC - Standardize data types
# MAGIC - Validate important fields
# MAGIC - Remove duplicate orders
# MAGIC - Calculate operational metrics
# MAGIC - Prepare trusted data for analytical processing
# MAGIC
# MAGIC ### Example transformations
# MAGIC - Approval duration
# MAGIC - Delivery duration
# MAGIC - Late-delivery identification
# MAGIC - Data standardization
# MAGIC
# MAGIC **Primary table:** `silver_orders`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥇 Gold Layer — Business-Ready Data
# MAGIC
# MAGIC The Gold layer combines curated datasets and creates analytical features required for business reporting and analytics.
# MAGIC
# MAGIC ### Responsibilities
# MAGIC - Combine orders, customers, products, payments and seller information
# MAGIC - Calculate order-level metrics
# MAGIC - Create business classifications
# MAGIC - Prepare analytical fact and summary datasets
# MAGIC - Serve SQL analytics and downstream consumers
# MAGIC
# MAGIC **Primary table:** `gold_order_fact`
# MAGIC
# MAGIC ### Example Gold Metrics
# MAGIC
# MAGIC - Total items
# MAGIC - Unique products
# MAGIC - Unique sellers
# MAGIC - Items revenue
# MAGIC - Total freight
# MAGIC - Average item price
# MAGIC - Total payment value
# MAGIC - Order total value
# MAGIC - Delivery performance
# MAGIC - Order value bucket
# MAGIC - Order size bucket
# MAGIC - Payment status
# MAGIC
# MAGIC ## Data Flow
# MAGIC
# MAGIC **Raw Source → Bronze → Silver → Gold → Analytics**
# MAGIC
# MAGIC This layered approach separates ingestion, data refinement and business logic, making the pipeline easier to maintain, test and scale.

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚙️ Batch Data Pipeline
# MAGIC
# MAGIC The batch pipeline processes ecommerce data through the Medallion Architecture and is orchestrated using Databricks Jobs.
# MAGIC
# MAGIC ## Pipeline Flow
# MAGIC
# MAGIC **01_Data_Ingestion**  
# MAGIC ↓  
# MAGIC **02_Bronze_Layer**  
# MAGIC ↓  
# MAGIC **03_Silver_Layer**  
# MAGIC ↓  
# MAGIC **04_Gold_Layer**
# MAGIC
# MAGIC After the Gold layer completes successfully, three analytical and validation tasks execute:
# MAGIC
# MAGIC - **07_Data_Quality**
# MAGIC - **05_SQL_Analytics**
# MAGIC - **08_Performance_Optimization**
# MAGIC
# MAGIC ## Task Responsibilities
# MAGIC
# MAGIC ### 01 — Data Ingestion
# MAGIC Ingests the source ecommerce datasets and prepares them for lakehouse processing.
# MAGIC
# MAGIC ### 02 — Bronze Layer
# MAGIC Stores the ingested data in the raw Bronze layer with ingestion metadata.
# MAGIC
# MAGIC ### 03 — Silver Layer
# MAGIC Cleans, validates and transforms the Bronze data into curated datasets.
# MAGIC
# MAGIC ### 04 — Gold Layer
# MAGIC Combines curated datasets and creates business-ready analytical tables and metrics.
# MAGIC
# MAGIC ### 07 — Data Quality
# MAGIC Validates:
# MAGIC - Required-field null checks
# MAGIC - Duplicate order IDs
# MAGIC - Valid order statuses
# MAGIC - Numeric business rules
# MAGIC
# MAGIC ### 05 — SQL Analytics
# MAGIC Provides analytical queries for business metrics and ecommerce insights.
# MAGIC
# MAGIC ### 08 — Performance Optimization
# MAGIC Analyzes Spark execution behavior and optimization techniques such as Photon execution, partition behavior and Adaptive Query Execution.
# MAGIC
# MAGIC ## Orchestration
# MAGIC
# MAGIC The pipeline was implemented as a Databricks Job with task dependencies.
# MAGIC
# MAGIC The complete batch workflow was successfully executed end-to-end, with all tasks completing successfully.
# MAGIC
# MAGIC ## Design Principle
# MAGIC
# MAGIC The dependency structure ensures that downstream analytical and validation tasks execute only after the Gold layer has successfully produced the required business-ready datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🌊 Structured Streaming Pipeline
# MAGIC
# MAGIC The project includes a dedicated Structured Streaming pipeline for incremental ecommerce data processing.
# MAGIC
# MAGIC The streaming implementation is maintained separately from the batch Gold notebook to keep batch and streaming workloads independent.
# MAGIC
# MAGIC ## Streaming Flow
# MAGIC
# MAGIC **Streaming Input**  
# MAGIC ↓  
# MAGIC **Bronze Stream**  
# MAGIC ↓  
# MAGIC **Silver Stream**  
# MAGIC ↓  
# MAGIC **Gold Stream**
# MAGIC
# MAGIC ## Key Components
# MAGIC
# MAGIC ### Streaming Input
# MAGIC New ecommerce records are read from the configured streaming input location.
# MAGIC
# MAGIC ### Bronze Stream
# MAGIC Captures incoming records while preserving the source structure and streaming metadata.
# MAGIC
# MAGIC ### Silver Stream
# MAGIC Applies cleaning, validation and required transformations to the incoming records.
# MAGIC
# MAGIC ### Gold Stream
# MAGIC Produces business-ready streaming data for downstream analytics.
# MAGIC
# MAGIC ## Checkpointing
# MAGIC
# MAGIC The streaming pipeline uses a dedicated checkpoint location to maintain streaming progress and provide fault-tolerant processing.
# MAGIC
# MAGIC Checkpoint state is kept separate from the batch pipeline.
# MAGIC
# MAGIC ## Trigger Strategy
# MAGIC
# MAGIC The pipeline uses the `availableNow` trigger.
# MAGIC
# MAGIC This allows Spark Structured Streaming to process the data currently available at execution time and then terminate after the available workload has been processed.
# MAGIC
# MAGIC ## Orchestration
# MAGIC
# MAGIC The streaming pipeline is deployed through a separate Databricks Job:
# MAGIC
# MAGIC **06_Structured_Streaming**
# MAGIC
# MAGIC This keeps the streaming workload independent from the batch orchestration workflow.
# MAGIC
# MAGIC ## Batch vs Streaming
# MAGIC
# MAGIC | Batch Pipeline | Streaming Pipeline |
# MAGIC |---|---|
# MAGIC | Processes datasets as batch workloads | Processes incremental data |
# MAGIC | Orchestrated through the batch Job | Orchestrated through a separate streaming Job |
# MAGIC | Bronze → Silver → Gold | Bronze Stream → Silver Stream → Gold Stream |
# MAGIC | Used for complete/repeated processing | Used for incremental processing |
# MAGIC | Executes downstream DQ, SQL and optimization tasks | Uses Structured Streaming checkpoints |
# MAGIC
# MAGIC The streaming Job was successfully executed using the configured `availableNow` processing model.

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Data Quality Framework
# MAGIC
# MAGIC A dedicated Data Quality framework was implemented to validate the reliability of the Bronze, Silver and Gold datasets.
# MAGIC
# MAGIC The framework validates critical technical and business rules before the data is considered ready for downstream consumption.
# MAGIC
# MAGIC ## 1. Null Validation
# MAGIC
# MAGIC Critical columns were checked for null values across the core layers.
# MAGIC
# MAGIC ### Result
# MAGIC
# MAGIC | Layer | Key Columns Checked | Status |
# MAGIC |---|---|---|
# MAGIC | Bronze | order_id, customer_id, order_status, order_purchase_timestamp | PASS |
# MAGIC | Silver | order_id, customer_id, order_status, order_purchase_timestamp | PASS |
# MAGIC | Gold | order_id, customer_id, order_status, order_purchase_timestamp, customer_unique_id | PASS |
# MAGIC
# MAGIC All checked critical fields returned **0 null records**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Duplicate Order ID Validation
# MAGIC
# MAGIC Order-level uniqueness was validated using `order_id`.
# MAGIC
# MAGIC | Table | Records | Unique IDs | Duplicates | Status |
# MAGIC |---|---:|---:|---:|---|
# MAGIC | bronze_orders | 99,441 | 99,441 | 0 | PASS |
# MAGIC | silver_orders | 99,252 | 99,252 | 0 | PASS |
# MAGIC | gold_order_fact | 99,252 | 99,252 | 0 | PASS |
# MAGIC
# MAGIC No duplicate `order_id` values were detected.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Order Status Validation
# MAGIC
# MAGIC The pipeline validates order statuses against the expected ecommerce status values.
# MAGIC
# MAGIC Observed statuses:
# MAGIC
# MAGIC - approved
# MAGIC - canceled
# MAGIC - created
# MAGIC - delivered
# MAGIC - invoiced
# MAGIC - processing
# MAGIC - shipped
# MAGIC - unavailable
# MAGIC
# MAGIC **Invalid statuses: 0**
# MAGIC
# MAGIC **Status: PASS**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Numeric Business Rule Validation
# MAGIC
# MAGIC The Gold order fact table was validated for invalid numeric business values.
# MAGIC
# MAGIC Validated metrics include:
# MAGIC
# MAGIC - `total_items`
# MAGIC - `unique_products`
# MAGIC - `unique_sellers`
# MAGIC - `items_revenue`
# MAGIC - `total_freight`
# MAGIC - `average_item_price`
# MAGIC - `minimum_item_price`
# MAGIC - `maximum_item_price`
# MAGIC
# MAGIC All validated metrics returned:
# MAGIC
# MAGIC **Invalid Records: 0**
# MAGIC
# MAGIC **Overall Status: PASS**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Result
# MAGIC
# MAGIC The implemented validation framework completed successfully with all tested rules passing.
# MAGIC
# MAGIC **Overall Data Quality Status: PASS**
# MAGIC
# MAGIC The framework provides an additional validation layer between data processing and downstream analytical consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # 📊 SQL Analytics & Business Insights
# MAGIC
# MAGIC The project includes a dedicated SQL Analytics layer for extracting business insights from the Gold datasets.
# MAGIC
# MAGIC The SQL analysis focuses on order performance, customer behavior, product performance, payment behavior and delivery metrics.
# MAGIC
# MAGIC ## Analytical Areas
# MAGIC
# MAGIC ### Order Analytics
# MAGIC - Total orders
# MAGIC - Orders by status
# MAGIC - Orders by time period
# MAGIC - Order value distribution
# MAGIC - Average order value
# MAGIC
# MAGIC ### Customer Analytics
# MAGIC - Customer order frequency
# MAGIC - Customer-level revenue
# MAGIC - Customer segmentation
# MAGIC - Repeat purchasing behavior
# MAGIC
# MAGIC ### Product Analytics
# MAGIC - Product-level sales
# MAGIC - Revenue contribution
# MAGIC - Product performance
# MAGIC - Product category analysis
# MAGIC
# MAGIC ### Seller Analytics
# MAGIC - Seller order volume
# MAGIC - Seller revenue contribution
# MAGIC - Seller performance metrics
# MAGIC
# MAGIC ### Delivery Analytics
# MAGIC - Average delivery duration
# MAGIC - Late-delivery analysis
# MAGIC - Delivery performance by order status
# MAGIC - Delivery delay analysis
# MAGIC
# MAGIC ### Payment Analytics
# MAGIC - Payment method distribution
# MAGIC - Payment value
# MAGIC - Installment behavior
# MAGIC - Payment status analysis
# MAGIC
# MAGIC ## SQL Techniques Used
# MAGIC
# MAGIC The SQL Analytics notebook demonstrates practical Data Engineering SQL patterns including:
# MAGIC
# MAGIC - `JOIN`
# MAGIC - `GROUP BY`
# MAGIC - `ORDER BY`
# MAGIC - `CASE WHEN`
# MAGIC - `COALESCE`
# MAGIC - Common Table Expressions (CTEs)
# MAGIC - Subqueries
# MAGIC - Window functions
# MAGIC - `ROW_NUMBER()`
# MAGIC - Aggregations
# MAGIC - Deduplication logic
# MAGIC
# MAGIC ## Analytical Layer
# MAGIC
# MAGIC The SQL queries operate primarily on the curated Gold datasets, keeping business analytics separate from raw ingestion and transformation logic.
# MAGIC
# MAGIC This separation allows the Gold layer to act as a stable analytical interface for downstream SQL consumers.
# MAGIC
# MAGIC ## Outcome
# MAGIC
# MAGIC The SQL Analytics notebook demonstrates how the engineered lakehouse datasets can be transformed into actionable ecommerce metrics while maintaining a clear separation between data engineering and analytical consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚡ Performance Optimization
# MAGIC
# MAGIC The project includes a dedicated performance analysis notebook to evaluate how Spark executes the Gold-layer workloads on Databricks Serverless Compute.
# MAGIC
# MAGIC ## 1. Photon Execution
# MAGIC
# MAGIC The Gold table execution plan showed:
# MAGIC
# MAGIC - `PhotonScan parquet`
# MAGIC - `PhotonColumnarToRow`
# MAGIC - `PhotonResultStage`
# MAGIC
# MAGIC The Photon explanation indicated that the query was fully supported by Photon.
# MAGIC
# MAGIC This demonstrates that the workload can take advantage of Databricks' Photon execution engine.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Partition Analysis
# MAGIC
# MAGIC Partition behavior was analyzed through Spark SQL execution plans because RDD-based APIs are not supported on the project's Serverless Compute environment.
# MAGIC
# MAGIC The execution plan confirmed a direct Photon scan of the Delta/Parquet-backed Gold table.
# MAGIC
# MAGIC ### Observation
# MAGIC
# MAGIC The analysis focused on:
# MAGIC
# MAGIC - Physical execution plan
# MAGIC - File scan behavior
# MAGIC - Column projection
# MAGIC - Photon execution
# MAGIC - Optimizer statistics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Optimizer Statistics
# MAGIC
# MAGIC The execution-plan analysis reported:
# MAGIC
# MAGIC **Gold table statistics: FULL**
# MAGIC
# MAGIC This allows the optimizer to use available table statistics when planning supported queries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Adaptive Query Execution
# MAGIC
# MAGIC Adaptive Query Execution (AQE) was evaluated as part of the optimization analysis.
# MAGIC
# MAGIC The environment reported:
# MAGIC
# MAGIC **AQE Status: Unavailable**
# MAGIC
# MAGIC However, the notebook documents the purpose of AQE:
# MAGIC
# MAGIC - Runtime query optimization
# MAGIC - Coalescing small shuffle partitions
# MAGIC - Join-strategy optimization
# MAGIC - Assistance with certain data-skew scenarios
# MAGIC
# MAGIC The project therefore distinguishes between optimization concepts and features actually available in the execution environment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Serverless Compute Considerations
# MAGIC
# MAGIC The project runs on Databricks Serverless Compute, which introduces certain limitations.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - Custom PySpark RDD APIs are not supported.
# MAGIC - Traditional `persist()` / cache operations may not be available in the same way as on dedicated compute.
# MAGIC - Performance analysis therefore relies on supported DataFrame and SQL execution techniques.
# MAGIC
# MAGIC The optimization notebook was designed around these constraints rather than introducing unsupported APIs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Optimization Principles Applied
# MAGIC
# MAGIC The project emphasizes:
# MAGIC
# MAGIC - Photon-compatible SQL/DataFrame operations
# MAGIC - Column pruning
# MAGIC - Efficient projections
# MAGIC - Appropriate filtering
# MAGIC - Avoiding unnecessary transformations
# MAGIC - Understanding physical execution plans
# MAGIC - Using optimizer statistics
# MAGIC - Avoiding unsupported RDD-based approaches
# MAGIC
# MAGIC ## Result
# MAGIC
# MAGIC The performance analysis successfully inspected the Gold workload and documented how the Databricks Serverless environment executes and optimizes the workload.
# MAGIC
# MAGIC The objective was not to force unsupported optimization techniques, but to understand the execution environment and apply optimizations compatible with the platform.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Job Orchestration & Production Workflow
# MAGIC
# MAGIC The project uses Databricks Jobs to orchestrate the complete data engineering workflow.
# MAGIC
# MAGIC ## Batch Job
# MAGIC
# MAGIC The batch pipeline is implemented as a dependency-driven workflow:
# MAGIC
# MAGIC 01_Data_Ingestion
# MAGIC         ↓
# MAGIC 02_Bronze_Layer
# MAGIC         ↓
# MAGIC 03_Silver_Layer
# MAGIC         ↓
# MAGIC 04_Gold_Layer
# MAGIC         ↓
# MAGIC  ┌──────┼──────────────────┐
# MAGIC  ↓      ↓                  ↓
# MAGIC Data    SQL Analytics      Performance
# MAGIC Quality

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Engineering Challenges & Solutions
# MAGIC
# MAGIC During development, several practical engineering challenges were encountered and resolved.
# MAGIC
# MAGIC ## 1. Serverless Compute Limitations
# MAGIC
# MAGIC ### Challenge
# MAGIC Some traditional Spark APIs, particularly RDD-based approaches, were not supported on Databricks Serverless Compute.
# MAGIC
# MAGIC ### Solution
# MAGIC Performance analysis was redesigned around supported Spark SQL and DataFrame execution plans.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Unity Catalog Compatibility
# MAGIC
# MAGIC ### Challenge
# MAGIC Some legacy Spark functionality was not supported with Unity Catalog.
# MAGIC
# MAGIC ### Solution
# MAGIC The pipeline was adapted to use Unity Catalog-compatible metadata and file-handling approaches.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Gold Layer Streaming Conflict
# MAGIC
# MAGIC ### Challenge
# MAGIC The Gold notebook initially contained legacy Structured Streaming cells alongside the batch Gold transformations.
# MAGIC
# MAGIC This caused the batch Gold Job to attempt recovery from a Structured Streaming checkpoint.
# MAGIC
# MAGIC ### Solution
# MAGIC The streaming section was separated from the batch Gold execution path.
# MAGIC
# MAGIC The dedicated `06_Structured_Streaming` notebook now handles the streaming workload, while `04_Gold_Layer` contains the batch Gold transformations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Streaming Checkpoint Management
# MAGIC
# MAGIC ### Challenge
# MAGIC Structured Streaming requires checkpoint state to maintain processing progress.
# MAGIC
# MAGIC An incompatible checkpoint can prevent a streaming query from recovering correctly.
# MAGIC
# MAGIC ### Solution
# MAGIC The streaming pipeline was isolated into its own notebook and Job with a dedicated checkpoint configuration.
# MAGIC
# MAGIC The existing checkpoint was not unnecessarily deleted during troubleshooting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Job Failure Recovery
# MAGIC
# MAGIC ### Challenge
# MAGIC The Gold task initially failed during Job execution because of the legacy streaming code.
# MAGIC
# MAGIC ### Solution
# MAGIC After correcting the Gold notebook, a Databricks Repair Run was used to rerun the failed portion of the workflow while retaining the successful upstream task results.
# MAGIC
# MAGIC The repaired batch Job subsequently completed successfully.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Data Quality Validation
# MAGIC
# MAGIC ### Challenge
# MAGIC A production-style pipeline requires validation beyond simply completing transformations.
# MAGIC
# MAGIC ### Solution
# MAGIC A dedicated Data Quality framework was implemented covering:
# MAGIC
# MAGIC - Null validation
# MAGIC - Duplicate order validation
# MAGIC - Order-status validation
# MAGIC - Numeric business-rule validation
# MAGIC
# MAGIC All implemented checks completed successfully.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Engineering Takeaway
# MAGIC
# MAGIC The project demonstrates practical Data Engineering problem solving rather than relying only on idealized pipeline execution.
# MAGIC
# MAGIC The implementation accounts for:
# MAGIC
# MAGIC - Platform limitations
# MAGIC - Unity Catalog compatibility
# MAGIC - Batch vs. streaming separation
# MAGIC - Checkpoint management
# MAGIC - Job dependency management
# MAGIC - Data quality
# MAGIC - Failure recovery

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏆 Project Results & Final Status
# MAGIC
# MAGIC The Realtime Ecommerce Lakehouse was successfully implemented as an end-to-end Databricks Data Engineering project.
# MAGIC
# MAGIC ## Data Processing Results
# MAGIC
# MAGIC - **Bronze records:** 99,441
# MAGIC - **Silver records:** 99,252
# MAGIC - **Gold order-fact records:** 99,252
# MAGIC
# MAGIC The reduction from Bronze to Silver reflects the implemented cleansing/deduplication logic.
# MAGIC
# MAGIC ## Data Quality Results
# MAGIC
# MAGIC All implemented validation categories completed successfully:
# MAGIC
# MAGIC - Critical-field null checks → **PASS**
# MAGIC - Duplicate `order_id` checks → **PASS**
# MAGIC - Order-status validation → **PASS**
# MAGIC - Numeric business-rule validation → **PASS**
# MAGIC
# MAGIC ## Batch Pipeline
# MAGIC
# MAGIC The complete Databricks batch workflow executed successfully:
# MAGIC
# MAGIC **Data Ingestion → Bronze → Silver → Gold → Data Quality / SQL Analytics / Performance**
# MAGIC
# MAGIC **Final Status: SUCCESS**
# MAGIC
# MAGIC ## Streaming Pipeline
# MAGIC
# MAGIC The dedicated Structured Streaming notebook was executed successfully using the configured `availableNow` processing model.
# MAGIC
# MAGIC **Final Status: SUCCESS**
# MAGIC
# MAGIC ## Performance Analysis
# MAGIC
# MAGIC The project successfully analyzed:
# MAGIC
# MAGIC - Photon execution
# MAGIC - Physical execution plans
# MAGIC - Partition behavior
# MAGIC - Optimizer statistics
# MAGIC - Adaptive Query Execution availability
# MAGIC - Serverless Compute limitations
# MAGIC
# MAGIC ## Engineering Outcome
# MAGIC
# MAGIC The project demonstrates an end-to-end lakehouse workflow covering:
# MAGIC
# MAGIC - Batch data engineering
# MAGIC - Structured Streaming
# MAGIC - Medallion Architecture
# MAGIC - Delta/Unity Catalog-based lakehouse development
# MAGIC - SQL analytics
# MAGIC - Data quality
# MAGIC - Spark performance analysis
# MAGIC - Databricks Job orchestration
# MAGIC - Failure recovery
# MAGIC
# MAGIC ## Overall Project Status
# MAGIC
# MAGIC **CORE DATABRICKS PIPELINE: COMPLETE ✅**
# MAGIC
# MAGIC The project is ready for final documentation, portfolio presentation and interview walkthrough.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎯 Project Summary
# MAGIC
# MAGIC The Realtime Ecommerce Lakehouse is an end-to-end Data Engineering project built on Databricks to demonstrate practical skills required for modern Data Engineering roles.
# MAGIC
# MAGIC ## What This Project Demonstrates
# MAGIC
# MAGIC ### Data Engineering
# MAGIC - Batch ETL pipeline development
# MAGIC - Bronze → Silver → Gold architecture
# MAGIC - Data cleansing and transformation
# MAGIC - Fact and analytical datasets
# MAGIC - Business metric generation
# MAGIC
# MAGIC ### Spark & PySpark
# MAGIC - DataFrame transformations
# MAGIC - Aggregations and joins
# MAGIC - Execution-plan analysis
# MAGIC - Photon execution
# MAGIC - Partition analysis
# MAGIC - Adaptive Query Execution concepts
# MAGIC - Serverless Compute considerations
# MAGIC
# MAGIC ### SQL
# MAGIC - Joins
# MAGIC - CTEs
# MAGIC - Window functions
# MAGIC - Aggregations
# MAGIC - Deduplication
# MAGIC - Business analytics
# MAGIC - Analytical transformations
# MAGIC
# MAGIC ### Streaming
# MAGIC - Spark Structured Streaming
# MAGIC - Incremental processing
# MAGIC - Streaming checkpoints
# MAGIC - `availableNow` trigger
# MAGIC - Separate streaming orchestration
# MAGIC
# MAGIC ### Data Quality
# MAGIC - Null validation
# MAGIC - Duplicate detection
# MAGIC - Domain validation
# MAGIC - Numeric business-rule validation
# MAGIC
# MAGIC ### Databricks
# MAGIC - Unity Catalog
# MAGIC - Delta-based lakehouse architecture
# MAGIC - Serverless Compute
# MAGIC - Databricks Jobs
# MAGIC - Task dependencies
# MAGIC - Repair Run
# MAGIC - Photon
# MAGIC
# MAGIC ## Project Architecture
# MAGIC
# MAGIC The project contains separate notebooks for:
# MAGIC
# MAGIC 1. Project Overview
# MAGIC 2. Data Ingestion
# MAGIC 3. Bronze Layer
# MAGIC 4. Silver Layer
# MAGIC 5. Gold Layer
# MAGIC 6. Structured Streaming
# MAGIC 7. Data Quality
# MAGIC 8. Performance Optimization
# MAGIC
# MAGIC The batch notebooks are orchestrated through a dependency-driven Databricks Job, while Structured Streaming is executed through a separate Job.
# MAGIC
# MAGIC ## Interview Focus
# MAGIC
# MAGIC The project is designed to support discussion around:
# MAGIC
# MAGIC - Why Medallion Architecture was used
# MAGIC - How Bronze, Silver and Gold differ
# MAGIC - How data quality is enforced
# MAGIC - How Spark jobs are optimized
# MAGIC - How Structured Streaming differs from batch processing
# MAGIC - How checkpoints work
# MAGIC - How Databricks Jobs handle dependencies and failures
# MAGIC - How the pipeline could be extended to AWS and dbt
# MAGIC
# MAGIC ## Current Status
# MAGIC
# MAGIC **Core Databricks Lakehouse Pipeline: COMPLETE ✅**
# MAGIC
# MAGIC **Batch Job: SUCCESS ✅**
# MAGIC
# MAGIC **Streaming Job: SUCCESS ✅**
# MAGIC
# MAGIC **Data Quality: PASS ✅**
# MAGIC
# MAGIC **Performance Analysis: COMPLETE ✅**

# COMMAND ----------

