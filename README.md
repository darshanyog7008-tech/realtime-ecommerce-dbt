# Real-Time E-Commerce Lakehouse

An end-to-end Data Engineering project built using Databricks, Apache Spark, Delta Lake, Kafka, Spark Structured Streaming, SQL, and dbt.

The project demonstrates how raw e-commerce data can be transformed into reliable, analytics-ready datasets using a Medallion Architecture, while also supporting real-time order processing.

---

## Project Overview

This project implements a scalable e-commerce data platform using both batch and streaming pipelines.

The pipeline processes e-commerce order data through:

**Raw Data → Bronze → Silver → Gold → Analytics**

A separate streaming path uses Kafka and Spark Structured Streaming for real-time order events.

The project also integrates dbt for analytics engineering, SQL transformations, testing, and documentation.

---

## Architecture

```text
                         E-COMMERCE DATA
                                |
                 +--------------+--------------+
                 |                             |
              BATCH                          KAFKA
                 |                             |
                 v                             v
          +-------------+              +---------------+
          |   BRONZE    |              |   STREAMING   |
          |    LAYER    |              |    SOURCE     |
          +------+------+              +-------+-------+
                 |                             |
                 v                             v
          +-------------+              +---------------+
          |   SILVER    |              |     SPARK     |
          |    LAYER    |              |  STRUCTURED   |
          +------+------+              |   STREAMING   |
                 |                     +-------+-------+
                 v                             |
          +-------------+                      |
          |    GOLD     |<---------------------+
          |    LAYER    |
          +------+------+ 
                 |
        +--------+---------+
        |        |         |
        v        v         v
     SQL      dbt       Analytics
