# 🍕 Food Delivery Analytics Platform
### Azure + Databricks | Manual CDC with LSN Watermarking | Medallion Architecture

![Azure](https://img.shields.io/badge/Azure-Event%20Hubs%20%7C%20SQL%20Database-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-DLT%20%7C%20Delta%20Lake-FF6B35?style=flat-square&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Unity%20Catalog-00A4EF?style=flat-square)
![Mosaic AI](https://img.shields.io/badge/Mosaic%20AI-Sentiment%20Analysis-7B2FBE?style=flat-square)

---

## 📌 Project Overview

A **production-grade, end-to-end food delivery analytics pipeline** built on Microsoft Azure and Databricks. The platform ingests real-time order streams and batch transactional data, processes them through a **Bronze → Silver → Gold medallion architecture**, applies AI-powered sentiment analysis on customer reviews, and delivers insights through live dashboards.

> **Inspired by** the original project by [Afaque Ahmad](https://www.linkedin.com/in/afaque7117) (Solutions Architect @ Databricks).  
> **Key difference:** Instead of using Lakeflow Connect's built-in CDC, I implemented **Change Data Capture from scratch** using LSN (Log Sequence Number) watermark logic — understanding the mechanism at the lowest level.

---

## 🏗️ Architecture

![Architecture Diagram](diagrams/Workflow_of_project.png)

### Two Ingestion Patterns

| Pattern | Source | Tool | Method |
|--------|--------|------|--------|
| **Real-time Streaming** | `streaming_orders` | Azure Event Hubs → DLT | Spark Structured Streaming |
| **Batch (CDC)** | `customers`, `restaurants`, `menu_items`, `historical_orders`, `reviews` | Azure SQL Database (DataGrip client) | Manual LSN Watermark + Delta MERGE |

---

## ✨ What Makes This Different

Most engineers use **Lakeflow Connect** where CDC is built in and abstracted away. I chose to implement it manually because:

- When production breaks at 2am, you need to understand what your tools are actually doing
- LSN tracking, watermarking, and merge logic are skills that transfer across any CDC system
- It demonstrates real understanding — not just clicking "enable CDC" in a UI

---

## 🔁 CDC Implementation (The Core Logic)

CDC is enabled at the Azure SQL source. Databricks then runs the following 4-step process on each pipeline execution:

```
Step 1 → GET MAX SOURCE LSN from Azure SQL CDC tables
Step 2 → QUERY only rows where Source LSN > Watermark LSN  (no full table scans)
Step 3 → APPLY Delta MERGE into Bronze table             (UPSERT — insert new, update changed)
Step 4 → UPDATE Watermark table with the new LSN         (ready for next run)
```

```python
# Simplified CDC pattern used in this project

# 1. Read current watermark
last_lsn = spark.sql("""
    SELECT last_lsn FROM watermark_table
    WHERE table_name = 'customers'
""").collect()[0]['last_lsn']

# 2. Query only changed rows from Azure SQL
cdc_df = spark.read.format("jdbc").options(
    url=JDBC_URL,
    query=f"""
        SELECT ct.*, c.*
        FROM cdc.dbo_customers_CT ct
        JOIN customers c ON ct.id = c.id
        WHERE ct.__$start_lsn > 0x{last_lsn}
        AND ct.__$operation IN (2, 4)  -- 2=INSERT, 4=UPDATE
    """,
    user=SQL_USER, password=SQL_PASSWORD
).load()

# 3. MERGE into Bronze Delta table
bronze_table = DeltaTable.forName(spark, f"{CATALOG}.bronze.customers")
bronze_table.alias("target").merge(
    cdc_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# 4. Update watermark
new_lsn = cdc_df.agg(F.max("__$start_lsn")).collect()[0][0]
spark.sql(f"""
    UPDATE watermark_table SET last_lsn = '{new_lsn}'
    WHERE table_name = 'customers'
""")
```

---

## 📦 Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion

| Table | Source | Method |
|-------|--------|--------|
| `01_bronze.orders` | Azure Event Hubs | DLT Streaming (append-only) |
| `01_bronze.historical_orders` | Azure SQL Database | One-time full load |
| `01_bronze.reviews` | Azure SQL Database | Manual CDC (LSN watermark) |

### 🥈 Silver Layer — Cleansed & Modelled

| Table | Description |
|-------|-------------|
| `02_silver.fact_orders` | Cleansed orders with joins applied |
| `02_silver.fact_order_items` | Exploded order line items |
| `02_silver.dim_customer` | Customer dimension (SCD-ready) |
| `02_silver.dim_restaurant` | Restaurant dimension |
| `02_silver.dim_menu_items` | Menu items dimension |
| `02_silver.fact_reviews` | Reviews fact table (sentiment-ready) |

### 🥇 Gold Layer — Analytics & AI

| Table | Sources | Output |
|-------|---------|--------|
| `03_gold.d_sales_summary` | fact_orders · agg items · customers | Sales summary by time & region |
| `03_gold.d_customer_360` | fact_orders · fact_order_items · dim_customers · dim_restaurants · fact_reviews | Customer 360 view |
| `03_gold.d_restaurant_reviews` | dim_restaurants · fact_reviews | Restaurant ratings + AI sentiment scores |

---

## 🤖 AI-Powered Sentiment Analysis

Customer reviews are scored automatically using Databricks `ai_query()` which calls **GPT / Meta models via Mosaic AI**:

```sql
SELECT
  review_id,
  review_text,
  ai_query(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Classify the sentiment of this food delivery review as Positive, Neutral, or Negative. Review: ', review_text)
  ) AS sentiment_label
FROM 02_silver.fact_reviews
```

Results flow into `03_gold.d_restaurant_reviews` and are surfaced on the dashboard.

---

## 📊 Dashboard

Built on **Databricks SQL**, the dashboard covers three views:

- **Sales Summary** — Revenue by time, region, and order volume trends
- **Customer 360** — Per-customer order history, spend, and review sentiment
- **Restaurant Performance** — Ratings, review sentiment scores, and order frequency

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Streaming Ingestion | Azure Event Hubs + Databricks DLT |
| Batch Ingestion | Azure SQL Database + JDBC + Custom CDC |
| SQL Client | DataGrip IDE |
| Storage & Processing | Delta Lake on Databricks |
| Governance | Unity Catalog |
| Orchestration | Spark Declarative Pipelines / DLT |
| AI | Mosaic AI · GPT-OSS / Meta Models via `ai_query()` |
| Analytics | Databricks SQL + Dashboards |

---

## 📁 Repository Structure

```
food-delivery-analytics-platform/
├── README.md
├── .gitignore
├── .env.example
├── 00_synthetic_data/             ← Scripts to generate synthetic order/review data
├── 01_pipelines/                  ← All Databricks notebooks by layer
│   ├── bronze/
│   │   ├── 01_bronze_orders.py            ← DLT streaming from Event Hubs
│   │   ├── 01_bronze_historical_orders.py ← One-time full load from Azure SQL
│   │   └── 01_bronze_reviews.py           ← CDC ingestion (LSN watermark + MERGE)
│   ├── silver/
│   │   ├── 02_silver_fact_orders.py
│   │   ├── 02_silver_fact_order_items.py
│   │   ├── 02_silver_dim_customer.py
│   │   ├── 02_silver_dim_restaurant.py
│   │   ├── 02_silver_dim_menu_items.py
│   │   └── 02_silver_fact_reviews.py
│   ├── gold/
│   │   ├── 03_gold_d_sales_summary.py
│   │   ├── 03_gold_d_customer_360.py
│   │   └── 03_gold_d_restaurant_reviews.py
│   └── ingestion/
│       ├── cdc_batch_ingestion.py         ← Core CDC logic: LSN watermark + Delta MERGE
│       └── watermark_table_setup.sql      ← Creates LSN watermark tracking table
├── Dashboard/                     ← Dashboard screenshots & SQL queries
├── diagrams/                      ← Architecture diagrams
│   └── architecture.png
└── config/
    └── sample_config.py           ← Dummy credentials template (never commit real config)
```

---

## 🚀 Setup Guide

### Prerequisites
- Azure subscription (Event Hubs + SQL Database)
- Databricks workspace with Unity Catalog enabled
- DataGrip (or any SQL client) connected to Azure SQL

### Step 1 — Configure credentials
```bash
cp config/sample_config.py config/config.py
# Fill in your Azure and Databricks credentials in config.py
# config.py is in .gitignore — it will never be committed
```

### Step 2 — Enable CDC on Azure SQL source
```sql
-- Run on your Azure SQL Database
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'customers',
    @role_name     = NULL;
-- Repeat for: restaurants, menu_items, historical_orders, reviews
```

### Step 3 — Create watermark table
```bash
# Run in Databricks SQL warehouse
ingestion/watermark_table_setup.sql
```

### Step 4 — Run notebooks in order
```
01_pipelines/ingestion/   → watermark_table_setup.sql first
01_pipelines/bronze/      → orders, historical_orders, reviews
01_pipelines/silver/      → all fact and dim tables
01_pipelines/gold/        → d_sales_summary, d_customer_360, d_restaurant_reviews
```

---

## 🔐 Credential Safety

| File | Status | Notes |
|------|--------|-------|
| `config/config.py` | ❌ Never committed | In `.gitignore` |
| `config/sample_config.py` | ✅ Safe | Dummy values only |
| `.env.example` | ✅ Safe | Template with placeholders |
| `01_pipelines/**` | ✅ Safe | Reference config variables, no hardcoded secrets |

---

## 🙏 Credits

This project was inspired by **[Afaque Ahmad](https://www.linkedin.com/in/afaque7117)** whose YouTube tutorial provided the original project blueprint. His content is one of the best hands-on Databricks resources available — highly recommend checking it out.

My contribution: replacing Lakeflow Connect with a fully manual CDC implementation using LSN watermarking, giving deeper understanding of what CDC actually does under the hood.

---

## 👤 Author

**Your Name**  
[LinkedIn](https://www.linkedin.com/in/shahid-banaras-b64720258/) · [GitHub](https://github.com/ShahidBanaras)

---

*Built with ☕ and a lot of Databricks cluster restarts.*
