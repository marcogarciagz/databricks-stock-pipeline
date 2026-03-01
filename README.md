# Databricks Stock Lakehouse Pipeline

End-to-end production-style data engineering project built on Azure Databricks, implementing a Medallion Architecture (Bronze / Silver / Gold) with governance, orchestration, data quality enforcement, ML integration, and CI/CD deployment using Databricks Asset Bundles.

## 🚀 Project Overview

This project simulates a real-world financial data platform for ingesting, transforming, governing, and consuming historical stock market data. The objective is not just analytics, but to design a production-ready Lakehouse system with:
* Incremental ingestion
* Idempotent processing
* Governance via Unity Catalog
* Data quality enforcement
* L integration
* BI consumption
* CI/CD deployment

## 🏗 Architecture Overview

**Source → ADLS Gen2 → Databricks (Bronze/Silver/Gold) → Unity Catalog → SQL Warehouse → Power BI**

![Architecture Diagram](/diagrams/architecture-overview.png)

Components

| Layer                | Purpose               |
|----------------------|-----------------------|
| ADLS Gen2            | Raw data storage      |
| Databricks           | Processing engine     |
| Delta Lake           | ACID storage layer    |
| Unity Catalog        | Governance & metadata |
| Databricks Workflows | Orchestration (DAG)   |
| SQL Warehouse        | Analytical serving    |
| Power BI             | Visualization         |
| DAB                  | CI/CD deployment      |

## 📂 Repository Structure

databricks-stock-pipeline/
│
├── notebooks/                  # Medallion notebooks
│   ├── 00_adls_connection_test.py
│   ├── _utils/
│   │   └── 01_adls_oauth_setup.py
│   ├── 01_bronze_ingest_stooq.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_features.py
│   ├── 04_unity_catalog.py
│   ├── 05_ml_forecasting.py
│   └── 06_data_quality_checks.py
│
├── dab/
│   └── stock_demo_bundle/      # Databricks Asset Bundle (DAB)
│
├── scripts/
│   ├── deploy.sh               # Validate & deploy bundle
│   └── run.sh                  # Trigger workflow job
│
├── diagrams/                   # Architecture & visuals
│
├── config/
│   └── example.env             # Placeholder config (no secrets)
│
└── README.md

## 🧱 Data Architecture
### 🥉 Bronze Layer – Raw Ingestion
* Source: Stooq historical stock data (CSV)
* Ingested using Auto Loader
* Stored as Delta
* Partitioned by ingestion_date
* Metadata added:
    - ingestion_ts
    - ingestion_date
    - source_file

Bronze preserves raw data with minimal transformation.

### 🥈 Silver Layer – Cleansing & Standardization

* Type casting
* Logical validation
* Deduplication using window functions
* Ensures trusted structured dataset

Silver transforms raw strings into typed, validated financial records.

### 🥇 Gold Layer – Feature Engineering

Features computed:
* Daily returns
* 10-day moving average
* 20-day moving average
* Rolling volatility

Gold is ML-ready and BI-ready.

### 📊 Machine Learning Layer

* Label: next-day return (via window lead)
* Time-based train/test split
* Baseline vs Gradient Boosted Tree
* RMSE & MAE evaluation
* Predictions written back as governed Gold table

Focus: integration into data platform, not trading alpha.

## 📂 Lakehouse Folder Hierarchy

abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/
└── stock-demo/
    ├── bronze/
    │   ├── stooq/
    │   │   └── ingestion_date=YYYY-MM-DD/
    │   │       ├── file_1.csv
    │   │       ├── file_2.csv
    │   │       └── ...
    │   │
    │   └── delta/
    │       └── stooq_prices_raw/
    │           ├── _delta_log/
    │           ├── ingestion_date=YYYY-MM-DD/
    │           │   ├── part-00000-....parquet
    │           │   ├── part-00001-....parquet
    │           │   └── ...
    │
    ├── silver/
    │   └── delta/
    │       └── prices_daily/
    │           ├── _delta_log/
    │           ├── part-00000-....parquet
    │           ├── part-00001-....parquet
    │           └── ...
    │
    ├── gold/
    │   └── delta/
    │       ├── features_daily/
    │       │   ├── _delta_log/
    │       │   ├── part-00000-....parquet
    │       │   └── ...
    │       │
    │       └── predictions_daily/
    │           ├── _delta_log/
    │           ├── part-00000-....parquet
    │           └── ...
    │
    └── checkpoints/
        └── bronze_stooq_autoloader/
            ├── offsets/
            ├── commits/
            └── sources/

## 🛡 Governance & Data Quality
### Unity Catalog

* Catalog + schema isolation
* External Delta tables
* SQL-accessible
* Permission-ready

### Data Quality

Dedicated DQ notebook:
* Null checks
* Logical validations
* Audit metrics table (dq_metrics)
* Pipeline gating (fails if rules violated)

### Delta Constraints

Example:

``CHECK (close > 0)``
``CHECK (high >= low)``

Enforces data correctness at storage level.

## 🔄 Orchestration

Implemented using Databricks Workflows (DAG):

**Bronze → Silver → DQ → Gold → ML**

![Databricks Workflow](/diagrams/tasks-workflow.png)

Features:
* Task dependencies
* Parameter injection (ingestion_date)
* Idempotent reruns
* Failure gating

## ⚙️ Incremental Ingestion Strategy

* Auto Loader in triggered mode:
* Detects new files
* Uses stable checkpoint
* Processes incrementally
* Avoids duplicate ingestion
* Idempotent by design.

## 📊 BI Integration

* SQL Warehouse exposes governed Gold tables
* Power BI connects via Databricks connector
* Star-schema style modeling
* Measures defined using DAX
* Dashboard built on curated dataset

![alt text](/diagrams/power-bi-dashboard.png)

## 🚀 CI/CD – Databricks Asset Bundles (DAB)

Infrastructure as Code using databricks.yml.
Supports:
* Bundle validation
* Environment targets (dev-ready)
* Automated deployment
* Automated job execution

### Run Locally
1️⃣ **Set environment variables**
``export DATABRICKS_HOST="https://adb-xxxxx.azuredatabricks.net"``
``export DATABRICKS_TOKEN="your_pat_here"``
2️⃣ **Deploy**
``./scripts/deploy.sh``
3️⃣ **Run Workflow**
./scripts/run.sh

## 🧠 Design Principles

* Separation of concerns (Bronze/Silver/Gold)
* Domain-aligned governance
* Idempotent processing
* Incremental ingestion
* Strong observability
* Infrastructure as Code
* Production-first mindset

## 📈 Potential Improvements

If I were to evolve this project from a showcase into something production-grade, the next step would be to formalize the ML lifecycle and environment management.
First, I’d integrate **MLflow Model Registry to track experiments, persist model artifacts, and manage versioned models with clear stage transitions** (e.g., Staging → Production). That would naturally connect to production-ready model serving, either via Databricks Model Serving or an external API layer, so predictions can be consumed in real time rather than only through batch tables.
In parallel, I’d introduce a **strong dev/prod separation and multi-environment promotion**. Today, everything runs in one workspace context; in a real organization I’d have separate environments with different catalogs/schemas, isolated storage locations, and different credentials. Deployment would then promote the same Asset Bundle across environments using target-specific configuration (dev/staging/prod), with approvals and quality gates in CI/CD.
From a data engineering perspective, I’d upgrade ingestion **from “triggered batch” to continuous streaming** where appropriate, especially **if the business needs near-real-time analytics**. That also connects to alerting and monitoring: adding pipeline SLAs, data freshness checks, job failure alerts, and DQ metric thresholds that trigger notifications (Teams/Email/PagerDuty). In production, observability is as important as correctness.
Another key improvement would be **CDC ingestion**. For market data this might not be the main driver, but **for enterprise sources** (ERP, trades, orders, risk systems), incremental changes are essential. I’d implement CDC patterns using change feeds (where available), Auto Loader incremental file ingestion, or event-driven ingestion—ensuring consistent upserts into Silver and Gold using Delta MERGE.
Overall, the theme is: the current project already demonstrates the architecture and engineering patterns, and these improvements would make it fully aligned with production requirements: governed lifecycle, controlled promotions, real-time capability, observability, incremental change handling, and reliable serving.

## 🛠 Technologies Used

* Azure Data Lake Gen2
* Azure Databricks
* Delta Lake
* Unity Catalog
* Databricks Workflows
* Spark MLlib
* Databricks SQL Warehouse
* Power BI
* Databricks Asset Bundles
* Azure DevOps (pipeline-ready)

## 🎯 Key Takeaways

This project demonstrates:
* End-to-end data engineering architecture
* Lakehouse design best practices
* Governance & DQ enforcement
* ML integration into data platform
* CI/CD deployment automation
* BI consumption from curated data