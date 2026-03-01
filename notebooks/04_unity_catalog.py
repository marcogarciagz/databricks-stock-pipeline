# Databricks notebook source
# MAGIC %md
# MAGIC # Registration (Bronze, Silver, and Gold) as Proper Tables (Unity Catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``04_unity_catalog``
# MAGIC
# MAGIC **Layer**: Governance / Table Registration (Unity Catalog)
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Register the medallion Delta datasets (Bronze/Silver/Gold) as governed UC tables.
# MAGIC * This enables:
# MAGIC   - Databricks SQL / Serverless Warehouse querying
# MAGIC   - Power BI connectivity via SQL Warehouse
# MAGIC   - Consistent catalog.schema.table references (instead of path-based reads)
# MAGIC
# MAGIC **Key governance concept**:
# MAGIC * In Unity Catalog, external storage paths (ADLS) must be governed by:
# MAGIC    1) Storage Credential (identity)
# MAGIC    2) External Location (path boundary)
# MAGIC  
# MAGIC * Without an External Location covering the ADLS path, UC blocks:
# MAGIC    ``CREATE TABLE ... LOCATION 'abfss://...'``
# MAGIC
# MAGIC What happened in this project:
# MAGIC  - Initial attempt failed with NO_PARENT_EXTERNAL_LOCATION_FOR_PATH
# MAGIC  - Fix:
# MAGIC    - Grant IAM role (Storage Blob Data Contributor) to the workspace’s
# MAGIC      'unity-catalog-access-connector' managed identity
# MAGIC    - Create UC Storage Credential (managed identity)
# MAGIC    - Create UC External Location for the project path
# MAGIC  - File Events provisioning failed (403) because additional Event Grid permissions were missing, but this is not required for batch pipelines, so we used "Force create".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0) Authentication (optional here)

# COMMAND ----------

# MAGIC %md
# MAGIC **OAuth required only if running path-based. If using UC tables only, this can be removed!**

# COMMAND ----------

# MAGIC %run ./_utils/01_adls_oauth_setup

# COMMAND ----------

display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Use catalog and create schema (database) for the project

# COMMAND ----------

# Catalog selection matters in UC: fully-qualified name is catalog.schema.table
spark.sql("USE CATALOG databricks_test")
spark.sql("CREATE SCHEMA IF NOT EXISTS stock_demo")
spark.sql("USE SCHEMA stock_demo")

spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Unity Catalog ``databricks_test``

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Register Delta paths as UC tables

# COMMAND ----------

# MAGIC %md
# MAGIC These Delta datasets already exist in ADLS from previous notebooks. Now they are registered so downstream consumers can query them as tables.
# MAGIC
# MAGIC *NOTE: this requires that the underlying ADLS path is covered by a UC External Location. The External Location used in this project is ``el_stock_demo``*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1) ``base`` definition

# COMMAND ----------

base = "abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2) Bronze: raw landing table (strings + ingestion metadata)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_stooq_prices_raw
USING DELTA
LOCATION '{base}/bronze/delta/stooq_prices_raw'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3) Silver: typed, cleaned, deduplicated daily prices

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver_prices_daily
USING DELTA
LOCATION '{base}/silver/delta/prices_daily'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4) Gold: curated feature set for BI + ML

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_features_daily
USING DELTA
LOCATION '{base}/gold/delta/features_daily'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Verification

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)
spark.sql("SELECT COUNT(*) FROM gold_features_daily").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Proper Unity Catalog-governed medallion architecture backed by ADLS via managed identity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Governance Verification

# COMMAND ----------

# Show governed external locations (permission to list them??)
try:
    spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)
except Exception as e:
    print("Cannot list external locations (permissions). Error:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apendix: problem with Metastore Admin
# MAGIC Since I was working in a Unity Catalog-enabled workspace, when ``CREATE TABLE ... USING DELTA ... LOCATION 'abfss:...'``, UC blocked it. WHY?
# MAGIC * Any external storage location (ADLS, S3, ...) must be registered as an **External Location**
# MAGIC * The External Location must reference a **Storage Credential**
# MAGIC * Both require **Metastore Admin privileges**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Since I'm not a Metastore Admin in this workspace:
# MAGIC * I cannot create a Storage Credential backed by a **Service Principal**
# MAGIC * I cannot create a External Location
# MAGIC * UC blocks table creation at arbitrary ADLS paths
# MAGIC This is **by design**, for enterprise-grade data governance.

# COMMAND ----------

spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)