# Databricks notebook source
# MAGIC %md
# MAGIC # 1) ADLS OAuth config and Bronze Ingestion Logic

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``00_adls_connection_test``
# MAGIC
# MAGIC **Layer**: Infrastructure/Connectivity Smoke Test
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Validate Databricks can read/write to ADLS Gen 2 via Service Principal (OAuth)
# MAGIC * Bootstrap the lake folder structure (bronze/silver/gold) and an ingestion folder

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) ADLS OAuth Configuration using Service Principal (Spark session-scoped)

# COMMAND ----------

# MAGIC %md
# MAGIC * Spark needs credentials to access ADLS Gen2 using ABFS (abfss://)
# MAGIC * These configs live in the active Spark session. If the cluster restarts, re-run this cell

# COMMAND ----------

# MAGIC %run ./_utils/01_adls_oauth_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Reading Test: Listing the container

# COMMAND ----------

# List the container. Returns an empty list
display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Write Test: creates an small file in ADLS

# COMMAND ----------

# Confirms write permissions
test_path = "abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/_connectivity_test/hello.txt"
dbutils.fs.put(test_path, "hello from databricks", overwrite=True)
print("Wrote:", test_path)

display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/_connectivity_test/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Bootstrap: medallion folders creation

# COMMAND ----------

# MAGIC %md
# MAGIC Establish a consistent lake layout early:
# MAGIC * ``bronze``: raw ingested data
# MAGIC * ``silver``: cleaned and typed data
# MAGIC * ``gold``: curated aggregates/features for BI/ML

# COMMAND ----------

# Create folders (bronze, silver, and gold) from Databricks
base = "abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo"

paths = [
    f"{base}/bronze/stooq/",
    f"{base}/silver/",
    f"{base}/gold/",
]

for p in paths:
    dbutils.fs.mkdirs(p)
    print("Created:", p)

display(dbutils.fs.ls(base))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Creation of an Ingestion Partition Folder (first increment)

# COMMAND ----------

# MAGIC %md
# MAGIC * Ingestion-date partitioning is a common pattern for raw landing zones (bronze)
# MAGIC * It supports reprocessing, backfills, and incremental loads

# COMMAND ----------

from datetime import date

today = date.today().isoformat()
ingestion_path = f"{base}/bronze/stooq/ingestion_date={today}/"
dbutils.fs.mkdirs(ingestion_path)

print("Ingestion path:", ingestion_path)
display(dbutils.fs.ls(f"{base}/bronze/stooq/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Sanity Check: list uploaded files for a known ``ingestion_date``

# COMMAND ----------

# MAGIC %md
# MAGIC Confirms that local --> ADLS upload worked and files are visible from Databricks

# COMMAND ----------

display(dbutils.fs.ls(
"abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/bronze/stooq/ingestion_date=2026-02-22/"
))