# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Stooq

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``01_bronze_ingest_stooq``
# MAGIC
# MAGIC **Layer**: Bronze (Raw Ingestion)
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Ingest Stooq daily ASCII files from ADLS landing zone into a Delta Bronze table
# MAGIC * Keep data as close to source as possible (raw strings), add ingestion + lineage metadata
# MAGIC
# MAGIC **Inputs**:
# MAGIC * ADLS path: ``/stock-demo/bronze/stooq/ingestion_date=YYYY-MM-DD/``
# MAGIC
# MAGIC **Ouputs**:
# MAGIC * Delta files (Bronze): /stock-demo/bronze/delta/stooq_prices_raw (partitioned by ingestion_date)
# MAGIC
# MAGIC **Bronze design principles**:
# MAGIC * Minimal transformations (no tipying/casting beyond metadata)
# MAGIC * Preserve source schema (string) for auditability and reprocessing
# MAGIC * Add lineage columns for traceability: ``ingestion_ts``, ``ingestion_date``, ``source_file``

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0) Authentication (Spark session-scoped)

# COMMAND ----------

# MAGIC %md
# MAGIC **OAuth required only if running path-based. If using UC tables only, this can be removed!**

# COMMAND ----------

# MAGIC %run ./_utils/01_adls_oauth_setup

# COMMAND ----------

display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/"))

# COMMAND ----------

# Sanity check (container list)
display(dbutils.fs.ls(base_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Parameters + Paths Definition

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1) Parameters (Workflow / Job)

# COMMAND ----------

# Create widget only if it doesn't exist yet
try:
    dbutils.widgets.get("ingestion_date")
except:
    dbutils.widgets.text("ingestion_date", "")

ingestion_date = dbutils.widgets.get("ingestion_date").strip()

if not ingestion_date:
    raise ValueError("Missing required parameter: ingestion_date (expected format YYYY-MM-DD)")

print("Using ingestion_date =", ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2) Parameters

# COMMAND ----------

from datetime import date

storage_account = "stockdatalakemgg"
container = "stock-demo"

base = f"abfss://{container}@{storage_account}.dfs.core.windows.net/stock-demo"

raw_path = f"{base}/bronze/stooq/ingestion_date={ingestion_date}/"
schema_path = f"{base}/bronze/schema/stooq"
checkpoint_path = f"{base}/bronze/checkpoints/stooq" # Auto Loader (triggered batch mode)

bronze_delta_path = f"{base}/bronze/delta/stooq_prices_raw"
bronze_table = "bronze_stooq_prices_raw"  # DB created later, for now used default

print("Raw path:", raw_path)
print("Bronze delta path:", bronze_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Reading Raw Stooq Files

# COMMAND ----------

# MAGIC %md
# MAGIC Stooq "daily" ASCII files are CSV-like with headers:
# MAGIC * ``<TICKER>``
# MAGIC * ``<PER>``
# MAGIC * ``<DATE>``
# MAGIC * ``<TIME>``
# MAGIC * ``<OPEN>``
# MAGIC * ``<HIGH>``
# MAGIC * ``<LOW>``
# MAGIC * ``<CLOSE>``
# MAGIC * ``<VOL>``
# MAGIC * ``<OPENINT>``
# MAGIC
# MAGIC *NOTE: Bronze best practice is to NOT infer schema (keep everything as string to preserve source fidelity).*

# COMMAND ----------

from pyspark.sql import functions as F

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(raw_path)
)

# Normalize column names (Stooq uses uppercase headers)
# Keep *_str naming to make it explicit these are raw string values
df = (df_raw
      .withColumnRenamed("<TICKER>", "ticker")
      .withColumnRenamed("<PER>", "period")
      .withColumnRenamed("<DATE>", "date_str")
      .withColumnRenamed("<TIME>", "time_str")
      .withColumnRenamed("<OPEN>", "open_str")
      .withColumnRenamed("<HIGH>", "high_str")
      .withColumnRenamed("<LOW>", "low_str")
      .withColumnRenamed("<CLOSE>", "close_str")
      .withColumnRenamed("<VOL>", "vol_str")
      .withColumnRenamed("<OPENINT>", "openint_str")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Add Ingestion + Lineage Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ``ingestion_date``: partition column (supports incremental loads and reprocessing)
# MAGIC
# MAGIC ``ingestion_ts``: load timestamp (useful for dedup in Silver, auditing)
# MAGIC
# MAGIC ``source_file``: lineage to original file path
# MAGIC
# MAGIC *NOTE: in Unity Catalog contexts, ``input_file_name()`` may be restricted.*

# COMMAND ----------

# Add ingestion metadata
df = (
    df
    .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path")) # Using '_metadata.file_path' is the recommended approach in Databricks
)

# Quick inspection
#display(df.limit(5), checkpointLocation='dbfs:/tmp/checkpoints')
#print("Rows:", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Writing Bronze Delta (append, partitioned by ``ingestion_date``)

# COMMAND ----------

# MAGIC %md
# MAGIC Append method:
# MAGIC * Bronze is an immutable landing table. Each ingestion adds a new partition
# MAGIC * For backfills, append a new ``ingestion_date`` or overwrite a specific partition

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1) Idempotent load for this ingestion_date

# COMMAND ----------

# MAGIC %md
# MAGIC In Bronze, reruns for the same ``ingestion_date`` should not duplicate rows. It delets the existing partition for ``ingestion_date``, then write fresh.

# COMMAND ----------

query = (
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_path)
      .option("mergeSchema", "true") # enabling schema evolution on the streaming writer
      .trigger(availableNow=True)
      .partitionBy("ingestion_date")
      .start(bronze_delta_path)
)

# Ensure job archestration waits for Bronze streaming to finish
query.awaitTermination()

bronze_after = spark.read.format("delta").load(bronze_delta_path)
print("Bronze total rows:", bronze_after.count())
display(
    bronze_after.select("ticker","period","date_str","close_str","ingestion_date","source_file")
               .orderBy("ticker","date_str")
               .limit(5)
)

print("Bronze written to:", bronze_delta_path)