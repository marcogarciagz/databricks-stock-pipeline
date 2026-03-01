# Databricks notebook source
# MAGIC %md
# MAGIC # Silver table creation

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``02_silver_transform``
# MAGIC
# MAGIC **Layer**: Silver (Cleaned + Typed)
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Transform Bronze raw Stooq prices into a typed, clean Silver Delta dataset
# MAGIC * Apply basic data quality rules and deduplicate by (``symbol``, ``date``)
# MAGIC
# MAGIC **Inputs**:
# MAGIC * Bronze Delta path: ``/stock-demo/bronze/delta/stooq_prices_raw``
# MAGIC
# MAGIC **Ouputs**:
# MAGIC * Silver Delta path: ``/stock-demo/silver/delta/prices_daily``
# MAGIC
# MAGIC **Silver design principles**:
# MAGIC * Enforce types (``date``, ``numeric``)
# MAGIC * Apply validity filters (nulls, positive prices, high >= low, etc.)
# MAGIC * Deduplicate using ``ingestion_ts`` to keep the latest loaded record per key
# MAGIC * Provide a stable schema for downstream Gold/BI/ML

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0) Authentication (only needed if reading/writing ADLS by path)

# COMMAND ----------

# MAGIC %md
# MAGIC **OAuth required only if running path-based. If using UC tables only, this can be removed!**

# COMMAND ----------

# MAGIC %run ./_utils/01_adls_oauth_setup

# COMMAND ----------

display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Silver paths definition

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

base = "abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo"

bronze_delta_path = f"{base}/bronze/delta/stooq_prices_raw"
silver_delta_path = f"{base}/silver/delta/prices_daily"

print("Bronze:", bronze_delta_path)
print("Silver:", silver_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Read Bronze and cast types

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze stores source fields as strings for fidelity. In Silver:
# MAGIC * Parse data from yyyyMMdd
# MAGIC * Cast OHLC and volume to numeric
# MAGIC * Standardize naming (``ticker`` --> ``symbol``) for consistent downstream usage

# COMMAND ----------

bronze = spark.read.format("delta").load(bronze_delta_path)

silver_stage = (
    bronze
    .withColumn("date", F.to_date(F.col("date_str").cast("string"), "yyyyMMdd"))
    .withColumn("open", F.col("open_str").cast("double"))
    .withColumn("high", F.col("high_str").cast("double"))
    .withColumn("low", F.col("low_str").cast("double"))
    .withColumn("close", F.col("close_str").cast("double"))
    .withColumn("volume", F.col("vol_str").cast("double"))  # Stooq volume sometimes looks large. Fixed later
    .withColumn("symbol", F.upper(F.col("ticker")))         # keep naming consistent for Gold/BI
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Basic data quality filters

# COMMAND ----------

# MAGIC %md
# MAGIC Rationals:
# MAGIC * Ensure essential fields are present
# MAGIC * Remove clearly invalid rows
# MAGIC * Keep rules lightweight in Silver. Stricter business rules could be added later

# COMMAND ----------

silver_stage = silver_stage.filter(
    (F.col("date").isNotNull()) &
    (F.col("symbol").isNotNull()) &
    (F.col("close").isNotNull()) &
    (F.col("close") > 0) &
    (F.col("high") >= F.col("low"))
)

display(
    silver_stage.select(
        "symbol","date","open","high","low","close","volume","ingestion_ts"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Deduplicate: keep last ingestion per symbol/date

# COMMAND ----------

# MAGIC %md
# MAGIC Dedup in Silver:
# MAGIC * Same (``symbol``, ``date``) could arrive multiple times (re-ingestion, backfill, late files)
# MAGIC * ``ingestion_ts`` enables deterministic "latest-wins" selection

# COMMAND ----------

w = Window.partitionBy("symbol", "date").orderBy(F.col("ingestion_ts").desc())

silver_dedup = (
    silver_stage
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Silver rows (deduped):", silver_dedup.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Select final Silver schema and write Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ``volume`` stored as LONG (integer-like). Many sources report volume as whole units.
# MAGIC
# MAGIC Keep ingestion metadata for lineage.

# COMMAND ----------

silver_final = (
    silver_dedup
    .select(
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        F.col("volume").cast("long").alias("volume"),
        "ingestion_ts",
        "ingestion_date",
        "source_file"
    )
)

# For first run: overwrite
# For production: maybe MERGE (upsert) by (symbol, date) with ingestion_ts ordering
(silver_final.write
    .format("delta")
    .mode("overwrite")
    .save(silver_delta_path)
)

print("Silver written to:", silver_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Silver verification

# COMMAND ----------

silver = spark.read.format("delta").load(silver_delta_path)

display(silver.orderBy("symbol","date").limit(10))
print("Silver total rows:", silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze rows = Silver rows = 233783