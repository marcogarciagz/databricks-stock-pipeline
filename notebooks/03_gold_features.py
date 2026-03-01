# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Logic

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``03_gold_features``
# MAGIC
# MAGIC **Layer**: Gold (Curated Features for BI + ML)
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Build an analytics-ready feature table from Silver daily prices
# MAGIC * Compute simple technical indicators used for dashboarding and ML:
# MAGIC   - 1-day return
# MAGIC   - moving averages
# MAGIC   - rolling volatibility (stddev of returns over 10, 20)
# MAGIC
# MAGIC **Inputs**:
# MAGIC * Silver Delta path: ``/stock-demo/silver/delta/prices_daily``
# MAGIC
# MAGIC **Ouputs**:
# MAGIC * Silver Delta path: ``/stock-demo/gold/delta/features_daily``
# MAGIC
# MAGIC **Gold design principles**:
# MAGIC * Curated and consistent schema (business/analytics facing)
# MAGIC * Features computed at daily grain (``symbol``, ``date``)
# MAGIC * Supports both BI (Power BI) and ML (forecasting notebook)

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
# MAGIC ## 1) Gold Path Definition

# COMMAND ----------

base = "abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo"
silver_delta_path = f"{base}/silver/delta/prices_daily"
gold_delta_path = f"{base}/gold/delta/features_daily"

print("Silver:", silver_delta_path)
print("Gold:", gold_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Read Silver

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.read.format("delta").load(silver_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Window specs (per symbol time series)

# COMMAND ----------

# MAGIC %md
# MAGIC ``w_symbol``: defines chronological ordering per ``symbol``
# MAGIC
# MAGIC ``w_10``, ``w_20``: rolling windows used for moving averages and volatility

# COMMAND ----------

w_symbol = Window.partitionBy("symbol").orderBy("date")
w_10 = w_symbol.rowsBetween(-9, 0) # last 10 rows including today
w_20 = w_symbol.rowsBetween(-19, 0) # last 20 rows including today

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ``return_id``:
# MAGIC * simple daily return: (``close_t - close_{t-1} / close_{t-1}``)
# MAGIC * return forecasting is generally more stable than price forecasting
# MAGIC
# MAGIC ``ma_10`` / ``ma_20``:
# MAGIC * rolling mean of close (simple trend indicator)
# MAGIC
# MAGIC ``volatility_10`` / ``volatility_20``:
# MAGIC * rolling std dev of daily returns (risky proxy)

# COMMAND ----------

gold_features = (
    silver
    .withColumn("return_1d",
        (F.col("close") - F.lag("close").over(w_symbol)) /
        F.lag("close").over(w_symbol)
    )
    .withColumn("ma_10", F.avg("close").over(w_10))
    .withColumn("ma_20", F.avg("close").over(w_20))
    .withColumn("volatility_10", F.stddev("return_1d").over(w_10))
    .withColumn("volatility_20", F.stddev("return_1d").over(w_20))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Drop initial null periods

# COMMAND ----------

# MAGIC %md
# MAGIC First few rows per symbol will have null rolling values (return = NULL) due to lag(close).
# MAGIC
# MAGIC These rows are dropped because downstream ML/BI generally expect numeric returns.

# COMMAND ----------

gold_features = gold_features.filter(F.col("return_1d").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) (Optional sanity checks) Null/NaN awereness

# COMMAND ----------

# MAGIC %md
# MAGIC Volatility windows require enough observations. Early rows may have NULL volatility.
# MAGIC
# MAGIC These rows are kept here because Gold is still an analytics table. ML notebook can filter/clean.
# MAGIC
# MAGIC *NOTE: these checks do not change data. They help understand feature completeness.*

# COMMAND ----------

null_counts = (
    gold_features.select(
        F.sum(F.col("ma_10").isNull().cast("int")).alias("ma_10_nulls"),
        F.sum(F.col("ma_20").isNull().cast("int")).alias("ma_20_nulls"),
        F.sum(F.col("volatility_10").isNull().cast("int")).alias("vol_10_nulls"),
        F.sum(F.col("volatility_20").isNull().cast("int")).alias("vol_20_nulls"),
    )
)
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Write Gold Delta

# COMMAND ----------

(
    gold_features.write
    .format("delta")
    .mode("overwrite")
    .save(gold_delta_path)
)

print("Gold written to:", gold_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Gold validation

# COMMAND ----------

gold = spark.read.format("delta").load(gold_delta_path)

display(
    gold.select(
        "symbol","date","close","return_1d","ma_10","ma_20","volatility_10"
    ).orderBy("symbol","date").limit(20)
)

print("Gold total rows:", gold.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Silver rows = 233,783
# MAGIC Gold rows = 233,758
# MAGIC
# MAGIC The difference (25 rows) corresponds to the first row per symbol, where ``return_id`` is null (because of ``lag``). That confirms the window logic is working properly.