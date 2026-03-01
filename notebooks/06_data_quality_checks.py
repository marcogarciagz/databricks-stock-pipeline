# Databricks notebook source
# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``06_data_quality_checks``
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Run minimal data quality checks after Silver and before Gold
# MAGIC * Persist DQ metrics for auditability
# MAGIC * Fail fast if critical gates are violated

# COMMAND ----------

#spark.sql("USE CATALOG databricks_test")
#spark.sql("USE SCHEMA stock_demo")

# One-time reset (only during development)
#spark.sql("DROP TABLE IF EXISTS dq_metrics")

# COMMAND ----------

spark.sql("USE CATALOG databricks_test")
spark.sql("USE SCHEMA stock_demo")

from pyspark.sql import functions as F

# Parameters (pushed down from the job)
try:
    dbutils.widgets.get("ingestion_date")
except:
    dbutils.widgets.text("ingestion_date", "")

ingestion_date = dbutils.widgets.get("ingestion_date").strip()
if not ingestion_date:
    raise ValueError("Missing required parameter: ingestion_date")

print("DQ checks for ingestion_date =", ingestion_date)

# Load Silver
silver = spark.table("silver_prices_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC For this minimal project, Silver is overwritten end-to-end, so we cannot isolate by ``ingestion_date`` unless we add incremental logic. However, we still log ``ingestion_date`` in ``dq_metrics`` to tie runs to workflow executions.

# COMMAND ----------

# MAGIC %md
# MAGIC # DQ metrics

# COMMAND ----------

metrics = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Row count

# COMMAND ----------

metrics["silver_row_count"] = silver.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Null checks on key columns

# COMMAND ----------

metrics["silver_null_symbol"] = silver.filter(F.col("symbol").isNull()).count()
metrics["silver_null_date"]   = silver.filter(F.col("date").isNull()).count()
metrics["silver_null_close"]  = silver.filter(F.col("close").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Domain checks

# COMMAND ----------

metrics["silver_nonpositive_close"] = silver.filter(F.col("close") <= 0).count()
metrics["silver_high_lt_low"]       = silver.filter(F.col("high") < F.col("low")).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Basic coverage checks

# COMMAND ----------

# MAGIC %md
# MAGIC Helps detect partial ingestion:

# COMMAND ----------

metrics["silver_distinct_symbols"] = silver.select("symbol").distinct().count()

print(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC # Persist DQ metrics (audit table)

# COMMAND ----------

dq_row = [(ingestion_date,)]  # single-row DF with ingestion_date
dq_df = spark.createDataFrame(dq_row, ["ingestion_date"])

for k, v in metrics.items():
    dq_df = dq_df.withColumn(k, F.lit(v).cast("bigint"))

dq_df = (
    dq_df
    .withColumn("dq_pass", F.lit(1))  # set later after gate checks
    .withColumn("run_ts", F.current_timestamp())
)

# Create table if not exists, then append results
# (A real setup might partition by ingestion_date)
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_metrics (
  ingestion_date STRING,
  silver_row_count BIGINT,
  silver_null_symbol BIGINT,
  silver_null_date BIGINT,
  silver_null_close BIGINT,
  silver_nonpositive_close BIGINT,
  silver_high_lt_low BIGINT,
  silver_distinct_symbols BIGINT,
  dq_pass INT,
  run_ts TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gates (fail fast)

# COMMAND ----------

# MAGIC %md
# MAGIC - Dataset must exist and have rows
# MAGIC - Key columns must not be null
# MAGIC - Price sanity rules must hold

# COMMAND ----------

gates_failed = []

if metrics["silver_row_count"] <= 0:
    gates_failed.append("silver_row_count <= 0")

if metrics["silver_null_symbol"] > 0:
    gates_failed.append("silver_null_symbol > 0")

if metrics["silver_null_date"] > 0:
    gates_failed.append("silver_null_date > 0")

if metrics["silver_null_close"] > 0:
    gates_failed.append("silver_null_close > 0")

if metrics["silver_nonpositive_close"] > 0:
    gates_failed.append("silver_nonpositive_close > 0")

if metrics["silver_high_lt_low"] > 0:
    gates_failed.append("silver_high_lt_low > 0")

# Update dq_pass
dq_pass = 0 if gates_failed else 1
dq_df = dq_df.withColumn("dq_pass", F.lit(dq_pass))

# Persist metrics (always write metrics, even if failing)
dq_df.write.mode("append").saveAsTable("dq_metrics")

print("Wrote DQ metrics to databricks_test.stock_demo.dq_metrics")

# If any gates failed, stop the workflow here
if gates_failed:
    raise ValueError("DQ gates failed: " + "; ".join(gates_failed))

print("DQ checks PASSED.")