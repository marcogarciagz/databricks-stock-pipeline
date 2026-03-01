# Databricks notebook source
# MAGIC %md
# MAGIC # Implementing ML forecasting (using UC tables)

# COMMAND ----------

# MAGIC %md
# MAGIC **Project**: Stock Lakehouse Demo (ADLS + Databricks + UC + Power BI)
# MAGIC
# MAGIC **Notebook**: ``05_ml_forecasting``
# MAGIC
# MAGIC **Layer**: Machine Learning (Training + Scoring) - Consumer of Gold
# MAGIC
# MAGIC **Purpose**:
# MAGIC * Train a simple forecasting model using features from Gold
# MAGIC * Predict next-day returns ``(t+1)`` from features observed at day ``t``
# MAGIC * Benchmark against a strong naive baseline (predict 0 return)
# MAGIC * White predictions to a governed Unity Catalog table for BI consumption
# MAGIC
# MAGIC **Inputs**:
# MAGIC * UC table: ``databricks_test.stock_demo.gold_features_daily``
# MAGIC
# MAGIC **Outputs**:
# MAGIC * UC table: ``databricks_test.stock_demo.gold_predictions_daily``
# MAGIC
# MAGIC Key ML/engineering decisions(talking point):
# MAGIC * Target is next-day return (more stationary than price)
# MAGIC * Time-based split (no random split) to avoid look-ahead bias
# MAGIC * Baseline-first evaluation (0-return predictor) to validate value-add
# MAGIC * Spark ML pipeline: categorical encoding + vector assembly + GBT regressor

# COMMAND ----------

# MAGIC %md
# MAGIC **OAuth required only if running path-based. If using UC tables only, this can be removed!**

# COMMAND ----------

# MAGIC %run ./_utils/01_adls_oauth_setup

# COMMAND ----------

display(dbutils.fs.ls("abfss://stock-demo@stockdatalakemgg.dfs.core.windows.net/stock-demo/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0) Unity Catalog context + parameters

# COMMAND ----------

spark.sql("USE CATALOG databricks_test")
spark.sql("USE SCHEMA stock_demo")

# Train/test cutoff date (time-aware validation)
train_end_date = "2022-12-31"

# Model identifier stored with predictions for lineage
model_name = "spark_gbt_next_day_return_v1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Loading Gold features

# COMMAND ----------

# MAGIC %md
# MAGIC Gold already has:
# MAGIC * close, volume
# MAGIC * return_1d
# MAGIC * moving averages
# MAGIC * volatility measures
# MAGIC
# MAGIC *NOTE: only columns needed are selected for this experiment.*

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold = spark.table("gold_features_daily")

df = gold.select(
    "symbol","date",
    "close","volume",
    "return_1d","ma_10","ma_20","volatility_10","volatility_20"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Define supervised learning label (``next-day return``)

# COMMAND ----------

# MAGIC %md
# MAGIC Expected output: features at day t --> predict return at day t+1
# MAGIC
# MAGIC ``label`` = lead(return_!d, 1) over (partition by symbol order by date)
# MAGIC
# MAGIC ``prediction_date`` = the day forecasted for (t+1)

# COMMAND ----------

w = Window.partitionBy("symbol").orderBy("date")

df_labeled = (
    df
    .withColumn("label", F.lead("return_1d", 1).over(w))
    .withColumn("prediction_date", F.lead("date", 1).over(w))  # date we are forecasting for
    .filter(F.col("label").isNotNull())
)

print("Total labeled rows:", df_labeled.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Data cleaning for Spark ML

# COMMAND ----------

# MAGIC %md
# MAGIC Spark ML models cannot handle NULL or NaN values inside feature vectors. Rolling features (especially volatility) can be NULL early in the series.
# MAGIC
# MAGIC Actions:
# MAGIC * Remove NULLs
# MAGIC * Remove NaNs
# MAGIC * Remove extreme values (guardrail against corrupt/odd data)

# COMMAND ----------

from pyspark.sql import functions as F

feature_cols = ["close","volume","return_1d","ma_10","ma_20","volatility_10","volatility_20"]

# Remove nulls in all features + label
df_clean = df_labeled
for c in feature_cols + ["label"]:
    df_clean = df_clean.filter(F.col(c).isNotNull())

# Remove NaNs (NaN is a special floating value, Spark provides isnan())
for c in feature_cols + ["label"]:
    df_clean = df_clean.filter(~F.isnan(F.col(c)))

# Guardrails: prevent extreme outliers from breaking training
# For daily returns, abs(x) < 1.0 is a generous bound: +/* 100% daily move
df_clean = df_clean.filter(
    (F.abs(F.col("label")) < 1.0) &
    (F.abs(F.col("return_1d")) < 1.0) &
    (F.col("volatility_10") < 1.0) &
    (F.col("volatility_20") < 1.0)
)

print("Rows after cleaning:", df_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Time-based train/test split (avoid look-ahead bias)

# COMMAND ----------

# MAGIC %md
# MAGIC Financial time series must be split chronologically. Random splits leak future information and overestimate performance.

# COMMAND ----------

train = df_clean.filter(F.col("date") <= F.lit(train_end_date))
test  = df_clean.filter(F.col("date") >  F.lit(train_end_date))

print("Train rows:", train.count())
print("Test rows:", test.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Baseline model (strong benchmark): predict 0 return

# COMMAND ----------

# MAGIC %md
# MAGIC In liquid markets, "predict 0" is often hard to beat for 1-day returns.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

test_baseline = test.withColumn("baseline_pred", F.lit(0.0))

e_rmse = RegressionEvaluator(labelCol="label", predictionCol="baseline_pred", metricName="rmse")
e_mae  = RegressionEvaluator(labelCol="label", predictionCol="baseline_pred", metricName="mae")

baseline_rmse = e_rmse.evaluate(test_baseline)
baseline_mae  = e_mae.evaluate(test_baseline)

print("Baseline RMSE:", baseline_rmse)
print("Baseline MAE :", baseline_mae)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Model selection: Gradient-Boosted Trees (Spark ML)

# COMMAND ----------

# MAGIC %md
# MAGIC Why GBTRegressor?
# MAGIC * Nonlinear model that can capture interactions between features
# MAGIC * Strong baseline model in tabular settings
# MAGIC * Native Spark ML implmentation (no external dependencies)
# MAGIC * Fast enough for demo
# MAGIC
# MAGIC Feature handling:
# MAGIC * symbol is categorical --> index + one-hot encode
# MAGIC * numeric features assembled into a single vector

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor

cat_col = "symbol"
num_cols = ["close","volume","return_1d","ma_10","ma_20","volatility_10","volatility_20"]

indexer = StringIndexer(inputCol=cat_col, outputCol="symbol_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["symbol_idx"], outputCols=["symbol_ohe"])

assembler = VectorAssembler(
    inputCols=["symbol_ohe"] + num_cols,
    outputCol="features",
    handleInvalid="keep"
)

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="label",
    predictionCol="y_pred",
    maxIter=50, # number of boosting iterations (trees)
    maxDepth=5, # tree depth (controls complexity)
    stepSize=0.1 # learning rate
)

pipeline = Pipeline(stages=[indexer, encoder, assembler, gbt])

# Fit on training data
model = pipeline.fit(train)

# Score on test data
pred = model.transform(test)

# COMMAND ----------

# MAGIC %md
# MAGIC Model training is now clean and stable. Features:
# MAGIC * Train rows dropped from 214,108 to 214,081
# MAGIC * Test rows: 19,625
# MAGIC * Training time: 2min

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Evaluating model vs baseline

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

e_rmse_model = RegressionEvaluator(labelCol="label", predictionCol="y_pred", metricName="rmse")
e_mae_model  = RegressionEvaluator(labelCol="label", predictionCol="y_pred", metricName="mae")

model_rmse = e_rmse_model.evaluate(pred)
model_mae  = e_mae_model.evaluate(pred)

print("Baseline RMSE:", baseline_rmse)
print("Baseline MAE :", baseline_mae)
print("Model RMSE   :", model_rmse)
print("Model MAE    :", model_mae)
print("RMSE improvement (baseline - model):", baseline_rmse - model_rmse)
print("MAE  improvement (baseline - model):", baseline_mae - model_mae)

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity check:

# COMMAND ----------

display(pred.select("symbol","date","prediction_date","label","y_pred").orderBy("symbol","date").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Persist predictions as a governed Gold table (for BI)

# COMMAND ----------

# MAGIC %md
# MAGIC Reason to write predictions to a UC table:
# MAGIC * Makes results dicoverable and queryable via Databricks SQL / Power BI
# MAGIC * Provides lineage fields for monitoring and reproducibility

# COMMAND ----------

from pyspark.sql import functions as F

pred_out = (
    pred.select(
        "symbol",
        "prediction_date",
        F.col("label").alias("y_true"),
        F.col("y_pred").alias("y_pred")
    )
    .withColumn("model_name", F.lit(model_name))
    .withColumn("train_end_date", F.lit(train_end_date))
    .withColumn("run_ts", F.current_timestamp())
)

# Overwrite for a demo
# In production, append by run_id and keep history for monitoring
pred_out.write.mode("overwrite").saveAsTable("gold_predictions_daily")

print("Saved table: databricks_test.stock_demo.gold_predictions_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Verifying outputs

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS n FROM gold_predictions_daily").show()
display(spark.table("gold_predictions_daily").orderBy("symbol","prediction_date").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC The model did not outperform the zero-return baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC This is realistic. Daily stock return prediction using:
# MAGIC * Simple technical features
# MAGIC * Tree model
# MAGIC * No hyperparameter tuning
# MAGIC
# MAGIC Therefore, it should not easily beat a zero-return baseline. In efficient markets:
# MAGIC * Next-day returns are close to white noise
# MAGIC * Simple models rarely produce meaningful predictive power

# COMMAND ----------

# MAGIC %md
# MAGIC Honestly, if the model had massively ouperformed the baseline, that would actually look suspicious.

# COMMAND ----------

# MAGIC %md
# MAGIC