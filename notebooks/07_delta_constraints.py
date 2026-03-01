# Databricks notebook source
spark.sql("USE CATALOG databricks_test")
spark.sql("USE SCHEMA stock_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Delta constraints

# COMMAND ----------

# Not null constraints (key columns)
spark.sql("ALTER TABLE silver_prices_daily ALTER COLUMN symbol SET NOT NULL")
spark.sql("ALTER TABLE silver_prices_daily ALTER COLUMN date SET NOT NULL")
spark.sql("ALTER TABLE silver_prices_daily ALTER COLUMN close SET NOT NULL")

# Check constraints (basic price sanity)
spark.sql("""
ALTER TABLE silver_prices_daily
ADD CONSTRAINT silver_close_positive CHECK (close > 0)
""")

spark.sql("""
ALTER TABLE silver_prices_daily
ADD CONSTRAINT silver_high_ge_low CHECK (high >= low)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Delta constraints

# COMMAND ----------

spark.sql("ALTER TABLE gold_features_daily ALTER COLUMN symbol SET NOT NULL")
spark.sql("ALTER TABLE gold_features_daily ALTER COLUMN date SET NOT NULL")
spark.sql("ALTER TABLE gold_features_daily ALTER COLUMN close SET NOT NULL")
spark.sql("ALTER TABLE gold_features_daily ALTER COLUMN return_1d SET NOT NULL")

spark.sql("""
ALTER TABLE gold_features_daily
ADD CONSTRAINT gold_close_positive CHECK (close > 0)
""")

# Optional: volatility must be non-negative when present
spark.sql("""
ALTER TABLE gold_features_daily
ADD CONSTRAINT gold_vol_nonnegative CHECK (
  volatility_10 IS NULL OR volatility_10 >= 0
)
""")

spark.sql("""
ALTER TABLE gold_features_daily
ADD CONSTRAINT gold_vol20_nonnegative CHECK (
  volatility_20 IS NULL OR volatility_20 >= 0
)
""")

print("Constraints applied.")