# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS OAuth Configuration using Service Principal (Spark session-scoped)

# COMMAND ----------

# MAGIC %md
# MAGIC * Spark needs credentials to access ADLS Gen2 using ABFS (abfss://)
# MAGIC * These configs live in the active Spark session. If the cluster restarts, re-run this cell

# COMMAND ----------

# Parameters
storage_account_name = "stockdatalakemgg"
container_name = "stock-demo"

# Credentials are retrieved from Databricks Secret Scope (not hardcoded)
configs = {
  f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "OAuth",
  f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net": dbutils.secrets.get(scope="stock-scope", key="client-id"),
  f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net": dbutils.secrets.get(scope="stock-scope", key="client-secret"),
  f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net":
    f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='stock-scope', key='tenant-id')}/oauth2/token"
}

for k, v in configs.items():
    spark.conf.set(k, v)

# Project root in ADLS
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
print("Base path:", base_path)