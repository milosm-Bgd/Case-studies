# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

#mywork
dbutils.secrets.get(scope = "getcase-scope", key = "getcase1dl-account-key")

# COMMAND ----------

getcase1dl_account_key = dbutils.secrets.get(scope = "getcase-scope", key = "getcase1dl-account-key")

# COMMAND ----------

#my work
spark.conf.set(
    "fs.azure.account.key.getcase1dl.dfs.core.windows.net",
    getcase1dl_account_key)

# COMMAND ----------

#my_work
display(dbutils.fs.ls("abfss://demo@getcase1dl.dfs.core.windows.net"))

# COMMAND ----------

#my_work
display(spark.read.csv("abfss://demo@getcase1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl.dfs.core.windows.net",
    formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

