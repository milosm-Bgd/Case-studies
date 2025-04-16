# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

#mywork: the configuration we used for re-configuring the cluster 'databricks-get_case-cluster' on Compute tab 
spark.conf.set(
    "fs.azure.account.key.getcase1dl.dfs.core.windows.net",
    "RtelR6v5UnXZccjoHr8Yn3ftUlrfvwgJZuEhXA03aU0rm35lJGZjh7GRrKp4Zvu1qubd+rhszaQk+AStQMLTZw==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@getcase1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@getcase1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

