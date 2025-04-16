# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'getcase-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'getcase-scope', key = 'getcase1dl-account-key')

# COMMAND ----------

