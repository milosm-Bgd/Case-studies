# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

#first we use get() method form Secrets Utility and assign the value to a variable, so we can use it
getcase_sas_token = dbutils.secrets.get(scope = "getcase-scope", key = "Getcase-SAS-Token") 

# COMMAND ----------

#next what we do , we copy the name of above variable and input it inside confiuration script down below instead of hard-coded value:

# COMMAND ----------

#mywork
spark.conf.set("fs.azure.account.auth.type.getcase1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.getcase1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.getcase1dl.dfs.core.windows.net",getcase_sas_token)

# COMMAND ----------

#my work
display(dbutils.fs.ls("abfss://demo@getcase1dl.dfs.core.windows.net"))

# COMMAND ----------

#mywork
display(spark.read.csv("abfss://demo@getcase1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

#my work
spark.conf.set("fs.azure.account.auth.type.getcase1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.getcase1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.getcase1dl.dfs.core.windows.net", "sp=rl&st=2024-10-06T16:37:06Z&se=2024-10-06T21:37:06Z&spr=https&sv=2022-11-02&sr=c&sig=GR3DfBl0UNnn5%2B7dCD3%2FuegC8g6p1q4cSDs0owVdToc%3D")

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

