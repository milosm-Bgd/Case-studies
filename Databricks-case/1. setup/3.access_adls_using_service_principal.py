# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-client-id")
tenant_id = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-client-secret")

# COMMAND ----------

#mywork
# Documentation link for Connect to Azure Data Lake Storage Gen2 and Blob Storage: 
# https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal
#Documentation lik to f"String function :https://realpython.com/python-f-strings/#f-strings-a-new-and-improved-way-to-format-strings-in-python 

# COMMAND ----------

#mywork
client_id = "57327ba5-3932-415d-9795-fd2b0772f552"
tenant_id = "fdf2b272-5466-4d91-b8b4-07ba40ab0b46"
client_secret = "CMa8Q~7TaPS07-ygONKsEZ4AzMy.c8IeeRI.~aM1"

# COMMAND ----------

#client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
#tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
#client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.getcase1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.getcase1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.getcase1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.getcase1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.getcase1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@getcase1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@getcase1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

