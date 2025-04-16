# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    
    #Get secrets from KeyVault
    client_id = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-client-id")
    tenant_id = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-tenant-id")
    client_secret = dbutils.secrets.get(scope = "getcase-scope", key = "getcase-app-client-secret")
    
    #Set configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Unmount with if any condition 
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount storage accoutn container using DBFS Utility
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting Raw Container 

# COMMAND ----------

mount_adls("getcase1dl", "raw")

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting Processed Container 

# COMMAND ----------

mount_adls('getcase1dl', 'processed')

# COMMAND ----------

mount_adls("getcase1dl", "presentation")

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Raw Container

# COMMAND ----------

mount_adls('getcase1dl', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Processed container 

# COMMAND ----------

mount_adls('getcase1dl', 'processed')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mounting Presentation Container 

# COMMAND ----------

mount_adls('getcase1dl', 'presentation')

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/getcase1dl/raw")

# COMMAND ----------

display(spark.read.csv('/mnt/getcase1dl/raw/product_to_csv.csv'))

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/getcase1dl/processed")

# COMMAND ----------

display(spark.read.parquet('/mnt/getcase1dl/processed/customer'))

# COMMAND ----------

display(spark.read.parquet('/mnt/getcase1dl/processed/product'))

# COMMAND ----------

display(spark.read.parquet('/mnt/getcase1dl/processed/orders'))

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/getcase1dl/presentation")