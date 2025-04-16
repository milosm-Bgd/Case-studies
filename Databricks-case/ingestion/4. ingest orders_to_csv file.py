# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingestion of orders_to_csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 : Read the orders_to _scv with DataFrame Reader API

# COMMAND ----------

#here we use DDL statements to make the necesarry schema 
orders_schema = "OrderID INT, LineNO INT, ProductID INT, Quantity INT, UnitPrice DECIMAL(10,8), Discount DECIMAL(12,11)"

# COMMAND ----------

orders_df = spark.read.schema(orders_schema).csv("/mnt/getcase1dl/raw/orders_to_csv.csv")

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Sada radimo na uklanjanju nepotrebnih kolona 

# COMMAND ----------

orders_dropped_col_df = orders_df.drop('LineNO', 'Discount')

# COMMAND ----------

#alternaive ways to drop the columns form DataFrmae
#1. orders_dropped_col_df = orders_df.drop(orders_df["LineNO"]) -> useful when we are joining 2 Datrames with Join constuctions , when we need to specify the column name we want to drop from specified DataFrame
# 2. from pyspark.sql.functions import col 
# orders_dropped_col_df = orders_df.drop(col('LineNO')) 

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 3: preimenovanje kolona i uvezivanje ingestion_date dodatne kolone 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

orders_final_df = orders_dropped_col_df.withColumnRenamed("OrderID","order_id")\
                .withColumnRenamed("ProductID","product_id")\
                    .withColumnRenamed("Quantity","quantity")\
                            .withColumnRenamed("UnitPrice","unit_price")\
                                .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(orders_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 : Pisanje podataka u parquet format (write data into parq format) 

# COMMAND ----------

orders_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/orders")

# COMMAND ----------

orders_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.orders")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed/orders

# COMMAND ----------

display(spark.read.parquet("/mnt/getcase1dl/processed/orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.orders

# COMMAND ----------

# MAGIC %md
# MAGIC sintaksa pisanje ouput-u u silver layer u delta formatu

# COMMAND ----------

# writing in delta_format
# orders_final_df.write.mode("overwrite").format("delta").save("/mnt/getcase1dl/processed/orders")