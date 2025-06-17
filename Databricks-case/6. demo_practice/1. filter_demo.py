# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

orders_demo_df = spark.read.parquet("/mnt/getcase1dl/processed/orders")

# COMMAND ----------

display(orders_demo_df)

# COMMAND ----------

orders_filtered_df = orders_demo_df.filter("product_id = 20 and quantity > 25")
display(orders_filtered_df)

# COMMAND ----------

orders_filtered_df = orders_demo_df.filter((orders_demo_df["product_id"] == 20) & (orders_demo_df["quantity"] > 25))
display(orders_filtered_df)

# COMMAND ----------

