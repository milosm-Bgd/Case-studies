# Databricks notebook source
# MAGIC %md
# MAGIC Ingest category_to_csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC Read the CSV file using Dataframe API Reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

spark.read.csv()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/raw

# COMMAND ----------

category_df = spark.read.csv(f"{raw_folder_path}/category_to_csv.csv")

# COMMAND ----------

type(category_df)

# COMMAND ----------

category_df.show()

# COMMAND ----------

display(category_df)

# COMMAND ----------

category_df = spark.read.option("header", True).csv("/mnt/getcase1dl/raw/category_to_csv.csv")

# COMMAND ----------

display(category_df)

# COMMAND ----------

category_df.printSchema()

# COMMAND ----------

category_df.describe().show()

# COMMAND ----------

category_df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/getcase1dl/raw/category_to_csv.csv")

# COMMAND ----------

display(category_df)

# COMMAND ----------

category_df.describe().show()

# COMMAND ----------

category_df.printSchema()

# COMMAND ----------

#playing with circuits.csv file
circuits_df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/getcase1dl/demo/circuits.csv")
circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType 

# COMMAND ----------

#play with circuits.csv file
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                     ])

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

category_schema = StructType(fields=[StructField("CategoryID", IntegerType(), False),
                                     StructField("CategoryName", StringType(), True),
                                     StructField("Description", StringType(), True)
                                     ])

# COMMAND ----------

category_df = spark.read\
    .option("header", True)\
        .schema(category_schema)\
            .csv(f"{raw_folder_path}/category_to_csv.csv")

# COMMAND ----------

display(category_df)

# COMMAND ----------

category_df.printSchema()

# COMMAND ----------

#playign with creatig DatAFrame from scratch
df_dummy = spark.createDataFrame([
    (2, "Alice"), (5, "Bob")], schema=["age", "name"])

# COMMAND ----------

df_dummy.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

category_final_df = category_df.withColumn("ingestion_date", current_timestamp())
display(category_final_df)

# COMMAND ----------

category_final_df = category_final_df\
    .withColumnRenamed("CategoryID", "category_id")\
        .withColumnRenamed("CategoryName", "category_name")\
            .withColumnRenamed("Description", "description")

# COMMAND ----------

display(category_final_df)

# COMMAND ----------

#function we created in another notebook, "common_functions"
# add_ingestion_date

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the final Dataframe to datalake in parquet format (processed layer)

# COMMAND ----------

category_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/category")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/category"))

# COMMAND ----------

#as we want to create managed table in processed container, we need to overwrite already created folder
category_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.category") 

# COMMAND ----------

#after we created the folder 'category' again in processed container, we try to query the data: 
display(spark.read.parquet(f"{processed_folder_path}/category"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.category

# COMMAND ----------

