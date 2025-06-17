# Databricks notebook source
# MAGIC %md
# MAGIC Ingest category_to_csv file
# MAGIC %md 
# MAGIC Read the CSV file using Dataframe API Reader

dbutils.widgets.help()

# MAGIC %run "../includes/configuration"

raw_folder_path
spark.read.csv()
dbutils.fs.mounts()
display(dbutils.fs.mounts())

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/raw

category_df = spark.read.csv(f"{raw_folder_path}/category_to_csv.csv")
type(category_df)
category_df.show()
display(category_df)

category_df = spark.read.option("header", True).csv("/mnt/getcase1dl/raw/category_to_csv.csv")
display(category_df)
category_df.printSchema()
category_df.describe().show()

category_df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/getcase1dl/raw/category_to_csv.csv")
display(category_df)
category_df.describe().show()
category_df.printSchema()

#playing with circuits.csv file
circuits_df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/getcase1dl/demo/circuits.csv")
circuits_df.printSchema()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType 

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

circuits_df.printSchema()


category_schema = StructType(fields=[StructField("CategoryID", IntegerType(), False),
                                     StructField("CategoryName", StringType(), True),
                                     StructField("Description", StringType(), True)
                                     ])

category_df = spark.read\
    .option("header", True)\
        .schema(category_schema)\
            .csv(f"{raw_folder_path}/category_to_csv.csv")

display(category_df)
category_df.printSchema()


#playign with creatig DatAFrame from scratch
df_dummy = spark.createDataFrame([
    (2, "Alice"), (5, "Bob")], schema=["age", "name"])

df_dummy.show()

from pyspark.sql.functions import current_timestamp

category_final_df = category_df.withColumn("ingestion_date", current_timestamp())
display(category_final_df)

category_final_df = category_final_df\
    .withColumnRenamed("CategoryID", "category_id")\
        .withColumnRenamed("CategoryName", "category_name")\
            .withColumnRenamed("Description", "description")

display(category_final_df)

#function we created in another notebook, "common_functions"
# add_ingestion_date

# MAGIC Writing the final Dataframe to datalake in parquet format (processed layer)
category_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/category")

display(spark.read.parquet(f"{processed_folder_path}/category"))

#as we want to create managed table in processed container, we need to overwrite already created folder
category_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.category") 

#after we created the folder 'category' again in processed container, we try to query the data: 
display(spark.read.parquet(f"{processed_folder_path}/category"))

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.category

