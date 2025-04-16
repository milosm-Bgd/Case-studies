# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest division_to_csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the division_to_csv file with DataFrame Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

division_schema = StructType(fields=[StructField("DivisionID", IntegerType(), False),
                                     StructField("DivisionName", StringType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Next we do reading the data to DataFrame 

# COMMAND ----------

division_df = spark.read\
    .option("header", True)\
        .schema(division_schema)\
            .csv("/mnt/getcase1dl/raw/division_to_csv.csv")

# COMMAND ----------

display(division_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

division_with_timestamp_df = division_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#example how to create 1 datetime column out of two columns consisting the date and time dimension
#we dont run the cell 
# form pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit 
#df_with_contatenated_date = original_df.withColumn("timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(division_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Select only the columns required 

# COMMAND ----------

# all the columns from the DataFrame are required (there are only 2 columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 4: Renaming the columns as to approach the pythn standards with naming variables 

# COMMAND ----------

division_renamed_df = division_with_timestamp_df\
    .withColumnRenamed("DivisionID","division_id")\
        .withColumnRenamed("DivisionName","division_name")

# COMMAND ----------

display(division_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5: Write the output to processed container in parquet format

# COMMAND ----------

division_renamed_df.write.mode('overwrite').parquet('/mnt/getcase1dl/processed/division')

# COMMAND ----------

division_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.division")

# COMMAND ----------

list_the_mounts = dbutils.fs.ls('/mnt/getcase1dl/processed/division')

# COMMAND ----------

display(list_the_mounts)

# COMMAND ----------



# COMMAND ----------

display(spark.read.parquet("/mnt/getcase1dl/processed/division"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.division