# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting the shipper_to_csv file to processed layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the shipper_to_csv file with DataFrame Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

shipper_schema = StructType(fields=[StructField("ShipperID", IntegerType(), False),
                                     StructField("CompanyName", StringType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Next we do reading the data to DataFrame 

# COMMAND ----------

shipper_df = spark.read\
    .option("header", True)\
        .schema(shipper_schema)\
            .csv("/mnt/getcase1dl/raw/shipper_to_csv.csv")

# COMMAND ----------

display(shipper_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Adding ingested_date column like Timestamp to DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

shipper_with_timestamp_df = shipper_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#example how to create 1 datetime column out of two columns consisting the date and time dimension
#we dont run the cell 
# form pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit 
#df_with_contatenated_date = original_df.withColumn("timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(shipper_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Select only the columns required 

# COMMAND ----------

# all the columns from the DataFrame are required (there are only 2 columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 5: Renaming the columns as to approach the python standards with naming variables 

# COMMAND ----------

shipper_renamed_df = shipper_with_timestamp_df\
    .withColumnRenamed("ShipperID","shipper_id")\
        .withColumnRenamed("CompanyName","company_shipper")

# COMMAND ----------

display(shipper_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6: Write the output to processed container in parquet format

# COMMAND ----------

shipper_renamed_df.write.mode('overwrite').parquet('/mnt/getcase1dl/processed/shipper')

# COMMAND ----------

# MAGIC %md
# MAGIC ovde biram opciju kreiranja Managed table i msestanje podataka u tabelu i database 

# COMMAND ----------

shipper_renamed_df.write.mode('overwrite').format('parquet').saveAsTable('acme_processed.shipper')

# COMMAND ----------

list_the_mounts = dbutils.fs.ls('/mnt/getcase1dl/processed/division')

# COMMAND ----------

display(list_the_mounts)

# COMMAND ----------

display(spark.read.parquet('/mnt/getcase1dl/processed/shipper'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.shipper