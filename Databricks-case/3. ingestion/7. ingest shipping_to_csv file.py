# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting the shipping_to_csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 : Read the file with DataFrame Reader API 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType

# COMMAND ----------

shipping_schema = StructType(fields=[StructField("OrderID", IntegerType(), False),
                                          StructField("LineNO", IntegerType(), True),
                                          StructField("ShipperID", IntegerType(), True),
                                          StructField("CustomerID", IntegerType(), True),
                                          StructField("ProductID", IntegerType(), True),
                                          StructField("EmployeeID", IntegerType(), True),
                                          StructField("ShipmentDate", DateType(), True)
                                          ])

# COMMAND ----------

shipping_df = spark.read\
            .option("header", True)\
                .option("dateFormat","dd/MM/yyyy")\
                    .schema(shipping_schema)\
                        .csv('/mnt/getcase1dl/raw/shipping_to_csv.csv')

# COMMAND ----------

display(shipping_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Renaming the column names (and creating derived columns from existing ones)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, year, col

# COMMAND ----------

shipping_renamed_df = shipping_df.withColumnRenamed("OrderID", "order_id")\
                                         .withColumnRenamed("LineNO", "line_no")\
                                             .withColumnRenamed("ShipperID", "shipper_id")\
                                                 .withColumnRenamed("CustomerID", "customer_id")\
                                                     .withColumnRenamed("ProductID", "product_id")\
                                                          .withColumnRenamed("EmployeeID", "employee_id")\
                                                              .withColumnRenamed("ShipmentDate", "shipping_date")

# COMMAND ----------

shipping_renamed_df = shipping_renamed_df\
        .withColumn("sh_fiscal_year", year(col("shipping_date")))

# COMMAND ----------

display(shipping_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Dropping unwanted columns & adding new columns 

# COMMAND ----------

shipping_with_ingestion_date_df = shipping_renamed_df.withColumn("ingestion_date", current_timestamp())
#display(shipping_with_ingestion_date_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Writing theDataFrame with DatFrame Writer API to datalake in parquet format (inside processed container=silver layer)

# COMMAND ----------

shipping_with_ingestion_date_df.write.mode("overwrite").partitionBy("sh_fiscal_year").parquet("/mnt/getcase1dl/processed/shipping_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC ipak biram opciju kreiranja Managed table i upisivanja u database "acme_processed"

# COMMAND ----------

shipping_with_ingestion_date_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.shipping")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed/shipping

# COMMAND ----------

display(spark.read.parquet("/mnt/getcase1dl/processed/shipping"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.shipping

# COMMAND ----------

