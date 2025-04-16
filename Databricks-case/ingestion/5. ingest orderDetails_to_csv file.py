# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the file orderDetails_to_csv 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 : Read the file with DataFrame Reader API 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType , TimestampType, DateType

# COMMAND ----------

orderDetails_schema = StructType(fields=[StructField("OrderID", IntegerType(), False),
                                          StructField("OrderDate", DateType(), True),
                                          StructField("CustomerID", IntegerType(), True),
                                          StructField("EmployeeID", IntegerType(), True),
                                          StructField("ShipperID", IntegerType(), True),
                                          StructField("Freight", DoubleType(), True)
                                          ])

# COMMAND ----------

orderDetails_df = spark.read\
            .option("header", True)\
                .option("dateFormat","dd/MM/yyyy")\
                    .schema(orderDetails_schema)\
                        .csv('/mnt/getcase1dl/raw/orderDetails_to_csv.csv')

# COMMAND ----------

display(orderDetails_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Renaming the column names (and creating derived columns from existing ones)

# COMMAND ----------

orderDetails_renamed_df = orderDetails_df.withColumnRenamed("OrderID", "order_id")\
                                         .withColumnRenamed("OrderDate", "order_date")\
                                             .withColumnRenamed("CustomerID", "customer_id")\
                                                 .withColumnRenamed("EmployeeID", "employee_id")\
                                                     .withColumnRenamed("ShipperID", "shipper_id")\
                                                          .withColumnRenamed("Freight", "freight")

# COMMAND ----------

display(orderDetails_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Dropping unwanted columns & adding new columns 

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, year,  col

# COMMAND ----------

# adding column "timestamp" based on exsting "order_date" column in date format and coverting it to timestamp format 
orderDetails_interim_df = orderDetails_renamed_df.withColumn("timestamp", to_timestamp(col("order_date"),"dd/MM/yyyy HH:mm:ss"))

# COMMAND ----------

#adding column "fiscal_year" based on exsting "order_date" column in date format and coverting it to year format
orderDetails_renamed_df = orderDetails_renamed_df.withColumn("fiscal_year", year(col("order_date")))

# COMMAND ----------

display(orderDetails_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#adding ingestion_date column in timestamp() format
orderDetails_with_ingestion_date_df = orderDetails_renamed_df.withColumn("ingestion_date", current_timestamp())
display(orderDetails_with_ingestion_date_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Writing theDataFrame with DataFrame Writer API to datalake in parquet format (inside processed container=silver layer)

# COMMAND ----------

orderDetails_with_ingestion_date_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/orderDetails")

# COMMAND ----------

# MAGIC %md
# MAGIC alternative option: writing into the proccesed container and saving as Managed table 

# COMMAND ----------

orderDetails_with_ingestion_date_df.write.mode("overwrite").partitionBy("fiscal_year").format("parquet").saveAsTable("acme_processed.orderDetails_partitioned")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed/orderDetails

# COMMAND ----------

display(spark.read.parquet('/mnt/getcase1dl/processed/orderDetails'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.orderDetails

# COMMAND ----------

