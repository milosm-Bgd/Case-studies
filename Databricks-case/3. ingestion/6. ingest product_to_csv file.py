# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 : Ingest product_to_csv file from Azure Data Lake 

# COMMAND ----------

# MAGIC %md
# MAGIC Read the CSV file using DataFrame Reader API

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType

# COMMAND ----------

product_schema = StructType(fields =[StructField("ProductID", IntegerType(), False),
                                    StructField("ProductName", StringType(), True),
                                    StructField("SupplierID", IntegerType(), True),
                                    StructField("CategoryID", IntegerType(), True),
                                    StructField("QuantityPerUnit", IntegerType(), True),
                                    StructField("UnitCost", DecimalType(6,2), True),
                                    StructField("UnitPrice", DecimalType(6,2), True),
                                    StructField("UnitsInStock", IntegerType(), True),
                                    StructField("UnitsOnOrder", IntegerType(), True)
                                    ])

# COMMAND ----------

product_df = spark.read.option("header", True)\
    .schema(product_schema)\
        .csv("/mnt/getcase1dl/raw/product_to_csv.csv")

# COMMAND ----------

display(product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Select only the required Columns

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Preimenovanje kolona radi kasnije analize podataka u SQL-u i Python-u (nazivi varijabli u Python standardu, i da bi se izbeglo prilikom pisanja sql query-a da dodje do zabune ukoliko neko bude pisao imena sve lower-case)

# COMMAND ----------

product_renamed_df = product_df.withColumnRenamed("ProductId","product_id")\
    .withColumnRenamed("ProductName","product_name")\
        .withColumnRenamed("SupplierId","supplier_id")\
            .withColumnRenamed("CategoryID","category_id")\
                .withColumnRenamed("QuantityPerUnit","qty_per_unit")\
                    .withColumnRenamed("UnitCost","unit_cost")\
                        .withColumnRenamed("UnitPrice","unit_price")\
                            .withColumnRenamed("UnitsInStock","units_in_stock")\
                                .withColumnRenamed("UnitsOnOrder","units_on_order")

# COMMAND ----------

# MAGIC %md 
# MAGIC 4. Adding new Column to DataFrame with "withColumn()" function 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

product_final_df = product_renamed_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("total_units", col("units_in_stock") + col("units_on_order"))\
            .withColumn("margin_value", (col("unit_price") - col("unit_cost")).cast(DecimalType(6,2)))\
                        .withColumn("buying_cost", (col("total_units") * col("unit_cost")).cast(DecimalType(6,2)))\
                                    .withColumn("expected_revenue", (col("total_units") * col("unit_price")).cast(DecimalType(6,2)))\
                                        .withColumn("ordered_amount", (col("units_on_order") * col("unit_price")).cast(DecimalType(6,2)))

# COMMAND ----------

display(product_final_df)

# COMMAND ----------

# napraviti da se poslednje 3 kreirane kolone preformatiraju kao DECIMAL(6,2)
# koristio strukturu .withColumn("colun_name", (col("unit_price") - col("unit_cost")).cast(DecimalType(6,2))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 : option with addign literal value into a column inside the DataFrame 

# COMMAND ----------

# primer imamo u kreiranju customer_df fajla 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5: Write data to Datalake as parquet

# COMMAND ----------

product_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/product")

# COMMAND ----------

# MAGIC %md
# MAGIC Ipak biram opciju kreiranja Managed table preko saveAsTable metode (smestanje u database acme_processed)

# COMMAND ----------

product_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.product")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed/product

# COMMAND ----------

display(spark.read.parquet("/mnt/getcase1dl/processed/product"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.product

# COMMAND ----------

