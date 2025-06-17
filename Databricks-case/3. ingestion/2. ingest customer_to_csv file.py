# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 : Ingest customer_to_csv file from Azure Data Lake 

# COMMAND ----------

# MAGIC %md
# MAGIC Read the CSV file using DataFrame Reader API

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/raw

# COMMAND ----------

customer_df = spark.read.csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
customer_df.show()

# COMMAND ----------

display(customer_df)

# COMMAND ----------

customer_df = spark.read.option("header",True).csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
display(customer_df)

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

customer_df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
customer_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, StringType, DoubleType

# COMMAND ----------

customer_schema = StructType(fields =[StructField("CustomerID", IntegerType(), False),
                                    StructField("CompanyName", StringType(), True),
                                    StructField("ContactName", StringType(), True),
                                    StructField("City", StringType(), True),
                                    StructField("Country", StringType(), True),
                                    StructField("DivisionID", IntegerType(), True),
                                    StructField("Address", StringType(), True),
                                    StructField("Fax", StringType(), True),
                                    StructField("Phone", StringType(), True),
                                    StructField("PostalCode", StringType(), True),
                                    StructField("StateProvince", StringType(), True),
                                    ])

# COMMAND ----------

customer_df = spark.read.option("header", True)\
    .schema(customer_schema)\
    .csv("/mnt/getcase1dl/raw/customer_to_csv.csv")

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Select only the required Columns

# COMMAND ----------

customer_selected_df = customer_df.select("CustomerID", "CompanyName", "ContactName", "City", "Country", "DivisionID")
display(customer_selected_df)

# COMMAND ----------

#another way of selecting the required columns 
customer_selected_df = customer_df.select(customer_df.CustomerID, customer_df.CompanyName, customer_df.ContactName, customer_df.City, customer_df.Country, customer_df.DivisionID)

# COMMAND ----------

#third way of selecting the required columns 
customer_selected_df = customer_df.select(customer_df["CustomerID"], customer_df["CompanyName"], customer_df["ContactName"], customer_df["City"], customer_df["Country"], customer_df["DivisionID"])

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

customer_selected_df = customer_df.select(col("CustomerID"), col("CompanyName"), col("ContactName"), col("City"), col("Country"), col("DivisionID"))
display(customer_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Preimenovanje kolona radi kasnije analize podataka u SQL-u i Python-u (nazivi varijabli u Python standardu, i da bi se izbeglo prilikom pisanja sql query-a da dodje do zabune ukoliko neko bude pisao imena sve lower-case)

# COMMAND ----------

customer_renamed_df = customer_selected_df.withColumnRenamed("CustomerID","customer_id")\
    .withColumnRenamed("CompanyName","company_name")\
        .withColumnRenamed("ContactName","contact_name")\
            .withColumnRenamed("City","city")\
                .withColumnRenamed("Country","country")\
                    .withColumnRenamed("DivisionID","division_id")

# COMMAND ----------

display(customer_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

# MAGIC %md 
# MAGIC 4. Adding new Column to DataFrame with "withColumn()" function 

# COMMAND ----------

customer_final_df = customer_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(customer_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 : option with addign literal value into a column inside the DataFrame 

# COMMAND ----------

customer_final_df = customer_renamed_df.withColumn("env", lit("Production"))
display(customer_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5: Write data to Datalake as parquet

# COMMAND ----------

display(customer_final_df)

# COMMAND ----------

customer_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/customer")

# COMMAND ----------

customer_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.customer")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed/customer

# COMMAND ----------

df = spark.read.parquet("/mnt/getcase1dl/processed/customer")

# COMMAND ----------

display(df)

# COMMAND ----------

display(spark.read.parquet("/mnt/getcase1dl/processed/customer"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_processed.customer