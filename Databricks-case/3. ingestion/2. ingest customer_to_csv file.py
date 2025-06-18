# Databricks notebook source
#  Step 1 : Ingest customer_to_csv file from Azure Data Lake 
# Read the CSV file using DataFrame Reader API

display(dbutils.fs.mounts())

# %fs
# ls /mnt/getcase1dl/raw


customer_df = spark.read.csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
customer_df.show()
display(customer_df)

customer_df = spark.read.option("header",True).csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
display(customer_df)
customer_df.printSchema()


customer_df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("/mnt/getcase1dl/raw/customer_to_csv.csv")
customer_df.printSchema()


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, StringType, DoubleType

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


customer_df = spark.read.option("header", True)\
    .schema(customer_schema)\
    .csv("/mnt/getcase1dl/raw/customer_to_csv.csv")

customer_df.printSchema()
display(customer_df)

#  Step 2 : Select only the required Columns

customer_selected_df = customer_df.select("CustomerID", "CompanyName", "ContactName", "City", "Country", "DivisionID")
display(customer_selected_df)

#another way of selecting the required columns 
customer_selected_df = customer_df.select(customer_df.CustomerID, customer_df.CompanyName, customer_df.ContactName, customer_df.City, customer_df.Country, customer_df.DivisionID)

#third way of selecting the required columns 
customer_selected_df = customer_df.select(customer_df["CustomerID"], customer_df["CompanyName"], customer_df["ContactName"], customer_df["City"], customer_df["Country"], customer_df["DivisionID"])


from pyspark.sql.functions import col
customer_selected_df = customer_df.select(col("CustomerID"), col("CompanyName"), col("ContactName"), col("City"), col("Country"), col("DivisionID"))
display(customer_selected_df)

#  Step 3: Column renaming for further data analysis in SQL and Python (variable names kept in Python standard, 
#  in order to avoid potential misunderstandings and errors when creating the scripts in SQL with lower-case naming standard)

customer_renamed_df = customer_selected_df.withColumnRenamed("CustomerID","customer_id")\
    .withColumnRenamed("CompanyName","company_name")\
        .withColumnRenamed("ContactName","contact_name")\
            .withColumnRenamed("City","city")\
                .withColumnRenamed("Country","country")\
                    .withColumnRenamed("DivisionID","division_id")
display(customer_renamed_df)

from pyspark.sql.functions import current_timestamp , lit

#  Adding new Column to DataFrame with "withColumn()" function 
customer_final_df = customer_renamed_df.withColumn("ingestion_date", current_timestamp())
display(customer_final_df)

#  Step 4 : option with addign literal value into a column inside the DataFrame 
customer_final_df = customer_renamed_df.withColumn("env", lit("Production"))
display(customer_final_df)

#  Step 5: Write data to Datalake as parquet
display(customer_final_df)
customer_final_df.write.mode("overwrite").parquet("/mnt/getcase1dl/processed/customer")
customer_final_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.customer")

#  %fs
#  ls /mnt/getcase1dl/processed/customer

df = spark.read.parquet("/mnt/getcase1dl/processed/customer")
display(df)
display(spark.read.parquet("/mnt/getcase1dl/processed/customer"))

#  %sql
#  SELECT * FROM acme_processed.customer