# Databricks notebook source
test_schema = "OrderID INT, OrderDate DATE, CustomerID INT, EmployeeID INT, ShipperID INT, Freight DECIMAL(5,2)"

# COMMAND ----------

test_df = spark.read.option("dateFormat","dd/MM/yyyy")\
                    .schema(test_schema)\
                        .csv('/mnt/getcase1dl/raw/orderDetails_to_csv.csv')

# COMMAND ----------

display(test_df)

# COMMAND ----------

