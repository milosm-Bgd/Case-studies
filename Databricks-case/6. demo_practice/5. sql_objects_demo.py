# Databricks notebook source
# MAGIC %md
# MAGIC creatigng temp views i n order to query thru data in SQL or python 

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW v_requirement_1
# MAGIC AS
# MAGIC SELECT * FROM acme_processed.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_requirement_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_requirement_2
# MAGIC AS
# MAGIC SELECT * FROM acme_processed.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_requirement_2
# MAGIC WHERE fiscal_year = 2012; 

# COMMAND ----------

# creating Managaed table 
# we are goign to write the data from our Azure Datalake Storage presentation layer into the dataframe 
# from there we are creating the sql tables, as to be able for querying the data for data analysis