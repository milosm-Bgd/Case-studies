-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS acme_presentation
LOCATION "/mnt/getcase1dl/presentation"

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in acme_raw;

-- COMMAND ----------

show tables from acme_processed;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use acme_presentation;

-- COMMAND ----------

show tables from acme_presentation;

-- COMMAND ----------

SELECT * FROM acme_processed.orderdetails_partitioned

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_df_one.createTempView("v_presentation_df_one")