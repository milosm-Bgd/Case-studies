-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS acme_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Create category table 

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE acme_raw;

-- COMMAND ----------

SHOW TABLES in acme_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.category;
CREATE TABLE IF NOT EXISTS acme_raw.category(
category_id int,
category_name STRING,
description STRING
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/category_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Create customer table 

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.customer;
CREATE TABLE IF NOT EXISTS acme_raw.customer(
customer_id INT,
company_name STRING,
contact_name STRING,
city STRING,
country STRING,
division_id INT
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/customer_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create division table

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.division;
CREATE TABLE IF NOT EXISTS acme_raw.division(
division_id INT,
division_name STRING
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/division_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.division;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Orders table

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.orders;
CREATE TABLE IF NOT EXISTS acme_raw.orders(
order_id	INT,
product_id	INT,
quantity	INT,
unit_price	DOUBLE,
discount	DOUBLE
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/orders_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create orderDetails table

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.orderDetails;
CREATE TABLE IF NOT EXISTS acme_raw.orderDetails(
order_id	INT,
order_date	DATE,
customer_id	INT,
employee_id	INT,
shipper_id	INT,
freight	DOUBLE
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/orderDetails_to_csv.csv", header true, dateFormat "dd/MM/yyyy")

-- COMMAND ----------

SELECT * FROM acme_raw.orderDetails;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Product table

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.product;
CREATE TABLE IF NOT EXISTS acme_raw.product(
product_id	Int,
product_name	String,
supplier_id	Int,
category_id	Int,
quantity_per_unit	Int,
unit_cost	Double,
unit_price	Double,
units_in_stock	Int,
units_on_order	Int
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/product_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Shipping table

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.shipping;
CREATE TABLE IF NOT EXISTS acme_raw.shipping(
order_id	Int,
line_no	Int,
shipper_id	Int,
customer_id	Int,
product_id	Int,
employee_id	Int,
shipping_date	date
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/shipping_to_csv.csv", header true, dateFormat "dd/MM/yyyy")

-- COMMAND ----------

SELECT * FROM acme_raw.shipping;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create shipper table 

-- COMMAND ----------

DROP TABLE IF EXISTS acme_raw.shipper;
CREATE TABLE IF NOT EXISTS acme_raw.shipper(
shipper_id	Int,
company_shipper	String
)
USING CSV
OPTIONS (path "/mnt/getcase1dl/raw/shipper_to_csv.csv", header true)

-- COMMAND ----------

SELECT * FROM acme_raw.shipper;

-- COMMAND ----------

