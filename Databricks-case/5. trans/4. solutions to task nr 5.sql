-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Task 5 :
-- MAGIC - pracenje korisnikovog rasta (prometa i kolicine koji narucuje po godinama)
-- MAGIC - Customer tbl , ako nam budu trebali podaci o korisniku (division, customer_id, company_name) 
-- MAGIC - Orders tabela koristi metriku za kolicinu (quantity) i cenu jedinice (unit_price)
-- MAGIC - OrdersDetails -> poseduje partitionBy("OrderDate") => cime smo kreirali kolonu 'fiscal_year', 
-- MAGIC                 -> raditi partitionBy("OrderDate") = svedeno po mesecima/godinama, videti krivu potraznje u vremenu

-- COMMAND ----------

-- pracenje korisnikovog rasta (prometa i kolicine koji narucuje po godinama)(e.g., Year-To-Date vs. Last Year-To-Date)  	
-- uradio partitionBy("column_name") = fiscal_year, kako bismo dobili po folderima dostribuirane podatke 
-- cime mozemo lakse da izvrsimo potrebne analize!!!
-- tabele orderDetails i Shipping , po koloni order_date tj. shipping_date 

-- COMMAND ----------

USE acme_presentation

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #pravimo novi df na osnovu "orders_orderDetails_customer"  ADLS foldera
-- MAGIC task_5_df = spark.read.parquet(f"{presentation_folder_path}/orders_orderDetails_customer")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC task_5_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_5_table_python")

-- COMMAND ----------

-- kreiramo tabelu putem DDL sintakse 
CREATE TABLE task_5_table_sql 
AS 
SELECT * 
FROM acme_presentation.task_5_table_python;

-- COMMAND ----------

SELECT * FROM task_5_table_sql;

-- COMMAND ----------

-- izracunavamo promet quantity*unit_cost i grupisemo po cusotmer_id
-- na raspolaganju sledece tabele
--  task_5_table_sql  , preko presentation foldera imamo join od 3 tabele 
--  task_5_proccesed_table , preko processed foldera / tabela Orders
-- orderDetails - customer_id , OrderDate (fiscal_year) 
-- Orders - quantity * unit_price per Order ID 
-- Customer - cusotmer id or Name

# a) pravimo prostu analizu sa kreiranejm VIEWs na razlicite godine 
#  napravimo jednu view tabelu za 2010. a drugu za 2011. god 
# radimo INNER JOIN v_2012 + v_2011  
# alternativno mozemo preko orderDetails tabele iz processed layera, partitioned by 'fiscal_year' koloni

# b) koristimo CTE ("with table_name") , ytd formulu i window function

-- COMMAND ----------

CREATE OR REPLACE VIEW v_2010_orders 
AS
SELECT * FROM task_5_table_sql
WHERE fiscal_year = 2010;

-- COMMAND ----------

CREATE OR REPLACE VIEW v_2011_orders 
AS
SELECT * FROM task_5_table_sql
WHERE fiscal_year = 2011;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC predlog koji sam dobio od Intellisense assistenta,  preko DDL da pisemo: 
-- MAGIC - CREATE TABLE acme_presentation.od_part2011_table USING parquet AS SELECT * FROM od_part2011_df

-- COMMAND ----------

VIEW_2010 JOIN VIEW_2011 ON condition , i prikazemo cusotmer_id , SUM(promet) GROUP BY customer_id
VIEW_2010 : Orders+ OrderDetails_part.2010 + Customer , da dobijemo promet (quantity * unit_price) po customer_id 
VIEW_2011 : Orders+ OrderDetails_part.2011 + Customer , da dobijemo promet (quantity * unit_price) po cusomer_id

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_2010_data
AS
SELECT c.customer_id, o.quantity, o.unit_price, ROUND(SUM(quantity * unit_price),2) as total_sales_2010
FROM acme_presentation.customer_table c
JOIN acme_presentation.od_part2010_table od ON c.customer_id = od.customer_id
JOIN acme_presentation.orders_table o ON  od.order_id = o.order_id
GROUP BY c.customer_id, o.quantity, o.unit_price
ORDER BY total_sales_2010 DESC;

-- COMMAND ----------

SELECT COUNT(DISTINCT customer_id) FROM v_2010_data

-- COMMAND ----------

select * from v_2010_data

-- COMMAND ----------

select  customer_id, ROUND(SUM(quantity * unit_price),2) as total_sales_per_customer_2010
from v_2010_data
group by customer_id
order by total_sales_per_customer_2010 DESC
--  ovo smeštamo u CTAS statement !

-- COMMAND ----------

DROP TABLE IF EXISTS ctas_1

-- COMMAND ----------

CREATE TABLE ctas_1 
AS
select  customer_id, ROUND(SUM(quantity * unit_price),2) as total_sales_per_customer_2010
from v_2010_data
group by customer_id
order by total_sales_per_customer_2010 DESC;

-- COMMAND ----------

select * from ctas_1 limit 10

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_2011_data
AS
SELECT c.customer_id, o.quantity, o.unit_price, SUM(ROUND(quantity * unit_price, 2)) as total_sales_2011
FROM acme_presentation.customer_table c
JOIN acme_presentation.od_part2011_table od ON c.customer_id = od.customer_id
JOIN acme_presentation.orders_table o ON  od.order_id = o.order_id
GROUP BY c.customer_id, o.quantity, o.unit_price
ORDER BY total_sales_2011 DESC;

-- COMMAND ----------

select  customer_id, SUM(ROUND(quantity * unit_price, 2)) as total_sales_per_customer_2011
from v_2011_data
group by customer_id
order by total_sales_per_customer_2011 DESC
--  ovo smeštam u drugi CTAS statement  , i zatim pravim  JOIN te dve nove tabele 

-- COMMAND ----------

DROP TABLE ctas_2

-- COMMAND ----------

CREATE TABLE ctas_2
AS
select  customer_id, ROUND(SUM(quantity * unit_price),2) as total_sales_per_customer_2011
from v_2011_data
group by customer_id
order by total_sales_per_customer_2011 DESC;

-- COMMAND ----------

SELECT 
ctas_1.customer_id,
ROUND(abs(SUM(ctas_2.total_sales_per_customer_2011) - SUM(ctas_1.total_sales_per_customer_2010)),2) as difference,
ROUND(abs(SUM(ctas_2.total_sales_per_customer_2011) - SUM(ctas_1.total_sales_per_customer_2010)),2) / SUM(ctas_2.total_sales_per_customer_2011) * 100 as percentage_growth
FROM ctas_1 
JOIN ctas_2 ON ctas_1.customer_id = ctas_2.customer_id
GROUP BY ctas_1.customer_id
ORDER BY percentage_growth
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ZA KRAJ UCITATI REZULTUJUCI SET U NOVI FOLDER ILI NOVU TABELU UNUTAR PRESENTATION LAYERA!
-- MAGIC TAKO I ZA OSTALE TASKOVE URADITI 