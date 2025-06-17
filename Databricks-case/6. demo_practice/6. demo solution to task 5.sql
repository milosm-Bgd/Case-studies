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
			-- tabele orderDetails i SHipping sam particionisao po order_date tj. shipping_date 

-- COMMAND ----------

USE acme_presentation

-- COMMAND ----------

select current_database()

-- COMMAND ----------

# pravimo novi df na osnovu "orders_orderDetails_customer" joina iz ADLS foldera: orders_orderDetails_customer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC task_5_df = spark.read.parquet(f"{presentation_folder_path}/orders_orderDetails_customer")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC task_5_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_5_table_python")

-- COMMAND ----------

--preskacemo Cell 
task_5_df = spark.read.parquet(f"{processed_folder_path}/orders")

-- COMMAND ----------

-- preskacemo Cell
task_5_df.write.mode("overwrite").format("parquet").saveAsTable("acme_processed.task_5_processed_table")

-- COMMAND ----------

--preskacemo Cell
%sql
--reference to Cell 10
SELECT COUNT(*) FROM acme_processed.task_5_processed_table;

-- COMMAND ----------

--preskacemo Cell
%sql
SELECT COUNT(DISTINCT order_id) FROM acme_processed.task_5_processed_table;

-- COMMAND ----------

-- kreiramo tabelu putem DDL sintakse 
CREATE TABLE task_5_table_sql 
AS 
SELECT * 
FROM acme_presentation.task_5_table_python;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SELECT * FROM task_5_table_sql;

-- COMMAND ----------

-- izracunavamo promet quantity*unit_cost i grupisemo po cusotmer_id
-- na raspolaganju sledece tabele
--  task_5_table_sql  , preko presentation foldera imamo join od 3 tabele 
--  task_5_proccesed_table , preko processed foldera / tabela orders
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

SELECT fiscal_year, customer_id , company_name, SUM(quantity * o_unit_price) as ostvarena_prodaja_po_korisniku,
RANK() OVER(PARTITION BY fiscal_year ORDER BY SUM(quantity * o_unit_price) DESC) AS TheRank
FROM v_2011_orders 
GROUP BY fiscal_year, customer_id, company_name
ORDER BY ostvarena_prodaja_po_korisniku DESC

-- COMMAND ----------

SELECT fiscal_year, customer_id , company_name, SUM(quantity * o_unit_price) as ostvarena_prodaja_po_korisniku 
FROM v_2010_orders
GROUP BY fiscal_year, customer_id, company_name
ORDER BY ostvarena_prodaja_po_korisniku DESC
LIMIT 10;

-- COMMAND ----------

SELECT fiscal_year, customer_id , company_name, SUM(quantity * o_unit_price) as ostvarena_prodaja_po_korisniku,
RANK() OVER(PARTITION BY fiscal_year ORDER BY SUM(quantity * o_unit_price) DESC) AS TheRank
FROM v_2010_orders 
GROUP BY fiscal_year, customer_id, company_name
ORDER BY ostvarena_prodaja_po_korisniku DESC

-- COMMAND ----------

CREATE OR REPLACE VIEW v_2010and2011_orders 
AS
SELECT * FROM task_5_table_sql
WHERE fiscal_year IN (2010,2011);

-- COMMAND ----------

SELECT fiscal_year, customer_id , company_name, SUM(quantity * o_unit_price) as ostvarena_prodaja_po_korisniku,
RANK() OVER(PARTITION BY fiscal_year ORDER BY SUM(quantity * o_unit_price) DESC) AS TheRank
FROM v_2010and2011_orders 
GROUP BY fiscal_year, customer_id, company_name
ORDER BY ostvarena_prodaja_po_korisniku DESC

-- COMMAND ----------

-- pravimo join dva view-a za analizu podataka iz 2010 i 2011

SELECT fiscal_year, customer_id , company_name, SUM(quantity * o_unit_price) as ostvarena_prodaja_po_korisniku,
FROM v_2010_orders v10 
INNER JOIN v_2011_orders v11
ON v10.customer_id = v11.customer_id
GROUP BY fiscal_year, customer_id, company_name
ORDER BY ostvarena_prodaja_po_korisniku DESC

-- COMMAND ----------

pravimo novu tabelu gde imamo sledece kolone 
customer_id, company_name, total_sales_2010 (quantity * o_unit_price), total_sales_2011, growth_in_sales(abs) 
pre nego kreiramo view , pravimo u presentation layeru tabelu koju ucitavamo iz "orderOrderDetailsCusomter" foldera 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC od_part2010_df = spark.read.parquet("/mnt/getcase1dl/processed/orderdetails_partitioned/fiscal_year=2010")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC od_part2010_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.od_part2010_table")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC od_part2011_df = spark.read.parquet("/mnt/getcase1dl/processed/orderdetails_partitioned/fiscal_year=2011")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC od_part2011_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.od_part2011_table")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_df = spark.read.parquet("/mnt/getcase1dl/processed/orders")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.orders_table")

-- COMMAND ----------

select * from acme_presentation.orders_table where order_id = 10248

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_df = spark.read.parquet("/mnt/getcase1dl/processed/customer")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.customer_table")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC predlog koji sam dobio od Intellisense assistenta,  preko DDL da pisemo: 
-- MAGIC - CREATE TABLE acme_presentation.od_part2011_table USING parquet AS SELECT * FROM od_part2011_df

-- COMMAND ----------

SELECT * FROM acme_presentation.od_part2010_table 

-- COMMAND ----------

SELECT * FROM acme_presentation.od_part2011_table

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use acme_presentation
-- alternativno pozivamo 'use acme_processed'

-- COMMAND ----------

VIEW_2010 JOIN VIEW_2011 ON condition , i prikayemo cusotmer_id , SUM(promet) GROUP BY customer_id
VIEW_2010 : Orders+ OrderDetails 2010 + Customer , da dobijemo promet (quantity * unit_price) po customer_id 
VIEW_2011 : Orders+ OrderDetails 2011 + Customer , da dobijemo promet (quantity * unit_price) po Cusomer_id

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_2010_data
AS
SELECT c.customer_id, o.quantity, o.unit_price, SUM(ROUND(quantity * unit_price, 2)) as total_sales_2010
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
-- pokusati ovo smestiit u CTAS statement

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
-- pokusati ovo smestiit u drugi CTAS statement  , napraviti JOIN dve nove tabele 

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
ROUND(abs(SUM(ctas_2.total_sales_per_customer_2011) - SUM(ctas_1.total_sales_per_customer_2010)),2)/SUM(ctas_2.total_sales_per_customer_2011)*100 as percentage_growth
FROM ctas_1 
JOIN ctas_2 ON ctas_1.customer_id = ctas_2.customer_id
GROUP BY ctas_1.customer_id
ORDER BY percentage_growth;

-- COMMAND ----------

SELECT v2010.customer_id,
      ROUND(SUM(v2010.quantity * v2010.unit_price),2) as total_sales_2010, 
      ROUND(SUM(v2011.quantity * v2011.unit_price),2) as total_sales_2011,
      ROUND(abs(SUM(v2011.quantity * v2011.unit_price) - SUM(v2010.quantity * v2010.unit_price)),2) as difference
FROM v_2010_data v2010
JOIN v_2011_data v2011 ON v2010.customer_id = v2011.customer_id
GROUP BY v2010.customer_id
ORDER BY difference DESC

-- COMMAND ----------

ideja> paritionby fical_year iz orderDetials za 2010 , isto to za 2011 
kreiramo dve raylicite tabele za svaku godinu  / dve razlicite particije 
joinujemo te dve tabele i tabelu Orders (quantity*unit price) gde cemo imati 2 kolone,  1 za total_sales iz 2010 i durga iz 2011 god.  

-- COMMAND ----------

SELECT * 
FROM acme_presentation.task_5_table_python
WHERE fiscal_year IN (2010, 2011)

-- COMMAND ----------

CREATE TABLE task_5a_table_sql 
AS 
SELECT * 
FROM acme_presentation.task_3a_python

-- COMMAND ----------

SELECT * FROM task_5a_table_sql WHERE product_name = 'Runner Shoes'

-- COMMAND ----------

SELECT  DATE_TRUNC('MONTH', order_date):: date
FROM task_5a_table_sql

-- COMMAND ----------

USE acme_presentation

-- COMMAND ----------

-- primer sa YTD racunicom
-- ytd_growth_rate = (current_results - last_year_result) / last_year_results * 100

WITH 
monthly_revenue AS (
    SELECT 
        customer_id,
        MIN(DATE_TRUNC('MONTH', order_date):: DATE) AS month,
        --DATE_TRUNC('YEAR', order_date):: DATE AS year,
        SUM(expected_revenue) AS tracking_revenue
    FROM task_5a_table_sql 
    WHERE order_date >= DATE_TRUNC('year', order_date):: DATE
    GROUP BY customer_id--, --month --year
    --GROUP BY DATE_TRUNC('month', order_date):: DATE
),
ytd_revenue AS (
    SELECT 
        customer_id,
        month,
        SUM(tracking_revenue) OVER (ORDER BY month) AS ytd_revenue
    FROM monthly_revenue
)

SELECT 
    customer_id,
    --month,
    ytd_revenue,
    LAG(ytd_revenue) OVER (ORDER BY month) AS previous_ytd_revenue,
    ROUND(CASE 
        WHEN LAG(ytd_revenue) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (ytd_revenue - LAG(ytd_revenue) OVER (ORDER BY month)):: float / LAG(ytd_revenue) OVER (ORDER BY month) * 100
    END,2) AS growth_percentage
FROM 
    ytd_revenue
ORDER BY 
    month;


-- COMMAND ----------

    SELECT DATE_TRUNC('month', order_date):: date AS month,
    expected_revenue AS tracking_revenue
    FROM task_5a_table_sql 
    WHERE order_date < DATE_TRUNC('YEAR', CURRENT_DATE())
    --GROUP BY DATE_TRUNC('month', order_date):: date

-- COMMAND ----------

SELECT DATE_TRUNC('YEAR', CURRENT_DATE())::DATE

-- COMMAND ----------

SELECT current_date()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ZA KRAJ UCITATI REZULTUJUCI SET U NOVI FOLDER ILI NOVU TABELU UNUTAR PRESENTATION LAYERA!
-- MAGIC TAKO I ZA OSTALE TASKOVE URADITI 