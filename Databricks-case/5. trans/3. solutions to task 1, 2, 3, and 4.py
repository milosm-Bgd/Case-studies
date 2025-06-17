# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Providng solution to task assignment 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### GET _ task requirements 

# COMMAND ----------

#* Analyze sales figures and compare them year-over-year.
	* racunanje margine/razlike u nabavnoj i prodajnoj ceni (join Product i Orders, preko ProductID-a) 
	* analiza prodaje u prethodne dve godine (radi komparacije prema fiskalnim godinama) 
	* analiza prodaje po Customer / Division / Category (join OrderDetails and Customer, Divison and Customer, Category-Product + Product-Orders) 
	* racunanje prosecne prodaje po transakciji i po korisniku (join Orders I OrderDetails preko OrderID-a)
	* pracenje korisnikovog rasta (prometa i kolicine koji narucuje po godinama)(e.g., Year-To-Date vs. Last Year-To-Date)  
		(join Customer I OrderDetails preko CustomerID-a)
		tabela Product (units_on_order) koristiti metriku za racunanje prometa , koji joinujemo + tabela Orders  
											AND Orders + OrderDetails
											AND OrderDetails + Customer
		-> hint: raditi partitionBy("column_name") =fiscal_year, kako bismo dobili po folderima distribuirane podatke 
			cime mozemo lakse da izvrsimo potrebne analize!!!
			-> videti lekciju 64 - Races File Partitioning
			-> tabele orderDetails i SHipping sam particionisao po order_date tj. shipping_date 
			   - > OTVOENO potrebno je napraviti Window rank() funkciju na dataframe-u gde stavimo godinu u partitionBy("fiscal_year")   



# COMMAND ----------

# MAGIC %md
# MAGIC 1.  racunanje marže/razlike u prodajnoj i nabavnoj ceni proizvoda  
# MAGIC       *  (join Product i Orders)
# MAGIC 2.   analiza prodaje u prethodne dve godine (komparacija prema fiskalnim godinama)
# MAGIC       * top trading country and most valuable trading partners 
# MAGIC       * most selling product
# MAGIC       * top 5 selling products
# MAGIC 3. analiza prodaje po Customer / Division / Category
# MAGIC       * join OrderDetails and Customer, 
# MAGIC       * join Divison and Customer, 
# MAGIC       * join Category-Product + Product-Orders
# MAGIC             koristeci GroupBy klauzulu! 
# MAGIC 4. racunanje prosecne prodaje po transakciji i po korisniku (join Orders i OrderDetails preko OrderID-a)
# MAGIC 5. pracenje korisnikovog rasta (prometa i kolicine koji narucuje po godinama)
# MAGIC 	* Product tabela (units_on_order) koristiti metriku za racunanje prometa sa sledecim spajanjem: 
# MAGIC         - Product join sa Orders tabelom (via "product_id")
# MAGIC         - Orders join sa OrderDetails tabelom (via "order_id")
# MAGIC         - OrderDetails join sa Customer tbl (via "customer_id")
# MAGIC       -> raditi partitionBy("OrderDate") = fiscal_year, kako bismo dobili po folderima distribuirane podatke po godinama
# MAGIC       -> raditi partitionBy("OrderDate") = svedeno po mesecima, videti krivu potraznje u toku godine
# MAGIC 			

# COMMAND ----------

# MAGIC %md
# MAGIC Task 1: racunanje marže/razlike u prodajnoj i nabavnoj ceni proizvoda 
# MAGIC - join tbl Orders to tbl Product 

# COMMAND ----------

task_1_df = spark.read.parquet(f"{presentation_folder_path}/orders_product_joined")

# COMMAND ----------

task_1_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_1_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE acme_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from task_1_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC --racunanje marže/razlike u prodajnoj i nabavnoj ceni proizvoda
# MAGIC SELECT 
# MAGIC   product_id, 
# MAGIC   product_name,  
# MAGIC   avg(margin_value)
# MAGIC FROM task_1_python
# MAGIC GROUP BY product_id, product_name
# MAGIC ORDER BY min(margin_value) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Task2 : analiza prodaje u odnosu na ponudjene godine 
# MAGIC (komparacija prema fiskalnim godinama)  
# MAGIC
# MAGIC -- pogledati poslednja 2 Cells : Cell 23 i Cell 24 (window funkcije)!
# MAGIC -- top trading country and most valuable trading partners
# MAGIC
# MAGIC * I join Orders, OrderDetails, Customer, Product 
# MAGIC * II join po Shipping, Orders i Product  ( na mesecnom / godisnjem preseku merenje obima prodaje)

# COMMAND ----------

task_2_df = spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup_all_cols")

# COMMAND ----------

task_2_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_2_python")

# COMMAND ----------

display(task_2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_year, country, SUM(expected_revenue) as expected_sales
# MAGIC FROM acme_presentation.task_2_python
# MAGIC WHERE fiscal_year IN (2011,2012)
# MAGIC GROUP BY fiscal_year, country
# MAGIC ORDER BY expected_sales desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --izlistaj mi najvece partnere iz 2011 god. i zemlje kojoj pripadaju po ocekivnom prometu i velicini porudzbenice
# MAGIC SELECT fiscal_year, country, company_name,  SUM(expected_revenue) as expected_sales , SUM(ordered_amount) as ordered_sales
# MAGIC FROM acme_presentation.task_2_python
# MAGIC WHERE fiscal_year = 2011
# MAGIC GROUP BY fiscal_year, country, company_name
# MAGIC ORDER BY expected_sales desc
# MAGIC LIMIT 5; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --izlistaj mi najvece partnere po velicini porudzbenice iz 2011.
# MAGIC SELECT fiscal_year, company_name, SUM(ordered_amount) as ordered_sales
# MAGIC FROM acme_presentation.task_2_python
# MAGIC WHERE fiscal_year = 2011
# MAGIC GROUP BY fiscal_year, company_name
# MAGIC ORDER BY ordered_sales desc
# MAGIC LIMIT 5; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- izlistaj mi najvece partnere po ocekivnom prometu i velicini porudzbenice sa godisnjim pregledom
# MAGIC -- vidi sledecu celiju sa window funkcijom 
# MAGIC SELECT fiscal_year, company_name,  SUM(expected_revenue) as expected_sales , SUM(ordered_amount) as ordered_sales
# MAGIC FROM acme_presentation.task_2_python
# MAGIC GROUP BY company_name, fiscal_year
# MAGIC ORDER BY expected_sales desc, ordered_sales desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- window rank() prethodni primer 
# MAGIC
# MAGIC SELECT fiscal_year, company_name, 
# MAGIC       SUM(expected_revenue) as expected_sales, 
# MAGIC       rank() OVER (PARTITION BY fiscal_year ORDER BY SUM(expected_revenue) desc) as rank_col
# MAGIC FROM acme_presentation.task_2_python
# MAGIC GROUP BY company_name ,fiscal_year
# MAGIC ORDER BY fiscal_year DESC; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Koristeci CTE ,vratiti prvih 5 poslovnih partnera po godinama sa apsekta ocekivane prodaje
# MAGIC
# MAGIC WITH MyTable AS 
# MAGIC (
# MAGIC SELECT fiscal_year, company_name, 
# MAGIC       SUM(expected_revenue) as expected_sales, 
# MAGIC       rank() OVER (PARTITION BY fiscal_year ORDER BY SUM(expected_revenue) desc) as rank_col
# MAGIC FROM acme_presentation.task_2_python
# MAGIC GROUP BY company_name ,fiscal_year
# MAGIC )
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM MyTable
# MAGIC WHERE rank_col <=5
# MAGIC ORDER BY fiscal_year desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Vratiti prva 3 poslovna partnera po godinama sa apsekta narucene vrednosti
# MAGIC
# MAGIC with mycteTable as 
# MAGIC ( SELECT fiscal_year, company_name, SUM(ordered_amount) as total_ordered_amount,
# MAGIC     rank() OVER (PARTITION BY fiscal_year ORDER BY SUM(ordered_amount) desc) as ranked_col
# MAGIC FROM acme_presentation.task_2_python
# MAGIC GROUP BY company_name ,fiscal_year) 
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM mycteTable 
# MAGIC WHERE ranked_col <= 3
# MAGIC ORDER BY fiscal_year desc;

# COMMAND ----------

# MAGIC %md
# MAGIC Task 3 : analiza prodaje po 
# MAGIC
# MAGIC - Customer / Division / Category
# MAGIC - join OrderDetails and Customer
# MAGIC - join Divison and Customer
# MAGIC - join  Category-Product +  Product-Orders  
# MAGIC - koristiti GroupBy 

# COMMAND ----------

# koristimo ADLE folder 'cust_growth_followup' za prikazivanje po diviziji i kategoriji proizvoda
task_3_df = spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup")

# COMMAND ----------

task_3_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_3_python")

# COMMAND ----------

# kopiramo prethodnu celiju i dodajemo _all_cols folder umesto 
# koristimo ADLE folder 'cust_growth_followup_all_cols' za prikazivanje po diviziji i kategoriji proizvoda
task_3_df = spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup_all_cols")

# COMMAND ----------

task_3_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_3a_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED acme_presentation.task_3a_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM acme_presentation.task_3a_python;
# MAGIC --ovde imamo 17000 naspram 3500 redova ,ukoliko ne ukljucimo folder sa all_cols

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acme_presentation.task_3a_python LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category_id, division_id , ROUND(AVG(unit_cost),2) AS average_cost_per_unit
# MAGIC FROM acme_presentation.task_3_python
# MAGIC GROUP BY category_id, division_id
# MAGIC ORDER BY average_cost_per_unit DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --pronaci diviziju koja najvise narucuje
# MAGIC
# MAGIC SELECT division_id, sum(ordered_amount) AS sum_of_ordered_values
# MAGIC FROM acme_presentation.task_3_python
# MAGIC GROUP BY division_id
# MAGIC ORDER BY sum_of_ordered_values DESC, division_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --pronaci divizije koje najvise narucuju po godinama
# MAGIC -- quantity * unit_cost = revenue / where fiscal_year = 2012
# MAGIC -- IDEJA, vrati division_name kao kolonu!!!!
# MAGIC
# MAGIC SELECT b.division_id, division_name, country , sum(quantity*unit_cost) AS sum_of_sales, fiscal_year
# MAGIC FROM acme_presentation.task_3a_python a
# MAGIC JOIN acme_presentation.task_3_div_python b ON b.division_id = a.division_id
# MAGIC WHERE fiscal_year = 2012
# MAGIC GROUP BY b.division_id , division_name, country, fiscal_year
# MAGIC ORDER BY sum_of_sales DESC, b.division_id;

# COMMAND ----------

#da bi uveli naziv divizije moramo da uvezemo prvo iz processed layera divison tabelu u presentation layer 
# pa na osnovu join-a sa task_3a_python tabelom da prikazemo trazene podatke po diviziji i kategoriji
task_3_division_df = spark.read.parquet(f"{processed_folder_path}/division")

# COMMAND ----------

task_3_division_df.write.mode("overwrite").format("parquet").saveAsTable("acme_presentation.task_3_div_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC --pronaci kupce sa njhovim obimom prometa (sum of sales) po određenoj godini i napraviti rang listu sa WIndow funkcijama 
# MAGIC -- quantity * unit_cost = revenue / where fiscal_year = 2012
# MAGIC -- NAPRAVITI CTE expression!  i ograniciti TheTRank <= 3 
# MAGIC
# MAGIC WITH RankCTETable 
# MAGIC AS 
# MAGIC (SELECT fiscal_year, 
# MAGIC company_name,
# MAGIC sum(quantity*unit_cost) AS sum_of_sales,
# MAGIC rank() OVER(PARTITION BY fiscal_year ORDER BY sum(quantity*unit_cost) DESC) AS TheRank
# MAGIC FROM acme_presentation.task_3a_python
# MAGIC GROUP BY company_name, fiscal_year)
# MAGIC
# MAGIC SELECT * FROM RankCTETable
# MAGIC WHERE TheRank <=5
# MAGIC ORDER BY fiscal_year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --pronaci 5 klijenata sa najvecim sumama marzi njihovog portfolija ( margin_value = unit_price - unit_cost ) 
# MAGIC -- po određenoj godini i napraviti rang lisu sa WIndow funkcijama 
# MAGIC
# MAGIC WITH RankCTETable 
# MAGIC AS 
# MAGIC (SELECT fiscal_year, 
# MAGIC company_name,
# MAGIC sum(unit_price - unit_cost) AS sum_of_margin,
# MAGIC rank() OVER(PARTITION BY fiscal_year ORDER BY sum(unit_price - unit_cost) DESC) AS MarginRank
# MAGIC FROM acme_presentation.task_3a_python
# MAGIC GROUP BY company_name, fiscal_year)
# MAGIC
# MAGIC SELECT * FROM RankCTETable
# MAGIC WHERE MarginRank <=5
# MAGIC ORDER BY fiscal_year DESC;
# MAGIC --ORDER BY sum_of_margin DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- napraviti sledecu opciju racunanja dobiti po korisniku , tj prikazati top 10 korisnika 
# MAGIC -- sa aspekta dobiti koju prave  
# MAGIC
# MAGIC -- kalkulacija ide  :  quantity * (unit_price - unit_cost)'
# MAGIC
# MAGIC SELECT
# MAGIC   company_name, SUM(quantity * (unit_price - unit_cost)) as sum_of_margin
# MAGIC FROM acme_presentation.task_2_python
# MAGIC WHERE fiscal_year = 2011
# MAGIC GROUP BY company_name
# MAGIC ORDER BY sum_of_margin DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Task 4 :  
# MAGIC - racunanje prosecne prodaje po transakciji i po korisniku 
# MAGIC - task_5_table_sql , iz 'orders_orderDetails_customer' foldera
# MAGIC *  koristimo (task_3_python) tabelu iz prethodnog taska  

# COMMAND ----------

# Average sales per transaction/customer = total sales /number of transactions (order_id) 
# and average sales per customer = total sales /number of customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --table OrderDetails calculating the distinct number of order_id 
# MAGIC SELECT COUNT(DISTINCT order_id) FROM acme_processed.orderDetails;

# COMMAND ----------

# MAGIC %sql
# MAGIC --table Orders calculating distinct number of order_id
# MAGIC SELECT COUNT(DISTINCT order_id) FROM acme_processed.orders;

# COMMAND ----------

spark.sql('SELECT current_schema()').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE acme_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average Sales per Transaction = Total Sales / Number of Transactions
# MAGIC -- Total Sales =  SUM(quantity*unit_price) , we retrieve from table Product 
# MAGIC -- Number of Transactions = COUNT(DISTINCT order_id) , we retrieve from table Orders/OrderDetails 
# MAGIC SELECT 
# MAGIC     customer_id, company_name,
# MAGIC     ROUND(SUM(quantity*o_unit_price) / COUNT(DISTINCT o_order_nr),2) AS average_sales_per_transaction
# MAGIC FROM task_5_table_sql
# MAGIC GROUP BY customer_id, company_name
# MAGIC ORDER BY average_sales_per_transaction DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Average sales per Customer = Total sales / Number of unique customers
# MAGIC -- Total sales = SUM(quantity * unit_price) 
# MAGIC -- Number of unique customers = count(distinct customer_id) 
# MAGIC SELECT 
# MAGIC   customer_id, company_name,
# MAGIC   ROUND(SUM(quantity*o_unit_price) / COUNT(DISTINCT customer_id),2) AS average_sales_per_customer
# MAGIC FROM task_5_table_sql
# MAGIC GROUP BY customer_id, company_name
# MAGIC ORDER BY customer_id, company_name

# COMMAND ----------

# MAGIC %md
# MAGIC ucitati rezultujuce tabele u novi presentation folder npr. "Solutions" i napraviti finalne tabele spremne za query 