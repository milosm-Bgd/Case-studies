# Databricks notebook source
# MAGIC %md
# MAGIC ### Joining different dataframes 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to read both Dataframes to our demo DataFrames in order to be activated for JOIN and other transformations

# COMMAND ----------

order_shipping_df = orders_demo_df.join(shipping_demo_df, orders_demo_df.order_id == shipping_demo_df.order_id, "inner").select(orders_demo_df.order_id, orders_demo_df.quantity, shipping_demo_df.customer_id, shipping_demo_df.shipping_date, shipping_demo_df.sh_fiscal_year)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Joining "product" dataframe and "orders" dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 	racunanje marže/razlike u prodajnoj i nabavnoj ceni proizvoda (join Product i Orders) 
# MAGIC

# COMMAND ----------

prvi_orders_df = spark.read.parquet("/mnt/getcase1dl/processed/orders")\
    .withColumnRenamed("ingestion_date", "po_ing_d")\
            .withColumnRenamed("order_id", "o_order_nr")\
                .withColumnRenamed("product_id", "o_product_id")\
                    .withColumnRenamed("unit_price", "o_unit_price")

# COMMAND ----------

prvi_products_df = spark.read.parquet("/mnt/getcase1dl/processed/product")

# COMMAND ----------

orders_products_df = prvi_orders_df.join(prvi_products_df, prvi_orders_df.o_product_id == prvi_products_df.product_id, "inner")

# COMMAND ----------

orders_products_select_df = prvi_orders_df.join(prvi_products_df, prvi_orders_df.o_product_id == prvi_products_df.product_id, "inner").select(prvi_products_df.product_id.alias("product_id"),"o_order_nr", "product_name", prvi_products_df.unit_price.alias("product_unit_price"), "unit_cost", "margin_value", prvi_products_df.expected_revenue.alias("expected_revenue"),"ordered_amount")

# COMMAND ----------

display(orders_products_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC upisivanje df-a u datalake parquet format 

# COMMAND ----------

orders_products_df.write.mode("overwrite").parquet("/mnt/getcase1dl/presentation/orders_product_joined")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

category_df = spark.read.parquet(f"{processed_folder_path}/category").withColumnRenamed("ingestion_date", "ca_ingestion_date")

# COMMAND ----------

customer_df = spark.read.parquet(f"{processed_folder_path}/customer").withColumnRenamed("ingestion_date", "cu_ingestion_date")

# COMMAND ----------

division_df = spark.read.parquet(f"{processed_folder_path}/division").withColumnRenamed("ingestion_date", "di_ingestion_date")

# COMMAND ----------

orderDetails_df = spark.read.parquet(f"{processed_folder_path}/orderdetails_partitioned")\
        .withColumnRenamed("customer_id", "customer_nr")\
            .withColumnRenamed("ingestion_date", "od_ingestion_date")\
                .withColumnRenamed("order_id", "od_order_nr")

# COMMAND ----------

orders_df = spark.read.parquet(f"{processed_folder_path}/orders")\
        .withColumnRenamed("ingestion_date", "po_ing_d")\
            .withColumnRenamed("order_id", "o_order_nr")\
                .withColumnRenamed("product_id", "o_product_id")\
                    .withColumnRenamed("unit_price", "o_unit_price")

# COMMAND ----------

product_df = spark.read.parquet(f"{processed_folder_path}/product").withColumnRenamed("ingestion_date", "p_ingestion_date")

# COMMAND ----------

shipper_df = spark.read.parquet(f"{processed_folder_path}/shipper").withColumnRenamed("ingestion_date", "s_ingestion_date")

# COMMAND ----------

shipping_df = spark.read.parquet(f"{processed_folder_path}/shipping_partitioned").withColumnRenamed("ingestion_date", "sh_ingestion_date").withColumnRenamed("product_id", "sh_product_id").withColumnRenamed("order_id", "sh_order_id").withColumnRenamed("shipper_id", "sh_shipper_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Joining multiple tables structure 

# COMMAND ----------

# tabela Product sadrzi (units_on_order) koristimo kao metriku za racunanje prometa 

# Product tbl koju join-ujemo sa Orders  
# Orders tbl join-ujemo sa OrderDetails
# OrderDetails tbl join-ujemo sa Customer

# COMMAND ----------

# orderDetails_df  + orders_df
#                  + customer_df
interim_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")

# COMMAND ----------

display(interim_df)

# COMMAND ----------

interim_df.write.mode("overwrite").parquet("/mnt/getcase1dl/presentation/orders_orderDetails_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC Below starts with bigger structures and joined Dataframes

# COMMAND ----------

# FIRST group of joined dataframes : test_df with 4 joined df!
test_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")\
                                .join(product_df, product_df.product_id == orders_df.o_product_id, "inner")

# COMMAND ----------

display(test_df.filter("product_name = 'Tennis Suit'"))

# COMMAND ----------

# second group of tables = orderDetails_df + orders_df + customer_df
second_join_of_tables_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")

# COMMAND ----------

test_1_df = second_join_of_tables_df.select("o_order_nr", "order_date",  "o_product_id", "quantity", "o_unit_price", "customer_id", "fiscal_year")

# COMMAND ----------

display(test_1_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#order_id and quantity columns were ambigious so i let it be without them
final_df = test_df.select(product_df.product_id.alias("p_id"), product_df.product_name, product_df.category_id, product_df.unit_cost, product_df.unit_price.alias("unit_price"), product_df.margin_value, orderDetails_df.order_date,orderDetails_df.fiscal_year, orderDetails_df.od_order_nr, product_df.ordered_amount, product_df.expected_revenue, customer_df.customer_id.alias("customer_id"), customer_df.company_name, customer_df.division_id).withColumn("created_at", current_timestamp()).where("ordered_amount > 0")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# upisivanje u Datalake presentation layer
# final_df = orderDetails + orders + customer + product df (sa selektovanim kokolonama)
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/cust_growth_followup") 

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup"))

# COMMAND ----------

# upisaivane test_df u pesentation layer 
# test_df = ukljucuje 4 df, sa svim kolonama!
test_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/cust_growth_followup_all_cols")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup_test"))

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup_all_cols"))

# COMMAND ----------

