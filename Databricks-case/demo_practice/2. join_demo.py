# Databricks notebook source
# MAGIC %md
# MAGIC # Joining orders dataframe and shipping dataframe

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to read both Dataframes to our demo DataFrames in order to be activated for JOIN and other transformations

# COMMAND ----------

orders_demo_df = spark.read.parquet("/mnt/getcase1dl/processed/orders").where("quantity BETWEEN 15 AND 35")

# COMMAND ----------

shipping_demo_df = spark.read.parquet("/mnt/getcase1dl/processed/shipping").where("customer_id = 9")

# COMMAND ----------

display(orders_demo_df)

# COMMAND ----------

display(shipping_demo_df)

# COMMAND ----------

order_shipping_df = orders_demo_df.join(shipping_demo_df, orders_demo_df.order_id == shipping_demo_df.order_id, "inner").select(orders_demo_df.order_id, orders_demo_df.quantity, shipping_demo_df.customer_id, shipping_demo_df.shipping_date)

# COMMAND ----------

display(order_shipping_df)

# COMMAND ----------

order_shipping_df.select("customer_id", "shipping_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Joining "product" dataframe and "orders" dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 	racunanje marže/razlike u prodajnoj i nabavnoj ceni proizvoda (join Product i Orders) 
# MAGIC

# COMMAND ----------

orders_demo_df = spark.read.parquet("/mnt/getcase1dl/processed/orders").filter("order_id BETWEEN 10300 AND 10340")

# COMMAND ----------

display(orders_demo_df)

# COMMAND ----------

products_demo_df = spark.read.parquet("/mnt/getcase1dl/processed/product")

# COMMAND ----------

display(products_demo_df)

# COMMAND ----------

orders_products_df = orders_demo_df.join(products_demo_df, orders_demo_df.product_id == products_demo_df.product_id, "inner").select(products_demo_df.product_id.alias("product_id"),"order_id", "product_name", products_demo_df.unit_price.alias("product_unit_price"), "unit_cost", "margin_value", products_demo_df.expected_revenue.alias("expected_revenue"),"sales")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

orders_products_df = orders_demo_df.join(products_demo_df, orders_demo_df.product_id == products_demo_df.product_id, "inner")

# COMMAND ----------

display(orders_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC  !!!! OTVORENO  :   ovo do sad ucitati u datalake parquet format !!! 

# COMMAND ----------

orders_products_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/orders_products_joined")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/getcase1dl/processed

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

category_df = spark.read.parquet(f"{processed_folder_path}/category").withColumnRenamed("ingestion_date", "ca_ingestion_date")

# COMMAND ----------

customer_df = spark.read.parquet(f"{processed_folder_path}/customer").withColumnRenamed("ingestion_date", "cu_ingestion_date")

# COMMAND ----------

division_df = spark.read.parquet(f"{processed_folder_path}/division").withColumnRenamed("ingestion_date", "di_ingestion_date")

# COMMAND ----------

orderDetails_df = spark.read.parquet(f"{processed_folder_path}/orderDetails").withColumnRenamed("customer_id", "customer_nr").withColumnRenamed("ingestion_date", "od_ingestion_date").withColumnRenamed("order_id", "od_order_nr")

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

shipping_df = spark.read.parquet(f"{processed_folder_path}/shipping").withColumnRenamed("ingestion_date", "sh_ingestion_date").withColumnRenamed("product_id", "sh_product_id").withColumnRenamed("order_id", "sh_order_id").withColumnRenamed("shipper_id", "sh_shipper_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Join customer to division

# COMMAND ----------

customer_division_df = customer_df.join(division_df, customer_df.division_id == division_df.division_id, "inner")

# COMMAND ----------

display(customer_division_df)

# COMMAND ----------

#next we are selecting only the columns we need to have presented 
customer_division_df = customer_df.join(division_df, customer_df.division_id == division_df.division_id, "inner").select(customer_df.customer_id, customer_df.company_name, division_df.division_id, division_df.division_name)
display(customer_division_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Joining multiple tables structure 

# COMMAND ----------

# tabela Product sadrzi (units_on_order) koristimo kao metriku za racunanje prometa 

# Product tbl koju join-ujemo sa Orders  
# Orders tbl join-ujemo sa OrderDetails
# OrderDetails tbl join-ujemo sa Customer

# COMMAND ----------

# first join of tables = orders_df + product_df

# COMMAND ----------

#order+product
orders_product_df = orders_df.join(product_df, orders_df.o_product_id == product_df.product_id, "inner")

# COMMAND ----------

test_2_df = orders_product_df.select("o_order_nr", "quantity", "product_name", "unit_price")

# COMMAND ----------

display(test_2_df)

# COMMAND ----------

# orderDetails_df  + orders_df
#                  + customer_df
interim_df = orderDetails_df.join(orders_df, orderDetails_df.order_id == orders_df.order_id, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")

# COMMAND ----------

display(interim_df)

# COMMAND ----------

new_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")\
                                .join(orders_product_df, orders_product_df.order_id == orderDetails_df.od_order_nr, "inner")

# COMMAND ----------

# df with 4 joined tables !
test_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")\
                                .join(product_df, product_df.product_id == orders_df.o_product_id, "inner")

# COMMAND ----------

display(test_df)

# COMMAND ----------

df_with_drop_ing_date = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")\
                                .join(orders_product_df, orders_product_df.o_order_nr == orderDetails_df.od_order_nr, "inner").drop("ingestion_date")

# COMMAND ----------

# second group of tables = orderDetails_df + orders_df + customer_df
second_join_of_tables_df = orderDetails_df.join(orders_df, orderDetails_df.od_order_nr == orders_df.o_order_nr, "inner")\
                            .join(customer_df, orderDetails_df.customer_nr == customer_df.customer_id, "inner")

# COMMAND ----------

test_1_df = second_join_of_tables_df.select("o_order_nr", "order_date", "customer_id")

# COMMAND ----------

display(test_1_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#order_id and quantity columns were ambigious so i let it be without them
final_df = new_df.select(product_df.product_id.alias("p_id"), product_df.product_name, product_df.category_id, product_df.unit_cost, product_df.unit_price.alias("unit_price"), product_df.margin_value, orderDetails_df.order_date, product_df.ordered_amount, product_df.expected_revenue, customer_df.customer_id.alias("customer_id"), customer_df.company_name, customer_df.division_id).withColumn("created_at", current_timestamp()).where("ordered_amount > 0")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# upisivanje u Datalake presentation layer
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/cust_growth_followup") 

# COMMAND ----------

# upisaivane test_df u pesentation layer 
test_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/cust_growth_followup_test")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup_test"))

# COMMAND ----------

