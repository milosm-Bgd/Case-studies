# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC Built-in aggregate functions
# MAGIC
# MAGIC DOcs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html

# COMMAND ----------

# MAGIC %md
# MAGIC Ispod navodim primer agregacije , (Simple Aggregate Finctions) sintaksa: demo_df.filter("column_name = 'some_value'").select(sum("column_name") [, [aggregationFunction("parameter="column_name"")]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC struktura pisnja aggregate funkcija :
# MAGIC agg_func() stavljamo argument po kojem zelimo da pravimo agregaciju , npr count("*")
# MAGIC to uokvirimo sa select()
# MAGIC na pvo mesto ide dataframe_name 
# MAGIC na kraju stavljamo show() method 

# COMMAND ----------

# dummy example 
# demo_df.filter("formula_1_driver_name='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

test_df = spark.read.parquet(f"{presentation_folder_path}/cust_growth_followup")

# COMMAND ----------

display(test_df)

# COMMAND ----------

demo_df = test_df.filter("p_id IN (45,46,47,48,49,50)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df.select(countDistinct("category_id")).show()

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, avg, max, min

# COMMAND ----------

test_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("product_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("product_name")).show()

# COMMAND ----------

test_df.select(countDistinct("product_name")).show()

# COMMAND ----------

demo_df.select(sum("expected_revenue")).show()

# COMMAND ----------

demo_df.filter("customer_id = 47").select(sum("expected_revenue")).show()

# COMMAND ----------

demo_df.filter("customer_id = 47 and order_date between '2009-01-01' and '2009-12-31'" ).select(sum("expected_revenue")).show()

# COMMAND ----------

#npr uzmemo po Category ID da izracunamo sumu prodaje / prometa (opciono ponuditi za period vremena ) 
# we are gonna use count(), sum() and countDistinct()
demo_df.filter("category_id=2").select(sum("expected_revenue")).show()


# COMMAND ----------

# po Category ID da izracunamo sumu dobiti (opciono ponuditi za period vremena )
test_df.filter("category_id=4").select(sum("margin_value")).show()

# COMMAND ----------

# ili po ProductName da izracunamo iznos (ordered_amount) koja se obrne za neki period vremema , po mesecima krivu dati 
demo_df.filter("product_name='Mr2 Trousers' and order_date between '2009-01-01' and '2009-06-30'").select(sum("ordered_amount")).withColumnRenamed("sum(ordered_amount)", "total_amount").show()


# COMMAND ----------

#  ili po ProductName da izracunamo prosecnu maržu za neki period vremema
demo_df.filter("product_name='Chantell Shirt'").select(avg("margin_value"))\
    .withColumnRenamed("avg(margin_value)", "avgerage margin").show()

# COMMAND ----------

demo_df.groupBy("fiscal_year").agg(sum("ordered_amount"))\
    .withColumnRenamed("sum(ordered_amount)", "total_ordered_value_in_USD").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, desc, asc

# COMMAND ----------

#na osnovu prethodnog primera napraviti Window funkciju koja izlistava po opadajucem redosledu proizvode u 
# odnosu na ukupnu narucenu vrednost i dodeljuje mu rang u posebnoj koloni

windowed_df = demo_df.groupBy("category_id")\
    .agg(sum("ordered_amount").alias("total ordered value"))

# COMMAND ----------

display(windowed_df)

# COMMAND ----------

#partitionBy staviti vremensku odrednicu (godinu) da bi rank() ili dense_rank() izbacio rang u koloni "rank" 
category_orderamount_spec = Window.partitionBy("category_id").orderBy(desc("total ordered value"))
windowed_df.withColumn("rank", rank().over(category_orderamount_spec)).show()

# COMMAND ----------

#drugi primer 
second_windowed_df = test_df.groupBy("p_id", "product_name")\
    .agg(sum("expected_revenue").alias("total expected revenue"))

# COMMAND ----------

display(second_windowed_df)

# COMMAND ----------

#partitionBy staviti vremensku odrednicu (godinu) da bi rank() ili dense_rank() izbacio rang u koloni "rank" 
window_spec = Window.partitionBy("p_id").orderBy(desc("total expected revenue"))
second_windowed_df.withColumn("rank", dense_rank().over(window_spec)).show()

# COMMAND ----------

#napraviti
#na mesecnom / godisnjem preseku obim po ProductID
JOIN Shipping I Orders  (by OrderID)

# na mesecnom / godisnjem preseku obim po CustomerID
JOIN Shipping I Orders  (by OrderID)

# COMMAND ----------

#napraviti u tabeli Shipping 
partitionBy "ShipmentDate"
parse(ShipmentDate) kao bi ekstrakovali year ili month , na osnovu koje mozemo da radimo partitionBy

# COMMAND ----------

#napraviti u tabeli OrderDetails 
 raditi partitionBy("OrderDate") = fiscal_year, kako bismo dobili po folderima distribuirane podatke po godinama 
 ->  svedeno po mesecima, videti krivu potraznje u toku godine

# COMMAND ----------

# za poredjenje po godinama probati napraviti Window rank() funkciju gde cemo videti rang listu po proizvodu/kategoriji po godini  na osnovu nekog kriterijma (kolicine, ili obrta)
# idemo sa partionBy("order_date") da podelimo ceo dataset po godinama za koje pravimo analizu
   _____ 
# zatim idemo sa filterom da izdvojimo jedan period , recimo 2011. i 2012. godinu 
demo_df = result_df.filter("fiscal_year in (2011, 2012)")
#onda idemo sa grupisanjem i agregacijom , uzimamo po kategoriji da sumiramo kolicine
  windowed_df = demo_df.groupBy( "fiscal_year", "caregory_id")\
  .agg(sum("quantity").alias("total_quantity"), ())")

  from pyspark.sql.window import Window
  from pyspark.sql.functions import desc, rank

  example_spec = Window.partitionBy("fiscal_year").orderBy(col("total_quantity").desc()
  demo_df.withColumn("rank_column", rank().over(example_spec)).show()

# COMMAND ----------

demo_df = 

# COMMAND ----------



# COMMAND ----------

