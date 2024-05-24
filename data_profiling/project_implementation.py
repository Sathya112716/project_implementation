# Databricks notebook source
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/vtex_test_data-1.json")
schema = spark.read.json(df.rdd.map(lambda row: row["orderDetails"])).schema

# COMMAND ----------

df1 = df.withColumn("orderDetails", from_json("orderDetails", schema)).select(col('*'), col('orderDetails.*')).drop("orderDetails")
df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2=df1.withColumn("totals",explode("totals"))
df2.select("totals.*").display()
df3=df1.withColumn("items",explode_outer("items"))
df3.select("items.*").display()
df4=df3.withColumn("offerings",explode("items.offerings"))
df4.select("offerings.*").display()
df5=df3.withColumn("attachmentOfferings",explode("items.attachmentOfferings"))
df5.select("attachmentOfferings.*").display()

# COMMAND ----------


