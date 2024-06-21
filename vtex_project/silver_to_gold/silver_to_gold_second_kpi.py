# Databricks notebook source
df_item1= spark.read.format("delta").table('azure_notebook.vtex_db.items')

# COMMAND ----------

product_id= df_item1.select("unique_id")
product_id.show()


# COMMAND ----------

df_clientProfileData1= spark.read.format("delta").table('azure_notebook.vtex_db.client_profile')

# COMMAND ----------

customer_id= df_clientProfileData1.select("email").count()
display(customer_id)

# COMMAND ----------

df_orders1= spark.read.format("delta").table('azure_notebook.vtex_db.order_details')

# COMMAND ----------

total_orders=df_orders1.count()
display(total_orders)


# COMMAND ----------

completed_orders1 = df_orders1.filter(df_orders1.is_completed == True)

# COMMAND ----------

completed_orders = completed_orders1.count()
print(completed_orders)

# COMMAND ----------

canceled_orders = df_orders1.filter(df_orders1.status == "canceled").count()
display(canceled_orders)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum,avg as spark_avg


# COMMAND ----------

total_ordered_value1 = df_orders1.withColumn("value", col("value").cast("int"))

# COMMAND ----------

total_ordered_value = total_ordered_value1.agg(spark_sum("value"))
display(total_ordered_value)

# COMMAND ----------

average_order_value = df_orders1.agg(spark_avg("value")).collect()[0][0]
display(average_order_value)

# COMMAND ----------

total_orders = df_orders1.count()
completed_orders = df_orders1.filter(df_orders1.status == "completed").count()
fulfillment_rate = (completed_orders / total_orders) * 100
display(fulfillment_rate)

# COMMAND ----------

canceled_orders = df_orders1.filter(df_orders1.status == "canceled").count()
cancellation_rate= (canceled_orders / total_orders) * 100
display(cancellation_rate)

# COMMAND ----------

df_client1= spark.read.format("delta").table('azure_notebook.vtex_db.client_profile')

# COMMAND ----------

order_clientdata = df_orders1.join(df_client1,df_orders1["order_id"] == df_orders1["order_id"], "inner").drop(df_client1["order_id"])
order_clientdata.display()


# COMMAND ----------

customer_lifetime_value = order_clientdata.groupBy('first_name', 'last_name').agg(spark_avg('value').alias('customer_lifetime_value')).drop('first_name','last_name')
display(customer_lifetime_value)

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

total_orders_per_customer = order_clientdata.groupBy('first_name','last_name').agg(count('order_id').alias('total_orders_per_customer')).drop('first_name','last_name')
display(total_orders_per_customer)

# COMMAND ----------

order_clientdata_item = order_clientdata.join(df_item1,order_clientdata['order_id'] == df_item1['order_id'],'inner').drop(df_item1['order_id'])
display(order_clientdata_item)

# COMMAND ----------

average_items_per_order = order_clientdata_item.groupBy('order_id','first_name','last_name').agg(spark_avg('d').alias('average_items_per_order'))
display(average_items_per_order)

# COMMAND ----------

total_quantity_sold = df_item1.groupBy('order_id').agg(spark_sum('value').alias('total_quantity_sold')).display()

# COMMAND ----------

total_sales_value = df_item1.groupBy('unique_id').agg(spark_sum('value').alias('total_sales_value')).display()

# COMMAND ----------

average_price = df_item1.groupBy('unique_id').agg(spark_avg('value').alias('average_price')).display()

# COMMAND ----------

