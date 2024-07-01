# Databricks notebook source file
# MAGIC %run ../bronze_to_silver/common_functions

# COMMAND ----------

from pyspark.sql.functions import count, when, datediff, avg, sum, countDistinct

# COMMAND ----------

# MAGIC %md
# MAGIC # orders Table Transformation

# COMMAND ----------

order_details_df = spark.read.format('delta').table('vtex_project.vtex_db.order_details')
display(order_details_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC PK of orders (silver table) - order_id is the primary key

# COMMAND ----------

# MAGIC %md
# MAGIC Time taken to fulfill the order (in days). Calculated as the difference between authorizedDate and creationDate. - full_fill_order

# COMMAND ----------

fulfill_order = order_details_df.withColumn('full_fill_order', datediff('creation_date', 'authorized_date'))
display(fulfill_order.select('full_fill_order'))

# COMMAND ----------

# MAGIC %md
# MAGIC Status of the order (e.g., completed, canceled).

# COMMAND ----------

display(order_details_df.select('status'))

# COMMAND ----------

# MAGIC %md
# MAGIC Count of orders per status.

# COMMAND ----------

count_of_orders_df = order_details_df.groupBy('status').count().alias('count_order_per_status')
display(count_of_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Sales channel through which the order was placed.

# COMMAND ----------

# sales_channel
sales_channel_order = order_details_df.select('sales_channel')
display(sales_channel_order)

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of orders per sales channel.

# COMMAND ----------

total_orders_per_channel = order_details_df.groupBy('sales_channel').count()
display(total_orders_per_channel)


# COMMAND ----------

# MAGIC %md
# MAGIC Total sales value per sales channel.

# COMMAND ----------

total_sales_value_per_channel = order_details_df.groupBy('sales_channel').sum('value').alias('total_sales_value')
display(total_sales_value_per_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC # payment_data_table

# COMMAND ----------

payments_data_df = spark.read.format('delta').table('vtex_project.vtex_db.payment_data')
display(payments_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Name of the payment method used.

# COMMAND ----------

payment_method_used = payments_data_df.select('payment_system_name').distinct()
display(payment_method_used)

# COMMAND ----------

# MAGIC %md
# MAGIC Count of orders per payment method.

# COMMAND ----------

payment_method_usage_count_df = payments_data_df.groupBy('payment_system_name').count()
display(payment_method_usage_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Total value of transactions per payment method.

# COMMAND ----------

payment_method_total_value = payments_data_df.groupBy('payment_system_name').sum('payments_value')
display(payment_method_total_value)

# COMMAND ----------

# MAGIC %md
# MAGIC # shipping_data

# COMMAND ----------

shipping_data_df = spark.read.format('delta').table('vtex_project.vtex_db.shipping_data')
display(shipping_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Average shipping cost.

# COMMAND ----------

avg_shipping_cost_df = shipping_data_df.agg(avg('price').alias('avg_shipping_cost'))
display(avg_shipping_cost_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Total shipping cost.

# COMMAND ----------

total_shipping_cost_df = shipping_data_df.agg(sum('price').alias('total_shipping_cost'))
display(total_shipping_cost_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Item

# COMMAND ----------

item_df = spark.read.format('delta').table('vtex_project.vtex_db.items')
display(item_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Unique identifier for each product. - unique_id

# COMMAND ----------

# MAGIC %md
# MAGIC Total quantity sold for each product

# COMMAND ----------

total_quantity_sold_df = item_df.groupBy('unique_id').agg(sum('value').alias('total_quantity_sold'))
display(total_quantity_sold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Total sales value for each product.

# COMMAND ----------

total_sales_value_df = item_df.groupBy("unique_id").agg(sum("selling_price").alias('total_sales_value'))
display(total_sales_value_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # cancellation_data_table

# COMMAND ----------

cancellation_data_df = spark.read.format('delta').table('vtex_project.vtex_db.cancellation_data')
display(cancellation_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Reason for order cancellation.

# COMMAND ----------

cancel_reason_df = cancellation_data_df.select('reason')
display(cancel_reason_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Count of orders canceled for each reason.

# COMMAND ----------

cancel_reason_count_df = cancellation_data_df.groupBy('reason').count()
display(cancel_reason_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Indicator if the order was returned (1 for yes, 0 for no).

# COMMAND ----------

is_return_df = cancellation_data_df.withColumn("is_return", when(cancellation_data_df["reason"].isNotNull(), 1).otherwise(0))
is_return_df = is_return_df.select('is_return')
display(is_return_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # client_profile

# COMMAND ----------

# MAGIC %md
# MAGIC Email of the customer.

# COMMAND ----------

client_profile_df= spark.read.format('delta').table('vtex_project.vtex_db.client_profile')
client_unique_id_df= client_profile_df.select("email")
display(client_unique_id_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## cancellation_data_table and Item (to identify returns)
# MAGIC
# MAGIC Count of returns per product.

# COMMAND ----------

joined_cancellation_item_df = cancellation_data_df.join(item_df, cancellation_data_df.order_id == item_df.order_id)
return_count_df = joined_cancellation_item_df.groupBy('vtex_project.vtex_db.items.order_id').agg(count('cancellation_data.order_id').alias('returns_count'))
display(return_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #orders and clientProfileData_table

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of orders placed by each customer.

# COMMAND ----------

joined_order_client_profile_df = order_df.join(client_profile_df, order_df.order_id == client_profile_df.order_id)
total_orders_per_customer_df = joined_order_client_profile_df.groupBy('vtex_project.vtex_db.client_profile.user_profile_id').agg(count('vtex_project.vtex_db.order_details.order_id').alias('total_orders_per_customer'))
display(total_orders_per_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # orders and clientProfileData_table (based on total_orders_per_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC Indicator if the customer is a repeat customer (1 for yes, 0 for no).

# COMMAND ----------

total_orders_per_customer_df = order_details_df.groupBy('order_id') \
    .agg(countDistinct('order_id').alias('total_orders'))

client_orders_df = client_profile_df.join(total_orders_per_customer_df, on='order_id', how='left')

client_orders_df = client_orders_df.withColumn(
    'is_repeat_customer',
    when(col('total_orders') > 1, 1).otherwise(0)
)
display(client_orders_df.select('email', 'total_orders', 'is_repeat_customer'))

# COMMAND ----------

display(client_profile_df.groupBy("email", "user_profile_id").count())


# COMMAND ----------

# MAGIC %md
# MAGIC total number of orders placed by each customer from client_profile_data

# COMMAND ----------

order_df= spark.read.format('delta').table('vtex_project.vtex_db.order_details')
display(order_df)

# COMMAND ----------


joined_df = order_df.join(client_profile_df, order_df.order_id == client_profile_df.order_id, "inner")
orders_count_df = joined_df.groupBy("user_profile_id").count().withColumnRenamed("count", "total_orders_per_customer")
display(orders_count_df)
