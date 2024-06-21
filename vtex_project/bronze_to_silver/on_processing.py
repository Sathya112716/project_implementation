# Databricks notebook source file
# MAGIC %run ./vtex_schema

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

from pyspark.sql.functions import from_json, explode_outer, col, current_date

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets

# COMMAND ----------

dbutils.widgets.text(name='data_lake_base_url',defaultValue='')
dbutils.widgets.text(name='audit_date', defaultValue='')
dbutils.widgets.text(name='database_name',defaultValue='')
dbutils.widgets.text(name='target_table', defaultValue='')

# COMMAND ----------

audit_date = dbutils.widgets.get('audit_date')
data_lake_base_url = dbutils.widgets.get('data_lake_base_url')
database_name = dbutils.widgets.get('database_name')
target_table = dbutils.widgets.get('target_table')

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/data/vtex_test_data (1).json")
flatten_df = df.withColumn("orderDetails", from_json("orderDetails", schema))
flatten_df.printSchema()

# COMMAND ----------

flatten_df = flatten_df.select("orderDetails.*")
display(flatten_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Started

# COMMAND ----------



# COMMAND ----------

order_details_df = selected_columns_df = flatten_df.select("orderId", "sequence", "marketplaceOrderId", "marketplaceServicesEndpoint", "sellerOrderId", "origin", "affiliateId", "salesChannel", "merchantName", "status","workflowIsInError", "statusDescription", "value", "creationDate", "lastChange", "orderGroup", "giftRegistryData", "callCenterOperatorData", "followUpEmail", "lastMessage", "hostname", "changesAttachment", "roundingError", "orderFormId", "isCompleted", "customData", "allowCancellation", "allowEdition", "isCheckedIn", "authorizedDate", "invoicedDate", "cancelReason", "taxData", "checkedInPickupPointId").withColumn("load_date", current_date())
order_details_df = rename_columns_to_snake_case(order_details_df)
display(order_details_df)

# COMMAND ----------

clientPreferencesData_df = flatten_df.select('orderId','sequence','clientPreferencesData.*').withColumn("load_date", current_date())
clientPreferencesData_df = rename_columns_to_snake_case(clientPreferencesData_df)
display(clientPreferencesData_df)

# COMMAND ----------

cancellationData_df = flatten_df.select('orderId','sequence','cancellationData.*').withColumn("load_date", current_date())
cancellationData_df = rename_columns_to_snake_case(cancellationData_df)
display(cancellationData_df)

# COMMAND ----------

itemMetadata_df = flatten_df.select('orderId', 'sequence','itemMetadata')
itemMetadata_df = itemMetadata_df.select('*','itemMetadata.*').withColumnRenamed('Id', 'item_metadata_id').select('*', explode_outer('Items').alias('item')).select('*','item.*')\
    .withColumnRenamed('Id', 'item_id').withColumnRenamed('Name', 'item_metadata_name').withColumnRenamed('SkuName', 'item_sku_name')\
    .select('*',explode_outer('AssemblyOptions')).select('*','col.*').drop('itemMetadata','items').withColumnRenamed('DetailUrl', 'item_metadata_detail_url').withColumnRenamed('Id', 'item_ids').withColumnRenamed('RefId', 'item_ref_id').withColumnRenamed('ProductId', 'item_product_id').withColumnRenamed('Seller', 'item_seller')
itemMetadata_df = itemMetadata_df.withColumnRenamed('Name', 'item_metadata_names')\
        .withColumnRenamed('ean', 'item_metadata_ean')\
            .withColumnRenamed('ImageUrl', 'item_metadata_image_url')\
                    .drop('item')
itemMetadata_df = itemMetadata_df.select('*','inputValues.*', 'Composition.*').drop('inputValues', 'Composition')
itemMetadata_df = itemMetadata_df.select('*', 'takeback.*', explode_outer('Items').alias('itemm')).drop('takeback', 'Items').withColumnRenamed('Domain', 'takeback_domain').withColumnRenamed('MaximumNumberOfCharacters', 'input_values_maximum_number_of_characters')
itemMetadata_df = itemMetadata_df.select('*', 'itemm.*', 'garantia-estendida.*').drop('itemm', 'col', 'AssemblyOptions').withColumn("load_date", current_date()).drop('garantia-estendida')
itemMetadata_df = rename_columns_to_snake_case(itemMetadata_df)
display(itemMetadata_df)

# COMMAND ----------

marketplace_df = flatten_df.select('orderId', 'sequence','marketPlace.*').withColumn("load_date", current_date())
marketplace_df = rename_columns_to_snake_case(marketplace_df)
display(marketplace_df)

# COMMAND ----------

storepreferenceData_df = flatten_df.select('orderId', 'sequence','storePreferencesData.*').select('*', 'currencyFormatInfo.*').drop('currencyFormatInfo').withColumn("load_date", current_date())
storepreferenceData_df = rename_columns_to_snake_case(storepreferenceData_df)
display(storepreferenceData_df)

# COMMAND ----------

commercialConditiondata_df = flatten_df.select('orderId', 'sequence', 'commercialConditionData.*').withColumn("load_date", current_date())
commercialConditiondata_df = rename_columns_to_snake_case(commercialConditiondata_df)
display(commercialConditiondata_df)

# COMMAND ----------

invoiceData_df = flatten_df.select('orderId', 'sequence','invoiceData.*')
invoiceData_df = invoiceData_df.select('*', 'address.*').drop('address')
invoiceData_df = invoiceData_df.select('*', explode_outer('geoCoordinates')).drop('geoCoordinates')
invoiceData_df = invoiceData_df.withColumnRenamed('col', 'geoCoordinates').withColumn("load_date", current_date())
invoiceData_df = rename_columns_to_snake_case(invoiceData_df)
display(invoiceData_df)

# COMMAND ----------

openTextField_df = flatten_df.select('orderId', 'sequence', 'opentextfield.*').withColumn("load_date", current_date())
openTextField_df = rename_columns_to_snake_case(openTextField_df)
display(openTextField_df)

# COMMAND ----------

sellers_df = flatten_df.select('orderId', 'sequence',explode_outer("sellers").alias('sellers'))
sellers_df = sellers_df.select('*','sellers.*').drop('sellers').withColumn("load_date", current_date())
sellers_df = rename_columns_to_snake_case(sellers_df)
display(sellers_df)

# COMMAND ----------

payment_data_df = flatten_df.select('orderId', 'sequence', 'paymentData.*')
payment_data_df = payment_data_df.withColumn("transactions", explode_outer("transactions"))
payment_data_df = payment_data_df.select("*", "transactions.*").drop("transactions")
payment_data_df = payment_data_df.select('*', explode_outer("payments")).drop("payments")
payment_data_df = payment_data_df.select("*", "col.*").drop('col').withColumnRenamed('id', 'payments_id').withColumnRenamed('redemptionCode', 'payments_redemption_code')\
    .withColumnRenamed('value', 'payments_value')
payment_data_df = payment_data_df.select('*', explode_outer('giftCards')).drop('giftCards').select('*', 'col.*').drop('col')
payment_data_df = payment_data_df.select('*', 'billingAddress.*').drop('billingAddress').select('*', explode_outer('geoCoordinates').alias('geoCoordinate')).drop('geoCoordinates').withColumn("load_date", current_date())
payment_data_df = rename_columns_to_snake_case(payment_data_df)
display(payment_data_df)

# COMMAND ----------

totals_df = flatten_df.select('orderId', 'sequence', explode_outer('totals')).select('*','col.*').drop('totals','col').withColumn("load_date", current_date())
totals_df = rename_columns_to_snake_case(totals_df)
display(totals_df)

# COMMAND ----------

client_profile_df = flatten_df.select('orderId', 'sequence',"clientProfileData.*").withColumn("load_date", current_date())
client_profile_df = rename_columns_to_snake_case(client_profile_df)
display(client_profile_df)

# COMMAND ----------

marketing_data_df = flatten_df.select('orderId', 'sequence','marketingData.*').select('*', explode_outer('marketingTags').alias('marketingTag')).drop('marketingTags').withColumn("load_date", current_date())
marketing_data_df = rename_columns_to_snake_case(marketing_data_df)
display(marketing_data_df)

# COMMAND ----------

rates_and_benefits_data_df = flatten_df.select('orderId', 'sequence', 'ratesAndBenefitsData.*').withColumnRenamed('id', 'rates_and_benefits_data_id').select('*', explode_outer('rateAndBenefitsIdentifiers').alias('rateAndBenefitsIdentifier')).drop('rateAndBenefitsIdentifiers').select('*', 'rateAndBenefitsIdentifier.*').drop('rateAndBenefitsIdentifier').withColumn("load_date", current_date())
rates_and_benefits_data_df = rename_columns_to_snake_case(rates_and_benefits_data_df)
display(rates_and_benefits_data_df)

# COMMAND ----------

shipping_data_df = flatten_df.select('orderId', 'sequence', 'shippingData.*').select('*', 'address.*').withColumnRenamed('entityId', 'address_entry_id').withColumnRenamed('addressId', 'address_address_id').withColumnRenamed('id', 'shipping_data_id').withColumnRenamed('versionId', 'address_version_id')\
    .select('*', explode_outer('logisticsInfo')).select('*', 'col.*').withColumnRenamed('addressId', 'logestics_info_address_id').withColumnRenamed('deliveryChannel', 'logestics_delivery_channel').withColumnRenamed('entityId', 'logestics_entry_id').withColumnRenamed('price', 'logestics_info_price').withColumnRenamed('shippingEstimate', 'logestics_shipping_estimate').withColumnRenamed('lockTTL', 'logestics_lock_ttl').withColumnRenamed('pickupPointId','logestics_pickup_point_id').withColumnRenamed('polygonName', 'logestics_polygon_name').withColumnRenamed('transitTime','logestics_transit_time')\
        .drop('selectedAddresses', 'address', 'logisticsInfo', 'col')

shipping_data_df = shipping_data_df.select('*','pickupStoreInfo.*', 'deliveryWindow.*').withColumnRenamed('price', 'delivery_window_price')\
    .withColumn('deliveryChannels', explode_outer('deliveryChannels'))\
        .withColumn('deliveryIds', explode_outer('deliveryIds'))\
            .withColumn('shipsTo', explode_outer('shipsTo'))\
                .withColumn('slas', explode_outer('slas'))\
                    .withColumn('geoCoordinates', explode_outer('geoCoordinates'))\
                        .select('*', 'slas.*').withColumnRenamed('id', 'slas_id').select('*','deliveryChannels.*').withColumnRenamed('dockid', 'slas_dockid')\
                            .withColumnRenamed('pickupPointId','slas_pickup_point_id')\
                                .withColumnRenamed('lockTtl', 'slas_lock_ttl').withColumnRenamed('polygonName','slas_polygon_name')\
                                .select('*', 'deliveryIds.*').withColumnRenamed('warehouseId', 'delivery_warehouse_id')\
                            .withColumn('kitItemDetails', explode_outer('kitItemDetails')).select('*', 'kitItemDetails.*')\
                                .drop('shipTo', 'pickupStoreInfo', 'deliveryWindow', 'address', 'deliveryChannels', 'deliveryIds' ,'slas', 'kitItemDetails').withColumn("load_date", current_date())
shipping_data_df = rename_columns_to_snake_case(shipping_data_df)
display(shipping_data_df)

# COMMAND ----------

items_df = flatten_df.select('orderId', 'sequence',explode_outer("Items").alias('Items'))
items_df = items_df.select('*',"Items.*").select('*', explode_outer('attachments')).select('*', 'col.*').drop('attachments', 'col')
items_df = items_df.withColumn("garantia_estendida_parsed", from_json(col("content.`garantia-estendida`"), garantia_estendida_schema)) \
       .withColumn("takeback_parsed", from_json(col("content.takeback"), takeback_schema)) \
       .drop("content")
items_df = items_df.select('*', explode_outer('priceTags').alias('priceTag')).drop('priceTags')
items_df = items_df.withColumn("priceTag", from_json(col("priceTag"), discount_schema))
items_df = items_df.select('*', 'priceTag.*').drop('priceTag', 'Items')
items_df = items_df.select('*', 'itemAttachment.*', 'additionalInfo.*', 'priceDefinition.*', 'garantia_estendida_parsed.*', 'takeback_parsed.*').drop('itemAttachment', 'additionalInfo', 'priceDefinition', 'garantia_estendida_parsed', 'takeback_parsed')
items_df = items_df.withColumn('offerings', explode_outer('offerings'))\
        .withColumn('attachmentOfferings', explode_outer('attachmentOfferings'))\
            .withColumn('categories', explode_outer('categories'))\
                .withColumn('bundleItems', explode_outer('bundleItems'))\
                    .withColumn('sellingPrices', explode_outer('sellingPrices'))
items_df = items_df.select('*','offerings.*', 'attachmentOfferings.*', 'categories.*', 'dimension.*', 'sellingPrices.*').withColumnRenamed('additionalInfo', 'components_additional_info').drop('offerings', 'attachmentOfferings', 'categories', 'sellingPrices')
items_df = items_df.select('*', 'dimension.*').select('*', explode_outer('schema').alias('schema_key', 'schema_value')).select('*', 'schema_value.*').drop('dimension', 'components_additional_info', 'schema', 'schema_value').withColumn("load_date", current_date())\
    .drop('cubicweight', 'height', 'id', 'length', 'name', 'price', 'productId', 'quantity', 'refId', 'weight', 'width', 'components', 'bundleItems')
items_df = rename_columns_to_snake_case(items_df)
display(items_df)

# COMMAND ----------

component_df = flatten_df.select('orderId', 'sequence',explode_outer("Items").alias('Items')).select('*', 'Items.*').drop('Items')
component_df = component_df.withColumn('components', explode_outer('components')).select('components.*').drop('components').withColumn("load_date", current_date())
component_df = rename_columns_to_snake_case(component_df)
display(component_df)

# COMMAND ----------

bundle_items_df = flatten_df.select('orderId', 'sequence',explode_outer("Items").alias('Items')).select('*', 'Items.*').drop('Items', 'itemAttachment')
bundle_items_df = bundle_items_df.select(explode_outer('bundleItems')).select('*','col.*').withColumnRenamed('name', 'items_name')\
    .select('*','itemAttachment.*').withColumnRenamed('name', 'item_attachment_name')\
        .withColumn('attachmentOfferings', explode_outer('attachmentOfferings'))\
            .select('*', 'attachmentOfferings.*')\
                .select('*', 'additionalInfo.*', 'schema.*')\
                    .select('*', 'takeback.*')\
                        .drop('itemAttachment', 'attachmentOfferings', 'additionalInfo', 'schema', 'takeback', 'col').withColumn("load_date", current_date())
bundle_items_df = bundle_items_df.dropna(how='all')
bundle_items_df = rename_columns_to_snake_case(bundle_items_df)
display(bundle_items_df)



# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Table
# MAGIC

# COMMAND ----------

spark.sql('CREATE DATABASE vtex_db')


# COMMAND ----------

spark.sql('USE vtex_db')

# COMMAND ----------

write_to_table(order_details_df, 'overwrite', 'order_details')

# COMMAND ----------

write_to_table(clientPreferencesData_df, 'overwrite', 'client_preferences_data')

# COMMAND ----------

write_to_table(cancellationData_df, 'overwrite', 'cancellation_data')

# COMMAND ----------

write_to_table(itemMetadata_df, 'overwrite', 'item_meta_data')

# COMMAND ----------

write_to_table(marketplace_df, 'overwrite', 'marketplace')

# COMMAND ----------

write_to_table(storepreferenceData_df, 'overwrite', 'store_preference_data')

# COMMAND ----------

write_to_table(commercialConditiondata_df, 'overwrite', 'commercial_condition_data')

# COMMAND ----------

write_to_table(invoiceData_df, 'overwrite', 'invoice_data')

# COMMAND ----------

write_to_table(openTextField_df, 'overwrite', 'open_text_field')

# COMMAND ----------

write_to_table(sellers_df, 'overwrite', 'sellers')

# COMMAND ----------

write_to_table(payment_data_df, 'overwrite', 'payment_data')

# COMMAND ----------

write_to_table(totals_df, 'overwrite', 'totals')

# COMMAND ----------

write_to_table(client_profile_df, 'overwrite', 'client_profile')

# COMMAND ----------

write_to_table(marketing_data_df, 'overwrite', 'marketing_data')

# COMMAND ----------

write_to_table(rates_and_benefits_data_df, 'overwrite', 'rates_and_benefits_data')

# COMMAND ----------

write_to_table(shipping_data_df, 'overwrite', 'shipping_data')

# COMMAND ----------

write_to_table(items_df, 'overwrite', 'items')

# COMMAND ----------

write_to_table(component_df, 'overwrite', 'component')

# COMMAND ----------

write_to_table(bundle_items_df, 'overwrite', 'bundle_items')

# COMMAND ----------

path = 'dbfs:/mnt/vtexproject/test'
df = component_df
database_name = 'azure_notebook.vtex_db'
target_table_name = 'component'
merge_col = 'id'
save_to_delta_with_merge(df, path, database_name, target_table_name, merge_col)

# COMMAND ----------

