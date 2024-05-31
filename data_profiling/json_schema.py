# Databricks notebook source
read_df =spark.read.format('json').load("dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/vtex_test_data-1.json")
read_df.display()

# COMMAND ----------

read_df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col ,explode,explode_outer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType, MapType, TimestampType

# COMMAND ----------

spark = SparkSession.builder.appName("OrderDetailsSchema").getOrCreate()

# Define the schema for the order details
order_details_schema = StructType([
    StructField("orderId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("marketplaceOrderId", StringType(), True),
    StructField("marketplaceServicesEndpoint", StringType(), True),
    StructField("sellerOrderId", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("affiliateId", StringType(), True),
    StructField("salesChannel", StringType(), True),
    StructField("merchantName", StringType(), True),
    StructField("status", StringType(), True),
    StructField("workflowIsInError", BooleanType(), True),
    StructField("statusDescription", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("creationDate", TimestampType(), True),
    StructField("lastChange", TimestampType(), True),
    StructField("orderGroup", StringType(), True),
    StructField("totals", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])), True),
    StructField("items", ArrayType(StructType([
        StructField("uniqueId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("productId", StringType(), True),
        StructField("ean", StringType(), True),
        StructField("lockId", StringType(), True),
        StructField("itemAttachment", StructType([
            StructField("content", MapType(StringType(), StringType()), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("attachments", ArrayType(StringType()), True),
        StructField("quantity", IntegerType(), True),
        StructField("seller", StringType(), True),
        StructField("name", StringType(), True),
        StructField("refId", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("listPrice", IntegerType(), True),
        StructField("manualPrice", IntegerType(), True),
        StructField("priceTags", ArrayType(StringType()), True),
        StructField("imageUrl", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("components", ArrayType(StringType()), True),
        StructField("bundleItems", ArrayType(StringType()), True),
        StructField("params", ArrayType(StringType()), True),
        StructField("offerings", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", IntegerType(), True)
        ])), True),
        StructField("attachmentOfferings", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("required", BooleanType(), True),
            StructField("schema", MapType(StringType(), StructType([
                StructField("MaximumNumberOfCharacters", IntegerType(), True),
                StructField("Domain", ArrayType(StringType()), True)
            ])), True)
        ])), True),
        StructField("sellerSku", StringType(), True),
        StructField("priceValidUntil", StringType(), True),
        StructField("commission", IntegerType(), True),
        StructField("tax", IntegerType(), True),
        StructField("preSaleDate", StringType(), True),
        StructField("additionalInfo", StructType([
            StructField("brandName", StringType(), True),
            StructField("brandId", StringType(), True),
            StructField("categoriesIds", StringType(), True),
            StructField("categories", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("productClusterId", StringType(), True),
            StructField("commercialConditionId", StringType(), True),
            StructField("dimension", StructType([
                StructField("cubicweight", FloatType(), True),
                StructField("height", FloatType(), True),
                StructField("length", FloatType(), True),
                StructField("weight", FloatType(), True),
                StructField("width", FloatType(), True)
            ]), True),
            StructField("offeringInfo", StringType(), True),
            StructField("offeringType", StringType(), True),
            StructField("offeringTypeId", StringType(), True)
        ]), True),
        StructField("measurementUnit", StringType(), True),
        StructField("unitMultiplier", FloatType(), True),
        StructField("sellingPrice", IntegerType(), True),
        StructField("isGift", BooleanType(), True),
        StructField("shippingPrice", IntegerType(), True),
        StructField("rewardValue", IntegerType(), True),
        StructField("freightCommission", IntegerType(), True),
        StructField("priceDefinition", StructType([
            StructField("sellingPrices", ArrayType(StructType([
                StructField("value", IntegerType(), True),
                StructField("quantity", IntegerType(), True)
            ])), True),
            StructField("calculatedSellingPrice", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("reason", StringType(), True)
        ]), True),
        StructField("taxCode", StringType(), True),
        StructField("parentItemIndex", IntegerType(), True),
        StructField("parentAssemblyBinding", StringType(), True),
        StructField("callCenterOperator", StringType(), True),
        StructField("serialNumbers", StringType(), True),
        StructField("assemblies", ArrayType(StringType()), True),
        StructField("costPrice", IntegerType(), True)
    ])), True),
    StructField("marketplaceItems", ArrayType(StringType()), True),
    StructField("clientProfileData", StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("documentType", StringType(), True),
        StructField("document", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("corporateName", StringType(), True),
        StructField("tradeName", StringType(), True),
        StructField("corporateDocument", StringType(), True),
        StructField("stateInscription", StringType(), True),
        StructField("corporatePhone", StringType(), True),
        StructField("isCorporate", BooleanType(), True),
        StructField("userProfileId", StringType(), True),
        StructField("userProfileVersion", StringType(), True),
        StructField("customerClass", StringType(), True)
    ]), True),
    StructField("giftRegistryData", StringType(), True),
    StructField("marketingData", StructType([
        StructField("id", StringType(), True),
        StructField("utmSource", StringType(), True),
        StructField("utmPartner", StringType(), True),
        StructField("utmMedium", StringType(), True),
        StructField("utmCampaign", StringType(), True),
        StructField("coupon", StringType(), True),
        StructField("utmiCampaign", StringType(), True),
        StructField("utmipage", StringType(), True),
        StructField("utmiPart", StringType(), True),
        StructField("marketingTags", ArrayType(StringType()), True)
    ]), True),
    StructField("ratesAndBenefitsData", StructType([
        StructField("id", StringType(), True),
        StructField("rateAndBenefitsIdentifiers", ArrayType(StringType()), True)
    ]), True),
    StructField("shippingData", StructType([
        StructField("id", StringType(), True),
        StructField("address",StructType([
            StructField("addressType", StringType(), True),
            StructField("receiverName", StringType(), True),
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("street", StringType(), True),
            StructField("number", StringType(), True),
            StructField("neighborhood", StringType(), True),
            StructField("complement", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("geoCoordinates", ArrayType(FloatType()), True)
        ]), True),
        StructField("logisticsInfo", ArrayType(StructType([
            StructField("itemIndex", IntegerType(), True),
            StructField("selectedSla", StringType(), True),
            StructField("lockTTL", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("listPrice", IntegerType(), True),
            StructField("sellingPrice", IntegerType(), True),
            StructField("deliveryWindow", StringType(), True),
            StructField("deliveryCompany", StringType(), True),
            StructField("shippingEstimate", StringType(), True),
            StructField("shippingEstimateDate", TimestampType(), True),
            StructField("slas", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("shippingEstimate", StringType(), True),
                StructField("deliveryWindow", StringType(), True),
                StructField("price", IntegerType(), True),
                StructField("deliveryChannel", StringType(), True),
                StructField("pickupStoreInfo", StructType([
                    StructField("additionalInfo", StringType(), True),
                    StructField("address", StructType([
                        StructField("addressType", StringType(), True),
                        StructField("receiverName", StringType(), True),
                        StructField("addressId", StringType(), True),
                        StructField("versionId", StringType(), True),
                        StructField("entityId", StringType(), True),
                        StructField("postalCode", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("street", StringType(), True),
                        StructField("number", StringType(), True),
                        StructField("neighborhood", StringType(), True),
                        StructField("complement", StringType(), True),
                        StructField("reference", StringType(), True),
                        StructField("geoCoordinates", ArrayType(FloatType()), True)
                    ]), True),
                    StructField("dockId", StringType(), True),
                    StructField("friendlyName", StringType(), True),
                    StructField("isPickupStore", BooleanType(), True)
                ]), True),
                StructField("polygonName", StringType(), True),
                StructField("lockTTL", StringType(), True),
                StructField("pickupPointId", StringType(), True),
                StructField("transitTime", StringType(), True),
                StructField("pickupDistance", FloatType(), True)
            ])), True),
            StructField("shipsTo", ArrayType(StringType()), True),
            StructField("deliveryIds", ArrayType(StructType([
                StructField("courierId", StringType(), True),
                StructField("courierName", StringType(), True),
                StructField("dockId", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("warehouseId", StringType(), True),
                StructField("accountCarrierName", StringType(), True),
                StructField("kitItemDetails", ArrayType(StringType()), True)
            ])), True),
            StructField("deliveryChannels", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("stockBalance", IntegerType(), True)
            ])), True),
            StructField("deliveryChannel", StringType(), True),
            StructField("pickupStoreInfo", StructType([
                StructField("additionalInfo", StringType(), True),
                StructField("address", StringType(), True),
                StructField("dockId", StringType(), True),
                StructField("friendlyName", StringType(), True),
                StructField("isPickupStore", BooleanType(), True)
            ]), True),
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("polygonName", StringType(), True),
            StructField("pickupPointId", StringType(), True),
            StructField("transitTime", StringType(), True)
        ])), True),
        StructField("trackingHints", StringType(), True),
        StructField("selectedAddresses", ArrayType(StructType([
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("addressType", StringType(), True),
            StructField("receiverName", StringType(), True),
            StructField("street", StringType(), True),
            StructField("number", StringType(), True),
            StructField("complement", StringType(), True),
            StructField("neighborhood", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("geoCoordinates", ArrayType(FloatType()), True)
        ])), True)
    ]), True),
    StructField("paymentData", StructType([
        StructField("giftCards", ArrayType(StringType()), True),
        StructField("transactions", ArrayType(StructType([
            StructField("isActive", BooleanType(), True),
            StructField("transactionId", StringType(), True),
            StructField("merchantName", StringType(), True),
            StructField("payments", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("paymentSystem", StringType(), True),
                StructField("paymentSystemName", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("installments", IntegerType(), True),
                StructField("referenceValue", IntegerType(), True),
                StructField("cardHolder", StringType(), True),
                StructField("cardNumber", StringType(), True),
                StructField("firstDigits", StringType(), True),
                StructField("lastDigits", StringType(), True),
                StructField("cvv2", StringType(), True),
                StructField("expireMonth", StringType(), True),
                StructField("expireYear", StringType(), True),
                StructField("url", StringType(), True),
                StructField("giftCardId", StringType(), True),
                StructField("giftCardName", StringType(), True),
                StructField("giftCardCaption", StringType(), True),
                StructField("redemptionCode", StringType(), True),
                StructField("group", StringType(), True),
                StructField("tid", StringType(), True),
                StructField("dueDate", StringType(), True),
                StructField("connectorResponses", MapType(StringType(), StringType()), True),
                StructField("giftCardProvider", StringType(), True),
                StructField("giftCardAsDiscount", StringType(), True),
                StructField("koinUrl", StringType(), True),
                StructField("accountId", StringType(), True),
                StructField("parentAccountId", StringType(), True),
                StructField("bankIssuedInvoiceIdentificationNumber", StringType(), True),
                StructField("bankIssuedInvoiceIdentificationNumberFormatted", StringType(), True),
                StructField("bankIssuedInvoiceBarCodeNumber", StringType(), True),
                StructField("bankIssuedInvoiceBarCodeType", StringType(), True),
                StructField("billingAddress", StringType(), True),
                StructField("paymentOrigin", StringType(), True)
            ])), True)
        ])), True)
    ]), True),
    StructField("packageAttachment", StructType([
        StructField("packages", ArrayType(StringType()), True)
    ]), True),
    StructField("sellers", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("logo", StringType(), True),
        StructField("fulfillmentEndpoint", StringType(), True)
    ])), True),
    StructField("callCenterOperatorData", StringType(), True),
    StructField("followUpEmail", StringType(), True),
    StructField("lastMessage", StringType(), True),
    StructField("hostname", StringType(), True),
    StructField("invoiceData", StringType(), True),
    StructField("changesAttachment", StringType(), True),
    StructField("openTextField", StringType(), True),
    StructField("roundingError", IntegerType(), True),
    StructField("orderFormId", StringType(), True),
    StructField("commercialConditionData", StringType(), True),
    StructField("isCompleted", BooleanType(), True),
    StructField("customData", StringType(), True),
    StructField("storePreferencesData", StructType([
        StructField("countryCode", StringType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("currencyFormatInfo", StructType([
            StructField("CurrencyDecimalDigits", IntegerType(), True),
            StructField("CurrencyDecimalSeparator", StringType(), True),
            StructField("CurrencyGroupSeparator", StringType(), True),
            StructField("CurrencyGroupSize", IntegerType(), True),
            StructField("StartsWithCurrencySymbol", BooleanType(), True)
        ]), True),
        StructField("currencyLocale", IntegerType(), True),
        StructField("currencySymbol", StringType(), True),
        StructField("timeZone", StringType(), True)
    ]), True),
    StructField("allowCancellation", BooleanType(), True),
    StructField("allowEdition", BooleanType(), True),
    StructField("isCheckedIn", BooleanType(), True),
    StructField("marketplace", StructType([
        StructField("baseURL", StringType(), True),
        StructField("isCertified", BooleanType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("authorizedDate", TimestampType(), True),
    StructField("invoicedDate", TimestampType(), True),
    StructField("cancelReason", StringType(), True),
    StructField("itemMetadata", StructType([
        StructField("Items", ArrayType(StructType([
            StructField("Id", StringType(), True),
            StructField("Seller", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("SkuName", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("RefId", StringType(), True),
            StructField("Ean", StringType(), True),
            StructField("ImageUrl", StringType(), True),
            StructField("DetailUrl", StringType(), True),
            StructField("AssemblyOptions", ArrayType(StructType([
                StructField("Id", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Required", BooleanType(), True),
                StructField("InputValues", MapType(StringType(), StructType([
                    StructField("MaximumNumberOfCharacters", IntegerType(), True),
                    StructField("Domain", ArrayType(StringType()), True)
                ])), True),
                StructField("Composition", StringType(), True)
            ])), True)
        ])), True)
    ]), True),
    StructField("subscriptionData", StringType(), True),
    StructField("taxData", StringType(), True),
    StructField("checkedInPickupPointId", StringType(), True),
    StructField("cancellationData", StringType(), True),
    StructField("clientPreferencesData", StructType([
        StructField("locale", StringType(), True),
        StructField("optinNewsLetter", BooleanType(), True)
    ]), True)
])


# COMMAND ----------

df_parsed = read_df.withColumn("orderDetails", from_json(col("orderDetails"), order_details_schema)) 

# COMMAND ----------

df_parsed.display()

# COMMAND ----------

df_final = df_parsed.select("orderDetails.*")


# COMMAND ----------

df_parsed.printSchema()

# COMMAND ----------


totals_df_final=df_final.withColumn("totals",explode(df_final["totals"])).select("orderId","totals.*")
items_df=df_final.withColumn("items",explode(df_final["items"])).select("orderId","sequence","sellerorderId","clientProfileData.email","items.*")
marketplaceItems_df_final=df_final.withColumn("marketplaceItems",explode(df_final["marketplaceItems"])).select("orderId","marketplaceItems")
clientProfileData_df_final=df_final.select("orderId","clientProfileData.*")
marketingData_df =df_final.select("orderId","marketingData.*")
ratesAndBenefitsData_df = df_final.select("orderId","ratesAndBenefitsData.*")
shippingData_df = df_final.select("orderId","sequence","shippingData.*")
paymentData_df = df_final.select("orderId","sequence","paymentData.*")
packageAttachment_df = df_final.select("orderId","packageAttachment.*")
sellers_df_final = df_final.withColumn("sellers",explode(df_final["sellers"])).select("orderId","sellers.*")
storePreferencesData_df =df_final.select("orderId","storePreferencesData.*")
marketplace_df_final = df_final.select("orderId","marketplace.*")
itemMetadata_df =df_final.select("orderId","sequence","itemMetadata.*")
clientPreferencesData_df_final =df_final.select("orderId","clientPreferencesData.*")






# COMMAND ----------

# DBTITLE 1,Item Table
items_df1=items_df.withColumn("attachments",explode_outer("attachments"))\
                  .withColumn("priceTags", explode_outer("priceTags")) \
                  .withColumn("components", explode_outer("components")) \
                  .withColumn("bundleItems", explode_outer("bundleItems")) \
                  .withColumn("params", explode_outer("params")) \
                  .withColumn("offerings", explode_outer("offerings")) \
                  .withColumn("attachmentOfferings", explode_outer("attachmentOfferings")) \
                  .withColumn("assemblies", explode_outer("assemblies"))
items_df2=items_df1.select("*",
    "itemAttachment.*",
    "offerings.*",
    "attachmentOfferings.*",
    "additionalInfo.*",
    "priceDefinition.*","attachments").drop("itemAttachment","offerings","attachmentOfferings","additionalInfo","priceDefinition")

items_df3=items_df2.select("*", explode_outer("content").alias("content_key", "content_value")).select("*", explode_outer("schema").alias("schema_key", "schema_value")).drop("content","schema")

items_df4=items_df3.withColumn("categories",explode_outer("categories"))\
                   .withColumn("sellingPrices",explode_outer("sellingPrices"))

items_final=items_df4.select("*","categories.*","dimension.*","sellingPrices.*","schema_value").drop("categories","dimension","sellingPrices","schema_value")




# COMMAND ----------

# DBTITLE 1,Rates and Benefits Table
ratesAndBenefitsData_df_final=ratesAndBenefitsData_df.withColumn("rateAndBenefitsIdentifiers",explode_outer("rateAndBenefitsIdentifiers"))

# COMMAND ----------

# DBTITLE 1,Shipping Data Table
shippingData_df1=shippingData_df.withColumn('logisticsInfo', explode_outer('logisticsInfo')) \
                                .withColumn("selectedAddresses",explode_outer("selectedAddresses"))
                                
shippingData_df2=shippingData_df1.select("*","logisticsInfo.*","address.*","selectedAddresses.*").drop("logisticsInfo","selectedAddresses","address")
shippingData_df3=shippingData_df2.withColumn("slas",explode_outer("slas"))\
                                 .withColumn("shipsTo",explode_outer("shipsTo"))\
                                 .withColumn("deliveryIds",explode_outer("deliveryIds"))\
                                 .withColumn("deliveryChannels",explode_outer("deliveryChannels"))
shippingData_df4=shippingData_df3.select("*","slas.*","deliveryIds.*","deliveryChannels.*","pickupStoreInfo.*").drop("slas","deliveryIds","deliveryChannels","pickupStoreInfo")
shippingData_df_final=shippingData_df4.withColumn("kitItemDetails",explode_outer("kitItemDetails"))

# COMMAND ----------

# DBTITLE 1,Payment Data Table
paymentData_df1=paymentData_df.withColumn("giftCards",explode_outer("giftCards"))\
                              .withColumn("transactions",explode_outer("transactions"))
paymentData_df2=paymentData_df1.select("*","transactions.*").drop("transactions")
paymentData_df3=paymentData_df2.withColumn("payments",explode_outer("payments"))
paymentData_df4=paymentData_df3.select("*","payments.*").drop("payments")
paymentData_df_final=paymentData_df4.select("*",explode_outer("connectorResponses").alias("connector_key","connector_value")).drop("connectorResponses")

# COMMAND ----------

# DBTITLE 1,Package Attachment Table
packageAttachment_df_final=packageAttachment_df.withColumn("packages",explode_outer("packages"))

# COMMAND ----------

# DBTITLE 1,Item Metadata Table
itemMetadata_df1=itemMetadata_df.withColumn("items",explode_outer("items"))
itemMetadata_df2=itemMetadata_df1.select("*","items.*").drop("items")
itemMetadata_df3=itemMetadata_df2.withColumn("AssemblyOptions",explode_outer("AssemblyOptions"))
itemMetadata_df4=itemMetadata_df3.select("*","AssemblyOptions.*").drop("AssemblyOptions")
itemMetadata_df5=itemMetadata_df4.select("*",explode_outer("InputValues").alias("input_key","input_value")).drop("InputValues")
itemMetadata_df6=itemMetadata_df5.select("*","input_value.*").drop("input_value")
itemMetadata_df_final=itemMetadata_df6.withColumn("domain",explode_outer("domain"))

# COMMAND ----------

# DBTITLE 1,final tables
totals_df_final=df_final.withColumn("totals",explode(df_final["totals"])).select("orderId","totals.*")
totals_df_final.display()
items_final=items_df4.select("*","categories.*","dimension.*","sellingPrices.*","schema_value").drop("categories","dimension","sellingPrices","schema_value")
items_final.display()
marketplaceItems_df_final=df_final.withColumn("marketplaceItems",explode(df_final["marketplaceItems"])).select("orderId","marketplaceItems")
marketplaceItems_df_final.display()
clientProfileData_df_final=df_final.select("orderId","clientProfileData.*")
clientProfileData_df_final.display()
marketingData_df_final=marketingData_df.withColumn("marketingTags",explode_outer("marketingTags"))
marketingData_df_final.display()
ratesAndBenefitsData_df_final=ratesAndBenefitsData_df.withColumn("rateAndBenefitsIdentifiers",explode_outer("rateAndBenefitsIdentifiers"))
ratesAndBenefitsData_df_final.display()
shippingData_df_final=shippingData_df4.withColumn("kitItemDetails",explode_outer("kitItemDetails"))
shippingData_df_final.display()
paymentData_df_final=paymentData_df4.select("*",explode_outer("connectorResponses").alias("connector_key","connector_value")).drop("connectorResponses")
paymentData_df_final.display()
packageAttachment_df_final=packageAttachment_df.withColumn("packages",explode_outer("packages")) 
packageAttachment_df_final.display()
sellers_df_final = df_final.withColumn("sellers",explode(df_final["sellers"])).select("orderId","sellers.*")
sellers_df_final.display()
storePreferencesData_df_final=storePreferencesData_df.select("*","currencyFormatInfo.*").drop("currencyFormatInfo")
storePreferencesData_df_final.display()
marketplace_df_final = df_final.select("orderId","marketplace.*")
marketplace_df_final.display()
itemMetadata_df_final=itemMetadata_df6.withColumn("domain",explode_outer("domain"))
itemMetadata_df_final.display()
clientPreferencesData_df_final =df_final.select("orderId","clientPreferencesData.*")
clientPreferencesData_df_final.display()





# COMMAND ----------

# DBTITLE 1,Primary Key
df_count=df_final.groupBy("orderId").count ()
df_count.display()

# COMMAND ----------

total_primary=totals_df_final.groupBy("orderId","id").count().display()


# COMMAND ----------

items_final.display()

# COMMAND ----------

items_primary=items_final.groupBy("orderId","uniqueId","email").count().display()


# COMMAND ----------

marketplaceItems_primary=marketplaceItems_df_final.groupBy("orderId").count().display()

# COMMAND ----------

client_primary=clientProfileData_df_final.groupBy("orderId","email").count().display()

# COMMAND ----------

marketing_primary=marketingData_df_final.groupBy("orderId").count().display()

# COMMAND ----------

marketing_primary=marketingData_df_final.groupBy("orderId").count().display()

# COMMAND ----------

ratesAndBenefitsData_primary=ratesAndBenefitsData_df_final.groupBy("orderId","id").count().display()

# COMMAND ----------

shippingData_primary=shippingData_df_final.groupBy("orderId","sequence","isPickupStore").count().display()

# COMMAND ----------

paymentData_primary=paymentData_df_final.groupBy("orderId").count().display()

# COMMAND ----------

packageAttachment_primary=packageAttachment_df_final.groupBy("orderId").count().display()

# COMMAND ----------

sellers_primary=sellers_df_final.groupBy("orderId").count().display()

# COMMAND ----------

storePreferencesData_primary=storePreferencesData_df_final.groupBy("orderId").count().display()

# COMMAND ----------

marketplace_primary=marketplace_df_final.groupBy("orderId").count().display()

# COMMAND ----------

itemMetadata_primary=itemMetadata_df_final.groupBy("orderId").count().display()

# COMMAND ----------

clientPreferencesData_primary=clientPreferencesData_df_final.groupBy("orderId").count().display()

# COMMAND ----------

df_count=df_final.groupBy("sequence").count()
df_count.display()

# COMMAND ----------

df_count=.groupBy("sequence").count()
df_count.display()

# COMMAND ----------


